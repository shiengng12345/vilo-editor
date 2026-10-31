//! Importing and exporting Redis connections in RedisInsight's file format (its Settings →
//! Import/Export of databases): a JSON array, optionally base64. Field names follow RedisInsight's
//! own importer and exporter, including the other names its importer accepts.
//!
//! RedisInsight keeps certificates and keys as PEM text inside the file, Vilo as file paths, so
//! importing hands back the PEM text for the caller to store, and exporting reads the files.

use std::path::Path;

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use serde_json::{Map, Value, json};

use crate::{redis_ssh::SshSettings, redis_tls::RedisTls};

/// RedisInsight refuses import files larger than this; so does Vilo.
pub const MAX_IMPORT_BYTES: usize = 10 * 1024 * 1024;

/// One connection read from a RedisInsight file.
#[derive(Debug, Default, PartialEq)]
pub struct Imported {
    pub name: String,
    /// A Vilo connection string: credentials, database, cluster or Sentinel in the scheme.
    pub uri: String,
    /// TLS settings; paths given in the file are kept, PEM text is in the fields below.
    pub tls: Option<RedisTls>,
    pub ca_pem: Option<String>,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
    pub ssh: Option<SshSettings>,
    pub ssh_key_pem: Option<String>,
    /// What the file had that Vilo does not use, said to the user.
    pub notes: Vec<String>,
}

/// One Vilo connection to export.
pub struct ToExport<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub uri: &'a str,
    pub tls: Option<&'a RedisTls>,
    pub ssh: Option<&'a SshSettings>,
}

/// The first of these paths (dotted for nested fields) that holds a value.
fn field<'a>(item: &'a Value, paths: &[&str]) -> Option<&'a Value> {
    paths.iter().find_map(|path| {
        path.split('.')
            .try_fold(item, |value, name| value.get(name))
            .filter(|value| !value.is_null() && value.as_str() != Some(""))
    })
}

fn text(item: &Value, paths: &[&str]) -> Option<String> {
    field(item, paths).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn number(item: &Value, paths: &[&str]) -> Option<u64> {
    field(item, paths).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
    })
}

fn flag(item: &Value, paths: &[&str]) -> Option<bool> {
    field(item, paths).and_then(Value::as_bool)
}

/// A certificate field may be PEM text or, in other tools' files, a path.
fn pem_or_path(value: Option<String>) -> (Option<String>, Option<std::path::PathBuf>) {
    match value {
        Some(value) if value.contains("-----BEGIN") => (Some(value), None),
        Some(value) => (None, Some(value.into())),
        None => (None, None),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Standalone,
    Cluster,
    Sentinel,
}

/// Reads a RedisInsight export (or base64 of one).
pub fn parse_export(content: &str) -> Result<Vec<Imported>> {
    anyhow::ensure!(
        content.len() <= MAX_IMPORT_BYTES,
        "The file is larger than {} MB.",
        MAX_IMPORT_BYTES / (1024 * 1024)
    );
    let trimmed = content.trim();
    let parsed: Value = match serde_json::from_str(trimmed) {
        Ok(value) => value,
        Err(error) => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(trimmed)
                .map_err(|_| error)
                .context("The file is not RedisInsight's JSON")?;
            serde_json::from_slice(&decoded).context("The file is not RedisInsight's JSON")?
        }
    };
    let Value::Array(items) = parsed else {
        bail!("The file is not a list of databases.");
    };
    anyhow::ensure!(!items.is_empty(), "The file has no databases.");
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            import_one(item).with_context(|| format!("Database {} in the file", index + 1))
        })
        .collect()
}

fn import_one(item: &Value) -> Result<Imported> {
    anyhow::ensure!(item.is_object(), "it is not an object");
    let host = text(item, &["host"]).context("it has no host")?;
    let port = number(item, &["port"]).unwrap_or(6379);
    let port = u16::try_from(port)
        .ok()
        .filter(|port| *port > 0)
        .context("its port is not valid")?;
    let name = text(item, &["name", "connectionName"]).unwrap_or_else(|| format!("{host}:{port}"));
    let username = text(item, &["username"]);
    let password = text(item, &["password", "auth"]);
    let database = number(item, &["db"]).unwrap_or(0);
    let sentinel_master = text(
        item,
        &[
            "sentinelMaster.name",
            "sentinelOptions.masterName",
            "sentinelOptions.name",
        ],
    );
    let kind = match text(item, &["connectionType"]).as_deref() {
        Some("CLUSTER") => Kind::Cluster,
        Some("SENTINEL") => Kind::Sentinel,
        Some("STANDALONE") => Kind::Standalone,
        _ => match text(item, &["type"]).as_deref() {
            Some("cluster") => Kind::Cluster,
            Some("sentinel") => Kind::Sentinel,
            Some("standalone") => Kind::Standalone,
            _ if flag(item, &["cluster"]) == Some(true) => Kind::Cluster,
            _ if sentinel_master.is_some() => Kind::Sentinel,
            // Vilo finds out on connecting whether a server is a cluster node.
            _ => Kind::Standalone,
        },
    };
    let mut notes = Vec::new();

    let (ca_pem, ca_path) = pem_or_path(text(
        item,
        &[
            "caCert.certificate",
            "caCert",
            "sslOptions.ca",
            "ssl_ca_cert_path",
        ],
    ));
    let (client_cert_pem, client_cert_path) = pem_or_path(text(
        item,
        &[
            "clientCert.certificate",
            "certificate",
            "sslOptions.cert",
            "ssl_local_cert_path",
        ],
    ));
    let (client_key_pem, client_key_path) = pem_or_path(text(
        item,
        &[
            "clientCert.key",
            "keyFile",
            "sslOptions.key",
            "ssl_private_key_path",
        ],
    ));
    let has_certificates = [&ca_pem, &client_cert_pem, &client_key_pem]
        .iter()
        .any(|pem| pem.is_some())
        || ca_path.is_some()
        || client_cert_path.is_some()
        || client_key_path.is_some();
    let tls_on = flag(item, &["tls", "ssl"]).unwrap_or(false) || has_certificates;
    let tls = tls_on.then(|| RedisTls {
        ca_file: ca_path,
        cert_file: client_cert_path,
        key_file: client_key_path,
        // RedisInsight: checked unless the file says otherwise; null means off.
        verify: match field(item, &["verifyServerCert", "sslOptions.rejectUnauthorized"]) {
            Some(value) => value.as_bool().unwrap_or(false),
            None => item
                .get("verifyServerCert")
                .is_none_or(|value| !value.is_null()),
        },
    });
    if client_cert_pem.is_some() != client_key_pem.is_some()
        && tls
            .as_ref()
            .is_some_and(|tls| tls.key_file.is_none() || tls.cert_file.is_none())
    {
        notes.push(
            "its client certificate came without its key (exported without passwords); add the key file".into(),
        );
    }

    let ssh_host = text(item, &["sshOptions.host", "ssh_host", "sshHost"]);
    let ssh = ssh_host.as_ref().map(|host| SshSettings {
        host: host.clone(),
        port: number(item, &["sshOptions.port", "ssh_port", "sshPort"])
            .and_then(|port| u16::try_from(port).ok()),
        user: text(item, &["sshOptions.username", "ssh_user", "sshUser"]).unwrap_or_default(),
        key_file: None,
        known_hosts: None,
    });
    let (ssh_key_pem, ssh_key_path) = pem_or_path(text(
        item,
        &[
            "sshOptions.privateKey",
            "sshOptions.privatekey",
            "ssh_private_key_path",
            "sshKeyFile",
        ],
    ));
    let ssh = ssh.map(|ssh| SshSettings {
        key_file: ssh_key_path,
        ..ssh
    });
    if ssh.is_some() {
        if text(
            item,
            &["sshOptions.password", "ssh_password", "sshPassword"],
        )
        .is_some()
        {
            notes.push(
                "its SSH password was not imported; Vilo signs in to SSH with a key or the agent"
                    .into(),
            );
        }
        if text(item, &["sshOptions.passphrase", "sshKeyPassphrase"]).is_some() {
            notes.push(
                "its SSH key passphrase was not imported; add the key to the agent instead".into(),
            );
        }
    } else if text(item, &["ssh_agent_path"]).is_some() {
        notes.push("its SSH agent setting has no SSH host, so it was left out".into());
    }

    let scheme = format!(
        "{}{}",
        if tls_on { "rediss" } else { "redis" },
        match kind {
            Kind::Standalone => "",
            Kind::Cluster => "-cluster",
            Kind::Sentinel => "-sentinel",
        }
    );
    let mut url = url::Url::parse(&format!("{scheme}://placeholder"))
        .context("could not build its connection string")?;
    url.set_host(Some(&host)).context("its host is not valid")?;
    url.set_port(Some(port)).ok();
    // Behind Sentinel RedisInsight keeps the master's sign-in under sentinelMaster and the
    // Sentinel's at the top; Vilo's string has the master's in the URL and the Sentinel's in
    // its query.
    let (data_username, data_password) = match kind {
        Kind::Sentinel => (
            text(item, &["sentinelMaster.username"]),
            text(
                item,
                &[
                    "sentinelMaster.password",
                    "sentinelOptions.nodePassword",
                    "sentinelOptions.sentinelPassword",
                ],
            ),
        ),
        _ => (username.clone(), password.clone()),
    };
    if let Some(username) = &data_username {
        url.set_username(username).ok();
    }
    if let Some(password) = &data_password {
        url.set_password(Some(password)).ok();
    }
    if database > 0 {
        url.set_path(&format!("/{database}"));
    }
    if kind == Kind::Sentinel {
        let master =
            sentinel_master.context("it is a Sentinel database without a master group name")?;
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("sentinelServiceName", &master);
        if let Some(username) = &username {
            pairs.append_pair("sentinelUsername", username);
        }
        if let Some(password) = &password {
            pairs.append_pair("sentinelPassword", password);
        }
    }
    Ok(Imported {
        name,
        uri: url.to_string(),
        tls,
        ca_pem,
        client_cert_pem,
        client_key_pem,
        ssh,
        ssh_key_pem,
        notes,
    })
}

/// A PEM file's text for the export, or a note saying why it is missing.
/// The PEM blocks of `text` whose label contains `kind` ("CERTIFICATE" or "PRIVATE KEY"), so a
/// file holding a certificate and its key together never puts the key in a certificate field.
fn pem_blocks(text: &str, kind: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("-----BEGIN ") {
        let block = &rest[start..];
        let Some(label_end) = block["-----BEGIN ".len()..].find("-----") else {
            break;
        };
        let label = &block["-----BEGIN ".len().."-----BEGIN ".len() + label_end];
        let end_marker = format!("-----END {label}-----");
        let Some(end) = block.find(&end_marker) else {
            break;
        };
        let whole = &block[..end + end_marker.len()];
        if label.contains(kind) {
            out.push_str(whole);
            out.push('\n');
        }
        rest = &block[end + end_marker.len()..];
    }
    out
}

/// The blocks of `kind` in a PEM file, or a note saying why there are none.
fn read_pem(
    path: &Path,
    what: &str,
    kind: &str,
    notes: &mut Vec<String>,
    name: &str,
) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let blocks = pem_blocks(&text, kind);
            if blocks.is_empty() {
                notes.push(format!(
                    "{name}: its {what} {} holds no {kind}",
                    path.display()
                ));
                return None;
            }
            Some(blocks)
        }
        Err(error) => {
            notes.push(format!(
                "{name}: its {what} {} could not be read ({error})",
                path.display()
            ));
            None
        }
    }
}

/// Writes Vilo connections as a RedisInsight export. Without `with_secrets` it leaves out what
/// RedisInsight's own export leaves out: passwords, the client key and the SSH private key.
/// Returns the JSON and notes about anything that could not be included.
pub fn export(connections: &[ToExport], with_secrets: bool) -> Result<(String, Vec<String>)> {
    let mut notes = Vec::new();
    let mut items = Vec::new();
    for connection in connections {
        let url = url::Url::parse(connection.uri)
            .with_context(|| format!("{}: its connection string is not valid", connection.name))?;
        let scheme = url.scheme().to_ascii_lowercase();
        let kind = if scheme.ends_with("-cluster") {
            "CLUSTER"
        } else if scheme.ends_with("-sentinel") {
            "SENTINEL"
        } else {
            "STANDALONE"
        };
        // Percent-decoding only: in a URL's user information `+` is a plus, not a space.
        let decode = |text: &str| {
            urlencoding::decode(text)
                .map(|text| text.into_owned())
                .unwrap_or_else(|_| text.to_string())
        };
        let parameter = |name: &str| {
            url.query_pairs()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
        };
        let username = (!url.username().is_empty()).then(|| decode(url.username()));
        let password = url.password().map(decode);
        let mut item = Map::new();
        item.insert("id".into(), json!(connection.id));
        item.insert("name".into(), json!(connection.name));
        item.insert(
            "host".into(),
            json!(url.host_str().unwrap_or_default().trim_matches(['[', ']'])),
        );
        item.insert(
            "port".into(),
            json!(
                url.port()
                    .unwrap_or(if kind == "SENTINEL" { 26379 } else { 6379 })
            ),
        );
        let path = url.path().trim_matches('/');
        let database = if path.is_empty() {
            0
        } else {
            path.parse::<u8>().with_context(|| {
                format!(
                    "{}: its database {path} is not a number from 0 to 255",
                    connection.name
                )
            })?
        };
        item.insert("db".into(), json!(database));
        item.insert("connectionType".into(), json!(kind));
        if kind == "SENTINEL" {
            // The other way round from Vilo's string; see `import_one`.
            if let Some(username) = parameter("sentinelUsername") {
                item.insert("username".into(), json!(username));
            }
            if with_secrets && let Some(password) = parameter("sentinelPassword") {
                item.insert("password".into(), json!(password));
            }
            let mut master = Map::new();
            master.insert(
                "name".into(),
                json!(parameter("sentinelServiceName").unwrap_or_default()),
            );
            if let Some(username) = &username {
                master.insert("username".into(), json!(username));
            }
            if with_secrets && let Some(password) = &password {
                master.insert("password".into(), json!(password));
            }
            item.insert("sentinelMaster".into(), Value::Object(master));
        } else {
            if let Some(username) = &username {
                item.insert("username".into(), json!(username));
            }
            if with_secrets && let Some(password) = &password {
                item.insert("password".into(), json!(password));
            }
        }
        let tls = crate::redis_tls::tls_scheme(connection.uri);
        item.insert("tls".into(), json!(tls));
        if tls {
            let settings = connection.tls.cloned().unwrap_or_default();
            item.insert("verifyServerCert".into(), json!(settings.verify));
            if let Some(path) = &settings.ca_file
                && let Some(pem) = read_pem(
                    path,
                    "CA certificate",
                    "CERTIFICATE",
                    &mut notes,
                    connection.name,
                )
            {
                item.insert(
                    "caCert".into(),
                    json!({ "name": format!("{} CA", connection.name), "certificate": pem }),
                );
            }
            if let Some(path) = &settings.cert_file
                && let Some(pem) = read_pem(
                    path,
                    "client certificate",
                    "CERTIFICATE",
                    &mut notes,
                    connection.name,
                )
            {
                let mut client = Map::new();
                client.insert("name".into(), json!(format!("{} client", connection.name)));
                client.insert("certificate".into(), json!(pem));
                if with_secrets {
                    match &settings.key_file {
                        Some(path) => {
                            if let Some(key) = read_pem(
                                path,
                                "client key",
                                "PRIVATE KEY",
                                &mut notes,
                                connection.name,
                            ) {
                                client.insert("key".into(), json!(key));
                            }
                        }
                        None => notes.push(format!(
                            "{}: its client certificate has no key file, so none was exported",
                            connection.name
                        )),
                    }
                }
                item.insert("clientCert".into(), Value::Object(client));
            }
        }
        if let Some(ssh) = connection.ssh {
            item.insert("ssh".into(), json!(true));
            let mut options = Map::new();
            options.insert("host".into(), json!(ssh.host));
            options.insert("port".into(), json!(ssh.port.unwrap_or(22)));
            if !ssh.user.is_empty() {
                options.insert("username".into(), json!(ssh.user));
            }
            if with_secrets
                && let Some(path) = &ssh.key_file
                && let Some(key) = read_pem(
                    path,
                    "SSH private key",
                    "PRIVATE KEY",
                    &mut notes,
                    connection.name,
                )
            {
                options.insert("privateKey".into(), json!(key));
            }
            if ssh.key_file.is_none() {
                notes.push(format!(
                    "{}: signs in to SSH with the agent, which RedisInsight cannot use from the file",
                    connection.name
                ));
            }
            item.insert("sshOptions".into(), Value::Object(options));
        }
        items.push(Value::Object(item));
    }
    let json = serde_json::to_string_pretty(&items).context("The export could not be written")?;
    Ok((json, notes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shaped like RedisInsight's own export (its export DTO and models), with secrets.
    const REDIS_INSIGHT: &str = r#"[
      {"id":"00000000-0000-0000-0000-000000000001","host":"standalone.example.com","port":6379,
       "name":"standalone","db":2,"username":"default","password":"p@ss word",
       "connectionType":"STANDALONE","tls":false},
      {"id":"00000000-0000-0000-0000-000000000002","host":"cluster.example.com","port":7000,
       "name":"cluster","connectionType":"CLUSTER","tls":true,"verifyServerCert":false,
       "caCert":{"id":"c","name":"ca","certificate":"-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n"},
       "clientCert":{"id":"k","name":"client","certificate":"-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----\n",
                     "key":"-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n"}},
      {"id":"00000000-0000-0000-0000-000000000003","host":"sentinel.example.com","port":26379,
       "name":"sentinel","username":"sentinel-user","password":"sentinel-pw","connectionType":"SENTINEL",
       "sentinelMaster":{"name":"mymaster","username":"data-user","password":"data-pw"},
       "ssh":true,"sshOptions":{"id":"s","host":"bastion.example.com","port":2222,"username":"me",
                                "password":"ssh-pw","privateKey":"-----BEGIN OPENSSH PRIVATE KEY-----\nSSH\n-----END OPENSSH PRIVATE KEY-----\n"}}
    ]"#;

    #[test]
    fn test_a_redis_insight_export_is_read() {
        let imported = parse_export(REDIS_INSIGHT).unwrap();
        assert_eq!(imported.len(), 3);

        let standalone = &imported[0];
        assert_eq!(standalone.name, "standalone");
        assert_eq!(
            standalone.uri,
            "redis://default:p%40ss%20word@standalone.example.com:6379/2"
        );
        assert_eq!(standalone.tls, None);

        let cluster = &imported[1];
        assert_eq!(cluster.uri, "rediss-cluster://cluster.example.com:7000");
        assert_eq!(cluster.tls.as_ref().map(|tls| tls.verify), Some(false));
        assert!(
            cluster
                .ca_pem
                .as_deref()
                .is_some_and(|pem| pem.contains("CA"))
        );
        assert!(cluster.client_key_pem.is_some());

        let sentinel = &imported[2];
        let url = url::Url::parse(&sentinel.uri).unwrap();
        assert_eq!(url.scheme(), "redis-sentinel");
        // The master's sign-in in the URL, the Sentinel's in the query.
        assert_eq!(
            (url.username(), url.password()),
            ("data-user", Some("data-pw"))
        );
        let query = url.query_pairs().collect::<Vec<_>>();
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "sentinelServiceName" && v == "mymaster")
        );
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "sentinelUsername" && v == "sentinel-user")
        );
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "sentinelPassword" && v == "sentinel-pw")
        );
        let ssh = sentinel.ssh.as_ref().unwrap();
        assert_eq!(
            (ssh.host.as_str(), ssh.port, ssh.user.as_str()),
            ("bastion.example.com", Some(2222), "me")
        );
        assert!(sentinel.ssh_key_pem.is_some());
        assert!(
            sentinel
                .notes
                .iter()
                .any(|note| note.contains("SSH password")),
            "{:?}",
            sentinel.notes
        );
    }

    #[test]
    fn test_other_names_and_shapes_are_read() {
        let base64 = base64::engine::general_purpose::STANDARD.encode(
            r#"[{"host":"h","port":"6380","connectionName":"old","auth":"pw","ssl":true,
                 "ssl_ca_cert_path":"/etc/ca.pem","cluster":true}]"#,
        );
        let imported = parse_export(&base64).unwrap();
        assert_eq!(imported[0].name, "old");
        assert_eq!(imported[0].uri, "rediss-cluster://:pw@h:6380");
        assert_eq!(
            imported[0].tls.as_ref().and_then(|tls| tls.ca_file.clone()),
            Some("/etc/ca.pem".into())
        );
        let unnamed = parse_export(r#"[{"host":"h"}]"#).unwrap();
        assert_eq!(
            (unnamed[0].name.as_str(), unnamed[0].uri.as_str()),
            ("h:6379", "redis://h:6379")
        );
        for bad in [
            "{}",
            "[]",
            "not json",
            r#"[{"port":1}]"#,
            r#"[{"host":"h","port":0}]"#,
            r#"[{"host":"h","connectionType":"SENTINEL"}]"#,
        ] {
            assert!(parse_export(bad).is_err(), "{bad}");
        }
        assert!(parse_export(&" ".repeat(MAX_IMPORT_BYTES + 1)).is_err());
    }

    #[test]
    fn test_exports_leave_secrets_out_unless_asked_and_read_back() {
        let dir = std::env::temp_dir().join(format!("vilo-export-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        let key = dir.join("client.key");
        let cert = dir.join("client.pem");
        std::fs::write(
            &ca,
            "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(
            &cert,
            "-----BEGIN CERTIFICATE-----\nC\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(
            &key,
            "-----BEGIN PRIVATE KEY-----\nK\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let tls = RedisTls {
            ca_file: Some(ca),
            cert_file: Some(cert),
            key_file: Some(key),
            verify: true,
        };
        let ssh = SshSettings {
            host: "bastion".into(),
            port: None,
            user: "me".into(),
            key_file: None,
            known_hosts: None,
        };
        let connections = [
            ToExport {
                id: "1",
                name: "plain",
                uri: "redis://u%2Bx:se+cr%26et@h:6379/3",
                tls: None,
                ssh: None,
            },
            ToExport {
                id: "2",
                name: "tls",
                uri: "rediss-cluster://h:7000",
                tls: Some(&tls),
                ssh: Some(&ssh),
            },
            ToExport {
                id: "3",
                name: "sentinel",
                uri: "redis-sentinel://data:dpw@s:26379?sentinelServiceName=m&sentinelUsername=su&sentinelPassword=spw",
                tls: None,
                ssh: None,
            },
        ];
        // A certificate file that also holds its key: the key is never a certificate.
        std::fs::write(
            &dir.join("client.pem"),
            "-----BEGIN CERTIFICATE-----\nC\n-----END CERTIFICATE-----\n-----BEGIN PRIVATE KEY-----\nK\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let (plain, notes) = export(&connections, false).unwrap();
        assert!(
            plain.contains("\"u+x\""),
            "a + in a user name stays a plus:\n{plain}"
        );
        for secret in ["cr&et", "dpw", "spw", "PRIVATE KEY"] {
            assert!(
                !plain.contains(secret),
                "{secret} left out by default:\n{plain}"
            );
        }
        assert!(
            plain.contains("BEGIN CERTIFICATE"),
            "certificates are not secrets"
        );
        assert!(notes.iter().any(|note| note.contains("agent")), "{notes:?}");

        let (full, _) = export(&connections, true).unwrap();
        let back = parse_export(&full).unwrap();
        let plain_back = url::Url::parse(&back[0].uri).unwrap();
        assert_eq!(
            (
                urlencoding::decode(plain_back.username()).unwrap().as_ref(),
                urlencoding::decode(plain_back.password().unwrap())
                    .unwrap()
                    .as_ref()
            ),
            ("u+x", "se+cr&et")
        );
        assert_eq!(plain_back.path(), "/3");
        assert_eq!(back[1].uri, "rediss-cluster://h:7000");
        assert!(back[1].client_key_pem.is_some());
        assert_eq!(
            back[1].ssh.as_ref().map(|ssh| ssh.host.as_str()),
            Some("bastion")
        );
        let sentinel = url::Url::parse(&back[2].uri).unwrap();
        assert_eq!(
            (sentinel.username(), sentinel.password()),
            ("data", Some("dpw"))
        );
        assert!(
            sentinel
                .query_pairs()
                .any(|(k, v)| k == "sentinelPassword" && v == "spw")
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
