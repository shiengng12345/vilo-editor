//! TLS for Redis connections: the server's certificate checked against the system's roots or a
//! CA file, an optional client certificate (mutual TLS), and, as RedisInsight allows, turning the
//! server check off for servers with self-signed certificates.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result, bail};
use rustls::{
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject as _},
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RedisTls {
    /// PEM certificates to trust instead of the system's roots.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate and its key, for servers that require one.
    #[serde(default)]
    pub cert_file: Option<PathBuf>,
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// Off accepts any server certificate; the connection is still encrypted.
    #[serde(default = "verify_by_default")]
    pub verify: bool,
}

fn verify_by_default() -> bool {
    true
}

impl Default for RedisTls {
    fn default() -> Self {
        Self {
            ca_file: None,
            cert_file: None,
            key_file: None,
            verify: true,
        }
    }
}

/// The TLS schemes and the plain scheme each is parsed as.
const TLS_SCHEMES: &[(&str, &str)] = &[
    ("rediss", "redis"),
    ("rediss-cluster", "redis-cluster"),
    ("rediss-sentinel", "redis-sentinel"),
    ("valkeys", "valkey"),
    ("valkeys-cluster", "valkey-cluster"),
    ("valkeys-sentinel", "valkey-sentinel"),
];

/// The connection string with a TLS scheme replaced by its plain one (any letter case), and
/// whether it asked for TLS.
pub fn split_tls_scheme(uri: &str) -> (String, bool) {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return (uri.to_string(), false);
    };
    let scheme = scheme.to_ascii_lowercase();
    match TLS_SCHEMES.iter().find(|(tls, _)| *tls == scheme) {
        Some((_, plain)) => (format!("{plain}://{rest}"), true),
        None => (format!("{scheme}://{rest}"), false),
    }
}

/// The plain schemes fred is given; anything else is refused before fred sees it, since fred
/// takes any scheme starting with `rediss` as TLS and would build its own TLS settings.
const PLAIN_SCHEMES: &[&str] = &[
    "redis",
    "redis-cluster",
    "redis-sentinel",
    "valkey",
    "valkey-cluster",
    "valkey-sentinel",
];

/// Like `split_tls_scheme`, refusing schemes that are not Redis connection strings.
pub fn plain_url(uri: &str) -> Result<(String, bool)> {
    let (url, tls) = split_tls_scheme(uri);
    let scheme = url
        .split_once("://")
        .map(|(scheme, _)| scheme)
        .unwrap_or_default();
    anyhow::ensure!(
        PLAIN_SCHEMES.contains(&scheme),
        "That is not a Redis connection string; it starts with redis://, rediss://, redis-cluster:// or rediss-cluster://."
    );
    Ok((url, tls))
}

/// Whether a Redis connection string asks for TLS.
pub fn tls_scheme(uri: &str) -> bool {
    split_tls_scheme(uri).1
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Accepts any server certificate, while still checking that the handshake is signed by it.
#[derive(Debug)]
struct AnyServer(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            signature,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Checks every server's certificate against the host the user connected to, whatever name a
/// cluster reports for its nodes, so a certificate for another name is never accepted.
#[derive(Debug)]
struct PinnedName {
    inner: Arc<WebPkiServerVerifier>,
    name: ServerName<'static>,
}

impl ServerCertVerifier for PinnedName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, &self.name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn certificates(path: &PathBuf, what: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_file_iter(path)
        .with_context(|| format!("Could not read the {what} file {}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("The {what} file {} is not valid PEM", path.display()))?;
    anyhow::ensure!(
        !certificates.is_empty(),
        "The {what} file {} holds no certificate",
        path.display()
    );
    Ok(certificates)
}

/// The rustls settings for these TLS options. Certificates are checked against `pinned` when
/// given, else against the name each server is reached by (behind Sentinel, the master it names).
pub fn client_config(settings: &RedisTls, pinned: Option<&str>) -> Result<ClientConfig> {
    let provider = provider();
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("TLS could not be set up")?;
    let builder = if settings.verify {
        let mut roots = RootCertStore::empty();
        match &settings.ca_file {
            Some(path) => {
                for certificate in certificates(path, "CA certificate")? {
                    roots.add(certificate).with_context(|| {
                        format!("The CA certificate in {} is not usable", path.display())
                    })?;
                }
            }
            None => {
                let native = rustls_native_certs::load_native_certs();
                roots.add_parsable_certificates(native.certs);
                anyhow::ensure!(
                    !roots.is_empty(),
                    "The system's trusted certificates could not be read; choose a CA certificate file."
                );
            }
        }
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .context("TLS could not be set up")?;
        match pinned {
            Some(host) => {
                let name = ServerName::try_from(host.to_string())
                    .with_context(|| format!("{host} is not a valid TLS server name"))?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(PinnedName { inner, name }))
            }
            None => builder.with_webpki_verifier(inner),
        }
    } else {
        // Not needed with the check off, but a path the user gave is still read, so a typo shows.
        if let Some(path) = &settings.ca_file {
            certificates(path, "CA certificate")?;
        }
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyServer(provider)))
    };
    match (&settings.cert_file, &settings.key_file) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(cert), Some(key)) => {
            let chain = certificates(cert, "client certificate")?;
            let key = PrivateKeyDer::from_pem_file(key)
                .with_context(|| format!("Could not read a private key from {}", key.display()))?;
            builder
                .with_client_auth_cert(chain, key)
                .context("The client certificate and key do not match")
        }
        _ => bail!("A client certificate needs both its certificate file and its key file."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_files() -> Option<PathBuf> {
        std::env::var("VILO_TEST_TLS_DIR").ok().map(PathBuf::from)
    }

    #[test]
    fn test_tls_schemes() {
        for uri in [
            "rediss://h:1",
            "rediss-cluster://h:1",
            "rediss-sentinel://h:1",
            "valkeys://h",
            "REDISS://h",
        ] {
            assert!(tls_scheme(uri), "{uri}");
        }
        for uri in [
            "redis://h",
            "redis-cluster://h",
            "redissx://h",
            "h:6379",
            "",
        ] {
            assert!(!tls_scheme(uri), "{uri}");
        }
        assert_eq!(
            split_tls_scheme("ReDiSs-Cluster://u:p@h:1?node=x:2"),
            ("redis-cluster://u:p@h:1?node=x:2".to_string(), true)
        );
        assert_eq!(
            split_tls_scheme("REDIS://h:1"),
            ("redis://h:1".to_string(), false)
        );
        for refused in ["redissx://h:1", "http://h", "h:6379", "mongodb://h"] {
            assert!(plain_url(refused).is_err(), "{refused}");
        }
        assert_eq!(
            plain_url("Rediss://h:1").unwrap(),
            ("redis://h:1".to_string(), true)
        );
    }

    #[test]
    fn test_settings_are_checked() {
        let half = RedisTls {
            cert_file: Some("client.crt".into()),
            ..RedisTls::default()
        };
        assert!(
            format!("{:#}", client_config(&half, Some("localhost")).unwrap_err())
                .contains("both its certificate")
        );
        let missing = RedisTls {
            ca_file: Some("/nonexistent/ca.crt".into()),
            ..RedisTls::default()
        };
        assert!(
            format!(
                "{:#}",
                client_config(&missing, Some("localhost")).unwrap_err()
            )
            .contains("Could not read")
        );
        let unchecked_typo = RedisTls {
            verify: false,
            ..missing
        };
        assert!(
            client_config(&unchecked_typo, Some("localhost")).is_err(),
            "a CA path given with the check off is still read"
        );
        assert!(
            client_config(&RedisTls::default(), Some("localhost")).is_ok(),
            "system roots"
        );
        assert!(
            client_config(
                &RedisTls {
                    verify: false,
                    ..RedisTls::default()
                },
                Some("localhost")
            )
            .is_ok()
        );
        let Some(dir) = test_files() else {
            return;
        };
        let mismatched = RedisTls {
            ca_file: Some(dir.join("ca.crt")),
            cert_file: Some(dir.join("client.crt")),
            key_file: Some(dir.join("server.key")),
            verify: true,
        };
        assert!(
            format!(
                "{:#}",
                client_config(&mismatched, Some("localhost")).unwrap_err()
            )
            .contains("do not match")
        );
        let not_pem = RedisTls {
            ca_file: Some(dir.join("server.ext")),
            ..RedisTls::default()
        };
        assert!(
            format!(
                "{:#}",
                client_config(&not_pem, Some("localhost")).unwrap_err()
            )
            .contains("no certificate")
        );
    }

    #[test]
    fn test_certificates_are_checked_against_the_connected_host() {
        let Some(dir) = test_files() else {
            return;
        };
        let server = certificates(&dir.join("server.crt"), "server certificate").unwrap();
        let (end_entity, _) = server.split_first().unwrap();
        let mut roots = RootCertStore::empty();
        for certificate in certificates(&dir.join("ca.crt"), "CA certificate").unwrap() {
            roots.add(certificate).unwrap();
        }
        let roots = Arc::new(roots);
        let pinned = |name: &str| PinnedName {
            inner: WebPkiServerVerifier::builder_with_provider(roots.clone(), provider())
                .build()
                .unwrap(),
            name: ServerName::try_from(name.to_string()).unwrap(),
        };
        let reported = ServerName::try_from("node7.internal".to_string()).unwrap();
        // The test certificate names localhost and 127.0.0.1.
        assert!(
            pinned("localhost")
                .verify_server_cert(end_entity, &[], &reported, &[], UnixTime::now())
                .is_ok(),
            "whatever name a node reports, the connected host decides"
        );
        let localhost = ServerName::try_from("localhost".to_string()).unwrap();
        assert!(
            pinned("redis.example.com")
                .verify_server_cert(end_entity, &[], &localhost, &[], UnixTime::now())
                .is_err(),
            "a certificate for another name is refused even when a node reports that name"
        );
    }

    fn mutual(dir: &std::path::Path) -> RedisTls {
        RedisTls {
            ca_file: Some(dir.join("ca.crt")),
            cert_file: Some(dir.join("client.crt")),
            key_file: Some(dir.join("client.key")),
            verify: true,
        }
    }

    #[tokio::test]
    async fn test_tls_against_redis() {
        use crate::redis::RedisSession;
        let (Some(dir), Ok(url)) = (test_files(), std::env::var("VILO_TEST_REDIS_TLS")) else {
            return;
        };
        let session = RedisSession::connect_with(&url, Some(3), Some(&mutual(&dir)), None)
            .await
            .unwrap();
        assert!(!session.is_clustered());
        session
            .call("SET", vec!["vilo_test:tls".into(), "1".into()])
            .await
            .unwrap();

        // The profiler's own connection uses the same TLS settings.
        let node = session.scan_nodes().remove(0);
        let monitor = session.monitor(&node, 5).await.unwrap();
        let mut started = false;
        for _ in 0..100 {
            let batch = monitor.take();
            assert!(batch.ended.is_none(), "{:?}", batch.ended);
            if batch.started {
                started = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(started, "MONITOR over TLS");
        drop(monitor);
        session
            .call("DEL", vec!["vilo_test:tls".into()])
            .await
            .unwrap();

        let without_client = RedisTls {
            cert_file: None,
            key_file: None,
            ..mutual(&dir)
        };
        assert!(
            RedisSession::connect_with(&url, None, Some(&without_client), None)
                .await
                .is_err(),
            "the server requires a client certificate"
        );
        let other_ca = RedisTls {
            ca_file: Some(dir.join("other.crt")),
            ..mutual(&dir)
        };
        assert!(
            RedisSession::connect_with(&url, None, Some(&other_ca), None)
                .await
                .is_err(),
            "a server certificate from another CA is refused"
        );
        let unchecked = RedisTls {
            ca_file: Some(dir.join("other.crt")),
            verify: false,
            ..mutual(&dir)
        };
        assert!(
            RedisSession::connect_with(&url, None, Some(&unchecked), None)
                .await
                .is_ok(),
            "with the check off, any server certificate is accepted"
        );
        // Any letter case still gets Vilo's TLS, the profiler's connection included.
        let shouting = url.replacen("rediss://", "REDISS://", 1);
        let loud = RedisSession::connect_with(&shouting, None, Some(&mutual(&dir)), None)
            .await
            .unwrap();
        let monitor = loud.monitor(&loud.scan_nodes().remove(0), 5).await.unwrap();
        let mut started = false;
        for _ in 0..100 {
            let batch = monitor.take();
            assert!(batch.ended.is_none(), "{:?}", batch.ended);
            if batch.started {
                started = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(started, "MONITOR over TLS for {shouting}");
        drop(monitor);
        let plain = url.replacen("rediss://", "redis://", 1);
        let error = RedisSession::connect_with(&plain, None, Some(&mutual(&dir)), None)
            .await
            .err()
            .expect("TLS settings without TLS");
        assert!(
            format!("{error:#}").contains("turn Use TLS on"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn test_a_tls_cluster_is_found_from_one_node() {
        use crate::redis::RedisSession;
        let (Some(dir), Ok(url)) = (test_files(), std::env::var("VILO_TEST_REDIS_TLS_CLUSTER"))
        else {
            return;
        };
        // A plain rediss:// URL to one node: the session finds the cluster and every primary.
        let session = RedisSession::connect_with(&url, None, Some(&mutual(&dir)), None)
            .await
            .unwrap();
        assert!(session.is_clustered());
        let nodes = session.scan_nodes();
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        for number in 0..30 {
            session
                .call(
                    "SET",
                    vec![format!("vilo_test:tls:{number}").into(), "1".into()],
                )
                .await
                .unwrap();
        }
        let mut total = 0;
        for node in &nodes {
            total += session.dbsize(node).await.unwrap();
        }
        assert!(total >= 30, "{total}");
        for number in 0..30 {
            session
                .call("DEL", vec![format!("vilo_test:tls:{number}").into()])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_unknown_schemes_are_refused_before_fred_sees_them() {
        // fred would take this as TLS and build its own settings, which needs a process
        // provider; it must be an error, not a panic.
        let error = crate::redis::RedisSession::connect("redissx://127.0.0.1:1")
            .await
            .err()
            .expect("an unknown scheme");
        assert!(
            format!("{error:#}").contains("not a Redis connection string"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn test_a_node_whose_cluster_cannot_be_used_stays_single() {
        // A node saying cluster_enabled:1 whose CLUSTER command does not answer, as behind
        // some managed cluster proxies.
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER_PROXY") else {
            return;
        };
        let session = crate::redis::RedisSession::connect(&url).await.unwrap();
        assert!(!session.is_clustered());
        assert!(
            session
                .notice()
                .is_some_and(|notice| notice.contains("cluster proxy")),
            "{:?}",
            session.notice()
        );
        session
            .call("SET", vec!["vilo_test:proxy".into(), "1".into()])
            .await
            .unwrap();
        session
            .call("DEL", vec!["vilo_test:proxy".into()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_ipv6_servers_connect() {
        use crate::redis::RedisSession;
        // Native servers listening on ::1 only (containers here do not forward IPv6).
        if let Ok(port) = std::env::var("VILO_TEST_IPV6_PORT") {
            let imported =
                crate::redis_insight::parse_export(&format!(r#"[{{"host":"::1","port":{port}}}]"#))
                    .unwrap();
            let session = RedisSession::connect(&imported[0].uri).await.unwrap();
            let pong = session.call("PING", Vec::new()).await.unwrap();
            assert_eq!(pong.as_string().as_deref(), Some("PONG"));
        }
        if let Ok(port) = std::env::var("VILO_TEST_IPV6_TLS_PORT") {
            // The test certificate names 127.0.0.1, not ::1, so verification is off; this proves
            // the bracketed host still becomes a valid TLS server name and the handshake completes.
            let settings = RedisTls {
                verify: false,
                ..RedisTls::default()
            };
            let session = RedisSession::connect_with(
                &format!("rediss://[::1]:{port}"),
                None,
                Some(&settings),
                None,
            )
            .await
            .unwrap();
            let pong = session.call("PING", Vec::new()).await.unwrap();
            assert_eq!(pong.as_string().as_deref(), Some("PONG"));
        }
    }

    #[tokio::test]
    async fn test_a_plain_cluster_is_found_from_one_node() {
        use crate::redis::RedisSession;
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let single = url.replacen("redis-cluster://", "redis://", 1);
        let session = RedisSession::connect(&single).await.unwrap();
        assert!(session.is_clustered(), "found from {single}");
        assert!(session.scan_nodes().len() > 1);
        let error = RedisSession::connect_to_database(&single, Some(2))
            .await
            .err()
            .expect("a cluster node has no database 2");
        assert!(
            format!("{error:#}").contains("only has database 0"),
            "{error:#}"
        );
        let explicit = RedisSession::connect(&format!("{url}/2"))
            .await
            .err()
            .expect("fred ignores a cluster URL's path; Vilo does not");
        assert!(
            format!("{explicit:#}").contains("only has database 0"),
            "{explicit:#}"
        );
        assert!(RedisSession::connect(&format!("{url}/0")).await.is_ok());
        let in_the_url = RedisSession::connect(&format!("{single}/3"))
            .await
            .err()
            .expect("a database in the URL itself");
        assert!(
            format!("{in_the_url:#}").contains("only has database 0"),
            "{in_the_url:#}"
        );
    }
}
