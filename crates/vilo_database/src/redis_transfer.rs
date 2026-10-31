//! Importing Redis connections from a RedisInsight file and exporting them to one. Imported
//! passwords go to the keychain like typed ones; certificates and keys, which the file holds as
//! text, are written as files only the user can read, since Vilo keeps paths.

use std::{
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
};

use anyhow::{Context as _, Result};
use gpui::{PathPromptOptions, Window, prelude::*};
use ui::prelude::*;
use vilo_database_core::redis_insight::{
    Imported, MAX_IMPORT_BYTES, ToExport, export, parse_export,
};

use crate::{Connection, DatabaseView, Kind, Level};

/// Writes `text` to a file readable only by the user. It goes to a new private file first and
/// replaces the target whole, so an existing file's looser permissions never see the text.
fn write_private(path: &Path, text: &str) -> Result<()> {
    let parent = path.parent().context("The file has no folder")?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".vilo-{}.tmp",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let written = options
        .open(&temporary)
        .and_then(|mut file| {
            file.write_all(text.as_bytes())?;
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&temporary, path));
    if let Err(error) = written {
        let error =
            anyhow::Error::from(error).context(format!("Could not write {}", path.display()));
        // The temporary file may hold secrets; if it cannot be removed, say where it is.
        return match std::fs::remove_file(&temporary) {
            Err(cleanup) if cleanup.kind() != std::io::ErrorKind::NotFound => {
                Err(error.context(format!(
                    "and {} (private to you) could not be removed: {cleanup}",
                    temporary.display()
                )))
            }
            _ => Err(error),
        };
    }
    Ok(())
}

/// Removes the files `store_files` may have written for a connection that is not added, and
/// says which could not be removed. Most were never written; a missing one is fine.
fn remove_files(id: &str) -> Vec<String> {
    let folder = paths::config_dir().join("database_certificates");
    ["ca", "client", "client-key", "ssh-key"]
        .into_iter()
        .filter_map(|suffix| {
            let path = folder.join(format!("{id}-{suffix}.pem"));
            match std::fs::remove_file(&path) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    Some(format!("{} could not be removed ({error})", path.display()))
                }
                _ => None,
            }
        })
        .collect()
}

/// Stores an imported connection's PEM texts as files and points its settings at them.
fn store_files(id: &str, imported: &mut Imported) -> Result<()> {
    let folder = paths::config_dir().join("database_certificates");
    let store = |pem: Option<String>, suffix: &str| -> Result<Option<PathBuf>> {
        let Some(pem) = pem else {
            return Ok(None);
        };
        let path = folder.join(format!("{id}-{suffix}.pem"));
        write_private(&path, &pem)?;
        Ok(Some(path))
    };
    let ca = store(imported.ca_pem.take(), "ca")?;
    let cert = store(imported.client_cert_pem.take(), "client")?;
    let key = store(imported.client_key_pem.take(), "client-key")?;
    let ssh_key = store(imported.ssh_key_pem.take(), "ssh-key")?;
    if let Some(tls) = &mut imported.tls {
        tls.ca_file = ca.or(tls.ca_file.take());
        tls.cert_file = cert.or(tls.cert_file.take());
        tls.key_file = key.or(tls.key_file.take());
    }
    if let Some(ssh) = &mut imported.ssh {
        ssh.key_file = ssh_key.or(ssh.key_file.take());
    }
    Ok(())
}

fn summary(head: String, notes: &[String]) -> SharedString {
    let mut text = head;
    for note in notes.iter().take(8) {
        text.push_str("\n· ");
        text.push_str(note);
    }
    if notes.len() > 8 {
        text.push_str(&format!("\n· …and {} more", notes.len() - 8));
    }
    text.into()
}

impl DatabaseView {
    pub(crate) fn import_redis_insight(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let site = self.current(Level::Site);
        let environment = self.current(Level::Environment);
        if site.is_empty() || environment.is_empty() {
            self.transfer_notice = Some(
                "Add a site and an environment on the left first; imported connections go there."
                    .into(),
            );
            cx.notify();
            return;
        }
        let paths = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Import".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = paths.await else {
                return;
            };
            let Some(path) = paths.into_iter().next() else {
                return;
            };
            let read = cx
                .background_spawn(async move {
                    let mut bytes = Vec::new();
                    // Read with a cap, so a file that grows after it was picked is still bounded.
                    std::fs::File::open(&path)?
                        .take(MAX_IMPORT_BYTES as u64 + 1)
                        .read_to_end(&mut bytes)?;
                    let text = String::from_utf8(bytes).context("The file is not UTF-8 text")?;
                    let imported = parse_export(&text)?;
                    let mut ready = Vec::new();
                    for mut connection in imported {
                        let id = uuid::Uuid::new_v4().simple().to_string();
                        if let Err(error) = store_files(&id, &mut connection) {
                            // Nothing is imported, so no key file written so far is kept.
                            let left = std::iter::once(id.as_str())
                                .chain(ready.iter().map(|(id, _): &(String, Imported)| id.as_str()))
                                .flat_map(remove_files)
                                .collect::<Vec<_>>();
                            if left.is_empty() {
                                return Err(error);
                            }
                            return Err(error.context(format!(
                                "and some files written for it remain: {}",
                                left.join("; ")
                            )));
                        }
                        ready.push((id, connection));
                    }
                    anyhow::Ok(ready)
                })
                .await;
            let ready = match read {
                Ok(ready) => ready,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.transfer_notice = Some(format!("Nothing was imported: {error:#}").into());
                        cx.notify();
                    })
                    .ok();
                    return;
                }
            };
            let mut added = Vec::new();
            let mut notes = Vec::new();
            for (id, imported) in ready {
                let connection = Connection {
                    id,
                    name: imported.name.clone(),
                    kind: Kind::Redis,
                    site: site.clone(),
                    environment: environment.clone(),
                    allow_writes: false,
                    redis_tls: imported.tls.clone(),
                    redis_ssh: imported.ssh.clone(),
                };
                let url = connection.credential_url();
                let saved = cx
                    .update(|_, cx| cx.write_credentials(&url, Kind::Redis.label(), imported.uri.as_bytes()))
                    .ok();
                // A connection whose string cannot be kept is not added: it could not connect.
                match saved {
                    Some(task) => match task.await {
                        Ok(()) => {
                            notes.extend(imported.notes.iter().map(|note| format!("{}: {note}", imported.name)));
                            if let Some(password) = imported.ssh.as_ref().and_then(|ssh| ssh.password.clone()) {
                                let url = connection.ssh_credential_url();
                                let saved = match cx.update(|_, cx| cx.write_credentials(&url, "SSH", password.0.as_bytes())) {
                                    Ok(task) => task.await,
                                    Err(_) => return,
                                };
                                if let Err(error) = saved {
                                    notes.push(format!(
                                        "{}: its SSH password could not be kept ({error:#}); type it in the connection",
                                        imported.name
                                    ));
                                }
                            }
                            added.push(connection);
                        }
                        Err(error) => {
                            notes.push(format!(
                                "{}: not imported, the keychain refused its connection string ({error:#})",
                                imported.name
                            ));
                            notes.extend(remove_files(&connection.id));
                        }
                    },
                    None => return,
                }
            }
            this.update(cx, |this, cx| {
                let count = added.len();
                this.connections.extend(added);
                let persist = this.persist(cx);
                this.transfer_notice = Some(summary(
                    format!("Imported {count} Redis connection{} into {site} · {environment}.", if count == 1 { "" } else { "s" }),
                    &notes,
                ));
                cx.spawn(async move |this, cx| {
                    if let Err(error) = persist.await {
                        this.update(cx, |this, cx| {
                            this.transfer_notice = Some(format!("The imported connections could not be saved: {error:#}").into());
                            cx.notify();
                        })
                        .ok();
                    }
                })
                .detach();
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Exports the Redis connections shown on the left, without passwords unless asked.
    pub(crate) fn export_redis_insight(
        &mut self,
        with_secrets: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let connections = self
            .connections
            .iter()
            .filter(|connection| connection.kind == Kind::Redis && self.in_view(connection))
            .cloned()
            .collect::<Vec<_>>();
        if connections.is_empty() {
            self.transfer_notice = Some("There are no Redis connections here to export.".into());
            cx.notify();
            return;
        }
        let directory = paths::home_dir().clone();
        let path = cx.prompt_for_new_path(&directory, Some("redis-connections.json"));
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(path))) = path.await else {
                return;
            };
            let mut uris = Vec::new();
            let mut notes = Vec::new();
            for connection in &connections {
                let Ok(read) = cx.update(|_, cx| cx.read_credentials(&connection.credential_url()))
                else {
                    return;
                };
                let mut connection = connection.clone();
                // Only an export with passwords carries the SSH one.
                if with_secrets && connection.redis_ssh.is_some() {
                    let url = connection.ssh_credential_url();
                    let Ok(saved) = cx.update(|_, cx| cx.read_credentials(&url)) else {
                        return;
                    };
                    match saved.await {
                        Ok(Some((_, password))) => {
                            if let (Some(ssh), Ok(password)) =
                                (connection.redis_ssh.as_mut(), String::from_utf8(password))
                            {
                                ssh.password =
                                    Some(vilo_database_core::redis_ssh::SshPassword(password));
                            }
                        }
                        Ok(None) => {}
                        Err(error) => notes.push(format!(
                            "{}: its SSH password could not be read ({error:#})",
                            connection.name
                        )),
                    }
                }
                match read.await {
                    Ok(Some((_, uri))) => match String::from_utf8(uri) {
                        Ok(uri) => uris.push((connection, uri)),
                        Err(_) => notes.push(format!(
                            "{}: its connection string is unreadable",
                            connection.name
                        )),
                    },
                    Ok(None) => notes.push(format!(
                        "{}: no connection string is saved",
                        connection.name
                    )),
                    Err(error) => notes.push(format!(
                        "{}: the keychain refused ({error:#})",
                        connection.name
                    )),
                }
            }
            let written = cx
                .background_spawn({
                    let path = path.clone();
                    async move {
                        let items = uris
                            .iter()
                            .map(|(connection, uri)| ToExport {
                                id: &connection.id,
                                name: &connection.name,
                                uri,
                                tls: connection.redis_tls.as_ref(),
                                ssh: connection.redis_ssh.as_ref(),
                            })
                            .collect::<Vec<_>>();
                        let (json, export_notes) = export(&items, with_secrets)?;
                        // Private even without passwords: it lists hosts and users.
                        write_private(&path, &json)?;
                        anyhow::Ok((items.len(), export_notes))
                    }
                })
                .await;
            this.update(cx, |this, cx| {
                this.transfer_notice = Some(match written {
                    Ok((count, export_notes)) => {
                        notes.extend(export_notes);
                        summary(
                            format!(
                                "Exported {count} Redis connection{} to {} ({}).",
                                if count == 1 { "" } else { "s" },
                                path.display(),
                                if with_secrets {
                                    "with passwords and keys; keep the file safe"
                                } else {
                                    "without passwords or keys"
                                }
                            ),
                            &notes,
                        )
                    }
                    Err(error) => format!("Nothing was exported: {error:#}").into(),
                });
                cx.notify();
            })
            .ok();
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_imported_files_are_private_and_used() {
        let dir = std::env::temp_dir().join(format!("vilo-transfer-{}", std::process::id()));
        let path = dir.join("key.pem");
        write_private(&path, "secret").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "secret");
        // An existing readable file is replaced by a private one, not written in place.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let open = dir.join("export.json");
            std::fs::write(&open, "old").unwrap();
            std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o644)).unwrap();
            write_private(&open, "passwords").unwrap();
            let mode = std::fs::metadata(&open).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            assert_eq!(std::fs::read_to_string(&open).unwrap(), "passwords");
        }
        std::fs::remove_dir_all(&dir).ok();

        let notes = (0..10).map(|n| format!("note {n}")).collect::<Vec<_>>();
        let text = summary("Done.".into(), &notes);
        assert!(text.contains("note 7") && !text.contains("note 8") && text.contains("2 more"));
    }
}
