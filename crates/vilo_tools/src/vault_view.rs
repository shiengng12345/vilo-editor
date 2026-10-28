mod compare;
mod import;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow};
use collections::{HashMap, HashSet};
use editor::Editor;
use gpui::{
    App, AsyncApp, ClipboardItem, Entity, FocusHandle, Focusable, Subscription, Task,
    TaskExt as _, Window, prelude::*,
};
use http_client::HttpClient;
use serde::{Deserialize, Serialize};
use ui::{
    Button, ButtonStyle, Checkbox, ContextMenu, IconButton, Indicator, PopoverMenu,
    ToggleState,
    Tooltip, prelude::*,
};
use util::ResultExt as _;

use crate::vault::{
    KvVersion, Mount, SecretRead, TokenInfo, VaultClient, VaultError, VersionEntry,
    VersionState, display_value, normalize_origin,
};

/// Copied values are cleared from the clipboard after this, if still there.
// shortcut: plain clipboard write; clipboard managers may keep a copy. Mark the
// item as concealed once GPUI exposes pasteboard types for it.
const CLIPBOARD_CLEAR_AFTER: Duration = Duration::from_secs(30);

/// A saved Vault: the non-secret part. Its token lives in the keychain, bound to `address`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    #[serde(default)]
    site: String,
    #[serde(default)]
    environment: String,
    /// Normalized origin, e.g. `http://127.0.0.1:8200/`.
    address: String,
    /// The user accepted that this http address sends the token and values in plain text.
    #[serde(default)]
    plaintext_acknowledged: bool,
}

impl Connection {
    fn is_plaintext(&self) -> bool {
        self.address.starts_with("http://")
    }

    fn credential_url(&self) -> String {
        format!("vilo-vault://{}", self.id)
    }

    fn label(&self) -> String {
        [self.site.as_str(), self.environment.as_str(), self.name.as_str()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" · ")
    }
}

/// Checks a token against its Vault, then saves it to the keychain bound to the
/// connection's address and reads it back. Root tokens need `accept_root`.
async fn store_token(
    connection: &Connection,
    token: &str,
    accept_root: bool,
    http: Arc<dyn HttpClient>,
    cx: &AsyncApp,
) -> Result<()> {
    let origin = normalize_origin(&connection.address)?;
    match VaultClient::new(http, origin, token.to_string())
        .lookup_self()
        .await
    {
        Ok(info) if info.is_root() && !accept_root => return Err(anyhow!(RootTokenNeedsConsent)),
        // A token that may not look itself up can still be valid for reads.
        Ok(_) | Err(VaultError::Forbidden) => {}
        Err(error) => return Err(anyhow!(error.to_string())),
    }
    let url = connection.credential_url();
    cx.update(|cx| cx.write_credentials(&url, &connection.address, token.as_bytes()))
        .await
        .context("Vilo could not save the token to the keychain")?;
    let read_back = cx.update(|cx| cx.read_credentials(&url)).await?;
    if read_back.as_ref().is_none_or(|(address, saved)| {
        address != &connection.address || saved.as_slice() != token.as_bytes()
    }) {
        return Err(anyhow!("The keychain did not keep the token."));
    }
    Ok(())
}

/// A client for a saved connection, or why there is none. The token is only
/// used when it was saved for this exact address.
async fn client_for(connection: &Connection, cx: &AsyncApp) -> Result<VaultClient, String> {
    let credential = cx
        .update(|cx| cx.read_credentials(&connection.credential_url()))
        .await
        .map_err(|_| "Could not read the token from the keychain.".to_string())?;
    let token = match credential {
        Some((address, token)) if address == connection.address => {
            String::from_utf8(token).map_err(|_| "The saved token is unreadable.".to_string())?
        }
        _ => return Err("No token saved for this address.".to_string()),
    };
    let origin = normalize_origin(&connection.address).map_err(|error| error.to_string())?;
    Ok(VaultClient::new(cx.update(|cx| cx.http_client()), origin, token)
        .with_backoff(cx.background_executor().clone()))
}

enum Fetch<T> {
    Loading,
    Loaded(T),
    Failed(SharedString),
}

struct Session {
    connection_id: String,
    client: VaultClient,
    /// `None` when the token may not look itself up; reads can still work.
    info: Option<TokenInfo>,
    mounts: Fetch<Vec<Mount>>,
}

struct OpenSecret {
    mount: Mount,
    path: String,
    /// `None` reads the current version.
    version: Option<u64>,
    read: Fetch<SecretRead>,
    versions: Option<Fetch<Vec<VersionEntry>>>,
}

struct ConnectionForm {
    /// The connection being edited, or `None` for a new one.
    editing: Option<String>,
    name: Entity<Editor>,
    site: Entity<Editor>,
    environment: Entity<Editor>,
    address: Entity<Editor>,
    token: Entity<Editor>,
    acknowledge_plaintext: bool,
    accept_root: bool,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

pub struct VaultView {
    focus_handle: FocusHandle,
    path: PathBuf,
    connections: Vec<Connection>,
    session: Option<Session>,
    session_error: Option<SharedString>,
    /// Bumped whenever the connection, token or open secret changes, so late
    /// answers for the previous one are dropped.
    generation: u64,
    folders: HashMap<(String, String), Fetch<Vec<String>>>,
    expanded: HashSet<(String, String)>,
    secret: Option<OpenSecret>,
    revealed: HashSet<String>,
    confirm_reveal_all: bool,
    form: Option<ConnectionForm>,
    collapsed_sites: HashSet<String>,
    import: Option<import::ImportState>,
    compare: Option<compare::CompareState>,
    go_to: Entity<Editor>,
    /// Sequence of the newest connections snapshot written to disk.
    persisted: Arc<Mutex<u64>>,
    persist_sequence: u64,
    tasks: Vec<Task<()>>,
    _subscriptions: Vec<Subscription>,
}

impl VaultView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let path = paths::config_dir().join("vault_connections.json");
        // Entries are read one by one so a single bad entry does not drop the rest.
        let connections = std::fs::read(&path)
            .ok()
            .and_then(|bytes| {
                serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
                    .map_err(|error| log::error!("ignoring unreadable {path:?}: {error}"))
                    .ok()
            })
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                serde_json::from_value::<Connection>(entry)
                    .map_err(|error| log::error!("skipping a Vault connection: {error}"))
                    .ok()
            })
            .collect();
        let go_to = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Go to path, e.g. FPMS-NT-V2/ccms/uat", window, cx);
            editor
        });
        let focus_handle = cx.focus_handle();
        // Revealed values are masked again whenever the page loses focus.
        let subscriptions = vec![cx.on_focus_out(&focus_handle, window, |this, _, _, cx| {
            if !this.revealed.is_empty() {
                this.revealed.clear();
                cx.notify();
            }
        })];
        let mut view = Self {
            focus_handle,
            path,
            connections,
            session: None,
            session_error: None,
            generation: 0,
            folders: HashMap::default(),
            expanded: HashSet::default(),
            secret: None,
            revealed: HashSet::default(),
            confirm_reveal_all: false,
            form: None,
            collapsed_sites: HashSet::default(),
            import: None,
            compare: None,
            go_to,
            persisted: Arc::default(),
            persist_sequence: 0,
            tasks: Vec::new(),
            _subscriptions: subscriptions,
        };
        if view.connections.is_empty() {
            view.open_form(None, window, cx);
        }
        view
    }

    fn persist(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        self.persist_sequence += 1;
        let sequence = self.persist_sequence;
        let persisted = self.persisted.clone();
        let path = self.path.clone();
        let connections = self.connections.clone();
        cx.background_spawn(async move {
            // Writes may finish out of order; an older snapshot never replaces a newer one.
            let mut written = persisted.lock().map_err(|_| anyhow!("connection store poisoned"))?;
            if *written > sequence {
                return Ok(());
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = path.with_extension("json.tmp");
            std::fs::write(&temporary, serde_json::to_vec_pretty(&connections)?)?;
            std::fs::rename(&temporary, &path)?;
            *written = sequence;
            Ok(())
        })
    }

    fn clear_session(&mut self) {
        self.generation += 1;
        self.session = None;
        self.session_error = None;
        self.folders.clear();
        self.expanded.clear();
        self.secret = None;
        self.revealed.clear();
        self.confirm_reveal_all = false;
        self.tasks.clear();
    }

    fn connect(&mut self, id: String, window: &mut Window, cx: &mut Context<Self>) {
        let Some(connection) = self.connections.iter().find(|c| c.id == id).cloned() else {
            return;
        };
        self.clear_session();
        self.form = None;
        self.import = None;
        self.compare = None;
        let generation = self.generation;
        self.session_error = Some("Connecting…".into());
        self.tasks.push(cx.spawn_in(window, async move |this, cx| {
            let result: Result<()> = async {
            let app = cx.update(|_, cx| cx.to_async())?;
            let Ok(client) = client_for(&connection, &app).await else {
                this.update_in(cx, |this, window, cx| {
                    if this.generation == generation {
                        this.open_form(Some(connection.id.clone()), window, cx);
                        if let Some(form) = &mut this.form {
                            form.message = Some("Paste the token for this Vault again.".into());
                        }
                        this.session_error = None;
                        cx.notify();
                    }
                })?;
                return Ok(());
            };
            let info = client.lookup_self().await;
            let info = match info {
                Ok(info) => Some(info),
                Err(VaultError::Forbidden) => None,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        if this.generation == generation {
                            this.session_error = Some(error.to_string().into());
                            cx.notify();
                        }
                    })?;
                    return Ok(());
                }
            };
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.session_error = None;
                this.session = Some(Session {
                    connection_id: connection.id.clone(),
                    client: client.clone(),
                    info,
                    mounts: Fetch::Loading,
                });
                cx.notify();
            })?;
            let mounts = client.kv_mounts().await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some(session) = &mut this.session {
                    session.mounts = match mounts {
                        Ok(mounts) => Fetch::Loaded(mounts),
                        Err(error) => Fetch::Failed(
                            format!("{error} Use “Go to path” to open a secret directly.").into(),
                        ),
                    };
                }
                cx.notify();
            })?;
            Ok(())
            }
            .await;
            result.log_err();
        }));
        cx.notify();
    }

    fn toggle_folder(&mut self, mount: Mount, folder: String, cx: &mut Context<Self>) {
        let key = (mount.path.clone(), folder.clone());
        if self.expanded.remove(&key) {
            cx.notify();
            return;
        }
        self.expanded.insert(key.clone());
        if matches!(self.folders.get(&key), Some(Fetch::Loaded(_))) {
            cx.notify();
            return;
        }
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        self.folders.insert(key.clone(), Fetch::Loading);
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = client.list(&mount, &folder).await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.folders.insert(
                    key,
                    match result {
                        Ok(mut children) => {
                            children.sort_by_key(|child| (!child.ends_with('/'), child.to_lowercase()));
                            Fetch::Loaded(children)
                        }
                        Err(VaultError::Forbidden) => {
                            Fetch::Failed("No permission to list this folder.".into())
                        }
                        Err(error) => Fetch::Failed(error.to_string().into()),
                    },
                );
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_secret(&mut self, mount: Mount, path: String, version: Option<u64>, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let same_secret = self
            .secret
            .as_ref()
            .is_some_and(|secret| secret.mount == mount && secret.path == path);
        let versions = if same_secret {
            self.secret.take().and_then(|secret| secret.versions)
        } else {
            None
        };
        self.generation += 1;
        self.revealed.clear();
        self.confirm_reveal_all = false;
        let generation = self.generation;
        let load_versions = mount.version == KvVersion::V2 && versions.is_none();
        self.secret = Some(OpenSecret {
            mount: mount.clone(),
            path: path.clone(),
            version,
            read: Fetch::Loading,
            versions: if load_versions { Some(Fetch::Loading) } else { versions },
        });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let read = client.read(&mount, &path, version).await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some(secret) = &mut this.secret {
                    secret.read = match read {
                        Ok(read) => Fetch::Loaded(read),
                        Err(VaultError::Forbidden) => {
                            Fetch::Failed("No permission to read this secret.".into())
                        }
                        Err(error) => Fetch::Failed(error.to_string().into()),
                    };
                }
                cx.notify();
            })
            .ok();
            if !load_versions {
                return;
            }
            // History is optional: its failure never hides the current values.
            let versions = client.versions(&mount, &path).await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some(secret) = &mut this.secret {
                    secret.versions = Some(match versions {
                        Ok(versions) => Fetch::Loaded(versions),
                        Err(VaultError::Forbidden) => {
                            Fetch::Failed("No permission to see version history.".into())
                        }
                        Err(error) => Fetch::Failed(error.to_string().into()),
                    });
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn go_to_path(&mut self, cx: &mut Context<Self>) {
        let input = self.go_to.read(cx).text(cx).trim().trim_matches('/').to_string();
        let Some(session) = &self.session else {
            return;
        };
        let known = match &session.mounts {
            Fetch::Loaded(mounts) => mounts.clone(),
            _ => Vec::new(),
        };
        // The longest known mount that prefixes the input wins; otherwise the
        // first segment is the mount and KV v2, the common default.
        let mount = known
            .iter()
            .filter(|mount| input == mount.path || input.starts_with(&format!("{}/", mount.path)))
            .max_by_key(|mount| mount.path.len())
            .cloned()
            .unwrap_or_else(|| Mount {
                path: input.split('/').next().unwrap_or_default().to_string(),
                version: KvVersion::V2,
            });
        let path = input
            .strip_prefix(&mount.path)
            .unwrap_or_default()
            .trim_matches('/')
            .to_string();
        if mount.path.is_empty() || path.is_empty() {
            return;
        }
        self.open_secret(mount, path, None, cx);
    }

    fn copy_value(&mut self, value: String, cx: &mut Context<Self>) {
        cx.write_to_clipboard(ClipboardItem::new_string(value.clone()));
        // Detached so the clear still happens after the page closes.
        cx.spawn(async move |_, cx| {
            cx.background_executor().timer(CLIPBOARD_CLEAR_AFTER).await;
            cx.update(|cx| {
                let still_ours = cx
                    .read_from_clipboard()
                    .and_then(|item| item.text())
                    .is_some_and(|text| text == value);
                if still_ours {
                    cx.write_to_clipboard(ClipboardItem::new_string(String::new()));
                }
            });
        })
        .detach();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        let field = |placeholder: &str, text: Option<&str>, masked: bool, window: &mut Window, cx: &mut Context<Self>| {
            let placeholder = placeholder.to_string();
            let text = text.map(str::to_string);
            cx.new(|cx| {
                let mut editor = Editor::single_line(window, cx);
                editor.set_placeholder_text(&placeholder, window, cx);
                if let Some(text) = text {
                    editor.set_text(text, window, cx);
                }
                if masked {
                    editor.set_masked(true, cx);
                }
                editor
            })
        };
        let name = field("Name, e.g. FPMS UAT", existing.as_ref().map(|c| c.name.as_str()), false, window, cx);
        let site = field("Site, e.g. CP", existing.as_ref().map(|c| c.site.as_str()), false, window, cx);
        let environment = field(
            "Environment, e.g. UAT",
            existing.as_ref().map(|c| c.environment.as_str()),
            false,
            window,
            cx,
        );
        let address = field(
            "Address, e.g. http://127.0.0.1:8200",
            existing.as_ref().map(|c| c.address.as_str()),
            false,
            window,
            cx,
        );
        let token = field(
            if existing.is_some() {
                "Token (leave empty to keep the saved one)"
            } else {
                "Token"
            },
            None,
            true,
            window,
            cx,
        );
        self.import = None;
        self.compare = None;
        self.form = Some(ConnectionForm {
            editing,
            name,
            site,
            environment,
            address,
            token,
            // Asked again on every save, so an edited address never inherits it.
            acknowledge_plaintext: false,
            accept_root: false,
            message: None,
            checking: false,
            _task: None,
        });
        cx.notify();
    }

    fn save_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(form) = &mut self.form else {
            return;
        };
        let name = form.name.read(cx).text(cx).trim().to_string();
        let site = form.site.read(cx).text(cx).trim().to_string();
        let environment = form.environment.read(cx).text(cx).trim().to_string();
        let address_input = form.address.read(cx).text(cx);
        let token_input = form.token.read(cx).text(cx).trim().to_string();
        let origin = match normalize_origin(&address_input) {
            Ok(origin) => origin,
            Err(error) => {
                form.message = Some(error.to_string().into());
                cx.notify();
                return;
            }
        };
        if name.is_empty() {
            form.message = Some("Give the connection a name.".into());
            cx.notify();
            return;
        }
        let existing = form
            .editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        let address_changed = existing
            .as_ref()
            .is_none_or(|existing| existing.address != origin.as_str());
        let plaintext = origin.scheme() == "http";
        if plaintext && !form.acknowledge_plaintext {
            form.message = Some(
                "This address uses http: the token and secret values cross the network \
                 unencrypted. Tick the box below to continue."
                    .into(),
            );
            cx.notify();
            return;
        }
        if token_input.is_empty() && (existing.is_none() || address_changed) {
            form.message = Some("Paste a token for this address.".into());
            cx.notify();
            return;
        }
        let connection = Connection {
            id: existing
                .as_ref()
                .map(|existing| existing.id.clone())
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
            name,
            site,
            environment,
            address: origin.to_string(),
            plaintext_acknowledged: plaintext,
        };
        let accept_root = form.accept_root;
        form.checking = true;
        form.message = Some("Checking the token…".into());
        let http = cx.http_client();
        form._task = Some(cx.spawn_in(window, async move |this, cx| {
            let result: Result<()> = async {
                if !token_input.is_empty() {
                    let app = cx.update(|_, cx| cx.to_async())?;
                    store_token(&connection, &token_input, accept_root, http, &app).await?;
                }
                let persist = this.update(cx, |this, cx| {
                    match this.connections.iter_mut().find(|c| c.id == connection.id) {
                        Some(existing) => *existing = connection.clone(),
                        None => this.connections.push(connection.clone()),
                    }
                    this.persist(cx)
                })?;
                persist.await.context("Vilo could not save the connection")?;
                Ok(())
            }
            .await;
            this.update_in(cx, |this, window, cx| match result {
                Ok(()) => {
                    this.form = None;
                    this.connect(connection.id.clone(), window, cx);
                }
                Err(error) => {
                    if let Some(form) = &mut this.form {
                        form.checking = false;
                        form.message = Some(if error.is::<RootTokenNeedsConsent>() {
                            "This is a root token: it can read and change everything in this \
                             Vault. Prefer a token with a narrower policy, or tick “Use this \
                             root token” to continue."
                                .into()
                        } else {
                            format!("{error:#}").into()
                        });
                    }
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn delete_connection(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(index) = self.connections.iter().position(|c| c.id == id) else {
            return;
        };
        let connection = self.connections.remove(index);
        if self
            .session
            .as_ref()
            .is_some_and(|session| session.connection_id == id)
        {
            self.clear_session();
        }
        self.form = None;
        let persist = self.persist(cx);
        cx.spawn(async move |_, cx| {
            persist.await?;
            cx.update(|cx| cx.delete_credentials(&connection.credential_url()))
                .await
                .context("Vilo could not remove the Vault token from the keychain")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }

    fn active_connection(&self) -> Option<&Connection> {
        let id = &self.session.as_ref()?.connection_id;
        self.connections.iter().find(|c| &c.id == id)
    }
}

#[derive(Debug)]
struct RootTokenNeedsConsent;

impl std::fmt::Display for RootTokenNeedsConsent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("root token needs consent")
    }
}

impl std::error::Error for RootTokenNeedsConsent {}

impl Focusable for VaultView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}


impl VaultView {
    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let active_id = self.session.as_ref().map(|session| session.connection_id.clone());
        let mut ordered = self.connections.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|c| {
            (
                c.site.to_lowercase(),
                c.environment.to_lowercase(),
                c.name.to_lowercase(),
            )
        });
        let mut sites: Vec<(String, Vec<&Connection>)> = Vec::new();
        for connection in ordered {
            match sites.last_mut() {
                Some((site, members)) if *site == connection.site => members.push(connection),
                _ => sites.push((connection.site.clone(), vec![connection])),
            }
        }
        let mut list = v_flex().gap_1();
        for (site, members) in sites {
            let collapsed = self.collapsed_sites.contains(&site);
            let has_active = members
                .iter()
                .any(|c| active_id.as_ref() == Some(&c.id));
            // Connections saved without a site.
            let site_name = if site.is_empty() {
                "Ungrouped".to_string()
            } else {
                site.clone()
            };
            let toggle_site = site.clone();
            let menu_site = site.clone();
            let view = cx.entity().downgrade();
            list = list.child(
                h_flex()
                    .id(SharedString::from(format!("vault-site-{site}")))
                    .group("vault-site")
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_sm()
                    .cursor_pointer()
                    .hover(|style| style.bg(colors.element_hover))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if !this.collapsed_sites.remove(&toggle_site) {
                            this.collapsed_sites.insert(toggle_site.clone());
                        }
                        cx.notify();
                    }))
                    .child(
                        Icon::new(if collapsed {
                            IconName::Folder
                        } else {
                            IconName::FolderOpen
                        })
                        .size(IconSize::Small)
                        .color(if has_active { Color::Accent } else { Color::Muted }),
                    )
                    .child(
                        Label::new(site_name)
                            .weight(if has_active {
                                gpui::FontWeight::BOLD
                            } else {
                                gpui::FontWeight::NORMAL
                            })
                            .truncate(),
                    )
                    .child(div().flex_1())
                    .child(
                        PopoverMenu::new(SharedString::from(format!("vault-site-menu-{site}")))
                            .trigger(
                                IconButton::new(
                                    SharedString::from(format!("vault-site-menu-button-{site}")),
                                    IconName::Ellipsis,
                                )
                                .icon_size(IconSize::Small),
                            )
                            .menu(move |window, cx| {
                                let view = view.clone();
                                let site = menu_site.clone();
                                Some(ContextMenu::build(window, cx, move |menu, _, _| {
                                    let view = view.clone();
                                    menu.entry("Add environment", None, move |window, cx| {
                                        view.update(cx, |this, cx| {
                                            this.open_form(None, window, cx);
                                            if let Some(form) = &this.form {
                                                form.site.update(cx, |editor, cx| {
                                                    editor.set_text(site.clone(), window, cx)
                                                });
                                            }
                                        })
                                        .ok();
                                    })
                                }))
                            }),
                    ),
            );
            if collapsed {
                continue;
            }
            let mut environments = v_flex()
                .ml(px(15.))
                .pl_2()
                .gap_0p5()
                .border_l_1()
                .border_color(colors.border_variant);
            for connection in &members {
                let id = connection.id.clone();
                let is_active = active_id.as_ref() == Some(&connection.id);
                let shares_environment = members
                    .iter()
                    .filter(|other| other.environment == connection.environment)
                    .count()
                    > 1;
                let label = match (connection.environment.is_empty(), shares_environment) {
                    (true, _) => connection.name.clone(),
                    (false, true) => format!("{} - {}", connection.environment, connection.name),
                    (false, false) => connection.environment.clone(),
                };
                let tooltip = format!("{}\n{}", connection.name, connection.address);
                environments = environments.child(
                    h_flex()
                        .id(SharedString::from(format!("vault-connection-{}", connection.id)))
                        .px_2()
                        .py_1()
                        .gap_2()
                        .rounded_md()
                        .cursor_pointer()
                        .when(is_active, |this| this.bg(colors.element_selected))
                        .hover(|style| style.bg(colors.element_hover))
                        .tooltip(Tooltip::text(tooltip))
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.connect(id.clone(), window, cx)
                        }))
                        .child(Indicator::dot().color(environment_color(&connection.environment)))
                        .child(
                            Label::new(label)
                                .weight(if is_active {
                                    gpui::FontWeight::BOLD
                                } else {
                                    gpui::FontWeight::NORMAL
                                })
                                .truncate(),
                        ),
                );
            }
            list = list.child(environments);
        }

        let mut tree = v_flex().gap_0p5();
        if let Some(session) = &self.session {
            match &session.mounts {
                Fetch::Loading => tree = tree.child(Label::new("Loading…").color(Color::Muted)),
                Fetch::Failed(message) => {
                    tree = tree.child(
                        Label::new(message.clone())
                            .size(LabelSize::Small)
                            .color(Color::Warning),
                    )
                }
                Fetch::Loaded(mounts) => {
                    for mount in mounts {
                        tree = self.render_folder(tree, mount, String::new(), 0, cx);
                    }
                }
            }
        }

        let explorer_title = self
            .active_connection()
            .map_or("EXPLORER".to_string(), |connection| {
                format!("EXPLORER · {}", connection.label().to_uppercase())
            });
        v_flex()
            .w(px(280.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(section_header(
                "CONNECTIONS",
                h_flex()
                    .child(
                        IconButton::new("vault-import", IconName::Download)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Import connections"))
                            .on_click(cx.listener(|this, _, window, cx| this.open_import(window, cx))),
                    )
                    .child(
                        IconButton::new("vault-export", IconName::ArrowUpRight)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Export connections (without tokens)"))
                            .on_click(cx.listener(|this, _, _, cx| this.export_connections(cx))),
                    )
                    .child(
                        IconButton::new("vault-add-connection", IconName::Plus)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Add a Vault connection"))
                            .on_click(cx.listener(|this, _, window, cx| this.open_form(None, window, cx))),
                    ),
            ))
            .child(
                v_flex()
                    .id("vault-connections")
                    .max_h(relative(0.45))
                    .overflow_y_scroll()
                    .px_1()
                    .pb_2()
                    .child(list),
            )
            .child(
                div()
                    .border_t_1()
                    .border_color(colors.border_variant)
                    .child(section_header(explorer_title, h_flex())),
            )
            .child(
                v_flex()
                    .id("vault-tree")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .px_1()
                    .pb_2()
                    .child(if self.session.is_some() {
                        tree.into_any_element()
                    } else {
                        div()
                            .px_3()
                            .py_2()
                            .child(
                                Label::new("Select a connection to browse its secrets.")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .into_any_element()
                    }),
            )
    }

    fn render_folder(
        &self,
        mut tree: gpui::Div,
        mount: &Mount,
        folder: String,
        depth: usize,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        let colors = cx.theme().colors().clone();
        let key = (mount.path.clone(), folder.clone());
        let expanded = self.expanded.contains(&key);
        let label = if folder.is_empty() {
            format!("{}/", mount.path)
        } else {
            folder
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .map(|name| format!("{name}/"))
                .unwrap_or_default()
        };
        let row_id = SharedString::from(format!("vault-folder-{}-{}", mount.path, folder));
        tree = tree.child({
            let row_mount = mount.clone();
            let folder = folder.clone();
            h_flex()
                .id(row_id)
                .pl(px(8. + depth as f32 * 14.))
                .py_0p5()
                .gap_1()
                .rounded_sm()
                .cursor_pointer()
                .hover(|style| style.bg(colors.element_hover))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.toggle_folder(row_mount.clone(), folder.clone(), cx)
                }))
                .child(
                    Icon::new(if expanded {
                        IconName::ChevronDown
                    } else {
                        IconName::ChevronRight
                    })
                    .size(IconSize::XSmall)
                    .color(Color::Muted),
                )
                .child(
                    Icon::new(if depth == 0 {
                        IconName::Server
                    } else if expanded {
                        IconName::FolderOpen
                    } else {
                        IconName::Folder
                    })
                    .size(IconSize::XSmall)
                    .color(Color::Muted),
                )
                .child(Label::new(label).size(LabelSize::Small).truncate())
                .when(depth == 0, |this| {
                    this.child(
                        Label::new(match mount.version {
                            KvVersion::V1 => "kv v1",
                            KvVersion::V2 => "kv v2",
                        })
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                    )
                })
        });
        if !expanded {
            return tree;
        }
        match self.folders.get(&key) {
            None | Some(Fetch::Loading) => {
                tree.child(
                    div()
                        .pl(px(22. + depth as f32 * 14.))
                        .child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted)),
                )
            }
            Some(Fetch::Failed(message)) => tree.child(
                div().pl(px(22. + depth as f32 * 14.)).child(
                    Label::new(message.clone())
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                ),
            ),
            Some(Fetch::Loaded(children)) if children.is_empty() => tree.child(
                div()
                    .pl(px(22. + depth as f32 * 14.))
                    .child(Label::new("Empty").size(LabelSize::Small).color(Color::Muted)),
            ),
            Some(Fetch::Loaded(children)) => {
                for child in children {
                    let child_path = format!("{folder}{child}");
                    if child.ends_with('/') {
                        tree = self.render_folder(tree, mount, child_path, depth + 1, cx);
                    } else {
                        let is_open = self.secret.as_ref().is_some_and(|secret| {
                            secret.mount == *mount && secret.path == child_path
                        });
                        let mount = mount.clone();
                        tree = tree.child(
                            h_flex()
                                .id(SharedString::from(format!(
                                    "vault-secret-{}-{}",
                                    mount.path, child_path
                                )))
                                .pl(px(22. + (depth + 1) as f32 * 14.))
                                .py_0p5()
                                .gap_1()
                                .rounded_sm()
                                .cursor_pointer()
                                .when(is_open, |this| this.bg(colors.element_selected))
                                .hover(|style| style.bg(colors.element_hover))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.open_secret(mount.clone(), child_path.clone(), None, cx)
                                }))
                                .child(
                                    Icon::new(IconName::Lock)
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                )
                                .child(Label::new(child.clone()).size(LabelSize::Small)),
                        );
                    }
                }
                tree
            }
        }
    }

    fn render_form(&self, form: &ConnectionForm, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors();
        let input = |label: &'static str, editor: &Entity<Editor>| {
            v_flex()
                .gap_1()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                .child(
                    div()
                        .px_2()
                        .py_1()
                        .rounded_sm()
                        .border_1()
                        .border_color(colors.border)
                        .bg(colors.editor_background)
                        .child(editor.clone()),
                )
        };
        let address = form.address.read(cx).text(cx);
        let plaintext = address.trim_start().starts_with("http://");
        let root_rejected = form
            .message
            .as_ref()
            .is_some_and(|message| message.contains("root token"));
        let editing = form.editing.clone();
        v_flex()
            .max_w(px(520.))
            .gap_3()
            .child(
                Headline::new(if form.editing.is_some() {
                    "Edit Vault connection"
                } else {
                    "Add a Vault connection"
                })
                .size(HeadlineSize::Small),
            )
            .child(input("Name", &form.name))
            .child(input("Site", &form.site))
            .child(input("Environment", &form.environment))
            .child(input("Address", &form.address))
            .child(input("Token", &form.token))
            .child(
                Label::new(
                    "The token is kept in the macOS keychain and only sent to this address. \
                     Changing the address asks for the token again.",
                )
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .when(plaintext, |this| {
                this.child(
                    Checkbox::new(
                        "vault-acknowledge-plaintext",
                        ToggleState::from(form.acknowledge_plaintext),
                    )
                    .label("I understand this http address sends the token and secret values unencrypted.")
                    .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                        if let Some(form) = &mut this.form {
                            form.acknowledge_plaintext = state.selected();
                        }
                        cx.notify();
                    })),
                )
            })
            .when(root_rejected || form.accept_root, |this| {
                this.child(
                    Checkbox::new("vault-accept-root", ToggleState::from(form.accept_root))
                        .label("Use this root token")
                        .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                            if let Some(form) = &mut this.form {
                                form.accept_root = state.selected();
                            }
                            cx.notify();
                        })),
                )
            })
            .when_some(form.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(
                    if form.checking {
                        Color::Muted
                    } else {
                        Color::Warning
                    },
                ))
            })
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("vault-save-connection", "Save and connect")
                            .style(ButtonStyle::Filled)
                            .disabled(form.checking)
                            .on_click(cx.listener(|this, _, window, cx| this.save_form(window, cx))),
                    )
                    .when(!self.connections.is_empty(), |this| {
                        this.child(
                            Button::new("vault-cancel-form", "Cancel").on_click(cx.listener(
                                |this, _, _, cx| {
                                    this.form = None;
                                    cx.notify();
                                },
                            )),
                        )
                    })
                    .when_some(editing, |this, id| {
                        this.child(
                            Button::new("vault-delete-connection", "Delete connection")
                                .style(ButtonStyle::Subtle)
                                .color(Color::Error)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.delete_connection(id.clone(), cx)
                                })),
                        )
                    }),
            )
    }

    fn render_secret(&self, secret: &OpenSecret, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors();
        let title = format!("{}/{}", secret.mount.path, secret.path);
        let mut header = h_flex().gap_2();
        if let Some(connection_id) = self.session.as_ref().map(|session| session.connection_id.clone()) {
            header = header.child(
                Button::new("vault-compare-secret", "Compare…")
                    .label_size(LabelSize::Small)
                    .start_icon(Icon::new(IconName::Diff).size(IconSize::Small))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_compare(Some((connection_id.clone(), title.clone())), window, cx)
                    })),
            );
        }
        if let Some(Fetch::Loaded(versions)) = &secret.versions
            && !versions.is_empty()
        {
            let current = match &secret.read {
                Fetch::Loaded(SecretRead::Found(found)) => found.version,
                Fetch::Loaded(SecretRead::Unavailable { version, .. }) => Some(*version),
                _ => secret.version,
            };
            let versions = versions.clone();
            let mount = secret.mount.clone();
            let path = secret.path.clone();
            let view = cx.entity().downgrade();
            header = header.child(
                PopoverMenu::new("vault-version-menu")
                    .trigger(
                        Button::new(
                            "vault-version",
                            current.map_or("Version".to_string(), |version| format!("Version {version}")),
                        )
                        .label_size(LabelSize::Small)
                        .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                    )
                    .menu(move |window, cx| {
                        let versions = versions.clone();
                        let mount = mount.clone();
                        let path = path.clone();
                        let view = view.clone();
                        Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                            for entry in &versions {
                                let state = match entry.state {
                                    VersionState::Live => "",
                                    VersionState::Deleted => " · deleted",
                                    VersionState::Destroyed => " · destroyed",
                                };
                                let label = format!(
                                    "Version {} · {}{state}",
                                    entry.version,
                                    entry.created_time.get(..10).unwrap_or(&entry.created_time)
                                );
                                let version = entry.version;
                                let mount = mount.clone();
                                let path = path.clone();
                                let view = view.clone();
                                menu = menu.entry(label, None, move |_, cx| {
                                    view.update(cx, |this, cx| {
                                        this.open_secret(mount.clone(), path.clone(), Some(version), cx)
                                    })
                                    .ok();
                                });
                            }
                            menu
                        }))
                    }),
            );
        }
        if let Some(Fetch::Failed(message)) = &secret.versions {
            header = header.child(Label::new(message.clone()).size(LabelSize::XSmall).color(Color::Muted));
        }

        let body = match &secret.read {
            Fetch::Loading => Label::new("Loading…").color(Color::Muted).into_any_element(),
            Fetch::Failed(message) => Label::new(message.clone()).color(Color::Warning).into_any_element(),
            Fetch::Loaded(SecretRead::Unavailable { version, state }) => Label::new(match state {
                VersionState::Destroyed => format!("Version {version} was destroyed; its values are gone."),
                _ => format!("Version {version} was deleted. Its values can be restored in Vault."),
            })
            .color(Color::Warning)
            .into_any_element(),
            Fetch::Loaded(SecretRead::Found(found)) => {
                let all_revealed = !found.values.is_empty()
                    && found.values.keys().all(|key| self.revealed.contains(key));
                let toolbar = h_flex()
                    .gap_2()
                    .child(
                        Label::new(format!("{} keys", found.values.len()))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .when_some(found.created_time.clone(), |this, time| {
                        this.child(
                            Label::new(format!("updated {}", time.get(..19).unwrap_or(&time).replace('T', " ")))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .child(div().flex_1())
                    .child(
                        Button::new(
                            "vault-reveal-all",
                            if all_revealed {
                                "Hide all"
                            } else if self.confirm_reveal_all {
                                "Click again to show every value"
                            } else {
                                "Show all"
                            },
                        )
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            let keys = match this.secret.as_ref().map(|secret| &secret.read) {
                                Some(Fetch::Loaded(SecretRead::Found(found))) => {
                                    found.values.keys().cloned().collect::<Vec<_>>()
                                }
                                _ => return,
                            };
                            if all_revealed {
                                this.revealed.clear();
                            } else if this.confirm_reveal_all {
                                this.revealed.extend(keys);
                                this.confirm_reveal_all = false;
                            } else {
                                this.confirm_reveal_all = true;
                            }
                            cx.notify();
                        })),
                    );
                let mut rows = v_flex()
                    .id("vault-values")
                    .flex_1()
                    .overflow_y_scroll()
                    .border_1()
                    .border_color(colors.border)
                    .rounded_md();
                for (index, (key, value)) in found.values.iter().enumerate() {
                    let revealed = self.revealed.contains(key);
                    let text = display_value(value);
                    let shown = if revealed {
                        text.clone()
                    } else {
                        "••••••••".to_string()
                    };
                    let reveal_key = key.clone();
                    rows = rows.child(
                        h_flex()
                            .px_3()
                            .py_1p5()
                            .gap_3()
                            .when(index > 0, |this| this.border_t_1().border_color(colors.border_variant))
                            .child(
                                div()
                                    .w(px(280.))
                                    .flex_none()
                                    .child(Label::new(key.clone()).size(LabelSize::Small).buffer_font(cx)),
                            )
                            .child(
                                div().flex_1().min_w_0().child(
                                    Label::new(shown)
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .color(if revealed { Color::Default } else { Color::Muted })
                                        .truncate(),
                                ),
                            )
                            .child(
                                IconButton::new(
                                    SharedString::from(format!("vault-reveal-{key}")),
                                    if revealed { IconName::EyeOff } else { IconName::Eye },
                                )
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text(if revealed { "Hide value" } else { "Show value" }))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if !this.revealed.remove(&reveal_key) {
                                        this.revealed.insert(reveal_key.clone());
                                    }
                                    cx.notify();
                                })),
                            )
                            .child(
                                IconButton::new(SharedString::from(format!("vault-copy-{key}")), IconName::Copy)
                                    .icon_size(IconSize::Small)
                                    .tooltip(Tooltip::text("Copy value (cleared from the clipboard after 30 s)"))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.copy_value(text.clone(), cx)
                                    })),
                            ),
                    );
                }
                v_flex().flex_1().min_h_0().gap_2().child(toolbar).child(rows).into_any_element()
            }
        };
        v_flex().flex_1().min_h_0().gap_3().child(header).child(body).into_any_element()
    }
}

impl Render for VaultView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let connection = self.active_connection().cloned();
        let info = self.session.as_ref().and_then(|session| session.info.clone());

        let editing_elsewhere = self.form.is_some() || self.import.is_some() || self.compare.is_some();

        let header = h_flex()
            .px_4()
            .py_2p5()
            .gap_4()
            .border_b_1()
            .border_color(colors.border_variant)
            .child(match &connection {
                Some(connection) => v_flex()
                    .flex_1()
                    .min_w_0()
                    .gap_0p5()
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Indicator::dot().color(environment_color(&connection.environment)))
                            .child(
                                Label::new(connection.name.clone())
                                    .size(LabelSize::Large)
                                    .weight(gpui::FontWeight::SEMIBOLD)
                                    .truncate(),
                            ),
                    )
                    .child(
                        div()
                            .id("vault-endpoint")
                            .tooltip(Tooltip::text(connection.address.clone()))
                            .child(
                                Label::new(connection.address.clone())
                                    .size(LabelSize::Small)
                                    .color(Color::Muted)
                                    .buffer_font(cx)
                                    .truncate(),
                            ),
                    )
                    .child(
                        Label::new(match &info {
                            Some(info) => format!(
                                "Token {} · {}",
                                info.display_name,
                                if info.ttl == 0 {
                                    "never expires".to_string()
                                } else {
                                    format!("expires in {}", format_ttl(info.ttl))
                                }
                            ),
                            None if self.session.is_some() => {
                                "Token details are not visible to this token".to_string()
                            }
                            None => String::new(),
                        })
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                    )
                    .into_any_element(),
                None => v_flex()
                    .flex_1()
                    .child(Label::new("Vault").size(LabelSize::Large).weight(gpui::FontWeight::SEMIBOLD))
                    .child(
                        Label::new("No connection selected")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .into_any_element(),
            })
            .child(
                h_flex()
                    .flex_none()
                    .gap_1()
                    .child(
                        Button::new("vault-compare", "Compare")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::Diff).size(IconSize::Small))
                            .on_click(cx.listener(|this, _, window, cx| this.open_compare(None, window, cx))),
                    )
                    .when_some(connection.as_ref().map(|c| c.id.clone()), |this, id| {
                        this.child(
                            Button::new("vault-edit-connection", "Edit connection")
                                .label_size(LabelSize::Small)
                                .start_icon(Icon::new(IconName::Pencil).size(IconSize::Small))
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.open_form(Some(id.clone()), window, cx)
                                })),
                        )
                    }),
            );

        let mut notices = Vec::new();
        if connection.as_ref().is_some_and(Connection::is_plaintext) {
            notices.push(security_notice(
                "notice-plaintext",
                Severity::Warning,
                "Unencrypted connection",
                "The token and every secret value cross the network in plain text over http.",
                cx,
            ));
        }
        if info.as_ref().is_some_and(TokenInfo::is_root) {
            notices.push(security_notice(
                "notice-root",
                Severity::Error,
                "Root token",
                "This session has full control of this Vault. Prefer a token scoped to the paths you read.",
                cx,
            ));
        }

        let path_bar = (self.session.is_some() && !editing_elsewhere).then(|| {
            let mut crumbs = h_flex().flex_1().min_w_0().gap_1();
            match &self.secret {
                Some(secret) => {
                    let segments = std::iter::once(secret.mount.path.clone())
                        .chain(secret.path.split('/').map(str::to_string))
                        .collect::<Vec<_>>();
                    let last = segments.len().saturating_sub(1);
                    for (index, segment) in segments.into_iter().enumerate() {
                        if index > 0 {
                            crumbs = crumbs.child(
                                Icon::new(IconName::ChevronRight)
                                    .size(IconSize::XSmall)
                                    .color(Color::Muted),
                            );
                        }
                        crumbs = crumbs.child(
                            Label::new(segment)
                                .size(LabelSize::Small)
                                .buffer_font(cx)
                                .color(if index == last { Color::Default } else { Color::Muted }),
                        );
                    }
                }
                None => {
                    crumbs = crumbs.child(
                        Label::new("No secret open")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                }
            }
            h_flex()
                .px_4()
                .py_1()
                .gap_3()
                .border_b_1()
                .border_color(colors.border_variant)
                .bg(colors.toolbar_background)
                .child(crumbs)
                .child(
                    h_flex()
                        .w(px(320.))
                        .flex_none()
                        .gap_1()
                        .child(
                            div()
                                .flex_1()
                                .px_2()
                                .py_0p5()
                                .rounded_sm()
                                .border_1()
                                .border_color(colors.border)
                                .bg(colors.editor_background)
                                .child(self.go_to.clone()),
                        )
                        .child(
                            Button::new("vault-go", "Open")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, _, cx| this.go_to_path(cx))),
                        ),
                )
        });

        let main = if let Some(form) = &self.form {
            self.render_form(form, cx).into_any_element()
        } else if let Some(import) = &self.import {
            self.render_import(import, cx)
        } else if let Some(compare) = &self.compare {
            self.render_compare(compare, cx)
        } else if let Some(message) = &self.session_error {
            empty_state(IconName::Server, message.clone(), None)
        } else if let Some(secret) = &self.secret {
            self.render_secret(secret, cx)
        } else if self.session.is_some() {
            empty_state(
                IconName::Lock,
                "No secret open".into(),
                Some("Pick a secret in the explorer, or type its path in the bar above.".into()),
            )
        } else if self.connections.is_empty() {
            empty_state(
                IconName::Server,
                "No connections yet".into(),
                Some("Add a Vault connection, or import several from a JSON file.".into()),
            )
        } else {
            empty_state(
                IconName::Server,
                "No connection selected".into(),
                Some("Choose a connection on the left to browse its secrets.".into()),
            )
        };

        h_flex()
            .key_context("VaultView")
            .track_focus(&self.focus_handle)
            .size_full()
            .items_start()
            .child(self.render_sidebar(cx))
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .bg(colors.editor_background)
                    .child(header)
                    .when(!notices.is_empty(), |this| {
                        this.child(
                            v_flex()
                                .px_4()
                                .py_2()
                                .gap_1()
                                .border_b_1()
                                .border_color(colors.border_variant)
                                .children(notices),
                        )
                    })
                    .children(path_bar)
                    .child(
                        v_flex()
                            .flex_1()
                            .min_h_0()
                            .p_4()
                            .gap_3()
                            .child(main),
                    ),
            )
    }
}

#[derive(Clone, Copy)]
enum Severity {
    Warning,
    Error,
}

/// A security notice: always visible while it applies, never dismissible.
fn security_notice(
    id: &'static str,
    severity: Severity,
    title: &'static str,
    consequence: &'static str,
    cx: &App,
) -> AnyElement {
    let status = cx.theme().status();
    let (color, background, border) = match severity {
        Severity::Warning => (Color::Warning, status.warning_background, status.warning),
        Severity::Error => (Color::Error, status.error_background, status.error),
    };
    h_flex()
        .id(id)
        .items_start()
        .gap_2()
        .px_2()
        .py_1()
        .rounded_sm()
        .border_l_2()
        .border_color(border)
        .bg(background)
        .child(
            div()
                .pt(px(2.))
                .child(Icon::new(IconName::Warning).size(IconSize::Small).color(color)),
        )
        .child(
            h_flex()
                .flex_1()
                .min_w_0()
                .flex_wrap()
                .gap_x_1p5()
                .child(Label::new(title).size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD).color(color))
                .child(Label::new(consequence).size(LabelSize::Small)),
        )
        .into_any_element()
}

fn empty_state(icon: IconName, title: SharedString, hint: Option<SharedString>) -> AnyElement {
    v_flex()
        .flex_1()
        .size_full()
        .items_center()
        .justify_center()
        .gap_2()
        .child(Icon::new(icon).size(IconSize::XLarge).color(Color::Muted))
        .child(Label::new(title).color(Color::Muted))
        .when_some(hint, |this, hint| {
            this.child(Label::new(hint).size(LabelSize::Small).color(Color::Muted))
        })
        .into_any_element()
}

/// A VS Code style section header for the explorer side bar.
fn section_header(title: impl Into<SharedString>, actions: impl IntoElement) -> impl IntoElement {
    h_flex()
        .h(px(26.))
        .pl_3()
        .pr_1()
        .justify_between()
        .child(
            Label::new(title)
                .size(LabelSize::XSmall)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(Color::Muted)
                .truncate(),
        )
        .child(actions)
}

/// QA-like environments orange, UAT blue, production red, as in the site list sample.
fn environment_color(environment: &str) -> Color {
    let environment = environment.to_lowercase();
    if environment.contains("prod") || environment == "live" {
        Color::Error
    } else if environment.contains("uat") {
        Color::Accent
    } else if environment.is_empty() {
        Color::Muted
    } else {
        Color::Warning
    }
}

fn format_ttl(seconds: u64) -> String {
    match seconds {
        seconds if seconds >= 86_400 => format!("{} d", seconds / 86_400),
        seconds if seconds >= 3_600 => format!("{} h", seconds / 3_600),
        seconds => format!("{} min", (seconds / 60).max(1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_ttl() {
        assert_eq!(format_ttl(30), "1 min");
        assert_eq!(format_ttl(7_200), "2 h");
        assert_eq!(format_ttl(3 * 86_400), "3 d");
    }
}
