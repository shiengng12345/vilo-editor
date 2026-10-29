use std::{path::PathBuf, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use editor::Editor;
use gpui::{App, AsyncApp, Entity, FocusHandle, Focusable, Subscription, Task, TaskExt as _, Window, prelude::*};
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, IconButton, Indicator, Tooltip, prelude::*};
use util::ResultExt as _;

use crate::argo::{AppDetail, AppSummary, ArgoClient, ArgoError, AuthMode, Event, Resource, normalize_base};

/// How often an open application is re-read while the page is in view.
const DETAIL_POLL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    #[serde(default)]
    site: String,
    #[serde(default)]
    environment: String,
    /// Normalized base URL.
    address: String,
    username: String,
    mode: AuthMode,
    /// Only these projects are listed; empty lists every project.
    #[serde(default)]
    projects: Vec<String>,
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-argo://{}", self.id)
    }

    /// The secret is used only for this address, user and kind of secret.
    fn credential_owner(&self) -> String {
        format!("{}|{}|{:?}", self.address, self.username, self.mode)
    }

    fn label(&self) -> String {
        [self.site.as_str(), self.environment.as_str(), self.name.as_str()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" · ")
    }
}

enum Fetch<T> {
    Loading,
    Loaded(T),
    Failed(SharedString),
}

struct Session {
    connection_id: String,
    client: ArgoClient,
    version: String,
}

enum RefreshState {
    Idle,
    Pending,
    /// Argo accepted or may have accepted it; the status shows when it is done.
    Requested(SharedString),
}

struct Detail {
    namespace: String,
    name: String,
    app: Fetch<AppDetail>,
    resources: Fetch<Vec<Resource>>,
    events: Fetch<Vec<Event>>,
    refresh: RefreshState,
    confirm_hard: bool,
    _poll: Task<()>,
}

struct Form {
    editing: Option<String>,
    name: Entity<Editor>,
    site: Entity<Editor>,
    environment: Entity<Editor>,
    address: Entity<Editor>,
    username: Entity<Editor>,
    secret: Entity<Editor>,
    projects: Entity<Editor>,
    mode: AuthMode,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

pub struct ArgoView {
    focus_handle: FocusHandle,
    path: PathBuf,
    connections: Vec<Connection>,
    session: Option<Session>,
    session_error: Option<SharedString>,
    /// Bumped on every connection change; late answers for an older one are dropped.
    generation: u64,
    apps: Fetch<Vec<AppSummary>>,
    filter: Entity<Editor>,
    detail: Option<Detail>,
    /// Polling only runs while the page is in view.
    visible: bool,
    form: Option<Form>,
    tasks: Vec<Task<()>>,
    _subscriptions: Vec<Subscription>,
}

fn editor(placeholder: &str, text: &str, masked: bool, window: &mut Window, cx: &mut App) -> Entity<Editor> {
    let placeholder = placeholder.to_string();
    let text = text.to_string();
    cx.new(|cx| {
        let mut editor = Editor::single_line(window, cx);
        editor.set_placeholder_text(&placeholder, window, cx);
        if !text.is_empty() {
            editor.set_text(text, window, cx);
        }
        if masked {
            editor.set_masked(true, cx);
        }
        editor
    })
}

async fn client_for(connection: &Connection, cx: &AsyncApp) -> Result<ArgoClient, String> {
    let credential = cx
        .update(|cx| cx.read_credentials(&connection.credential_url()))
        .await
        .map_err(|_| "Could not read the secret from the keychain.".to_string())?;
    let secret = match credential {
        Some((owner, secret)) if owner == connection.credential_owner() => {
            String::from_utf8(secret).map_err(|_| "The saved secret is unreadable.".to_string())?
        }
        _ => return Err("No secret saved for this address and user. Edit the connection.".to_string()),
    };
    let base = normalize_base(&connection.address).map_err(|error| error.to_string())?;
    Ok(ArgoClient::new(cx.update(|cx| cx.http_client()), base, &connection.username, connection.mode, &secret))
}

fn fetched<T>(result: Result<T, ArgoError>) -> Fetch<T> {
    match result {
        Ok(value) => Fetch::Loaded(value),
        Err(error) => Fetch::Failed(error.to_string().into()),
    }
}

fn health_color(health: &str) -> Color {
    match health {
        "Healthy" => Color::Success,
        "Progressing" => Color::Accent,
        "Degraded" => Color::Error,
        "Missing" => Color::Warning,
        _ => Color::Muted,
    }
}

fn sync_color(sync: &str) -> Color {
    match sync {
        "Synced" => Color::Success,
        "OutOfSync" => Color::Warning,
        _ => Color::Muted,
    }
}

impl ArgoView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let path = paths::config_dir().join("argo_connections.json");
        let connections = std::fs::read(&path)
            .ok()
            .and_then(|bytes| {
                serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
                    .map_err(|error| log::error!("ignoring unreadable {path:?}: {error}"))
                    .ok()
            })
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| serde_json::from_value::<Connection>(entry).log_err())
            .collect::<Vec<_>>();
        let focus_handle = cx.focus_handle();
        let filter = editor("Filter applications", "", false, window, cx);
        let subscriptions = vec![
            cx.on_focus_in(&focus_handle, window, |this, _, cx| {
                this.visible = true;
                cx.notify();
            }),
            cx.on_focus_out(&focus_handle, window, |this, _, _, cx| {
                this.visible = false;
                cx.notify();
            }),
            cx.subscribe(&filter, |_, _, _: &editor::EditorEvent, cx| cx.notify()),
        ];
        let mut view = Self {
            focus_handle,
            path,
            connections,
            session: None,
            session_error: None,
            generation: 0,
            apps: Fetch::Loaded(Vec::new()),
            filter,
            detail: None,
            visible: true,
            form: None,
            tasks: Vec::new(),
            _subscriptions: subscriptions,
        };
        if view.connections.is_empty() {
            view.open_form(None, window, cx);
        }
        view
    }

    fn persist(&self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let path = self.path.clone();
        let connections = self.connections.clone();
        // shortcut: saves are rare and user-driven, so two in flight at once are not ordered.
        cx.background_spawn(async move {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = path.with_extension("json.tmp");
            std::fs::write(&temporary, serde_json::to_vec_pretty(&connections)?)?;
            std::fs::rename(&temporary, &path)?;
            Ok(())
        })
    }

    fn connect(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(connection) = self.connections.iter().find(|c| c.id == id).cloned() else {
            return;
        };
        self.generation += 1;
        let generation = self.generation;
        self.session = None;
        self.detail = None;
        self.form = None;
        self.apps = Fetch::Loading;
        self.tasks.clear();
        self.session_error = Some("Connecting…".into());
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = async {
                let client = client_for(&connection, cx).await.map_err(|error| anyhow!(error))?;
                let version = client.verify().await?;
                anyhow::Ok((client, version))
            }
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                match result {
                    Ok((client, version)) => {
                        this.session_error = None;
                        this.session = Some(Session { connection_id: connection.id.clone(), client, version });
                        this.load_apps(cx);
                    }
                    Err(error) => this.session_error = Some(error.to_string().into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn load_apps(&mut self, cx: &mut Context<Self>) {
        let Some(session) = &self.session else {
            return;
        };
        let client = session.client.clone();
        let projects = self
            .connections
            .iter()
            .find(|c| c.id == session.connection_id)
            .map(|c| c.projects.clone())
            .unwrap_or_default();
        let generation = self.generation;
        self.apps = Fetch::Loading;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = client.applications(&projects).await;
            this.update(cx, |this, cx| {
                if this.generation == generation {
                    this.apps = match result {
                        Ok(mut apps) => {
                            apps.sort_by(|a, b| (&a.project, &a.name).cmp(&(&b.project, &b.name)));
                            Fetch::Loaded(apps)
                        }
                        Err(error) => Fetch::Failed(error.to_string().into()),
                    };
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_app(&mut self, namespace: String, name: String, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let generation = self.generation;
        let (poll_namespace, poll_name) = (namespace.clone(), name.clone());
        // Re-read while open and in view. Never sends a refresh.
        let poll = cx.spawn(async move |this, cx| {
            loop {
                let visible = this.read_with(cx, |this, _| this.visible).unwrap_or(false);
                if visible {
                    let app = client.application(&poll_namespace, &poll_name).await;
                    let resources = client.resources(&poll_namespace, &poll_name).await;
                    let events = client.events(&poll_namespace, &poll_name).await;
                    let unauthorized = [app.as_ref().err(), resources.as_ref().err(), events.as_ref().err()]
                        .into_iter()
                        .flatten()
                        .any(|error| *error == ArgoError::Unauthorized);
                    let applied = this.update(cx, |this, cx| {
                        let Some(detail) = this
                            .detail
                            .as_mut()
                            .filter(|detail| this.generation == generation && detail.namespace == poll_namespace && detail.name == poll_name)
                        else {
                            return false;
                        };
                        if let Ok(app) = &app
                            && let Fetch::Loaded(apps) = &mut this.apps
                            && let Some(row) = apps.iter_mut().find(|row| row.namespace == poll_namespace && row.name == poll_name)
                        {
                            *row = app.summary.clone();
                        }
                        if let (Ok(app), RefreshState::Requested(_)) = (&app, &detail.refresh)
                            && app.summary.operation.as_deref() != Some("Running")
                        {
                            detail.refresh = RefreshState::Idle;
                        }
                        // Missing rights on one part leave the rest usable.
                        detail.app = fetched(app);
                        detail.resources = fetched(resources);
                        detail.events = fetched(events);
                        cx.notify();
                        true
                    });
                    if !matches!(applied, Ok(true)) || unauthorized {
                        return;
                    }
                }
                cx.background_executor().timer(DETAIL_POLL).await;
            }
        });
        self.detail = Some(Detail {
            namespace,
            name,
            app: Fetch::Loading,
            resources: Fetch::Loading,
            events: Fetch::Loading,
            refresh: RefreshState::Idle,
            confirm_hard: false,
            _poll: poll,
        });
        cx.notify();
    }

    fn refresh_app(&mut self, hard: bool, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let Some(detail) = &mut self.detail else {
            return;
        };
        if matches!(detail.refresh, RefreshState::Pending) {
            return;
        }
        if hard && !detail.confirm_hard {
            detail.confirm_hard = true;
            cx.notify();
            return;
        }
        detail.confirm_hard = false;
        detail.refresh = RefreshState::Pending;
        let (namespace, name) = (detail.namespace.clone(), detail.name.clone());
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = client.refresh(&namespace, &name, hard).await;
            this.update(cx, |this, cx| {
                if let Some(detail) = this
                    .detail
                    .as_mut()
                    .filter(|detail| this.generation == generation && detail.namespace == namespace && detail.name == name)
                {
                    detail.refresh = RefreshState::Requested(match result {
                        Ok(()) => "Refresh requested; status updates here when Argo is done.".into(),
                        // Argo may have started it before the answer was lost; do not send it again.
                        Err(ArgoError::Network(_)) | Err(ArgoError::Status(504)) => {
                            "Refresh sent, but Argo did not confirm it. Watch the status before refreshing again.".into()
                        }
                        Err(error) => error.to_string().into(),
                    });
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        let value = |pick: fn(&Connection) -> String| existing.as_ref().map(pick).unwrap_or_default();
        self.form = Some(Form {
            name: editor("Name, e.g. CP UAT", &value(|c| c.name.clone()), false, window, cx),
            site: editor("Site, e.g. CP", &value(|c| c.site.clone()), false, window, cx),
            environment: editor("Environment, e.g. UAT", &value(|c| c.environment.clone()), false, window, cx),
            address: editor("https://argocd.example.com/", &value(|c| c.address.clone()), false, window, cx),
            username: editor("Username", &value(|c| c.username.clone()), false, window, cx),
            secret: editor(
                if existing.is_some() { "Leave empty to keep the saved one" } else { "Password or API token" },
                "",
                true,
                window,
                cx,
            ),
            projects: editor("Projects to list, comma separated (empty lists all)", &value(|c| c.projects.join(", ")), false, window, cx),
            mode: existing.as_ref().map_or(AuthMode::Password, |c| c.mode),
            editing,
            message: None,
            checking: false,
            _task: None,
        });
        cx.notify();
    }

    fn save_form(&mut self, cx: &mut Context<Self>) {
        let Some(form) = &mut self.form else {
            return;
        };
        let text = |editor: &Entity<Editor>, cx: &App| editor.read(cx).text(cx).trim().to_string();
        let (name, site, environment, username, secret) = (
            text(&form.name, cx),
            text(&form.site, cx),
            text(&form.environment, cx),
            text(&form.username, cx),
            text(&form.secret, cx),
        );
        let projects = text(&form.projects, cx)
            .split(',')
            .map(|project| project.trim().to_string())
            .filter(|project| !project.is_empty())
            .collect::<Vec<_>>();
        let base = match normalize_base(&form.address.read(cx).text(cx)) {
            Ok(base) => base,
            Err(error) => {
                form.message = Some(error.to_string().into());
                cx.notify();
                return;
            }
        };
        if name.is_empty() || username.is_empty() {
            form.message = Some("Fill in a name and a username.".into());
            cx.notify();
            return;
        }
        let existing = form.editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        let connection = Connection {
            id: existing.as_ref().map_or_else(|| uuid::Uuid::new_v4().simple().to_string(), |c| c.id.clone()),
            name,
            site,
            environment,
            address: base.to_string(),
            username,
            mode: form.mode,
            projects,
        };
        // A saved secret is reused only for the same address, user and kind.
        let keeps_secret = existing.as_ref().is_some_and(|existing| existing.credential_owner() == connection.credential_owner());
        if secret.is_empty() && !keeps_secret {
            form.message = Some("Enter the password or API token.".into());
            cx.notify();
            return;
        }
        form.checking = true;
        form.message = Some("Checking…".into());
        form._task = Some(cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                if !secret.is_empty() {
                    let client = ArgoClient::new(cx.update(|cx| cx.http_client()), base, &connection.username, connection.mode, &secret);
                    client.verify().await?;
                    let url = connection.credential_url();
                    let owner = connection.credential_owner();
                    cx.update(|cx| cx.write_credentials(&url, &owner, secret.as_bytes()))
                        .await
                        .context("Vilo could not save the secret to the keychain")?;
                    let read_back = cx.update(|cx| cx.read_credentials(&url)).await?;
                    if read_back.as_ref().is_none_or(|(saved_owner, saved)| saved_owner != &owner || saved.as_slice() != secret.as_bytes()) {
                        return Err(anyhow!("The keychain did not keep the secret."));
                    }
                }
                let persist = this.update(cx, |this, cx| {
                    match this.connections.iter_mut().find(|c| c.id == connection.id) {
                        Some(existing) => *existing = connection.clone(),
                        None => this.connections.push(connection.clone()),
                    }
                    this.persist(cx)
                })?;
                persist.await.context("Vilo could not save the connection")
            }
            .await;
            this.update(cx, |this, cx| match result {
                Ok(()) => this.connect(connection.id.clone(), cx),
                Err(error) => {
                    if let Some(form) = &mut this.form {
                        form.checking = false;
                        form.message = Some(match error.downcast_ref::<ArgoError>() {
                            Some(argo) => argo.to_string().into(),
                            None => format!("{error:#}").into(),
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
        if self.session.as_ref().is_some_and(|session| session.connection_id == id) {
            self.generation += 1;
            self.session = None;
            self.detail = None;
            self.apps = Fetch::Loaded(Vec::new());
        }
        self.form = None;
        let persist = self.persist(cx);
        cx.spawn(async move |_, cx| {
            persist.await?;
            cx.update(|cx| cx.delete_credentials(&connection.credential_url()))
                .await
                .context("Vilo could not remove the Argo CD secret from the keychain")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }
}

impl Focusable for ArgoView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

fn section_header(title: impl Into<SharedString>) -> gpui::Div {
    h_flex()
        .h(px(26.))
        .pl_3()
        .pr_1()
        .justify_between()
        .child(Label::new(title).size(LabelSize::XSmall).weight(gpui::FontWeight::SEMIBOLD).color(Color::Muted))
}

impl ArgoView {
    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let active = self.session.as_ref().map(|session| session.connection_id.clone());
        let mut connections = v_flex().gap_0p5().px_1();
        for connection in &self.connections {
            let id = connection.id.clone();
            connections = connections.child(
                h_flex()
                    .id(SharedString::from(format!("argo-connection-{}", connection.id)))
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .cursor_pointer()
                    .when(active.as_ref() == Some(&connection.id), |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .tooltip(Tooltip::text(format!("{} as {}", connection.address, connection.username)))
                    .on_click(cx.listener(move |this, _, _, cx| this.connect(id.clone(), cx)))
                    .child(Icon::new(IconName::Server).size(IconSize::Small).color(Color::Muted))
                    .child(Label::new(connection.label()).truncate()),
            );
        }

        let query = self.filter.read(cx).text(cx).to_lowercase();
        let mut apps = v_flex().gap_0p5().px_1();
        match &self.apps {
            Fetch::Loading => apps = apps.child(div().px_3().child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted))),
            Fetch::Failed(message) => {
                apps = apps.child(div().px_3().child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning)))
            }
            Fetch::Loaded(list) => {
                let mut project = None;
                for app in list.iter().filter(|app| query.is_empty() || app.name.to_lowercase().contains(&query)) {
                    if project != Some(&app.project) {
                        project = Some(&app.project);
                        apps = apps.child(div().px_2().pt_1().child(Label::new(app.project.clone()).size(LabelSize::XSmall).color(Color::Muted)));
                    }
                    let selected = self.detail.as_ref().is_some_and(|detail| detail.namespace == app.namespace && detail.name == app.name);
                    let (namespace, name) = (app.namespace.clone(), app.name.clone());
                    apps = apps.child(
                        h_flex()
                            .id(SharedString::from(format!("argo-app-{}-{}", app.namespace, app.name)))
                            .pl_3()
                            .pr_2()
                            .py_0p5()
                            .gap_1p5()
                            .rounded_sm()
                            .cursor_pointer()
                            .when(selected, |this| this.bg(colors.element_selected))
                            .hover(|style| style.bg(colors.element_hover))
                            .tooltip(Tooltip::text(format!("{} · {}", app.health, app.sync)))
                            .on_click(cx.listener(move |this, _, _, cx| this.open_app(namespace.clone(), name.clone(), cx)))
                            .child(Indicator::dot().color(health_color(&app.health)))
                            .child(Label::new(app.name.clone()).size(LabelSize::Small).truncate())
                            .child(div().flex_1())
                            .when(app.sync != "Synced", |this| {
                                this.child(Label::new(app.sync.clone()).size(LabelSize::XSmall).color(sync_color(&app.sync)))
                            }),
                    );
                }
            }
        }

        v_flex()
            .w(px(300.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(
                section_header("CONNECTIONS").child(
                    IconButton::new("argo-add-connection", IconName::Plus)
                        .icon_size(IconSize::Small)
                        .tooltip(Tooltip::text("Add an Argo CD connection"))
                        .on_click(cx.listener(|this, _, window, cx| this.open_form(None, window, cx))),
                ),
            )
            .child(connections)
            .child(
                section_header("APPLICATIONS").mt_2().border_t_1().border_color(colors.border_variant).child(
                    IconButton::new("argo-reload-apps", IconName::RotateCw)
                        .icon_size(IconSize::Small)
                        .tooltip(Tooltip::text("Reload the list (does not refresh apps in Argo)"))
                        .on_click(cx.listener(|this, _, _, cx| this.load_apps(cx))),
                ),
            )
            .when(self.session.is_some(), |this| {
                this.child(
                    div()
                        .mx_2()
                        .mb_1()
                        .px_2()
                        .py_0p5()
                        .rounded_sm()
                        .border_1()
                        .border_color(colors.border)
                        .bg(colors.editor_background)
                        .child(self.filter.clone()),
                )
            })
            .child(v_flex().id("argo-apps").flex_1().min_h_0().overflow_y_scroll().pb_2().child(apps))
    }

    fn render_form(&self, form: &Form, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let input = |label: &'static str, editor: &Entity<Editor>| {
            v_flex()
                .gap_1()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                .child(div().px_2().py_1().rounded_sm().border_1().border_color(colors.border).bg(colors.editor_background).child(editor.clone()))
        };
        let mode_button = |id: &'static str, label: &'static str, mode: AuthMode, cx: &mut Context<Self>| {
            Button::new(id, label)
                .label_size(LabelSize::Small)
                .style(if form.mode == mode { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(form) = &mut this.form {
                        form.mode = mode;
                    }
                    cx.notify();
                }))
        };
        let editing = form.editing.clone();
        v_flex()
            .id("argo-form")
            .max_w(px(560.))
            .overflow_y_scroll()
            .gap_3()
            .child(Headline::new(if form.editing.is_some() { "Edit Argo CD connection" } else { "Add an Argo CD connection" }).size(HeadlineSize::Small))
            .child(input("Name", &form.name))
            .child(h_flex().gap_3().child(div().flex_1().child(input("Site", &form.site))).child(div().flex_1().child(input("Environment", &form.environment))))
            .child(input("Address", &form.address))
            .child(input("Username", &form.username))
            .child(
                v_flex()
                    .gap_1()
                    .child(Label::new("Sign in with").size(LabelSize::Small).color(Color::Muted))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(mode_button("argo-mode-password", "Password", AuthMode::Password, cx))
                            .child(mode_button("argo-mode-token", "API token", AuthMode::Token, cx)),
                    ),
            )
            .child(input(if form.mode == AuthMode::Password { "Password" } else { "API token" }, &form.secret))
            .child(input("Projects", &form.projects))
            .child(
                Label::new(
                    "Kept in the macOS keychain and only sent to this address. With a password, Vilo signs in for a \
                     session and signs in again once when it expires. Vilo only reads from Argo CD and asks it to refresh.",
                )
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .when_some(form.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(if form.checking { Color::Muted } else { Color::Warning }))
            })
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("argo-save", "Save and connect")
                            .style(ButtonStyle::Filled)
                            .disabled(form.checking)
                            .on_click(cx.listener(|this, _, _, cx| this.save_form(cx))),
                    )
                    .when(!self.connections.is_empty(), |this| {
                        this.child(Button::new("argo-cancel", "Cancel").on_click(cx.listener(|this, _, _, cx| {
                            this.form = None;
                            cx.notify();
                        })))
                    })
                    .when_some(editing, |this, id| {
                        this.child(
                            Button::new("argo-delete", "Delete connection")
                                .style(ButtonStyle::Subtle)
                                .color(Color::Error)
                                .on_click(cx.listener(move |this, _, _, cx| this.delete_connection(id.clone(), cx))),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_detail(&self, detail: &Detail, cx: &mut Context<Self>) -> AnyElement {
        let section = |title: &'static str| Label::new(title).size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD);
        let base = self
            .session
            .as_ref()
            .and_then(|session| self.connections.iter().find(|c| c.id == session.connection_id))
            .map(|c| c.address.clone());
        let mut body = v_flex().gap_3();
        match &detail.app {
            Fetch::Loading => body = body.child(Label::new("Loading…").color(Color::Muted)),
            Fetch::Failed(message) => body = body.child(Label::new(message.clone()).color(Color::Warning)),
            Fetch::Loaded(app) => {
                let summary = &app.summary;
                body = body.child(
                    h_flex()
                        .gap_3()
                        .flex_wrap()
                        .child(h_flex().gap_1().child(Indicator::dot().color(health_color(&summary.health))).child(Label::new(summary.health.clone())))
                        .child(h_flex().gap_1().child(Indicator::dot().color(sync_color(&summary.sync))).child(Label::new(summary.sync.clone())))
                        .when_some(summary.operation.clone(), |this, operation| {
                            this.child(Label::new(format!("Last operation: {operation}")).size(LabelSize::Small).color(Color::Muted))
                        })
                        .when(summary.deleting, |this| this.child(Label::new("Being deleted").color(Color::Error)))
                        .child(Label::new(format!("{} → {}", summary.project, summary.destination)).size(LabelSize::Small).color(Color::Muted)),
                );
                if let Some(message) = &app.operation_message {
                    body = body.child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Muted));
                }
                for (kind, message) in &app.conditions {
                    body = body.child(Label::new(format!("{kind}: {message}")).size(LabelSize::Small).color(Color::Warning));
                }
                if !summary.images.is_empty() {
                    body = body
                        .child(section("Images"))
                        .children(summary.images.iter().map(|image| Label::new(image.clone()).size(LabelSize::Small).buffer_font(cx)));
                }
                body = body.child(section("Sources")).children(app.sources.iter().enumerate().map(|(index, source)| {
                    Label::new(format!(
                        "{} · {} · {}{}",
                        source.repo,
                        source.path,
                        source.target_revision,
                        app.revisions.get(index).map_or(String::new(), |revision| format!(" → {}", &revision[..revision.len().min(12)]))
                    ))
                    .size(LabelSize::Small)
                    .buffer_font(cx)
                }));
                if !app.history.is_empty() {
                    body = body.child(section("History")).children(app.history.iter().take(10).map(|entry| {
                        Label::new(format!(
                            "#{} · {} · {}",
                            entry.id,
                            entry.deployed_at.replace('T', " ").trim_end_matches('Z'),
                            entry.revisions.iter().map(|revision| &revision[..revision.len().min(12)]).collect::<Vec<_>>().join(", ")
                        ))
                        .size(LabelSize::Small)
                        .buffer_font(cx)
                    }));
                }
            }
        }
        body = body.child(section("Resources"));
        body = match &detail.resources {
            Fetch::Loading => body.child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted)),
            Fetch::Failed(message) => body.child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Muted)),
            Fetch::Loaded(resources) => body.children(resources.iter().take(200).map(|resource| {
                h_flex()
                    .gap_2()
                    .child(Indicator::dot().color(resource.health.as_deref().map_or(Color::Muted, health_color)))
                    .child(Label::new(format!("{} {}", resource.kind, resource.name)).size(LabelSize::Small))
                    .when_some(resource.message.clone(), |this, message| this.child(Label::new(message).size(LabelSize::XSmall).color(Color::Muted).truncate()))
            })),
        };
        body = body.child(section("Events"));
        body = match &detail.events {
            Fetch::Loading => body.child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted)),
            Fetch::Failed(message) => body.child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Muted)),
            Fetch::Loaded(events) if events.is_empty() => body.child(Label::new("No recent events.").size(LabelSize::Small).color(Color::Muted)),
            Fetch::Loaded(events) => body.children(events.iter().take(30).map(|event| {
                Label::new(format!("{} · {} · {}", event.last_seen.replace('T', " ").trim_end_matches('Z'), event.reason, event.message))
                    .size(LabelSize::Small)
                    .color(if event.kind == "Warning" { Color::Warning } else { Color::Default })
                    .truncate()
            })),
        };

        let pending = matches!(detail.refresh, RefreshState::Pending);
        let link = base.map(|base| format!("{base}applications/{}/{}", detail.namespace, detail.name));
        v_flex()
            .id("argo-detail")
            .flex_1()
            .overflow_y_scroll()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(Headline::new(detail.name.clone()).size(HeadlineSize::Small))
                    .child(Label::new(detail.namespace.clone()).size(LabelSize::Small).color(Color::Muted))
                    .child(div().flex_1())
                    .child(
                        Button::new("argo-refresh", if pending { "Refreshing…" } else { "Refresh" })
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::RotateCw).size(IconSize::Small))
                            .disabled(pending)
                            .tooltip(Tooltip::text("Ask Argo to compare with Git again. Does not deploy."))
                            .on_click(cx.listener(|this, _, _, cx| this.refresh_app(false, cx))),
                    )
                    .child(
                        Button::new("argo-hard-refresh", if detail.confirm_hard { "Click again: hard refresh" } else { "Hard refresh" })
                            .label_size(LabelSize::Small)
                            .style(if detail.confirm_hard { ButtonStyle::Tinted(ui::TintColor::Warning) } else { ButtonStyle::Subtle })
                            .disabled(pending)
                            .tooltip(Tooltip::text("Also regenerates manifests, which costs Argo more work. Does not deploy."))
                            .on_click(cx.listener(|this, _, _, cx| this.refresh_app(true, cx))),
                    )
                    .when_some(link, |this, link| {
                        this.child(
                            IconButton::new("argo-open-web", IconName::ArrowUpRight)
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Open in Argo CD"))
                                .on_click(move |_, _, cx| cx.open_url(&link)),
                        )
                    }),
            )
            .when_some(
                match &detail.refresh {
                    RefreshState::Requested(message) => Some(message.clone()),
                    _ => None,
                },
                |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Muted)),
            )
            .child(body)
            .into_any_element()
    }
}

impl Render for ArgoView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let connection = self
            .session
            .as_ref()
            .and_then(|session| self.connections.iter().find(|c| c.id == session.connection_id))
            .cloned();
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
                    .child(Label::new(connection.label()).size(LabelSize::Large).weight(gpui::FontWeight::SEMIBOLD))
                    .child(Label::new(connection.address.clone()).size(LabelSize::Small).color(Color::Muted).buffer_font(cx).truncate())
                    .child(
                        Label::new(format!(
                            "Signed in as {} · Argo CD {} · read-only, refresh allowed",
                            connection.username,
                            self.session.as_ref().map_or("", |session| session.version.as_str())
                        ))
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                    )
                    .into_any_element(),
                None => v_flex()
                    .flex_1()
                    .child(Label::new("Argo CD").size(LabelSize::Large).weight(gpui::FontWeight::SEMIBOLD))
                    .child(Label::new("No connection selected").size(LabelSize::Small).color(Color::Muted))
                    .into_any_element(),
            })
            .when_some(connection.as_ref().map(|c| c.id.clone()), |this, id| {
                this.child(
                    Button::new("argo-edit-connection", "Edit connection")
                        .label_size(LabelSize::Small)
                        .start_icon(Icon::new(IconName::Pencil).size(IconSize::Small))
                        .on_click(cx.listener(move |this, _, window, cx| this.open_form(Some(id.clone()), window, cx))),
                )
            });
        let main = if let Some(form) = &self.form {
            self.render_form(form, cx)
        } else if let Some(message) = &self.session_error {
            Label::new(message.clone()).color(Color::Muted).into_any_element()
        } else if let Some(detail) = &self.detail {
            self.render_detail(detail, cx)
        } else if self.session.is_some() {
            Label::new("Pick an application on the left.").color(Color::Muted).into_any_element()
        } else {
            Label::new("Choose a connection on the left.").color(Color::Muted).into_any_element()
        };
        h_flex()
            .key_context("ArgoView")
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
                    .child(v_flex().flex_1().min_h_0().p_4().gap_3().child(main)),
            )
    }
}
