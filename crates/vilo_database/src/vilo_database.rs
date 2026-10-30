mod mongo_tab;
mod redis_view;

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use collections::HashMap;
use editor::Editor;
use gpui::{
    App, AsyncApp, Entity, FocusHandle, Focusable, ListHorizontalSizingBehavior, Task,
    TaskExt as _, UniformListScrollHandle, Window, prelude::*, uniform_list,
};
use gpui_tokio::Tokio;
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, ContextMenu, IconButton, PopoverMenu, Tooltip, prelude::*};
use util::ResultExt as _;
use vilo_database_core::{
    mongo::{CollectionInfo, DatabaseInfo, MongoSession},
    redis::RedisSession,
};

use mongo_tab::{MongoTab, SavedQuery, load_history};
use redis_view::RedisState;
use workspace::Workspace;

const MODULE_ID: &str = "database";
const UNASSIGNED_SITE: &str = "Unassigned";
const QUERY_TIME_LIMIT: Duration = Duration::from_secs(30);
/// Counting can be slow on big collections; past this the total is simply not shown.
const COUNT_TIME_LIMIT: Duration = Duration::from_secs(5);

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, cx| {
        workspace.register_module(
            MODULE_ID,
            "icons/vilo/codicons/database.svg",
            "Database",
            |window, cx| {
                let workspace = cx.entity().downgrade();
                let view = cx.new(|cx| DatabaseView::new(workspace, window, cx));
                let focus_handle = view.focus_handle(cx);
                (view.into(), focus_handle)
            },
            cx,
        );
    })
    .detach();
}

/// Runs a driver call on Tokio to completion even if the view stops waiting:
/// the MongoDB driver warns that dropping an operation midway can leave it inconsistent.
fn run<R: Send + 'static>(
    cx: &App,
    future: impl Future<Output = Result<R>> + Send + 'static,
) -> Task<Result<R>> {
    let join = Tokio::handle(cx).spawn(future);
    cx.background_spawn(async move { join.await? })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Kind {
    MongoDb,
    Redis,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::MongoDb => "MongoDB",
            Kind::Redis => "Redis",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    kind: Kind,
    /// The site the database belongs to (CP, NP, ZP, BP …); the sidebar shows one site at a time.
    #[serde(default)]
    site: String,
    /// e.g. CP QAT; shown on every page so results are never mistaken for another environment's.
    environment: String,
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-database://{}", self.id)
    }

    fn site_label(&self) -> String {
        if self.site.trim().is_empty() {
            UNASSIGNED_SITE.to_string()
        } else {
            self.site.trim().to_string()
        }
    }

    fn production(&self) -> bool {
        let text = format!("{} {}", self.environment, self.name).to_lowercase();
        text.contains("prod") || text.contains("live")
    }
}

enum Fetch<T> {
    Loading,
    Loaded(T),
    Failed(SharedString),
}

fn fetched<T>(result: Result<T>) -> Fetch<T> {
    match result {
        Ok(value) => Fetch::Loaded(value),
        Err(error) => Fetch::Failed(format!("{error:#}").into()),
    }
}

#[derive(Clone)]
enum Session {
    Mongo(Arc<MongoSession>),
    Redis(Arc<RedisSession>),
}

struct Form {
    editing: Option<String>,
    kind: Kind,
    site: Entity<Editor>,
    name: Entity<Editor>,
    environment: Entity<Editor>,
    uri: Entity<Editor>,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

pub struct DatabaseView {
    focus_handle: FocusHandle,
    /// For opening mongosh in a Vilo terminal.
    workspace: gpui::WeakEntity<Workspace>,
    path: PathBuf,
    connections: Vec<Connection>,
    active: Option<String>,
    session: Option<Session>,
    status: Option<SharedString>,
    /// Bumped on every connection change and query; answers for an older one are dropped.
    generation: u64,
    form: Option<Form>,
    databases: Fetch<Vec<DatabaseInfo>>,
    collections: HashMap<String, Fetch<Vec<CollectionInfo>>>,
    /// The site whose connections the sidebar lists.
    site: Option<String>,
    /// Searches connections and collections at once, keeping each match's parents visible.
    search: Entity<Editor>,
    tabs: Vec<MongoTab>,
    active_tab: Option<usize>,
    history: Vec<SavedQuery>,
    /// Readable dates in this Mac's time zone instead of UTC.
    local_dates: bool,
    pattern: Entity<Editor>,
    workbench_input: Entity<Editor>,
    redis_state: Option<RedisState>,
    tasks: Vec<Task<()>>,
    _subscriptions: Vec<gpui::Subscription>,
}

fn editor(
    placeholder: &str,
    text: &str,
    masked: bool,
    window: &mut Window,
    cx: &mut App,
) -> Entity<Editor> {
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

async fn connect(connection: &Connection, uri: String, cx: &AsyncApp) -> Result<Session> {
    let handle_task = match connection.kind {
        Kind::MongoDb => cx.update(|cx| {
            run(cx, async move {
                Ok(Session::Mongo(Arc::new(MongoSession::connect(&uri).await?)))
            })
        }),
        Kind::Redis => cx.update(|cx| {
            run(cx, async move {
                Ok(Session::Redis(Arc::new(RedisSession::connect(&uri).await?)))
            })
        }),
    };
    handle_task.await
}

impl DatabaseView {
    pub fn new(
        workspace: gpui::WeakEntity<Workspace>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let path = paths::config_dir().join("database_connections.json");
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
        let search = editor("Search connections and collections", "", false, window, cx);
        let subscriptions = vec![
            cx.subscribe(&search, |this, _, _: &editor::EditorEvent, cx| {
                // Searching needs every database's collections; load the ones not opened yet.
                if !this.search.read(cx).text(cx).trim().is_empty() {
                    this.load_all_collections(cx);
                }
                cx.notify();
            }),
        ];
        let mut view = Self {
            focus_handle: cx.focus_handle(),
            workspace,
            path,
            connections,
            active: None,
            session: None,
            status: None,
            generation: 0,
            form: None,
            databases: Fetch::Loaded(Vec::new()),
            collections: HashMap::default(),
            site: None,
            search,
            tabs: Vec::new(),
            active_tab: None,
            history: load_history(),
            local_dates: false,
            pattern: editor("Filter by Key Name or Pattern", "", false, window, cx),
            workbench_input: editor(
                "Enter a Redis command, e.g. HGETALL user:1",
                "",
                false,
                window,
                cx,
            ),
            redis_state: None,
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

    /// Every site that has a connection, sorted, with unassigned ones last.
    fn sites(&self) -> Vec<String> {
        let mut sites = self
            .connections
            .iter()
            .map(Connection::site_label)
            .collect::<Vec<_>>();
        sites.sort_by_key(|site| (site == UNASSIGNED_SITE, site.clone()));
        sites.dedup();
        sites
    }

    fn current_site(&self) -> String {
        let sites = self.sites();
        self.site
            .clone()
            .filter(|site| sites.contains(site))
            .or_else(|| sites.first().cloned())
            .unwrap_or_default()
    }

    fn active_connection(&self) -> Option<&Connection> {
        self.active.as_ref().and_then(|id| {
            self.connections
                .iter()
                .find(|connection| &connection.id == id)
        })
    }

    fn reset(&mut self) {
        self.generation += 1;
        self.session = None;
        self.status = None;
        self.databases = Fetch::Loaded(Vec::new());
        self.collections.clear();
        self.tabs.clear();
        self.active_tab = None;
        self.redis_state = None;
        self.tasks.clear();
    }

    fn connect(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(connection) = self
            .connections
            .iter()
            .find(|connection| connection.id == id)
            .cloned()
        else {
            return;
        };
        self.reset();
        self.active = Some(id);
        self.form = None;
        self.status = Some("Connecting…".into());
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = async {
                let credential = cx
                    .update(|cx| cx.read_credentials(&connection.credential_url()))
                    .await
                    .context("Could not read the connection string from the keychain")?;
                let (_, uri) = credential
                    .ok_or_else(|| anyhow!("No connection string saved. Edit the connection."))?;
                let uri =
                    String::from_utf8(uri).context("The saved connection string is unreadable")?;
                connect(&connection, uri, cx).await
            }
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                match result {
                    Ok(session) => {
                        this.status = None;
                        let mongo = matches!(session, Session::Mongo(_));
                        this.session = Some(session);
                        if mongo {
                            this.load_databases(cx);
                        } else {
                            this.redis_state = Some(RedisState::new());
                            this.start_redis_stats(cx);
                            this.scan(true, cx);
                        }
                    }
                    Err(error) => this.status = Some(format!("{error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn mongo(&self) -> Option<Arc<MongoSession>> {
        match &self.session {
            Some(Session::Mongo(session)) => Some(session.clone()),
            _ => None,
        }
    }

    fn redis(&self) -> Option<Arc<RedisSession>> {
        match &self.session {
            Some(Session::Redis(session)) => Some(session.clone()),
            _ => None,
        }
    }

    fn load_databases(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        self.databases = Fetch::Loading;
        let generation = self.generation;
        let task = run(cx, async move { session.databases().await });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == generation {
                    this.databases = fetched(result);
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn toggle_database(&mut self, database: String, cx: &mut Context<Self>) {
        if self.collections.remove(&database).is_some() {
            cx.notify();
            return;
        }
        self.load_collections(database, cx);
    }

    fn load_all_collections(&mut self, cx: &mut Context<Self>) {
        let Fetch::Loaded(databases) = &self.databases else {
            return;
        };
        let missing = databases
            .iter()
            .map(|database| database.name.clone())
            .filter(|name| !self.collections.contains_key(name))
            .collect::<Vec<_>>();
        for database in missing {
            self.load_collections(database, cx);
        }
    }

    fn load_collections(&mut self, database: String, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        self.collections.insert(database.clone(), Fetch::Loading);
        let generation = self.generation;
        let name = database.clone();
        let task = run(cx, async move { session.collections(&name).await });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == generation && this.collections.contains_key(&database) {
                    this.collections.insert(database, fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        self.form = Some(Form {
            kind: existing.as_ref().map_or(Kind::MongoDb, |c| c.kind),
            site: editor(
                "Site, e.g. CP",
                &existing
                    .as_ref()
                    .map(|c| c.site.clone())
                    .or_else(|| self.site.clone().filter(|site| site != UNASSIGNED_SITE))
                    .unwrap_or_default(),
                false,
                window,
                cx,
            ),
            name: editor(
                "Name, e.g. CP fpms",
                &existing
                    .as_ref()
                    .map(|c| c.name.clone())
                    .unwrap_or_default(),
                false,
                window,
                cx,
            ),
            environment: editor(
                "Environment, e.g. QAT",
                &existing
                    .as_ref()
                    .map(|c| c.environment.clone())
                    .unwrap_or_default(),
                false,
                window,
                cx,
            ),
            uri: editor(
                if existing.is_some() {
                    "Leave empty to keep the saved one"
                } else {
                    "mongodb://… or redis://…"
                },
                "",
                true,
                window,
                cx,
            ),
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
        let (site, name, environment, uri) = (
            text(&form.site, cx),
            text(&form.name, cx),
            text(&form.environment, cx),
            text(&form.uri, cx),
        );
        if site.is_empty() || name.is_empty() || environment.is_empty() {
            form.message = Some("Fill in the site, a name and an environment.".into());
            cx.notify();
            return;
        }
        let existing = form
            .editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        if uri.is_empty()
            && existing
                .as_ref()
                .is_none_or(|existing| existing.kind != form.kind)
        {
            form.message = Some("Enter the connection string.".into());
            cx.notify();
            return;
        }
        let connection = Connection {
            id: existing.as_ref().map_or_else(
                || uuid::Uuid::new_v4().simple().to_string(),
                |c| c.id.clone(),
            ),
            name,
            kind: form.kind,
            site: site.clone(),
            environment,
        };
        self.site = Some(site);
        form.checking = true;
        form.message = Some("Connecting…".into());
        form._task = Some(cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                if !uri.is_empty() {
                    let expected = match connection.kind {
                        Kind::MongoDb => {
                            uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")
                        }
                        Kind::Redis => uri.starts_with("redis://") || uri.starts_with("rediss://"),
                    };
                    anyhow::ensure!(
                        expected,
                        "That is not a {} connection string.",
                        connection.kind.label()
                    );
                    connect(&connection, uri.clone(), cx).await?;
                    let url = connection.credential_url();
                    cx.update(|cx| {
                        cx.write_credentials(&url, connection.kind.label(), uri.as_bytes())
                    })
                    .await
                    .context("Vilo could not save the connection string to the keychain")?;
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
                        form.message = Some(format!("{error:#}").into());
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
        if self.active.as_ref() == Some(&id) {
            self.reset();
            self.active = None;
        }
        self.form = None;
        let persist = self.persist(cx);
        cx.spawn(async move |_, cx| {
            persist.await?;
            cx.update(|cx| cx.delete_credentials(&connection.credential_url()))
                .await
                .context("Vilo could not remove the connection string from the keychain")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }
}

impl Focusable for DatabaseView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

fn section(title: &'static str) -> gpui::Div {
    h_flex().h(px(26.)).pl_3().pr_1().justify_between().child(
        Label::new(title)
            .size(LabelSize::XSmall)
            .weight(gpui::FontWeight::SEMIBOLD)
            .color(Color::Muted),
    )
}

fn boxed(child: impl IntoElement, cx: &App) -> gpui::Div {
    let colors = cx.theme().colors();
    div()
        .px_2()
        .py_1()
        .rounded_sm()
        .border_1()
        .border_color(colors.border)
        .bg(colors.editor_background)
        .child(child)
}

/// Monospaced lines in a virtual list; long lines scroll sideways so every row keeps one height.
fn render_lines(
    id: &'static str,
    lines: Vec<SharedString>,
    scroll: &UniformListScrollHandle,
    cx: &mut Context<DatabaseView>,
) -> AnyElement {
    let colors = cx.theme().colors().clone();
    let widest = lines
        .iter()
        .enumerate()
        .max_by_key(|(_, line)| line.len())
        .map(|(index, _)| index);
    let list = uniform_list(id, lines.len(), move |range, _, cx| {
        range
            .filter_map(|index| lines.get(index).cloned())
            .map(|line| {
                div()
                    .px_2()
                    .whitespace_nowrap()
                    .child(Label::new(line).size(LabelSize::Small).buffer_font(cx))
                    .into_any_element()
            })
            .collect::<Vec<_>>()
    })
    .with_horizontal_sizing_behavior(ListHorizontalSizingBehavior::Unconstrained)
    .with_width_from_item(widest)
    .track_scroll(scroll)
    .flex_1();
    v_flex()
        .flex_1()
        .min_h_0()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(colors.border)
        .bg(colors.editor_background)
        .child(list)
        .into_any_element()
}

impl DatabaseView {
    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let search = self.search.read(cx).text(cx).trim().to_lowercase();
        let matches = |name: &str| search.is_empty() || name.to_lowercase().contains(&search);
        let mut tree = v_flex().gap_0p5().px_1();
        let sites = self.sites();
        let current_site = self.current_site();
        for kind in [Kind::MongoDb, Kind::Redis] {
            let in_site = self
                .connections
                .iter()
                .filter(|connection| {
                    connection.kind == kind && connection.site_label() == current_site
                })
                .collect::<Vec<_>>();
            tree = tree.child(
                div().px_2().pt_2().pb_0p5().child(
                    Label::new(kind.label().to_uppercase())
                        .size(LabelSize::XSmall)
                        .weight(gpui::FontWeight::SEMIBOLD)
                        .color(Color::Muted),
                ),
            );
            if in_site.is_empty() {
                tree = tree.child(
                    div().px_3().child(
                        Label::new(format!("No {} connections in {current_site}", kind.label()))
                            .size(LabelSize::Small)
                            .color(Color::Disabled),
                    ),
                );
            }
            for connection in in_site {
                let active = self.active.as_ref() == Some(&connection.id);
                let mongo_open = active && matches!(self.session, Some(Session::Mongo(_)));
                // Under a search, the active connection's matching collections keep it visible.
                if !matches(&connection.name) && !mongo_open {
                    continue;
                }
                let id = connection.id.clone();
                tree = tree.child(
                    h_flex()
                        .id(SharedString::from(format!(
                            "database-connection-{}",
                            connection.id
                        )))
                        .px_2()
                        .py_1()
                        .gap_1p5()
                        .rounded_md()
                        .cursor_pointer()
                        .when(active, |this| this.bg(colors.element_selected))
                        .hover(|style| style.bg(colors.element_hover))
                        .on_click(cx.listener(move |this, _, _, cx| this.connect(id.clone(), cx)))
                        .child(
                            Icon::new(if active {
                                IconName::ChevronDown
                            } else {
                                IconName::ChevronRight
                            })
                            .size(IconSize::XSmall)
                            .color(Color::Muted),
                        )
                        .child(
                            Icon::new(IconName::DatabaseZap)
                                .size(IconSize::Small)
                                .color(Color::Muted),
                        )
                        .child(Label::new(connection.name.clone()).truncate())
                        .child(div().flex_1())
                        .child(
                            Label::new(connection.environment.clone())
                                .size(LabelSize::XSmall)
                                .color(if connection.production() {
                                    Color::Warning
                                } else {
                                    Color::Muted
                                }),
                        ),
                );
                if !mongo_open {
                    continue;
                }
                match &self.databases {
                    Fetch::Loading => {
                        tree = tree.child(
                            div().pl_8().child(
                                Label::new("Loading…")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            ),
                        )
                    }
                    Fetch::Failed(message) => {
                        tree = tree.child(
                            div().pl_8().child(
                                Label::new(message.clone())
                                    .size(LabelSize::Small)
                                    .color(Color::Warning),
                            ),
                        )
                    }
                    Fetch::Loaded(databases) => {
                        for database in databases {
                            let collections = self.collections.get(&database.name);
                            let matching = match collections {
                                Some(Fetch::Loaded(collections)) => collections
                                    .iter()
                                    .filter(|collection| {
                                        matches(&collection.name) || matches(&database.name)
                                    })
                                    .collect::<Vec<_>>(),
                                _ => Vec::new(),
                            };
                            if !search.is_empty() && matching.is_empty() && !matches(&database.name)
                            {
                                continue;
                            }
                            let open = collections.is_some();
                            let name = database.name.clone();
                            tree = tree.child(
                                h_flex()
                                    .id(SharedString::from(format!(
                                        "database-db-{}",
                                        database.name
                                    )))
                                    .pl_6()
                                    .pr_2()
                                    .py_0p5()
                                    .gap_1()
                                    .rounded_sm()
                                    .cursor_pointer()
                                    .hover(|style| style.bg(colors.element_hover))
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.toggle_database(name.clone(), cx)
                                    }))
                                    .child(
                                        Icon::new(if open {
                                            IconName::ChevronDown
                                        } else {
                                            IconName::ChevronRight
                                        })
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                    )
                                    .child(
                                        Icon::new(IconName::DatabaseZap)
                                            .size(IconSize::XSmall)
                                            .color(Color::Muted),
                                    )
                                    .child(
                                        Label::new(database.name.clone())
                                            .size(LabelSize::Small)
                                            .truncate(),
                                    ),
                            );
                            match collections {
                                Some(Fetch::Loading) => {
                                    tree = tree.child(
                                        div().pl_12().child(
                                            Label::new("Loading…")
                                                .size(LabelSize::XSmall)
                                                .color(Color::Muted),
                                        ),
                                    )
                                }
                                Some(Fetch::Failed(message)) => {
                                    tree = tree.child(
                                        div().pl_12().child(
                                            Label::new(message.clone())
                                                .size(LabelSize::XSmall)
                                                .color(Color::Warning),
                                        ),
                                    )
                                }
                                Some(Fetch::Loaded(_)) => {
                                    for collection in matching {
                                        let selected = self.tab().is_some_and(|tab| {
                                            tab.database == database.name
                                                && tab.collection == collection.name
                                        });
                                        let (database_name, collection_name) =
                                            (database.name.clone(), collection.name.clone());
                                        tree = tree.child(
                                            h_flex()
                                                .id(SharedString::from(format!(
                                                    "database-coll-{}-{}",
                                                    database.name, collection.name
                                                )))
                                                .pl_12()
                                                .pr_2()
                                                .py_0p5()
                                                .gap_1p5()
                                                .rounded_sm()
                                                .cursor_pointer()
                                                .when(selected, |this| {
                                                    this.bg(colors.element_selected)
                                                })
                                                .hover(|style| style.bg(colors.element_hover))
                                                .on_click(cx.listener(
                                                    move |this, _, window, cx| {
                                                        this.open_collection(
                                                            database_name.clone(),
                                                            collection_name.clone(),
                                                            window,
                                                            cx,
                                                        )
                                                    },
                                                ))
                                                .child(
                                                    Icon::new(IconName::Folder)
                                                        .size(IconSize::XSmall)
                                                        .color(Color::Muted),
                                                )
                                                .child(
                                                    Label::new(collection.name.clone())
                                                        .size(LabelSize::Small)
                                                        .truncate(),
                                                )
                                                .when(collection.kind != "collection", |this| {
                                                    this.child(div().flex_1()).child(
                                                        Label::new(collection.kind.clone())
                                                            .size(LabelSize::XSmall)
                                                            .color(Color::Muted),
                                                    )
                                                }),
                                        );
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
            }
        }
        let view = cx.entity().downgrade();
        let site_menu = PopoverMenu::new("database-site")
            .trigger(
                Button::new(
                    "database-site-button",
                    if current_site.is_empty() {
                        "No site".to_string()
                    } else {
                        current_site
                    },
                )
                .style(ButtonStyle::Outlined)
                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu(move |window, cx| {
                let sites = sites.clone();
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for site in &sites {
                        let view = view.clone();
                        let site = site.clone();
                        menu = menu.entry(site.clone(), None, move |_, cx| {
                            let site = site.clone();
                            view.update(cx, |this, cx| {
                                this.site = Some(site);
                                cx.notify();
                            })
                            .ok();
                        });
                    }
                    menu
                }))
            });
        v_flex()
            .w(px(300.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(
                section("SITE")
                    .child(div().flex_1().pl_2().child(site_menu))
                    .child(
                        h_flex()
                            .when(matches!(self.session, Some(Session::Mongo(_))), |this| {
                                this.child(
                                    IconButton::new("database-reload", IconName::RotateCw)
                                        .icon_size(IconSize::Small)
                                        .tooltip(Tooltip::text("Reload databases"))
                                        .on_click(
                                            cx.listener(|this, _, _, cx| this.load_databases(cx)),
                                        ),
                                )
                            })
                            .child(
                                IconButton::new("database-add-connection", IconName::Plus)
                                    .icon_size(IconSize::Small)
                                    .tooltip(Tooltip::text("Add a MongoDB or Redis connection"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.open_form(None, window, cx)
                                    })),
                            ),
                    ),
            )
            .child(div().mx_2().mb_1().child(boxed(self.search.clone(), cx)))
            .child(
                v_flex()
                    .id("database-tree")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .pb_2()
                    .child(tree),
            )
    }

    fn render_form(&self, form: &Form, cx: &mut Context<Self>) -> AnyElement {
        let input = |label: &'static str, editor: &Entity<Editor>, cx: &App| {
            v_flex()
                .gap_1()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                .child(boxed(editor.clone(), cx))
        };
        let kind_button = |id: &'static str, kind: Kind, cx: &mut Context<Self>| {
            Button::new(id, kind.label())
                .label_size(LabelSize::Small)
                .style(if form.kind == kind {
                    ButtonStyle::Filled
                } else {
                    ButtonStyle::Subtle
                })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(form) = &mut this.form {
                        form.kind = kind;
                    }
                    cx.notify();
                }))
        };
        let editing = form.editing.clone();
        v_flex()
            .id("database-form")
            .max_w(px(560.))
            .gap_3()
            .child(Headline::new(if form.editing.is_some() { "Edit connection" } else { "Add a connection" }).size(HeadlineSize::Small))
            .child(h_flex().gap_1().child(kind_button("database-kind-mongo", Kind::MongoDb, cx)).child(kind_button("database-kind-redis", Kind::Redis, cx)))
            .child(input("Name", &form.name, cx))
            .child(input("Site", &form.site, cx))
            .child(input("Environment", &form.environment, cx))
            .child(input("Connection string", &form.uri, cx))
            .child(
                Label::new(
                    "The connection string, password included, is kept in the macOS keychain and only used to connect. \
                     This version only reads. Environments named prod or live are marked on every page.",
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
                        Button::new("database-save", "Save and connect")
                            .style(ButtonStyle::Filled)
                            .disabled(form.checking)
                            .on_click(cx.listener(|this, _, _, cx| this.save_form(cx))),
                    )
                    .when(!self.connections.is_empty(), |this| {
                        this.child(Button::new("database-cancel", "Cancel").on_click(cx.listener(|this, _, _, cx| {
                            this.form = None;
                            cx.notify();
                        })))
                    })
                    .when_some(editing, |this, id| {
                        this.child(
                            Button::new("database-delete", "Delete connection")
                                .style(ButtonStyle::Subtle)
                                .color(Color::Error)
                                .on_click(cx.listener(move |this, _, _, cx| this.delete_connection(id.clone(), cx))),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_header(
        &self,
        connection: &Connection,
        title: String,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let id = connection.id.clone();
        h_flex()
            .gap_2()
            .child(Headline::new(title).size(HeadlineSize::Small))
            .child(
                Label::new(format!(
                    "{} · {}",
                    connection.kind.label(),
                    connection.environment
                ))
                .size(LabelSize::Small)
                .color(if connection.production() {
                    Color::Warning
                } else {
                    Color::Muted
                }),
            )
            .child(div().flex_1())
            .child(
                Button::new("database-edit", "Edit connection")
                    .label_size(LabelSize::Small)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_form(Some(id.clone()), window, cx)
                    })),
            )
    }
}

impl Render for DatabaseView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let connection = self.active_connection().cloned();
        let full_bleed = self.form.is_none() && self.session.is_some();
        let main = match (&self.form, &connection, &self.session) {
            (Some(form), _, _) => self.render_form(form, cx),
            (None, Some(connection), Some(Session::Mongo(_))) => self.render_mongo(connection, cx),
            (None, Some(connection), Some(Session::Redis(_))) => self.render_redis(connection, cx),
            (None, Some(connection), None) => v_flex()
                .gap_3()
                .child(self.render_header(connection, connection.name.clone(), cx))
                .child(
                    Label::new(
                        self.status
                            .clone()
                            .unwrap_or_else(|| "Not connected.".into()),
                    )
                    .color(Color::Muted),
                )
                .into_any_element(),
            (None, None, _) => Label::new("Pick a connection on the left, or add one.")
                .color(Color::Muted)
                .into_any_element(),
        };
        h_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().colors().editor_background)
            .child(self.render_sidebar(cx))
            // Mongo and Redis pages run edge to edge like Compass and RedisInsight.
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .when(!full_bleed, |this| this.p_4())
                    .child(main),
            )
    }
}
