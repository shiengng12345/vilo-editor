use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use collections::HashMap;
use editor::Editor;
use gpui::{
    App, AsyncApp, Entity, FocusHandle, Focusable, ListHorizontalSizingBehavior, Task, TaskExt as _, UniformListScrollHandle, Window,
    prelude::*, uniform_list,
};
use gpui_tokio::Tokio;
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, IconButton, Tooltip, prelude::*};
use util::ResultExt as _;
use vilo_database_core::{
    mongo::{CollectionInfo, DatabaseInfo, FindRequest, MongoSession, parse_document, to_display_json},
    redis::{KeyInfo, RedisSession},
};
use workspace::Workspace;

const MODULE_ID: &str = "database";
const PAGE_SIZES: [u32; 3] = [25, 50, 100];
const SCAN_COUNT: u32 = 200;
const QUERY_TIME_LIMIT: Duration = Duration::from_secs(30);

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, cx| {
        workspace.register_module(
            MODULE_ID,
            "icons/vilo/codicons/database.svg",
            "Database",
            |window, cx| {
                let view = cx.new(|cx| DatabaseView::new(window, cx));
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
fn run<R: Send + 'static>(cx: &App, future: impl Future<Output = Result<R>> + Send + 'static) -> Task<Result<R>> {
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
    /// e.g. CP QAT; shown on every page so results are never mistaken for another environment's.
    environment: String,
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-database://{}", self.id)
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
    name: Entity<Editor>,
    environment: Entity<Editor>,
    uri: Entity<Editor>,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

struct MongoPage {
    lines: Vec<SharedString>,
    shown: usize,
    has_more: bool,
}

struct KeyDetail {
    info: KeyInfo,
    /// The raw value of a string key; other types get their own viewers.
    value: Option<Vec<u8>>,
}

pub struct DatabaseView {
    focus_handle: FocusHandle,
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
    selected_collection: Option<(String, String)>,
    filter: Entity<Editor>,
    projection: Entity<Editor>,
    sort: Entity<Editor>,
    page_size: u32,
    skip: u64,
    page: Option<Fetch<MongoPage>>,
    pattern: Entity<Editor>,
    keys: Vec<Vec<u8>>,
    /// The next SCAN cursor; `None` once the walk came back to "0".
    cursor: Option<String>,
    scanning: bool,
    scan_error: Option<SharedString>,
    selected_key: Option<Vec<u8>>,
    key_detail: Option<Fetch<KeyDetail>>,
    hex: bool,
    lines_scroll: UniformListScrollHandle,
    keys_scroll: UniformListScrollHandle,
    tasks: Vec<Task<()>>,
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

/// Printable ASCII as is, everything else as `\xNN`, so different bytes never look the same.
fn escape_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b' '..=b'~' if *byte != b'\\' => (*byte as char).to_string(),
            b'\\' => "\\\\".to_string(),
            _ => format!("\\x{byte:02x}"),
        })
        .collect()
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect::<Vec<_>>().join(" ")
}

fn describe_ttl(ttl_ms: i64) -> String {
    match ttl_ms {
        -1 => "no expiry".into(),
        -2 => "key does not exist".into(),
        ms if ms < 1000 => format!("{ms} ms"),
        ms => format!("{} s", ms / 1000),
    }
}

async fn connect(connection: &Connection, uri: String, cx: &AsyncApp) -> Result<Session> {
    let handle_task = match connection.kind {
        Kind::MongoDb => cx.update(|cx| run(cx, async move { Ok(Session::Mongo(Arc::new(MongoSession::connect(&uri).await?))) })),
        Kind::Redis => cx.update(|cx| run(cx, async move { Ok(Session::Redis(Arc::new(RedisSession::connect(&uri).await?))) })),
    };
    handle_task.await
}

impl DatabaseView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
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
        let mut view = Self {
            focus_handle: cx.focus_handle(),
            path,
            connections,
            active: None,
            session: None,
            status: None,
            generation: 0,
            form: None,
            databases: Fetch::Loaded(Vec::new()),
            collections: HashMap::default(),
            selected_collection: None,
            filter: editor("Filter, e.g. {\"status\": \"active\"}", "", false, window, cx),
            projection: editor("Project, e.g. {\"name\": 1}", "", false, window, cx),
            sort: editor("Sort, e.g. {\"_id\": -1}", "", false, window, cx),
            page_size: PAGE_SIZES[0],
            skip: 0,
            page: None,
            pattern: editor("Key pattern, e.g. user:*", "*", false, window, cx),
            keys: Vec::new(),
            cursor: None,
            scanning: false,
            scan_error: None,
            selected_key: None,
            key_detail: None,
            hex: false,
            lines_scroll: UniformListScrollHandle::new(),
            keys_scroll: UniformListScrollHandle::new(),
            tasks: Vec::new(),
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

    fn active_connection(&self) -> Option<&Connection> {
        self.active.as_ref().and_then(|id| self.connections.iter().find(|connection| &connection.id == id))
    }

    fn reset(&mut self) {
        self.generation += 1;
        self.session = None;
        self.status = None;
        self.databases = Fetch::Loaded(Vec::new());
        self.collections.clear();
        self.selected_collection = None;
        self.page = None;
        self.skip = 0;
        self.keys.clear();
        self.cursor = None;
        self.scanning = false;
        self.scan_error = None;
        self.selected_key = None;
        self.key_detail = None;
        self.tasks.clear();
    }

    fn connect(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(connection) = self.connections.iter().find(|connection| connection.id == id).cloned() else {
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
                let (_, uri) = credential.ok_or_else(|| anyhow!("No connection string saved. Edit the connection."))?;
                let uri = String::from_utf8(uri).context("The saved connection string is unreadable")?;
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

    fn open_collection(&mut self, database: String, collection: String, cx: &mut Context<Self>) {
        self.selected_collection = Some((database, collection));
        self.skip = 0;
        self.find(cx);
    }

    fn find(&mut self, cx: &mut Context<Self>) {
        let (Some(session), Some((database, collection))) = (self.mongo(), self.selected_collection.clone()) else {
            return;
        };
        let parse = |editor: &Entity<Editor>, label: &str, cx: &App| {
            let text = editor.read(cx).text(cx);
            parse_document(&text).map_err(|error| anyhow!("{label}: {error:#}"))
        };
        let parsed = (|| {
            let filter = parse(&self.filter, "Filter", cx)?;
            let projection = parse(&self.projection, "Project", cx)?;
            let sort = parse(&self.sort, "Sort", cx)?;
            anyhow::Ok((filter, projection, sort))
        })();
        let (filter, projection, sort) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                self.page = Some(Fetch::Failed(format!("{error:#}").into()));
                cx.notify();
                return;
            }
        };
        let request = FindRequest {
            database,
            collection,
            filter,
            projection: (!projection.is_empty()).then_some(projection),
            sort: (!sort.is_empty()).then_some(sort),
            skip: self.skip,
            page_size: self.page_size,
            max_time: Some(QUERY_TIME_LIMIT),
        };
        self.generation += 1;
        let generation = self.generation;
        self.page = Some(Fetch::Loading);
        let task = run(cx, async move { session.find_page(&request).await });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await.map(|page| {
                let shown = page.documents.len();
                // Formatting happens off the UI thread; the list then only hands out shared lines.
                let mut lines = Vec::new();
                for document in &page.documents {
                    lines.extend(to_display_json(document).lines().map(|line| SharedString::from(line.to_string())));
                    lines.push(SharedString::default());
                }
                MongoPage { lines, shown, has_more: page.has_more }
            });
            this.update(cx, |this, cx| {
                if this.generation == generation {
                    this.page = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn scan(&mut self, restart: bool, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        if restart {
            self.generation += 1;
            self.keys.clear();
            self.cursor = Some("0".into());
            self.selected_key = None;
            self.key_detail = None;
        }
        let Some(cursor) = self.cursor.clone() else {
            return;
        };
        let pattern = self.pattern.read(cx).text(cx);
        let pattern = if pattern.trim().is_empty() { "*".to_string() } else { pattern };
        self.scanning = true;
        self.scan_error = None;
        let generation = self.generation;
        let task = run(cx, async move { session.scan(&cursor, &pattern, SCAN_COUNT, None).await });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.scanning = false;
                match result {
                    Ok(page) => {
                        // SCAN may return a key again on a later page.
                        for key in page.keys {
                            if !this.keys.contains(&key) {
                                this.keys.push(key);
                            }
                        }
                        this.cursor = (page.cursor != "0").then_some(page.cursor);
                    }
                    Err(error) => this.scan_error = Some(format!("{error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_key(&mut self, key: Vec<u8>, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        self.selected_key = Some(key.clone());
        self.key_detail = Some(Fetch::Loading);
        let generation = self.generation;
        let task = run(cx, {
            let key = key.clone();
            async move {
                let info = session.key_info(&key).await?;
                let value = if info.kind == "string" { session.get_string(&key).await? } else { None };
                Ok(KeyDetail { info, value })
            }
        });
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == generation && this.selected_key.as_ref() == Some(&key) {
                    this.key_detail = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        self.form = Some(Form {
            kind: existing.as_ref().map_or(Kind::MongoDb, |c| c.kind),
            name: editor("Name, e.g. CP fpms", &existing.as_ref().map(|c| c.name.clone()).unwrap_or_default(), false, window, cx),
            environment: editor("Environment, e.g. QAT", &existing.as_ref().map(|c| c.environment.clone()).unwrap_or_default(), false, window, cx),
            uri: editor(
                if existing.is_some() { "Leave empty to keep the saved one" } else { "mongodb://… or redis://…" },
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
        let (name, environment, uri) = (text(&form.name, cx), text(&form.environment, cx), text(&form.uri, cx));
        if name.is_empty() || environment.is_empty() {
            form.message = Some("Fill in a name and an environment.".into());
            cx.notify();
            return;
        }
        let existing = form.editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        if uri.is_empty() && existing.as_ref().is_none_or(|existing| existing.kind != form.kind) {
            form.message = Some("Enter the connection string.".into());
            cx.notify();
            return;
        }
        let connection = Connection {
            id: existing.as_ref().map_or_else(|| uuid::Uuid::new_v4().simple().to_string(), |c| c.id.clone()),
            name,
            kind: form.kind,
            environment,
        };
        form.checking = true;
        form.message = Some("Connecting…".into());
        form._task = Some(cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                if !uri.is_empty() {
                    let expected = match connection.kind {
                        Kind::MongoDb => uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://"),
                        Kind::Redis => uri.starts_with("redis://") || uri.starts_with("rediss://"),
                    };
                    anyhow::ensure!(expected, "That is not a {} connection string.", connection.kind.label());
                    connect(&connection, uri.clone(), cx).await?;
                    let url = connection.credential_url();
                    cx.update(|cx| cx.write_credentials(&url, connection.kind.label(), uri.as_bytes()))
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
    h_flex()
        .h(px(26.))
        .pl_3()
        .pr_1()
        .justify_between()
        .child(Label::new(title).size(LabelSize::XSmall).weight(gpui::FontWeight::SEMIBOLD).color(Color::Muted))
}

fn boxed(child: impl IntoElement, cx: &App) -> gpui::Div {
    let colors = cx.theme().colors();
    div().px_2().py_1().rounded_sm().border_1().border_color(colors.border).bg(colors.editor_background).child(child)
}

/// Monospaced lines in a virtual list; long lines scroll sideways so every row keeps one height.
fn render_lines(id: &'static str, lines: Vec<SharedString>, scroll: &UniformListScrollHandle, cx: &mut Context<DatabaseView>) -> AnyElement {
    let colors = cx.theme().colors().clone();
    let widest = lines.iter().enumerate().max_by_key(|(_, line)| line.len()).map(|(index, _)| index);
    let list = uniform_list(id, lines.len(), move |range, _, cx| {
        range
            .filter_map(|index| lines.get(index).cloned())
            .map(|line| div().px_2().whitespace_nowrap().child(Label::new(line).size(LabelSize::Small).buffer_font(cx)).into_any_element())
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
        let mut connections = v_flex().gap_0p5().px_1();
        for connection in &self.connections {
            let id = connection.id.clone();
            connections = connections.child(
                h_flex()
                    .id(SharedString::from(format!("database-connection-{}", connection.id)))
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .cursor_pointer()
                    .when(self.active.as_ref() == Some(&connection.id), |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .on_click(cx.listener(move |this, _, _, cx| this.connect(id.clone(), cx)))
                    .child(Icon::new(IconName::DatabaseZap).size(IconSize::Small).color(Color::Muted))
                    .child(Label::new(connection.name.clone()).truncate())
                    .child(div().flex_1())
                    .child(
                        Label::new(connection.environment.clone())
                            .size(LabelSize::XSmall)
                            .color(if connection.production() { Color::Warning } else { Color::Muted }),
                    ),
            );
        }
        let mut tree = v_flex().gap_0p5().px_1();
        if matches!(self.session, Some(Session::Mongo(_))) {
            match &self.databases {
                Fetch::Loading => tree = tree.child(div().px_3().child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted))),
                Fetch::Failed(message) => {
                    tree = tree.child(div().px_3().child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning)))
                }
                Fetch::Loaded(databases) => {
                    for database in databases {
                        let open = self.collections.get(&database.name);
                        let name = database.name.clone();
                        tree = tree.child(
                            h_flex()
                                .id(SharedString::from(format!("database-db-{}", database.name)))
                                .px_2()
                                .py_0p5()
                                .gap_1()
                                .rounded_sm()
                                .cursor_pointer()
                                .hover(|style| style.bg(colors.element_hover))
                                .on_click(cx.listener(move |this, _, _, cx| this.toggle_database(name.clone(), cx)))
                                .child(
                                    Icon::new(if open.is_some() { IconName::ChevronDown } else { IconName::ChevronRight })
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                )
                                .child(Label::new(database.name.clone()).size(LabelSize::Small).truncate()),
                        );
                        match open {
                            Some(Fetch::Loading) => {
                                tree = tree.child(div().pl_8().child(Label::new("Loading…").size(LabelSize::XSmall).color(Color::Muted)))
                            }
                            Some(Fetch::Failed(message)) => {
                                tree = tree.child(div().pl_8().child(Label::new(message.clone()).size(LabelSize::XSmall).color(Color::Warning)))
                            }
                            Some(Fetch::Loaded(collections)) => {
                                for collection in collections {
                                    let selected = self.selected_collection.as_ref()
                                        == Some(&(database.name.clone(), collection.name.clone()));
                                    let (database_name, collection_name) = (database.name.clone(), collection.name.clone());
                                    tree = tree.child(
                                        h_flex()
                                            .id(SharedString::from(format!("database-coll-{}-{}", database.name, collection.name)))
                                            .pl_6()
                                            .pr_2()
                                            .py_0p5()
                                            .rounded_sm()
                                            .cursor_pointer()
                                            .when(selected, |this| this.bg(colors.element_selected))
                                            .hover(|style| style.bg(colors.element_hover))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.open_collection(database_name.clone(), collection_name.clone(), cx)
                                            }))
                                            .child(Label::new(collection.name.clone()).size(LabelSize::Small).truncate())
                                            .when(collection.kind != "collection", |this| {
                                                this.child(div().flex_1()).child(Label::new(collection.kind.clone()).size(LabelSize::XSmall).color(Color::Muted))
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
        v_flex()
            .w(px(300.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(
                section("CONNECTIONS").child(
                    IconButton::new("database-add-connection", IconName::Plus)
                        .icon_size(IconSize::Small)
                        .tooltip(Tooltip::text("Add a MongoDB or Redis connection"))
                        .on_click(cx.listener(|this, _, window, cx| this.open_form(None, window, cx))),
                ),
            )
            .child(connections)
            .when(matches!(self.session, Some(Session::Mongo(_))), |this| {
                this.child(
                    section("DATABASES").mt_2().border_t_1().border_color(colors.border_variant).child(
                        IconButton::new("database-reload", IconName::RotateCw)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Reload databases"))
                            .on_click(cx.listener(|this, _, _, cx| this.load_databases(cx))),
                    ),
                )
                .child(v_flex().id("database-tree").flex_1().min_h_0().overflow_y_scroll().pb_2().child(tree))
            })
    }

    fn render_form(&self, form: &Form, cx: &mut Context<Self>) -> AnyElement {
        let input = |label: &'static str, editor: &Entity<Editor>, cx: &App| {
            v_flex().gap_1().child(Label::new(label).size(LabelSize::Small).color(Color::Muted)).child(boxed(editor.clone(), cx))
        };
        let kind_button = |id: &'static str, kind: Kind, cx: &mut Context<Self>| {
            Button::new(id, kind.label())
                .label_size(LabelSize::Small)
                .style(if form.kind == kind { ButtonStyle::Filled } else { ButtonStyle::Subtle })
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

    fn render_header(&self, connection: &Connection, title: String, cx: &mut Context<Self>) -> impl IntoElement {
        let id = connection.id.clone();
        h_flex()
            .gap_2()
            .child(Headline::new(title).size(HeadlineSize::Small))
            .child(Label::new(format!("{} · {}", connection.kind.label(), connection.environment)).size(LabelSize::Small).color(
                if connection.production() { Color::Warning } else { Color::Muted },
            ))
            .child(div().flex_1())
            .child(
                Button::new("database-edit", "Edit connection")
                    .label_size(LabelSize::Small)
                    .on_click(cx.listener(move |this, _, window, cx| this.open_form(Some(id.clone()), window, cx))),
            )
    }

    fn render_mongo(&self, connection: &Connection, cx: &mut Context<Self>) -> AnyElement {
        let Some((database, collection)) = self.selected_collection.clone() else {
            return v_flex()
                .gap_3()
                .child(self.render_header(connection, connection.name.clone(), cx))
                .child(Label::new("Pick a collection on the left.").color(Color::Muted))
                .into_any_element();
        };
        let page_button = |size: u32, cx: &mut Context<Self>| {
            Button::new(SharedString::from(format!("database-page-size-{size}")), size.to_string())
                .label_size(LabelSize::Small)
                .style(if self.page_size == size { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.page_size = size;
                    this.skip = 0;
                    this.find(cx);
                }))
        };
        let (shown, has_more) = match &self.page {
            Some(Fetch::Loaded(page)) => (page.shown, page.has_more),
            _ => (0, false),
        };
        let skip = self.skip;
        let page_size = u64::from(self.page_size);
        let status = match &self.page {
            Some(Fetch::Loaded(_)) if shown == 0 => "No documents match.".to_string(),
            Some(Fetch::Loaded(_)) => format!("{}–{}{}", skip + 1, skip + shown as u64, if has_more { " · more match" } else { "" }),
            Some(Fetch::Loading) => "Running…".into(),
            _ => String::new(),
        };
        let body = match &self.page {
            Some(Fetch::Failed(message)) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Error).into_any_element(),
            Some(Fetch::Loaded(page)) if !page.lines.is_empty() => render_lines("database-documents", page.lines.clone(), &self.lines_scroll, cx),
            _ => div().into_any_element(),
        };
        v_flex()
            .size_full()
            .gap_2()
            .child(self.render_header(connection, format!("{database}.{collection}"), cx))
            .child(
                h_flex()
                    .gap_2()
                    .child(div().flex_1().min_w_0().child(boxed(self.filter.clone(), cx)))
                    .child(div().w(px(200.)).child(boxed(self.projection.clone(), cx)))
                    .child(div().w(px(160.)).child(boxed(self.sort.clone(), cx)))
                    .child(
                        Button::new("database-find", "Find")
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.skip = 0;
                                this.find(cx);
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_1()
                    .child(Label::new("Per page").size(LabelSize::Small).color(Color::Muted))
                    .children(PAGE_SIZES.map(|size| page_button(size, cx)))
                    .child(div().w(px(12.)))
                    .child(Label::new(status).size(LabelSize::Small).color(Color::Muted))
                    .child(div().flex_1())
                    .child(
                        IconButton::new("database-previous", IconName::ChevronLeft)
                            .icon_size(IconSize::Small)
                            .disabled(skip == 0)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.skip = skip.saturating_sub(page_size);
                                this.find(cx);
                            })),
                    )
                    .child(
                        IconButton::new("database-next", IconName::ChevronRight)
                            .icon_size(IconSize::Small)
                            .disabled(!has_more)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.skip = skip + page_size;
                                this.find(cx);
                            })),
                    )
                    .child(
                        IconButton::new("database-refresh", IconName::RotateCw)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Run the query again"))
                            .on_click(cx.listener(|this, _, _, cx| this.find(cx))),
                    ),
            )
            .child(body)
            .into_any_element()
    }

    fn render_redis(&self, connection: &Connection, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let keys = self.keys.clone();
        let selected = self.selected_key.clone();
        let view = cx.entity().downgrade();
        let list = uniform_list("database-keys", keys.len(), move |range, _, cx| {
            range
                .filter_map(|index| keys.get(index).cloned())
                .map(|key| {
                    let is_selected = selected.as_ref() == Some(&key);
                    let view = view.clone();
                    let label = escape_bytes(&key);
                    h_flex()
                        .id(SharedString::from(format!("database-key-{}", hex_bytes(&key))))
                        .px_2()
                        .cursor_pointer()
                        .when(is_selected, |this| this.bg(cx.theme().colors().element_selected))
                        .hover(|style| style.bg(cx.theme().colors().element_hover))
                        .on_click(move |_, _, cx| {
                            let key = key.clone();
                            view.update(cx, |this, cx| this.open_key(key, cx)).ok();
                        })
                        .child(Label::new(label).size(LabelSize::Small).buffer_font(cx).truncate())
                        .into_any_element()
                })
                .collect::<Vec<_>>()
        })
        .track_scroll(&self.keys_scroll)
        .flex_1();
        let detail = match &self.key_detail {
            None => Label::new("Pick a key.").size(LabelSize::Small).color(Color::Muted).into_any_element(),
            Some(Fetch::Loading) => Label::new("Loading…").size(LabelSize::Small).color(Color::Muted).into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Error).into_any_element(),
            Some(Fetch::Loaded(detail)) => {
                let key = self.selected_key.clone().unwrap_or_default();
                let value = match (&detail.value, detail.info.kind.as_str()) {
                    (Some(value), _) => {
                        let shown = if self.hex {
                            hex_bytes(value)
                        } else {
                            String::from_utf8(value.clone()).unwrap_or_else(|_| escape_bytes(value))
                        };
                        let lines = shown.lines().map(|line| SharedString::from(line.to_string())).collect::<Vec<_>>();
                        render_lines("database-value", lines, &self.lines_scroll, cx)
                    }
                    (None, "none") => Label::new("The key no longer exists.").size(LabelSize::Small).color(Color::Warning).into_any_element(),
                    (None, kind) => Label::new(format!("A viewer for {kind} values comes next.")).size(LabelSize::Small).color(Color::Muted).into_any_element(),
                };
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .gap_2()
                    .child(Label::new(escape_bytes(&key)).buffer_font(cx))
                    .child(
                        h_flex()
                            .gap_3()
                            .child(Label::new(detail.info.kind.clone()).size(LabelSize::Small))
                            .child(Label::new(format!("TTL: {}", describe_ttl(detail.info.ttl_ms))).size(LabelSize::Small).color(Color::Muted))
                            .child(div().flex_1())
                            .when(detail.value.is_some(), |this| {
                                this.child(
                                    Button::new("database-hex", if self.hex { "Show text" } else { "Show hex" })
                                        .label_size(LabelSize::Small)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.hex = !this.hex;
                                            cx.notify();
                                        })),
                                )
                            }),
                    )
                    .child(value)
                    .into_any_element()
            }
        };
        v_flex()
            .size_full()
            .gap_2()
            .child(self.render_header(connection, connection.name.clone(), cx))
            .child(
                h_flex()
                    .gap_2()
                    .child(div().flex_1().min_w_0().child(boxed(self.pattern.clone(), cx)))
                    .child(Button::new("database-scan", "Scan").style(ButtonStyle::Filled).on_click(cx.listener(|this, _, _, cx| this.scan(true, cx)))),
            )
            .child(
                h_flex()
                    .flex_1()
                    .min_h_0()
                    .gap_3()
                    .child(
                        v_flex()
                            .w(px(360.))
                            .flex_none()
                            .h_full()
                            .gap_1()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .child(
                                        Label::new(format!(
                                            "{} keys{}",
                                            self.keys.len(),
                                            if self.cursor.is_some() { " so far" } else { "" }
                                        ))
                                        .size(LabelSize::Small)
                                        .color(Color::Muted),
                                    )
                                    .child(div().flex_1())
                                    .when(self.cursor.is_some(), |this| {
                                        this.child(
                                            Button::new("database-scan-more", if self.scanning { "Scanning…" } else { "Load more" })
                                                .label_size(LabelSize::Small)
                                                .disabled(self.scanning)
                                                .on_click(cx.listener(|this, _, _, cx| this.scan(false, cx))),
                                        )
                                    }),
                            )
                            .when_some(self.scan_error.clone(), |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Error)))
                            .child(
                                v_flex()
                                    .flex_1()
                                    .min_h_0()
                                    .py_1()
                                    .rounded_md()
                                    .border_1()
                                    .border_color(colors.border)
                                    .bg(colors.editor_background)
                                    .child(list),
                            ),
                    )
                    .child(v_flex().flex_1().min_w_0().h_full().child(detail)),
            )
            .into_any_element()
    }
}

impl Render for DatabaseView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let connection = self.active_connection().cloned();
        let main = match (&self.form, &connection, &self.session) {
            (Some(form), _, _) => self.render_form(form, cx),
            (None, Some(connection), Some(Session::Mongo(_))) => self.render_mongo(connection, cx),
            (None, Some(connection), Some(Session::Redis(_))) => self.render_redis(connection, cx),
            (None, Some(connection), None) => v_flex()
                .gap_3()
                .child(self.render_header(connection, connection.name.clone(), cx))
                .child(Label::new(self.status.clone().unwrap_or_else(|| "Not connected.".into())).color(Color::Muted))
                .into_any_element(),
            (None, None, _) => Label::new("Pick a connection on the left, or add one.").color(Color::Muted).into_any_element(),
        };
        h_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().colors().editor_background)
            .child(self.render_sidebar(cx))
            .child(v_flex().flex_1().min_w_0().h_full().p_4().child(main))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_never_look_alike() {
        assert_eq!(escape_bytes(b"user:1"), "user:1");
        assert_eq!(escape_bytes(b"a\x00\xff"), "a\\x00\\xff");
        // A real backslash must not read like an escape.
        assert_eq!(escape_bytes(b"a\\x00"), "a\\\\x00");
        assert_ne!(escape_bytes(b"a\\x00"), escape_bytes(b"a\x00"));
        assert_eq!(hex_bytes(b"\x01\xab"), "01 ab");
        assert_eq!(describe_ttl(-1), "no expiry");
        assert_eq!(describe_ttl(-2), "key does not exist");
        assert_eq!(describe_ttl(65_000), "65 s");
    }
}
