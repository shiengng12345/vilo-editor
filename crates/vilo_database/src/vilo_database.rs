mod mongo_tab;
mod mongo_write;
mod redis_analyze;
mod redis_bulk;
mod redis_profiler;
mod redis_pubsub;
mod redis_search;
mod redis_stream;
mod redis_view;
mod redis_workbench;
mod redis_write;

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
use ui::{Button, ButtonStyle, ContextMenu, IconButton, Tooltip, prelude::*, right_click_menu};
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
    /// Saved only for non-production connections; production asks again on every run of Vilo.
    #[serde(default)]
    allow_writes: bool,
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-database://{}", self.id)
    }

    fn at(&self, level: Level) -> &str {
        match level {
            Level::Site => self.site.trim(),
            Level::Environment => self.environment.trim(),
        }
    }

    fn label_at(&self, level: Level) -> String {
        match self.at(level) {
            "" => UNASSIGNED_SITE.to_string(),
            name => name.to_string(),
        }
    }

    fn set_at(&mut self, level: Level, name: String) {
        match level {
            Level::Site => self.site = name,
            Level::Environment => self.environment = name,
        }
    }

    fn site_label(&self) -> String {
        self.label_at(Level::Site)
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

/// The two lists the rail keeps after the kind: sites (CP, BP …) and environments (QAT, UAT …).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Level {
    Site = 0,
    Environment = 1,
}

impl Level {
    fn noun(self) -> &'static str {
        match self {
            Level::Site => "site",
            Level::Environment => "environment",
        }
    }

    fn placeholder(self) -> &'static str {
        match self {
            Level::Site => "e.g. CP",
            Level::Environment => "e.g. QAT",
        }
    }

    fn path(self) -> PathBuf {
        paths::config_dir().join(match self {
            Level::Site => "database_sites.json",
            Level::Environment => "database_environments.json",
        })
    }
}

#[derive(Clone, PartialEq)]
enum Pick {
    Kind(Kind),
    Name(Level, String),
}

/// A site or environment being added (`original` is `None`) or renamed in the rail.
struct NameEdit {
    level: Level,
    original: Option<String>,
    editor: Entity<Editor>,
}

/// Present when the site, environment and connection columns are pinned open.
fn rail_pin_path() -> PathBuf {
    paths::config_dir().join("database_rail_pinned")
}

fn write_names(level: Level, names: Vec<String>, cx: &mut App) {
    cx.background_spawn(async move {
        let path = level.path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("json.tmp");
        std::fs::write(&temporary, serde_json::to_vec_pretty(&names)?)?;
        std::fs::rename(&temporary, &path)?;
        anyhow::Ok(())
    })
    .detach_and_log_err(cx);
}

/// This run's unlogged writes first, keeping their reason, then the log's other dangling writes.
fn writes_to_check(
    unlogged: &[(String, String)],
    dangling: &[(String, String)],
) -> Vec<(String, String)> {
    let mut writes = unlogged.to_vec();
    for entry in dangling {
        if !writes.iter().any(|known| known.0 == entry.0) {
            writes.push(entry.clone());
        }
    }
    writes
}

/// Checks a name typed in the rail against the others in its list (case is ignored).
fn check_name(name: &str, others: &[String]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Type a name.");
    }
    if name.eq_ignore_ascii_case(UNASSIGNED_SITE) {
        return Err("That name is reserved.");
    }
    if others.iter().any(|other| other.eq_ignore_ascii_case(name)) {
        return Err("That name already exists.");
    }
    Ok(())
}

struct RedisFields {
    host: Entity<Editor>,
    port: Entity<Editor>,
    username: Entity<Editor>,
    password: Entity<Editor>,
    database: Entity<Editor>,
    tls: bool,
}

/// The parts of a Redis connection, as the form shows them.
#[derive(Debug, Default, PartialEq)]
struct RedisParts {
    host: String,
    port: String,
    username: String,
    password: String,
    database: String,
    tls: bool,
}

impl RedisParts {
    fn to_uri(&self) -> Result<String> {
        let scheme = if self.tls { "rediss" } else { "redis" };
        let port = if self.port.is_empty() {
            "6379"
        } else {
            &self.port
        };
        port.parse::<u16>().context("Port must be a number")?;
        let mut url = url::Url::parse(&format!("{scheme}://{}:{port}", self.host))
            .context("Host is not valid")?;
        // The url crate percent-encodes, so passwords with @ or / survive.
        url.set_username(&self.username)
            .map_err(|_| anyhow!("Username is not valid"))?;
        if !self.password.is_empty() {
            url.set_password(Some(&self.password))
                .map_err(|_| anyhow!("Password is not valid"))?;
        }
        if !self.database.is_empty() {
            self.database
                .parse::<u8>()
                .context("Database must be a number from 0")?;
            url.set_path(&format!("/{}", self.database));
        }
        Ok(url.to_string())
    }

    fn from_uri(uri: &str) -> Option<Self> {
        let url = url::Url::parse(uri).ok()?;
        let decode = |text: &str| {
            urlencoding::decode(text)
                .map(|text| text.into_owned())
                .unwrap_or_else(|_| text.to_string())
        };
        Some(Self {
            host: url.host_str()?.to_string(),
            port: url.port().map(|port| port.to_string()).unwrap_or_default(),
            username: decode(url.username()),
            password: url.password().map(decode).unwrap_or_default(),
            database: url.path().trim_start_matches('/').to_string(),
            tls: url.scheme() == "rediss",
        })
    }
}

struct Form {
    editing: Option<String>,
    kind: Kind,
    /// Taken from the rail; a connection that already has a site or environment keeps it.
    site: String,
    name: Entity<Editor>,
    environment: String,
    uri: Entity<Editor>,
    /// Redis is entered as host, port and so on, like RedisInsight, and saved as a redis:// URL.
    redis: Option<RedisFields>,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

pub struct DatabaseView {
    focus_handle: FocusHandle,
    /// For opening mongosh in a Vilo terminal.
    workspace: gpui::WeakEntity<Workspace>,
    /// JavaScript, for self-closing brackets and highlighting in the query fields; loaded once.
    query_language: Option<Arc<language::Language>>,
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
    /// MongoDB or Redis: the rail on the far left picks which kind the sidebar and page show.
    kind_view: Kind,
    /// The site and environment whose connections the sidebar lists, by `Level`.
    selected: [Option<String>; 2],
    /// Sites and environments the user keeps, shared by MongoDB and Redis, by `Level`.
    names: [Vec<String>; 2],
    name_edit: Option<NameEdit>,
    name_message: Option<(Level, SharedString)>,
    /// Searches connections and collections at once, keeping each match's parents visible.
    search: Entity<Editor>,
    tabs: Vec<MongoTab>,
    active_tab: Option<usize>,
    history: Vec<SavedQuery>,
    /// Readable dates in this Mac's time zone instead of UTC.
    local_dates: bool,
    pattern: Entity<Editor>,
    /// Searches the values of the open Redis key.
    value_search: Entity<Editor>,
    /// Searches the Command Helper's offline documentation.
    helper_search: Entity<Editor>,
    workbench_input: Entity<Editor>,
    /// Workbench read commands, oldest first, kept in database_redis_history.json.
    command_history: Vec<String>,
    /// Where Up/Down stands in `command_history` while browsing it.
    history_position: Option<usize>,
    /// Why the command history could not be read or saved.
    history_problem: Option<SharedString>,
    redis_state: Option<RedisState>,
    doc_write: Option<mongo_write::DocWrite>,
    redis_write: Option<redis_write::RedisWriteForm>,
    admin_write: Option<redis_analyze::AdminWriteForm>,
    bulk_delete: Option<redis_bulk::BulkForm>,
    workbench_run: Option<redis_workbench::WorkbenchRun>,
    /// Production connections with writes switched on for this run of Vilo only.
    session_writes: collections::HashSet<String>,
    /// Documents whose last write has an unknown outcome; writing to them waits for a refresh.
    unknown_writes: collections::HashSet<String>,
    /// The write log was read since start, so locks of unsettled publishes are restored.
    write_log_checked: bool,
    write_notice: Option<SharedString>,
    /// Writes from earlier runs whose result never reached the write log.
    dangling_writes: Vec<(String, String)>,
    /// Writes of this run whose result line could not be saved. Kept apart from the list read
    /// back from the log, because a line can be readable even though saving it failed.
    unlogged_writes: Vec<(String, String)>,
    /// Operation ids in `unlogged_writes` that are administration writes; until they are checked
    /// no new administration write starts.
    admin_to_check: collections::HashSet<String>,
    /// Why the write log could not be read or updated, shown until it works again.
    audit_problem: Option<SharedString>,
    /// While a page is open the site, environment and connection columns fold away; hovering the
    /// kind icons shows them over the page.
    rail_peek: bool,
    /// Pinned columns stay in place even while a page is open.
    rail_pinned: bool,
    /// Resting the pointer on a kind, site or environment picks it after a short pause.
    hover_pick: Option<(Pick, Task<()>)>,
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
        // Saved names, plus any a connection uses that the list does not have yet.
        let names = [Level::Site, Level::Environment].map(|level| {
            let mut names: Vec<String> = std::fs::read(level.path())
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).log_err())
                .unwrap_or_default();
            for connection in &connections {
                let name = connection.at(level);
                if !name.is_empty() && !names.iter().any(|known| known == name) {
                    names.push(name.to_string());
                }
            }
            names
        });
        let search = editor("Search connections and collections", "", false, window, cx);
        let helper_search = editor("Search for a command", "", false, window, cx);
        let workbench_input = {
            let input = editor(
                "Enter a Redis command, e.g. HGETALL user:1",
                "",
                false,
                window,
                cx,
            );
            input.update(cx, |input, _| {
                input
                    .set_completion_provider(Some(std::rc::Rc::new(redis_view::CommandCompletions)))
            });
            input
        };
        let subscriptions = vec![
            // The Command Helper follows the search and the command being typed.
            cx.subscribe(
                &helper_search,
                |this, _, event: &editor::EditorEvent, cx| {
                    // A new search replaces the command picked from the last one.
                    if matches!(event, editor::EditorEvent::Edited { .. })
                        && let Some(state) = this.redis_state.as_mut()
                    {
                        state.clear_helper_pick();
                    }
                    cx.notify();
                },
            ),
            cx.subscribe(&workbench_input, |_, _, _: &editor::EditorEvent, cx| {
                cx.notify()
            }),
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
            kind_view: Kind::MongoDb,
            selected: [None, None],
            names,
            name_edit: None,
            name_message: None,
            search,
            tabs: Vec::new(),
            active_tab: None,
            history: load_history(),
            local_dates: false,
            pattern: editor("Filter by Key Name or Pattern", "", false, window, cx),
            value_search: editor("Search this key's values (Enter)", "", false, window, cx),
            helper_search,
            workbench_input,
            command_history: Vec::new(),
            history_position: None,
            history_problem: None,
            redis_state: None,
            doc_write: None,
            redis_write: None,
            admin_write: None,
            bulk_delete: None,
            workbench_run: None,
            session_writes: Default::default(),
            unknown_writes: Default::default(),
            write_log_checked: false,
            write_notice: None,
            dangling_writes: Vec::new(),
            unlogged_writes: Vec::new(),
            admin_to_check: Default::default(),
            audit_problem: None,
            rail_peek: false,
            query_language: None,
            hover_pick: None,
            rail_pinned: rail_pin_path().exists(),
            tasks: Vec::new(),
            _subscriptions: subscriptions,
        };
        view.check_write_log(cx);
        match redis_view::load_command_history() {
            Ok(history) => view.command_history = history,
            Err(error) => {
                view.history_problem =
                    Some(format!("Command history could not be read: {error:#}").into())
            }
        }
        let workspace = view.workspace.clone();
        cx.spawn(async move |this, cx| {
            let language = workspace
                .read_with(cx, |workspace, cx| {
                    workspace
                        .project()
                        .read(cx)
                        .languages()
                        .language_for_name("JavaScript")
                })?
                .await?;
            this.update(cx, |this, cx| {
                for tab in &this.tabs {
                    tab.set_query_language(&language, cx);
                }
                this.query_language = Some(language);
            })
        })
        .detach_and_log_err(cx);
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

    /// The user's names at this level, sorted.
    fn names(&self, level: Level) -> Vec<String> {
        let mut names = self.names[level as usize].clone();
        names.sort_by_key(|name| name.to_lowercase());
        names
    }

    fn start_name_edit(
        &mut self,
        level: Level,
        original: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let editor = editor(
            level.placeholder(),
            original.as_deref().unwrap_or_default(),
            false,
            window,
            cx,
        );
        window.focus(&editor.focus_handle(cx), cx);
        self.name_edit = Some(NameEdit {
            level,
            original,
            editor,
        });
        self.name_message = None;
        cx.notify();
    }

    fn save_name_edit(&mut self, cx: &mut Context<Self>) {
        let Some(edit) = &self.name_edit else {
            return;
        };
        let level = edit.level;
        let name = edit.editor.read(cx).text(cx).trim().to_string();
        let original = edit.original.clone();
        let list = &mut self.names[level as usize];
        let others = list
            .iter()
            .filter(|known| Some(*known) != original.as_ref())
            .cloned()
            .collect::<Vec<_>>();
        if let Err(message) = check_name(&name, &others) {
            self.name_message = Some((level, message.into()));
            cx.notify();
            return;
        }
        match original {
            Some(old) => {
                for known in list.iter_mut() {
                    if *known == old {
                        *known = name.clone();
                    }
                }
                // Connections move with the renamed site or environment.
                let mut moved = false;
                for connection in &mut self.connections {
                    if connection.at(level) == old {
                        connection.set_at(level, name.clone());
                        moved = true;
                    }
                }
                if moved {
                    self.persist(cx).detach_and_log_err(cx);
                }
                let selected = &mut self.selected[level as usize];
                if selected.as_deref() == Some(old.as_str()) {
                    *selected = Some(name);
                }
            }
            None => {
                list.push(name.clone());
                self.selected[level as usize] = Some(name);
            }
        }
        self.name_edit = None;
        self.name_message = None;
        write_names(level, self.names[level as usize].clone(), cx);
        cx.notify();
    }

    fn delete_name(&mut self, level: Level, name: String, cx: &mut Context<Self>) {
        let used = self
            .connections
            .iter()
            .filter(|connection| connection.at(level) == name)
            .count();
        if used > 0 {
            self.name_message = Some((
                level,
                format!(
                    "{name} has {used} connection{}; move or delete them first.",
                    if used == 1 { "" } else { "s" }
                )
                .into(),
            ));
            cx.notify();
            return;
        }
        self.names[level as usize].retain(|known| *known != name);
        let selected = &mut self.selected[level as usize];
        if selected.as_deref() == Some(name.as_str()) {
            *selected = None;
        }
        self.name_message = None;
        write_names(level, self.names[level as usize].clone(), cx);
        cx.notify();
    }

    fn current(&self, level: Level) -> String {
        let names = self.names(level);
        self.selected[level as usize]
            .clone()
            .filter(|name| names.contains(name))
            .or_else(|| names.first().cloned())
            .unwrap_or_default()
    }

    /// Picks `pick` once the pointer has rested on it; passing over on the way elsewhere picks nothing.
    fn hover_pick(&mut self, hovered: bool, pick: Pick, cx: &mut Context<Self>) {
        // Leaving one row and entering the next can arrive in either order; only cancel your own.
        if !hovered {
            if self
                .hover_pick
                .as_ref()
                .is_some_and(|(pending, _)| *pending == pick)
            {
                self.hover_pick = None;
            }
            return;
        }
        let task = cx.spawn({
            let pick = pick.clone();
            async move |this, cx| {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(100))
                    .await;
                this.update(cx, |this, cx| {
                    match pick {
                        Pick::Kind(kind) => this.kind_view = kind,
                        Pick::Name(level, name) => this.selected[level as usize] = Some(name),
                    }
                    this.form = None;
                    this.hover_pick = None;
                    cx.notify();
                })
                .ok();
            }
        });
        self.hover_pick = Some((pick, task));
    }

    /// Whether the connection belongs to the kind, site and environment picked in the rail.
    /// A connection without a site or environment shows under every one, so none is ever hidden.
    fn in_view(&self, connection: &Connection) -> bool {
        connection.kind == self.kind_view
            && [Level::Site, Level::Environment].iter().all(|level| {
                let current = self.current(*level);
                current.is_empty()
                    || connection.at(*level).is_empty()
                    || connection.at(*level) == current
            })
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
        // Its preview belongs to the session that is going; a run keeps its own session.
        if self
            .bulk_delete
            .as_ref()
            .is_some_and(redis_bulk::BulkForm::closable)
        {
            self.bulk_delete = None;
        }
        if self
            .workbench_run
            .as_ref()
            .is_some_and(redis_workbench::WorkbenchRun::closable)
        {
            self.workbench_run = None;
        }
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
        if self.active.as_ref() == Some(&id) && self.session.is_some() {
            self.form = None;
            cx.notify();
            return;
        }
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
                            this.load_redis_databases(cx);
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
            kind: existing.as_ref().map_or(self.kind_view, |c| c.kind),
            site: existing
                .as_ref()
                .map(|c| c.site.trim().to_string())
                .filter(|site| !site.is_empty())
                .unwrap_or_else(|| self.current(Level::Site)),
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
            environment: existing
                .as_ref()
                .map(|c| c.environment.trim().to_string())
                .filter(|environment| !environment.is_empty())
                .unwrap_or_else(|| self.current(Level::Environment)),
            uri: editor(
                if existing.is_some() {
                    "Leave empty to keep the saved one"
                } else {
                    "mongodb://… or mongodb+srv://…"
                },
                "",
                false,
                window,
                cx,
            ),
            redis: (existing.as_ref().map_or(self.kind_view, |c| c.kind) == Kind::Redis).then(
                || RedisFields {
                    host: editor("e.g. 127.0.0.1", "", false, window, cx),
                    port: editor("6379", "", false, window, cx),
                    username: editor("default", "", false, window, cx),
                    password: editor(
                        if existing.is_some() {
                            "Leave empty to keep the saved one"
                        } else {
                            ""
                        },
                        "",
                        true,
                        window,
                        cx,
                    ),
                    database: editor("0", "", false, window, cx),
                    tls: false,
                },
            ),
            editing,
            message: None,
            checking: false,
            _task: None,
        });
        // Editing shows the saved connection string so it can be checked or changed in place.
        if let Some(connection) = existing
            && let Some(form) = &mut self.form
        {
            let uri = form.uri.clone();
            let read = cx.read_credentials(&connection.credential_url());
            form._task = Some(cx.spawn_in(window, async move |this, cx| match read.await {
                Ok(Some((_, saved))) => {
                    let saved = String::from_utf8_lossy(&saved).to_string();
                    this.update_in(cx, |this, window, cx| {
                        let Some(form) = &mut this.form else {
                            return;
                        };
                        match (&mut form.redis, RedisParts::from_uri(&saved)) {
                            (Some(redis), Some(parts)) => {
                                for (editor, text) in [
                                    (&redis.host, parts.host),
                                    (&redis.port, parts.port),
                                    (&redis.username, parts.username),
                                    (&redis.password, parts.password),
                                    (&redis.database, parts.database),
                                ] {
                                    editor
                                        .update(cx, |editor, cx| editor.set_text(text, window, cx));
                                }
                                redis.tls = parts.tls;
                            }
                            _ => uri.update(cx, |uri, cx| uri.set_text(saved, window, cx)),
                        }
                        cx.notify();
                    })
                    .log_err();
                }
                Ok(None) => {}
                Err(error) => {
                    log::error!("Could not read the saved connection string: {error:#}")
                }
            }));
        }
        cx.notify();
    }

    fn save_form(&mut self, cx: &mut Context<Self>) {
        let Some(form) = &mut self.form else {
            return;
        };
        let text = |editor: &Entity<Editor>, cx: &App| editor.read(cx).text(cx).trim().to_string();
        let (site, name, environment, mut uri) = (
            form.site.clone(),
            text(&form.name, cx),
            form.environment.clone(),
            text(&form.uri, cx),
        );
        if let Some(redis) = &form.redis {
            let parts = RedisParts {
                host: text(&redis.host, cx),
                port: text(&redis.port, cx),
                username: text(&redis.username, cx),
                password: text(&redis.password, cx),
                database: text(&redis.database, cx),
                tls: redis.tls,
            };
            // An edited connection whose fields never loaded keeps its saved string.
            uri = if parts.host.is_empty() {
                String::new()
            } else {
                match parts.to_uri() {
                    Ok(uri) => uri,
                    Err(error) => {
                        form.message = Some(format!("{error:#}").into());
                        cx.notify();
                        return;
                    }
                }
            };
        }
        if site.is_empty() || environment.is_empty() {
            form.message = Some("Add a site and an environment on the left first.".into());
            cx.notify();
            return;
        }
        if name.is_empty() {
            form.message = Some("Fill in a name.".into());
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
            form.message = Some(if form.redis.is_some() {
                "Fill in the host.".into()
            } else {
                "Enter the connection string.".into()
            });
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
            environment: environment.clone(),
            // A new address starts with writes off again.
            allow_writes: uri.is_empty() && existing.as_ref().is_some_and(|c| c.allow_writes),
        };
        if !uri.is_empty() {
            self.session_writes.remove(&connection.id);
        }
        for (level, name) in [(Level::Site, site), (Level::Environment, environment)] {
            if !self.names[level as usize].contains(&name) {
                self.names[level as usize].push(name.clone());
                write_names(level, self.names[level as usize].clone(), cx);
            }
            self.selected[level as usize] = Some(name);
        }
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
                Ok(()) => {
                    // An edited connection must reconnect with its new settings.
                    this.active = None;
                    this.connect(connection.id.clone(), cx)
                }
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
    /// The first level of the module: MongoDB and Redis, each with the user's sites under it.
    /// MongoDB and Redis as icons, then the sites both share, then (in the sidebar) the site's connections.
    fn render_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let mut kinds = v_flex()
            .w(px(48.))
            .flex_none()
            .h_full()
            .pt_2()
            .gap_1()
            .items_center()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background);
        for (kind, icon, color) in [
            (Kind::MongoDb, "icons/vilo/mongodb.svg", 0x00ED64),
            (Kind::Redis, "icons/vilo/redis.svg", 0xFF4438),
        ] {
            let selected = self.kind_view == kind;
            kinds = kinds.child(
                div()
                    .id(SharedString::from(format!(
                        "database-kind-{}",
                        kind.label()
                    )))
                    .size(px(36.))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded_md()
                    .cursor_pointer()
                    .when(selected, |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .tooltip(Tooltip::text(kind.label()))
                    .on_hover(cx.listener(move |this, hovered: &bool, _, cx| {
                        this.hover_pick(*hovered, Pick::Kind(kind), cx)
                    }))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.kind_view = kind;
                        this.form = None;
                        cx.notify();
                    }))
                    .child(
                        Icon::from_path(icon)
                            .size(IconSize::Medium)
                            .color(Color::Custom(gpui::rgb(color).into())),
                    ),
            );
        }
        kinds.child(div().flex_1()).child(
            div().pb_2().child(
                IconButton::new(
                    "database-rail-pin",
                    if self.rail_pinned {
                        IconName::Unpin
                    } else {
                        IconName::Pin
                    },
                )
                .icon_size(IconSize::Small)
                .toggle_state(self.rail_pinned)
                .tooltip(Tooltip::text(if self.rail_pinned {
                    "Unpin: hide sites and connections while a page is open"
                } else {
                    "Pin sites and connections open"
                }))
                .on_click(cx.listener(|this, _, _, cx| {
                    this.rail_pinned = !this.rail_pinned;
                    this.rail_peek = false;
                    let pinned = this.rail_pinned;
                    cx.background_spawn(async move {
                        let path = rail_pin_path();
                        if pinned {
                            if let Some(parent) = path.parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            std::fs::write(&path, b"")?;
                        } else if path.exists() {
                            std::fs::remove_file(&path)?;
                        }
                        anyhow::Ok(())
                    })
                    .detach_and_log_err(cx);
                    cx.notify();
                })),
            ),
        )
    }

    /// A column as narrow as the kind icons; adding and renaming happen in a form in the page.
    fn render_names(&self, level: Level, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let noun = level.noun();
        let current = self.current(level);
        let mut column = v_flex()
            .id(SharedString::from(format!("database-{noun}s")))
            .w(px(48.))
            .flex_none()
            .h_full()
            .pt_2()
            .gap_1()
            .overflow_y_scroll()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background);
        for name in self.names(level) {
            let selected = current == name;
            let select_name = name.clone();
            let row = div()
                .id(SharedString::from(format!("database-{noun}-row-{name}")))
                .mx_1()
                .h(px(36.))
                .flex()
                .items_center()
                .justify_center()
                .rounded_md()
                .cursor_pointer()
                .when(selected, |this| this.bg(colors.element_selected))
                .hover(|style| style.bg(colors.element_hover))
                .tooltip(Tooltip::text(format!(
                    "{name} · right-click to rename or delete"
                )))
                .on_hover({
                    let name = select_name.clone();
                    cx.listener(move |this, hovered: &bool, _, cx| {
                        this.hover_pick(*hovered, Pick::Name(level, name.clone()), cx)
                    })
                })
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.selected[level as usize] = Some(select_name.clone());
                    this.form = None;
                    cx.notify();
                }))
                .child(
                    Label::new(name.clone())
                        .size(LabelSize::Small)
                        .weight(if selected {
                            gpui::FontWeight::SEMIBOLD
                        } else {
                            gpui::FontWeight::NORMAL
                        })
                        .color(if selected {
                            Color::Default
                        } else {
                            Color::Muted
                        })
                        .truncate(),
                );
            let view = cx.entity().downgrade();
            column = column.child(
                right_click_menu(SharedString::from(format!("database-{noun}-menu-{name}")))
                    .trigger(move |_, _, _| row)
                    .menu(move |window, cx| {
                        let (view, name) = (view.clone(), name.clone());
                        ContextMenu::build(window, cx, move |menu, _, _| {
                            let (rename_view, rename_name) = (view.clone(), name.clone());
                            menu.entry(format!("Rename {noun}"), None, move |window, cx| {
                                rename_view
                                    .update(cx, |this, cx| {
                                        this.start_name_edit(
                                            level,
                                            Some(rename_name.clone()),
                                            window,
                                            cx,
                                        )
                                    })
                                    .ok();
                            })
                            .entry(
                                format!("Delete {noun}"),
                                None,
                                move |_, cx| {
                                    view.update(cx, |this, cx| {
                                        this.delete_name(level, name.clone(), cx)
                                    })
                                    .ok();
                                },
                            )
                        })
                    }),
            );
        }
        column.child(
            div().flex().justify_center().child(
                IconButton::new(format!("database-{noun}-add"), IconName::Plus)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text(format!("Add a {noun}")))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.start_name_edit(level, None, window, cx)
                    })),
            ),
        )
    }

    /// Adding or renaming a site or environment, or why one could not be deleted.
    fn render_name_form(&self, cx: &mut Context<Self>) -> AnyElement {
        let edit = self.name_edit.as_ref();
        let level = edit
            .map(|edit| edit.level)
            .or(self.name_message.as_ref().map(|(level, _)| *level))
            .unwrap_or(Level::Site);
        let noun = level.noun();
        let title = match edit {
            Some(NameEdit {
                original: Some(original),
                ..
            }) => format!("Rename {noun} {original}"),
            Some(_) => format!("Add a {noun}"),
            None => format!("Delete {noun}"),
        };
        let message = self
            .name_message
            .as_ref()
            .map(|(_, message)| message.clone());
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            this.name_edit = None;
            this.name_message = None;
            cx.notify();
        };
        v_flex()
            .id("database-name-form")
            .gap_3()
            .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| this.save_name_edit(cx)))
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(Headline::new(title).size(HeadlineSize::Small))
            .when_some(edit, |this, edit| {
                this.child(
                    v_flex()
                        .gap_1()
                        .child(
                            Label::new(match level {
                                Level::Site => "Site name",
                                Level::Environment => "Environment name",
                            })
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                        )
                        .child(boxed(edit.editor.clone(), cx)),
                )
            })
            .when_some(message, |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .when(edit.is_some(), |this| {
                        this.child(
                            Button::new("database-name-save", "Save")
                                .style(ButtonStyle::Filled)
                                .on_click(cx.listener(|this, _, _, cx| this.save_name_edit(cx))),
                        )
                    })
                    .child(
                        Button::new(
                            "database-name-cancel",
                            if edit.is_some() { "Cancel" } else { "OK" },
                        )
                        .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let search = self.search.read(cx).text(cx).trim().to_lowercase();
        let matches = |name: &str| search.is_empty() || name.to_lowercase().contains(&search);
        let mut tree = v_flex().gap_0p5().px_1();
        let place = format!(
            "{} {}",
            self.current(Level::Site),
            self.current(Level::Environment)
        );
        {
            let kind = self.kind_view;
            let in_site = self
                .connections
                .iter()
                .filter(|connection| self.in_view(connection))
                .collect::<Vec<_>>();
            if in_site.is_empty() {
                tree = tree.child(
                    div().px_3().child(
                        Label::new(format!("No {} connections in {place}", kind.label()))
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
                let (edit_id, menu_id) = (connection.id.clone(), connection.id.clone());
                let open_edit = cx.listener(move |this, _: &gpui::MouseDownEvent, window, cx| {
                    this.open_form(Some(menu_id.clone()), window, cx)
                });
                let group =
                    SharedString::from(format!("database-connection-group-{}", connection.id));
                let row = h_flex()
                    .id(SharedString::from(format!(
                        "database-connection-{}",
                        connection.id
                    )))
                    .group(group.clone())
                    .px_2()
                    .py_1()
                    .gap_1p5()
                    .rounded_md()
                    .cursor_pointer()
                    .when(active, |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .on_click(cx.listener(move |this, _, _, cx| this.connect(id.clone(), cx)))
                    .on_mouse_down(gpui::MouseButton::Right, open_edit)
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
                        IconButton::new(
                            SharedString::from(format!(
                                "database-connection-edit-{}",
                                connection.id
                            )),
                            IconName::Pencil,
                        )
                        .icon_size(IconSize::XSmall)
                        .visible_on_hover(group)
                        .tooltip(Tooltip::text("Edit connection"))
                        .on_click(cx.listener(
                            move |this, _, window, cx| {
                                cx.stop_propagation();
                                this.open_form(Some(edit_id.clone()), window, cx)
                            },
                        )),
                    );
                tree = tree.child(row);
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
        v_flex()
            .w(px(240.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(
                h_flex()
                    .h(px(26.))
                    .pl_3()
                    .pr_1()
                    .child(div().flex_1())
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
        let editing = form.editing.clone();
        v_flex()
            .id("database-form")
            .gap_3()
            .child(Headline::new(if form.editing.is_some() { "Edit connection" } else { "Add a connection" }).size(HeadlineSize::Small))
            .child(input("Name", &form.name, cx))
            .child(
                Label::new(format!(
                    "{} · Site {} · Environment {}",
                    form.kind.label(),
                    form.site,
                    form.environment
                ))
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .map(|this| match &form.redis {
                None => this.child(input("Connection string", &form.uri, cx)),
                Some(redis) => this
                    .child(
                        h_flex()
                            .gap_3()
                            .child(div().flex_1().child(input("Host", &redis.host, cx)))
                            .child(div().w(px(140.)).child(input("Port", &redis.port, cx))),
                    )
                    .child(
                        h_flex()
                            .gap_3()
                            .child(div().flex_1().child(input("Username", &redis.username, cx)))
                            .child(div().flex_1().child(input("Password", &redis.password, cx))),
                    )
                    .child(
                        h_flex()
                            .gap_3()
                            .items_end()
                            .child(div().w(px(140.)).child(input("Database", &redis.database, cx)))
                            .child(
                                Button::new("database-redis-tls", "Use TLS")
                                    .style(if redis.tls { ButtonStyle::Filled } else { ButtonStyle::Outlined })
                                    .start_icon(Icon::new(if redis.tls { IconName::Check } else { IconName::Circle }).size(IconSize::XSmall))
                                    .toggle_state(redis.tls)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        if let Some(redis) = this.form.as_mut().and_then(|form| form.redis.as_mut()) {
                                            redis.tls = !redis.tls;
                                        }
                                        cx.notify();
                                    })),
                            ),
                    ),
            })
            .child(
                Label::new(
                    "The connection details, password included, are kept in the macOS keychain and only used to connect. \
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

impl DatabaseView {
    /// Reads the write log for writes left without a result; a read failure stays on screen with
    /// a retry instead of hiding the check.
    fn check_write_log(&mut self, cx: &mut Context<Self>) {
        let read = cx.background_spawn(async { mongo_write::read_dangling_writes() });
        cx.spawn(async move |this, cx| {
            let result = read.await;
            this.update(cx, |this, cx| {
                match result {
                    // The log is the record: a write whose result could not be logged has no result
                    // line there either, so the fresh list still holds it.
                    Ok(dangling) => {
                        // An administration write still to check keeps blocking new ones after a restart.
                        let admin = [
                            vilo_database_core::redis::AdminWrite::SlowlogReset.name(),
                            vilo_database_core::redis::AdminWrite::SlowlogThreshold { micros: 0 }
                                .name(),
                        ];
                        for (operation, _, kind, lock) in &dangling {
                            if admin.contains(&kind.as_str()) {
                                this.admin_to_check.insert(operation.clone());
                            }
                            // A publish without a settled answer keeps its channel locked.
                            if let Some(lock) = lock {
                                redis_pubsub::recover_publish_lock(lock, operation);
                            }
                        }
                        // A publish still awaiting its answer in this process is not unsettled.
                        this.dangling_writes = dangling
                            .into_iter()
                            .filter(|(operation, ..)| !redis_pubsub::publish_pending(operation))
                            .map(|(operation, summary, ..)| (operation, summary))
                            .collect();
                        // A damaged line might hide an unsettled publish, so publishing waits
                        // until the user checked it.
                        this.write_log_checked = !this
                            .dangling_writes
                            .iter()
                            .any(|(operation, _)| operation.starts_with("damaged:"));
                        this.audit_problem = None;
                    }
                    Err(error) => this.audit_problem = Some(
                        format!(
                            "Vilo could not check the write log for unfinished writes: {error:#}"
                        )
                        .into(),
                    ),
                }
                cx.notify();
            })
        })
        .detach_and_log_err(cx);
    }

    /// Writes to check: those of this run that could not be logged, then those the log has no
    /// result for.
    fn writes_to_check(&self) -> Vec<(String, String)> {
        writes_to_check(&self.unlogged_writes, &self.dangling_writes)
    }

    fn render_dangling_writes(&self, cx: &mut Context<Self>) -> AnyElement {
        let writes = self.writes_to_check();
        let count = writes.len();
        v_flex()
            .m_2()
            .p_2()
            .gap_1()
            .rounded_md()
            .bg(cx.theme().status().warning_background)
            .when_some(self.audit_problem.clone(), |this, problem| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(Icon::new(IconName::Warning).size(IconSize::Small).color(Color::Error))
                        .child(Label::new(problem).size(LabelSize::Small))
                        .child(div().flex_1())
                        .child(
                            Button::new("database-audit-retry", "Check again")
                                .label_size(LabelSize::Small)
                                .style(ButtonStyle::Outlined)
                                .on_click(cx.listener(|this, _, _, cx| this.check_write_log(cx))),
                        ),
                )
            })
            .when(count > 0, |this| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(Icon::new(IconName::Warning).size(IconSize::Small).color(Color::Warning))
                        .child(
                            Label::new(format!(
                                "{count} write{} may or may not have happened: the write log has no result for {}. Check {} before writing there again.",
                                if count == 1 { "" } else { "s" },
                                if count == 1 { "it" } else { "them" },
                                if count == 1 { "its target" } else { "their targets" },
                            ))
                            .size(LabelSize::Small),
                        )
                        .child(div().flex_1())
                        .child(
                            Button::new("database-dangling-checked", "I checked them")
                                .label_size(LabelSize::Small)
                                .style(ButtonStyle::Outlined)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    // Consumer group acknowledgements and claims are checked in their group.
                                    let operations = this
                                        .writes_to_check()
                                        .into_iter()
                                        .map(|(operation, _)| operation)
                                        .filter(|operation| !redis_pubsub::group_operation(operation))
                                        .collect::<Vec<_>>();
                                    let task = cx.background_spawn({
                                        let operations = operations.clone();
                                        async move { mongo_write::acknowledge_writes(&operations) }
                                    });
                                    // The list only clears once the acknowledgement is in the log.
                                    cx.spawn(async move |this, cx| {
                                        let result = task.await;
                                        // Released before touching the window, which may be closed by now.
                                        if result.is_ok() {
                                            redis_pubsub::acknowledge_publishes(&operations);
                                        }
                                        this.update(cx, |this, cx| {
                                            match result {
                                                Ok(()) => {
                                                    this.dangling_writes.retain(|(operation, _)| {
                                                        !operations.contains(operation)
                                                    });
                                                    this.unlogged_writes.retain(|(operation, _)| {
                                                        !operations.contains(operation)
                                                    });
                                                    this.audit_problem = None;
                                                    // Read again, so publishing resumes once nothing is left to check.
                                                    this.check_write_log(cx);
                                                }
                                                Err(error) => {
                                                    this.audit_problem = Some(
                                                        format!("Vilo could not record that these were checked: {error:#}")
                                                            .into(),
                                                    )
                                                }
                                            }
                                            cx.notify();
                                        })
                                    })
                                    .detach_and_log_err(cx);
                                })),
                        ),
                )
                .child(
                    v_flex()
                        .id("database-dangling-list")
                        .max_h(px(160.))
                        .overflow_y_scroll()
                        .children(writes.into_iter().map(|(operation, summary)| {
                            let summary = if redis_pubsub::group_operation(&operation) {
                                format!("{summary} · check it in its consumer group")
                            } else {
                                summary
                            };
                            Label::new(summary)
                                .size(LabelSize::XSmall)
                                .buffer_font(cx)
                                .color(Color::Muted)
                        })),
                )
            })
            .into_any_element()
    }
}

impl Render for DatabaseView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // The open connection shows only under its own kind and site; switching keeps it connected.
        let connection = self
            .active_connection()
            .filter(|connection| self.in_view(connection))
            .cloned();
        let full_bleed = self.session.is_some() && connection.is_some();
        let naming = self.name_edit.is_some() || self.name_message.is_some();
        let dialog = if naming {
            Some((false, self.render_name_form(cx)))
        } else if let Some(form) = &self.admin_write {
            Some((false, self.render_admin_write(form, cx)))
        } else if let Some(form) = &self.bulk_delete {
            Some((false, self.render_bulk_delete(form, cx)))
        } else if let Some(run) = &self.workbench_run {
            Some((false, self.render_workbench_run(run, cx)))
        } else if let Some(form) = &self.redis_write {
            Some((false, self.render_redis_write(form, cx)))
        } else if let Some(write) = &self.doc_write {
            Some((write.editing(), self.render_doc_write(write, cx)))
        } else {
            self.form
                .as_ref()
                .map(|form| (true, self.render_form(form, cx)))
        };
        // A save that is still connecting keeps the dialog open; closing it would cancel the save halfway.
        let closable =
            !self.connections.is_empty() && !self.form.as_ref().is_some_and(|form| form.checking);
        let main = match (&connection, &self.session) {
            (Some(connection), Some(Session::Mongo(_))) => self.render_mongo(connection, cx),
            (Some(connection), Some(Session::Redis(_))) => self.render_redis(connection, cx),
            (Some(connection), None) => v_flex()
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
            (None, _) => Label::new(format!(
                "Pick a {} connection on the left, or add one.",
                self.kind_view.label()
            ))
            .color(Color::Muted)
            .into_any_element(),
        };
        // Mongo folds once a collection is open; Redis has no collection step, so it folds on connect.
        let folded = !self.rail_pinned
            && full_bleed
            && (self.tab().is_some() || matches!(self.session, Some(Session::Redis(_))));
        if !folded {
            self.rail_peek = false;
        }
        let colors = cx.theme().colors().clone();
        h_flex()
            .track_focus(&self.focus_handle)
            .relative()
            .size_full()
            .bg(colors.editor_background)
            .child(
                div()
                    .id("database-kinds")
                    .h_full()
                    .on_hover(cx.listener(move |this, hovered: &bool, _, cx| {
                        if folded && *hovered && !this.rail_peek {
                            this.rail_peek = true;
                            cx.notify();
                        }
                    }))
                    .child(self.render_rail(cx)),
            )
            .when(!folded, |this| {
                this.child(self.render_names(Level::Site, cx))
                    .child(self.render_names(Level::Environment, cx))
                    .child(self.render_sidebar(cx))
            })
            // Mongo and Redis pages run edge to edge like Compass and RedisInsight.
            .child(
                v_flex()
                    .id("database-page")
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .when(!full_bleed, |this| this.p_4())
                    .when(
                        !self.dangling_writes.is_empty()
                            || !self.unlogged_writes.is_empty()
                            || self.audit_problem.is_some(),
                        |this| this.child(self.render_dangling_writes(cx)),
                    )
                    // The peek closes when the pointer reaches the page, not when it leaves the
                    // peek: right-click menus and dialogs sit outside it and must keep it open.
                    .on_hover(cx.listener(|this, hovered: &bool, _, cx| {
                        if *hovered && this.rail_peek {
                            this.rail_peek = false;
                            cx.notify();
                        }
                    }))
                    .child(main),
            )
            .when(folded && self.rail_peek, |this| {
                this.child(
                    h_flex()
                        .id("database-rail-peek")
                        .absolute()
                        .top_0()
                        .left(px(48.))
                        .h_full()
                        .shadow_lg()
                        // Its gaps must not count as hovering the page underneath, which closes it.
                        .occlude()
                        .child(self.render_names(Level::Site, cx))
                        .child(self.render_names(Level::Environment, cx))
                        .child(self.render_sidebar(cx)),
                )
            })
            .when_some(dialog, |this, (big, dialog)| {
                this.child(
                    div()
                        .id("database-name-backdrop")
                        .absolute()
                        .inset_0()
                        .flex()
                        .items_center()
                        .justify_center()
                        .bg(gpui::black().opacity(0.4))
                        .occlude()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            if this.name_edit.is_some() || this.name_message.is_some() {
                                this.name_edit = None;
                                this.name_message = None;
                            } else if let Some(form) = &this.admin_write {
                                if !form.sending_now() {
                                    this.admin_write = None;
                                }
                            } else if let Some(form) = &this.bulk_delete {
                                if form.closable() {
                                    this.bulk_delete = None;
                                }
                            } else if let Some(run) = &this.workbench_run {
                                if run.closable() {
                                    this.workbench_run = None;
                                }
                            } else if let Some(form) = &this.redis_write {
                                if !form.sending_now() {
                                    this.redis_write = None;
                                }
                            } else if let Some(write) = &this.doc_write {
                                if !write.sending_now() {
                                    this.doc_write = None;
                                }
                            } else if closable {
                                this.form = None;
                            }
                            cx.notify();
                        }))
                        .child(
                            div()
                                .id("database-name-dialog")
                                // The connection form takes most of the page; the name form stays small.
                                .when(big, |this| {
                                    this.w(gpui::relative(0.8))
                                        .h(gpui::relative(0.8))
                                        .p_6()
                                        .overflow_y_scroll()
                                })
                                .when(!big, |this| this.w(px(380.)))
                                .p_4()
                                .rounded_lg()
                                .border_1()
                                .border_color(colors.border)
                                .bg(colors.elevated_surface_background)
                                .shadow_lg()
                                // Clicks inside the dialog must not reach the backdrop, which closes it.
                                .on_click(|_, _, cx| cx.stop_propagation())
                                .child(dialog),
                        ),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writes_to_check_keep_unlogged_reasons() {
        let pair = |operation: &str, summary: &str| (operation.to_string(), summary.to_string());
        let unlogged = [pair("a", "a · its result is not in the log: disk full")];
        let dangling = [pair("a", "a"), pair("b", "b")];
        assert_eq!(
            writes_to_check(&unlogged, &dangling),
            vec![
                pair("a", "a · its result is not in the log: disk full"),
                pair("b", "b")
            ]
        );
        assert_eq!(writes_to_check(&[], &dangling), dangling.to_vec());
    }

    #[test]
    fn test_redis_parts_round_trip() {
        let parts = RedisParts {
            host: "10.0.0.5".into(),
            port: "6380".into(),
            username: "default".into(),
            password: "p@ss/w+o=r&d%".into(),
            database: "2".into(),
            tls: true,
        };
        let uri = parts.to_uri().unwrap();
        assert!(uri.starts_with("rediss://default:"));
        assert_eq!(RedisParts::from_uri(&uri), Some(parts));
        assert!(
            RedisParts {
                host: "h".into(),
                port: "x".into(),
                ..Default::default()
            }
            .to_uri()
            .is_err()
        );
    }

    #[test]
    fn test_site_names_are_checked() {
        let sites = vec!["CP".to_string(), "BP".to_string()];
        assert_eq!(check_name("NP", &sites), Ok(()));
        assert!(check_name("", &sites).is_err());
        assert!(check_name("cp", &sites).is_err());
        assert!(check_name("unassigned", &sites).is_err());
    }
}
