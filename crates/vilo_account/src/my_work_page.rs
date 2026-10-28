use std::time::SystemTime;

use anyhow::{Context as _, Result, anyhow};
use collections::{HashMap, HashSet};
use editor::Editor;
use futures::{StreamExt as _, future::join_all, stream};
use gpui::{
    AnyElement, App, AsyncApp, ClipboardItem, Entity, FocusHandle, Focusable, KeyDownEvent, Pixels,
    Subscription, Task, WeakEntity, Window, actions, prelude::*, px,
};
use ui::{
    Avatar, Button, ButtonStyle, ContextMenu, Divider, IconButton, IconName, PopoverMenu, Tooltip,
    prelude::*,
};
use url::Url;
use workspace::Workspace;

use crate::{
    AccountEvent, AccountStore, WelcomeModal,
    meegle::{MeegleSession, TokenRejected},
};

actions!(
    vilo_account,
    [
        /// Shows the Meegle My Work page in place of the code layout.
        OpenMyWork
    ]
);

pub const MODULE_ID: &str = "my-work";
const ENRICH_CONCURRENCY: usize = 4;
const DRAWER_WIDTH: Pixels = px(420.);
/// Below this page width the detail drawer covers the table instead of sitting beside it.
const DRAWER_OVERLAY_BELOW: Pixels = px(1100.);
const TABLE_MIN_WIDTH: Pixels = px(1040.);

pub fn init(store: Entity<AccountStore>, cx: &mut App) {
    cx.observe_new(move |workspace: &mut Workspace, _, cx| {
        let store = store.clone();
        workspace.register_module(
            MODULE_ID,
            "icons/vilo/codicons/checklist.svg",
            "My Work",
            move |window, cx| {
                let workspace = cx.weak_entity();
                let store = store.clone();
                let page = cx.new(|cx| MyWorkPage::new(workspace, store, window, cx));
                let focus_handle = page.focus_handle(cx);
                (page.into(), focus_handle)
            },
            cx,
        );
        workspace.register_action(|workspace, _: &OpenMyWork, window, cx| {
            workspace.activate_module(MODULE_ID, window, cx);
        });
    })
    .detach();
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum WorkList {
    Todo,
    ThisWeek,
    Overdue,
    Done,
}

impl WorkList {
    const ALL: [WorkList; 4] = [
        WorkList::Todo,
        WorkList::ThisWeek,
        WorkList::Overdue,
        WorkList::Done,
    ];

    fn label(self) -> &'static str {
        match self {
            WorkList::Todo => "To Do",
            WorkList::ThisWeek => "This Week",
            WorkList::Overdue => "Overdue",
            WorkList::Done => "Done",
        }
    }

    fn action(self) -> &'static str {
        match self {
            WorkList::Todo => "todo",
            WorkList::ThisWeek => "this_week",
            WorkList::Overdue => "overdue",
            WorkList::Done => "done",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct WorkItem {
    id: u64,
    name: SharedString,
    type_key: String,
    project_key: String,
    project_name: SharedString,
    state: SharedString,
    due: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct Brief {
    type_name: SharedString,
    status: SharedString,
    owners: Vec<SharedString>,
    creator: Option<SharedString>,
    created: Option<SharedString>,
    updated: Option<SharedString>,
    updated_by: Option<SharedString>,
    template: Option<SharedString>,
    roles: Vec<(SharedString, Vec<SharedString>)>,
    simple_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct Comment {
    author: SharedString,
    time: Option<SharedString>,
    text: SharedString,
}

#[derive(Clone, Debug, PartialEq)]
struct HistoryEntry {
    operator: SharedString,
    action: SharedString,
    time: Option<SharedString>,
}

#[derive(Clone, Debug, PartialEq)]
struct Node {
    name: SharedString,
    state: NodeState,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum NodeState {
    Done,
    Current,
    Pending,
}

enum Fetch<T> {
    Loading,
    Loaded(T),
    Failed(SharedString),
}

#[derive(Clone, Copy, PartialEq)]
enum DrawerTab {
    Overview,
    Comments,
    History,
}

struct Drawer {
    item: WorkItem,
    tab: DrawerTab,
    nodes: Option<Fetch<Vec<Node>>>,
    comments: Option<Fetch<Vec<Comment>>>,
    history: Option<Fetch<Vec<HistoryEntry>>>,
    /// One task per drawer; replacing the drawer drops it, so a late answer for a
    /// previous item can never land in the current one.
    _tasks: Vec<Task<()>>,
}

struct ListState {
    items: Vec<WorkItem>,
    total: usize,
    pages_loaded: u32,
}

/// Vilo: the signed-in user's Meegle work as a full-window module.
pub struct MyWorkPage {
    workspace: WeakEntity<Workspace>,
    store: Entity<AccountStore>,
    focus_handle: FocusHandle,
    filter_editor: Entity<Editor>,
    list: WorkList,
    lists: HashMap<WorkList, ListState>,
    counts: HashMap<WorkList, usize>,
    loading: bool,
    error: Option<SharedString>,
    updated_at: Option<SystemTime>,
    /// Bumped on refresh and account changes; answers for an older generation are dropped.
    generation: u64,
    load_task: Option<Task<()>>,
    more_task: Option<Task<()>>,
    briefs: HashMap<u64, Brief>,
    brief_failures: HashSet<u64>,
    briefs_pending: HashSet<u64>,
    enrich_task: Option<Task<()>>,
    type_filter: Option<String>,
    space_filter: Option<String>,
    page_size: usize,
    page: usize,
    selected: Option<u64>,
    drawer: Option<Drawer>,
    /// Lists whose first page failed while others loaded.
    list_errors: HashMap<WorkList, SharedString>,
    /// Width of the page the last time it was laid out; decides drawer placement.
    page_width: Pixels,
    _subscriptions: Vec<Subscription>,
}

/// After this long away, coming back to the page refreshes it.
const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(120);

impl MyWorkPage {
    fn new(
        workspace: WeakEntity<Workspace>,
        store: Entity<AccountStore>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let filter_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Filter by title or ID…", window, cx);
            editor
        });
        let mut subscriptions = vec![cx.subscribe(&filter_editor, |this, _, event, cx| {
            if let editor::EditorEvent::BufferEdited = event {
                this.page = 0;
                this.enrich_visible(cx);
                cx.notify();
            }
        })];
        subscriptions.push(cx.observe(&store, |_, _, cx| cx.notify()));
        subscriptions.push(cx.subscribe(&store, |this, _, event, cx| match event {
            AccountEvent::SignedIn => this.refresh(cx),
            AccountEvent::SignedOut => this.reset(cx),
        }));
        let mut page = Self {
            workspace,
            store,
            focus_handle: cx.focus_handle(),
            filter_editor,
            list: WorkList::Todo,
            lists: HashMap::default(),
            counts: HashMap::default(),
            loading: false,
            error: None,
            updated_at: None,
            generation: 0,
            load_task: None,
            more_task: None,
            briefs: HashMap::default(),
            brief_failures: HashSet::default(),
            briefs_pending: HashSet::default(),
            enrich_task: None,
            type_filter: None,
            space_filter: None,
            page_size: 20,
            page: 0,
            selected: None,
            drawer: None,
            list_errors: HashMap::default(),
            page_width: px(0.),
            _subscriptions: subscriptions,
        };
        // Returning to the module after a while shows fresh data, not a stale snapshot.
        let focus_handle = page.focus_handle.clone();
        page._subscriptions
            .push(cx.on_focus_in(&focus_handle, window, |this, _, cx| {
                let stale = this
                    .updated_at
                    .and_then(|updated_at| updated_at.elapsed().ok())
                    .is_some_and(|elapsed| elapsed > STALE_AFTER);
                if stale && !this.loading {
                    this.refresh(cx);
                }
            }));
        page.refresh(cx);
        page
    }

    fn reset(&mut self, cx: &mut Context<Self>) {
        self.generation += 1;
        self.load_task = None;
        self.more_task = None;
        self.enrich_task = None;
        self.lists.clear();
        self.counts.clear();
        self.briefs.clear();
        self.brief_failures.clear();
        self.briefs_pending.clear();
        self.drawer = None;
        self.selected = None;
        self.error = None;
        self.list_errors.clear();
        self.loading = false;
        self.page = 0;
        cx.notify();
    }

    /// Loads the first page of every list; their totals feed the count cards.
    fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.store.read(cx).account().is_none() {
            self.reset(cx);
            return;
        }
        // Keep showing the current data until the new answer arrives.
        self.generation += 1;
        self.more_task = None;
        self.enrich_task = None;
        self.briefs_pending.clear();
        self.error = None;
        self.loading = true;
        let generation = self.generation;
        let store = self.store.clone();
        self.load_task = Some(cx.spawn(async move |this, cx| {
            let started = std::time::Instant::now();
            // A session sends its requests one at a time, so each list gets its own.
            let calls = WorkList::ALL.map(|list| {
                let store = store.clone();
                let mut cx = cx.clone();
                async move {
                    let result = with_session(&store, &mut cx, async |session| {
                        let response = session
                            .call(
                                "list_todo",
                                serde_json::json!({ "action": list.action(), "page_num": 1 }),
                            )
                            .await?;
                        parse_work_items(&response)
                    })
                    .await;
                    (list, result)
                }
            });
            let lists = join_all(calls).await;
            log::info!("my work: loaded lists in {:?}", started.elapsed());
            let result = if lists.iter().all(|(_, result)| result.is_err()) {
                match lists.into_iter().next() {
                    Some((_, Err(error))) => Err(error),
                    _ => Ok(Vec::new()),
                }
            } else {
                Ok(lists)
            };
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.loading = false;
                match result {
                    Ok(lists) => {
                        this.lists.clear();
                        this.counts.clear();
                        this.list_errors.clear();
                        this.briefs.clear();
                        this.brief_failures.clear();
                        for (list, result) in lists {
                            match result {
                                Ok((items, total)) => {
                                    this.counts.insert(list, total);
                                    this.lists.insert(
                                        list,
                                        ListState {
                                            items,
                                            total,
                                            pages_loaded: 1,
                                        },
                                    );
                                }
                                Err(error) => {
                                    this.list_errors.insert(list, describe_error(&error));
                                }
                            }
                        }
                        this.updated_at = Some(SystemTime::now());
                        this.enrich_visible(cx);
                    }
                    Err(error) => this.error = Some(describe_error(&error)),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn load_more(&mut self, cx: &mut Context<Self>) {
        let list = self.list;
        let Some(state) = self.lists.get(&list) else {
            return;
        };
        let page_num = state.pages_loaded + 1;
        let generation = self.generation;
        let store = self.store.clone();
        self.more_task = Some(cx.spawn(async move |this, cx| {
            let result = with_session(&store, cx, async |session| {
                let response = session
                    .call(
                        "list_todo",
                        serde_json::json!({ "action": list.action(), "page_num": page_num }),
                    )
                    .await?;
                parse_work_items(&response)
            })
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.more_task = None;
                match result {
                    Ok((items, total)) => {
                        if let Some(state) = this.lists.get_mut(&list) {
                            let known = state
                                .items
                                .iter()
                                .map(|item| item.id)
                                .collect::<HashSet<_>>();
                            state
                                .items
                                .extend(items.into_iter().filter(|item| !known.contains(&item.id)));
                            state.total = total;
                            state.pages_loaded = page_num;
                        }
                        this.enrich_visible(cx);
                    }
                    Err(error) => this.error = Some(describe_error(&error)),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn select_list(&mut self, list: WorkList, cx: &mut Context<Self>) {
        if self.list == list {
            return;
        }
        self.list = list;
        self.page = 0;
        self.selected = None;
        self.type_filter = None;
        self.space_filter = None;
        self.enrich_visible(cx);
        cx.notify();
    }

    fn filtered(&self, cx: &App) -> Vec<WorkItem> {
        let query = self.filter_editor.read(cx).text(cx).trim().to_lowercase();
        let Some(state) = self.lists.get(&self.list) else {
            return Vec::new();
        };
        state
            .items
            .iter()
            .filter(|item| {
                self.type_filter
                    .as_ref()
                    .is_none_or(|type_key| *type_key == item.type_key)
                    && self
                        .space_filter
                        .as_ref()
                        .is_none_or(|project_key| *project_key == item.project_key)
                    && (query.is_empty()
                        || item.name.to_lowercase().contains(&query)
                        || item
                            .id
                            .to_string()
                            .starts_with(query.trim_start_matches('#')))
            })
            .cloned()
            .collect()
    }

    fn visible(&self, cx: &App) -> Vec<WorkItem> {
        self.filtered(cx)
            .into_iter()
            .skip(self.page * self.page_size)
            .take(self.page_size)
            .collect()
    }

    /// Fetches details for the rows on screen that do not have them yet.
    fn enrich_visible(&mut self, cx: &mut Context<Self>) {
        let missing = self
            .visible(cx)
            .into_iter()
            .filter(|item| {
                !self.briefs.contains_key(&item.id) && !self.briefs_pending.contains(&item.id)
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return;
        }
        for item in &missing {
            self.briefs_pending.insert(item.id);
            self.brief_failures.remove(&item.id);
        }
        let generation = self.generation;
        let store = self.store.clone();
        let previous = self.enrich_task.take();
        self.enrich_task = Some(cx.spawn(async move |this, cx| {
            // Let an earlier batch finish rather than cancelling its rows half-way.
            if let Some(previous) = previous {
                previous.await;
            }
            // A session sends its requests one at a time, so open one per lane.
            let lane_count = ENRICH_CONCURRENCY.min(missing.len());
            let opened = join_all((0..lane_count).map(|_| {
                let store = store.clone();
                let mut cx = cx.clone();
                async move { open_session(&store, &mut cx).await }
            }))
            .await;
            let mut sessions = Vec::new();
            let mut first_error = None;
            for result in opened {
                match result {
                    Ok(session) => sessions.push(session),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            let sessions = match first_error {
                Some(error) if sessions.is_empty() => {
                    log::warn!("loading Meegle item details failed: {error:#}");
                    this.update(cx, |this, cx| {
                        if this.generation == generation {
                            for item in &missing {
                                this.briefs_pending.remove(&item.id);
                                this.brief_failures.insert(item.id);
                            }
                            cx.notify();
                        }
                    })
                    .ok();
                    return;
                }
                _ => sessions,
            };
            // Each lane works through every n-th row, so the top rows load first.
            let mut lanes = sessions.iter().map(|_| Vec::new()).collect::<Vec<_>>();
            for (index, item) in missing.into_iter().enumerate() {
                if let Some(lane) = lanes.get_mut(index % sessions.len()) {
                    lane.push(item);
                }
            }
            let mut results = stream::select_all(sessions.iter().zip(lanes).map(
                |(session, items)| {
                    Box::pin(stream::iter(items).then(move |item| async move {
                        let response = session
                            .call(
                                "get_workitem_brief",
                                serde_json::json!({
                                    "project_key": item.project_key,
                                    "work_item_id": item.id.to_string(),
                                }),
                            )
                            .await
                            .and_then(|response| parse_brief(&response));
                        (item.id, response)
                    }))
                },
            ));
            while let Some((id, result)) = results.next().await {
                let applied = this.update(cx, |this, cx| {
                    if this.generation != generation {
                        return false;
                    }
                    this.briefs_pending.remove(&id);
                    match result {
                        Ok(brief) => {
                            this.briefs.insert(id, brief);
                        }
                        Err(error) => {
                            log::warn!("loading Meegle item details failed: {error:#}");
                            this.brief_failures.insert(id);
                        }
                    }
                    cx.notify();
                    true
                });
                if !matches!(applied, Ok(true)) {
                    return;
                }
            }
        }));
    }

    fn enrich_item(&mut self, id: u64, cx: &mut Context<Self>) {
        self.briefs.remove(&id);
        self.enrich_visible(cx);
        if let Some(item) = self
            .drawer
            .as_ref()
            .map(|drawer| drawer.item.clone())
            .filter(|item| item.id == id && !self.briefs_pending.contains(&id))
        {
            // The drawer's item may be outside the visible page.
            self.briefs_pending.insert(item.id);
            let generation = self.generation;
            let store = self.store.clone();
            cx.spawn(async move |this, cx| {
                let result = with_session(&store, cx, async |session| {
                    session
                        .call(
                            "get_workitem_brief",
                            serde_json::json!({
                                "project_key": item.project_key,
                                "work_item_id": item.id.to_string(),
                            }),
                        )
                        .await
                        .and_then(|response| parse_brief(&response))
                })
                .await;
                this.update(cx, |this, cx| {
                    if this.generation != generation {
                        return;
                    }
                    this.briefs_pending.remove(&item.id);
                    match result {
                        Ok(brief) => {
                            this.briefs.insert(item.id, brief);
                        }
                        Err(_) => {
                            this.brief_failures.insert(item.id);
                        }
                    }
                    cx.notify();
                })
                .ok();
            })
            .detach();
        }
        cx.notify();
    }

    fn open_drawer(&mut self, item: WorkItem, cx: &mut Context<Self>) {
        self.selected = Some(item.id);
        let mut drawer = Drawer {
            item,
            tab: DrawerTab::Overview,
            nodes: None,
            comments: None,
            history: None,
            _tasks: Vec::new(),
        };
        self.load_drawer_tab(&mut drawer, DrawerTab::Overview, cx);
        self.drawer = Some(drawer);
        cx.notify();
    }

    fn select_drawer_tab(&mut self, tab: DrawerTab, cx: &mut Context<Self>) {
        let Some(mut drawer) = self.drawer.take() else {
            return;
        };
        drawer.tab = tab;
        self.load_drawer_tab(&mut drawer, tab, cx);
        self.drawer = Some(drawer);
        cx.notify();
    }

    fn load_drawer_tab(&mut self, drawer: &mut Drawer, tab: DrawerTab, cx: &mut Context<Self>) {
        let (tool, already_loading) = match tab {
            DrawerTab::Overview => ("get_node_detail", drawer.nodes.is_some()),
            DrawerTab::Comments => ("list_workitem_comments", drawer.comments.is_some()),
            DrawerTab::History => ("get_workitem_op_record", drawer.history.is_some()),
        };
        if already_loading {
            return;
        }
        match tab {
            DrawerTab::Overview => drawer.nodes = Some(Fetch::Loading),
            DrawerTab::Comments => drawer.comments = Some(Fetch::Loading),
            DrawerTab::History => drawer.history = Some(Fetch::Loading),
        }
        let item_id = drawer.item.id;
        let arguments = serde_json::json!({
            "project_key": drawer.item.project_key,
            "work_item_id": item_id.to_string(),
        });
        let generation = self.generation;
        let store = self.store.clone();
        drawer._tasks.push(cx.spawn(async move |this, cx| {
            let result = with_session(&store, cx, async |session| {
                session.call(tool, arguments.clone()).await
            })
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(drawer) = this
                    .drawer
                    .as_mut()
                    .filter(|drawer| drawer.item.id == item_id)
                else {
                    return;
                };
                match tab {
                    DrawerTab::Overview => {
                        drawer.nodes = Some(fetched(result.and_then(|value| parse_nodes(&value))))
                    }
                    DrawerTab::Comments => {
                        drawer.comments =
                            Some(fetched(result.and_then(|value| parse_comments(&value))))
                    }
                    DrawerTab::History => {
                        drawer.history =
                            Some(fetched(result.and_then(|value| parse_history(&value))))
                    }
                }
                cx.notify();
            })
            .ok();
        }));
    }

    fn open_in_meegle(&self, item: &WorkItem, cx: &mut App) {
        if let Some(url) = self.item_url(item, cx) {
            cx.open_url(url.as_str());
        }
    }

    fn item_url(&self, item: &WorkItem, cx: &App) -> Option<Url> {
        let issuer = Url::parse(&self.store.read(cx).account()?.issuer).ok()?;
        let simple_name = self.briefs.get(&item.id)?.simple_name.clone()?;
        detail_url(&issuer, &simple_name, &item.type_key, item.id).ok()
    }

    fn open_sign_in(&self, window: &mut Window, cx: &mut Context<Self>) {
        let store = self.store.clone();
        self.workspace
            .update(cx, |workspace, cx| {
                workspace.toggle_modal(window, cx, |window, cx| {
                    WelcomeModal::new(store, window, cx)
                })
            })
            .ok();
    }

    fn handle_key(&mut self, event: &KeyDownEvent, window: &mut Window, cx: &mut Context<Self>) {
        // Arrow keys belong to the filter box while it is being typed in.
        if self.filter_editor.focus_handle(cx).is_focused(window) {
            return;
        }
        let visible = self.visible(cx);
        let position = self
            .selected
            .and_then(|id| visible.iter().position(|item| item.id == id));
        let handled = match event.keystroke.key.as_str() {
            "down" => {
                let next =
                    position.map_or(0, |index| (index + 1).min(visible.len().saturating_sub(1)));
                self.selected = visible.get(next).map(|item| item.id);
                true
            }
            "up" => {
                let previous = position.map_or(0, |index| index.saturating_sub(1));
                self.selected = visible.get(previous).map(|item| item.id);
                true
            }
            "enter" => {
                if let Some(item) = position.and_then(|index| visible.get(index)).cloned() {
                    self.open_drawer(item, cx);
                }
                true
            }
            "escape" if self.drawer.is_some() => {
                self.drawer = None;
                true
            }
            _ => false,
        };
        if handled {
            cx.stop_propagation();
            cx.notify();
        }
    }
}

fn fetched<T>(result: Result<T>) -> Fetch<T> {
    match result {
        Ok(value) => Fetch::Loaded(value),
        Err(error) => Fetch::Failed(describe_error(&error)),
    }
}

fn describe_error(error: &anyhow::Error) -> SharedString {
    if error.is::<TokenRejected>() {
        "Your Meegle session needs to be renewed.".into()
    } else {
        format!("{error:#}").into()
    }
}

/// Runs a batch of calls on one session. If Meegle rejects the token part-way,
/// the batch runs once more on a session with a renewed token.
async fn with_session<T>(
    store: &Entity<AccountStore>,
    cx: &mut AsyncApp,
    batch: impl AsyncFn(&MeegleSession) -> Result<T>,
) -> Result<T> {
    let session = open_session(store, cx).await?;
    match batch(&session).await {
        Err(error) if error.is::<TokenRejected>() => {
            store.update(cx, |store, _| {
                store.invalidate_access_token(session.access_token())
            });
            let session = open_session(store, cx).await?;
            let result = batch(&session).await;
            if result
                .as_ref()
                .is_err_and(|error| error.is::<TokenRejected>())
            {
                store.update(cx, |store, cx| {
                    store.reject_access_token(session.access_token(), cx)
                });
            }
            result
        }
        result => result,
    }
}

/// A session for a batch of calls. A rejected token is dropped and replaced once;
/// a second rejection marks the account as needing a new sign-in.
async fn open_session(store: &Entity<AccountStore>, cx: &mut AsyncApp) -> Result<MeegleSession> {
    let resource = store
        .read_with(cx, |store, _| store.resource())
        .context("Sign in to Meegle first")?;
    let token = store
        .update(cx, |store, cx| store.access_token(cx))
        .await
        .map_err(|error| anyhow!(error))?;
    match MeegleSession::connect(&resource, &token, cx).await {
        Err(error) if error.is::<TokenRejected>() => {
            store.update(cx, |store, _| store.invalidate_access_token(&token));
            let token = store
                .update(cx, |store, cx| store.access_token(cx))
                .await
                .map_err(|error| anyhow!(error))?;
            let result = MeegleSession::connect(&resource, &token, cx).await;
            if result
                .as_ref()
                .is_err_and(|error| error.is::<TokenRejected>())
            {
                store.update(cx, |store, cx| store.reject_access_token(&token, cx));
            }
            result
        }
        result => result,
    }
}

fn text(value: &serde_json::Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn parse_work_items(response: &serde_json::Value) -> Result<(Vec<WorkItem>, usize)> {
    let list = response
        .get("list")
        .and_then(|list| list.as_array())
        .context("Meegle returned an unexpected work item list")?;
    let items = list
        .iter()
        .filter_map(|entry| {
            let id = entry.pointer("/work_item_info/work_item_id")?.as_u64()?;
            let state = text(entry, "/node_info/node_name")
                .or_else(|| text(entry, "/state_info/start_state_key_name"))
                .unwrap_or_default();
            Some(WorkItem {
                id,
                name: text(entry, "/work_item_info/work_item_name")
                    .unwrap_or_else(|| format!("#{id}"))
                    .into(),
                type_key: text(entry, "/work_item_info/work_item_type_key").unwrap_or_default(),
                project_key: text(entry, "/project_key").unwrap_or_default(),
                project_name: text(entry, "/project_name").unwrap_or_default().into(),
                state: state.into(),
                due: text(entry, "/schedule/end_time"),
            })
        })
        .collect::<Vec<_>>();
    let total = response
        .get("total")
        .and_then(|total| total.as_u64())
        .map_or(items.len(), |total| total as usize);
    Ok((items, total))
}

fn names(value: Option<&serde_json::Value>) -> Vec<SharedString> {
    value
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|person| text(person, "/name"))
        .map(SharedString::from)
        .collect()
}

fn parse_brief(response: &serde_json::Value) -> Result<Brief> {
    let attribute = response
        .get("work_item_attribute")
        .context("Meegle returned an unexpected work item")?;
    let owners = response
        .get("work_item_fields")
        .and_then(|fields| fields.as_array())
        .into_iter()
        .flatten()
        .find(|field| {
            field.get("key").and_then(|key| key.as_str()) == Some("current_status_operator")
        })
        .map(|field| names(field.get("value")))
        .unwrap_or_default();
    let roles = attribute
        .get("role_members")
        .and_then(|roles| roles.as_array())
        .into_iter()
        .flatten()
        .filter_map(|role| Some((text(role, "/name")?.into(), names(role.get("members")))))
        .collect();
    Ok(Brief {
        type_name: text(attribute, "/work_item_type/name")
            .unwrap_or_default()
            .into(),
        status: text(attribute, "/work_item_status/name")
            .unwrap_or_default()
            .into(),
        owners,
        creator: text(attribute, "/create_by/name").map(Into::into),
        created: text(attribute, "/create_time").map(|time| short_time(&time).into()),
        updated: text(attribute, "/update_time").map(|time| short_time(&time).into()),
        updated_by: text(attribute, "/updated_by/name").map(Into::into),
        template: text(attribute, "/template/name").map(Into::into),
        roles,
        simple_name: text(attribute, "/owned_project/simple_name"),
    })
}

/// `2026-04-09T02:15:36Z` → `2026-04-09 02:15` (UTC as Meegle sends it).
fn short_time(time: &str) -> String {
    time::OffsetDateTime::parse(time, &time::format_description::well_known::Rfc3339)
        .ok()
        .and_then(|time| format_millis(time.unix_timestamp() * 1000))
        .unwrap_or_else(|| time.replace('T', " ").chars().take(16).collect())
}

fn parse_comments(response: &serde_json::Value) -> Result<Vec<Comment>> {
    let comments = response
        .get("comments")
        .and_then(|comments| comments.as_array())
        .context("Meegle returned unexpected comments")?;
    Ok(comments
        .iter()
        .filter_map(|comment| {
            let body = text(comment, "/content")
                .or_else(|| text(comment, "/comment_content"))
                .or_else(|| text(comment, "/text"))?;
            Some(Comment {
                author: text(comment, "/operator/name")
                    .or_else(|| text(comment, "/creator/name"))
                    .or_else(|| text(comment, "/user/name"))
                    .unwrap_or_else(|| "Someone".into())
                    .into(),
                time: text(comment, "/created_at")
                    .or_else(|| text(comment, "/create_time"))
                    .map(|time| short_time(&time).into()),
                text: body.into(),
            })
        })
        .collect())
}

fn parse_history(response: &serde_json::Value) -> Result<Vec<HistoryEntry>> {
    let records = response
        .get("op_records")
        .and_then(|records| records.as_array())
        .context("Meegle returned unexpected history")?;
    Ok(records
        .iter()
        .map(|record| HistoryEntry {
            operator: text(record, "/operator").unwrap_or_default().into(),
            action: text(record, "/operation_type")
                .map(|action| action.replace('_', " "))
                .unwrap_or_default()
                .into(),
            time: record
                .get("operation_time")
                .and_then(|time| time.as_i64())
                .and_then(format_millis)
                .map(Into::into),
        })
        .collect())
}

fn format_millis(millis: i64) -> Option<String> {
    let time = time::OffsetDateTime::from_unix_timestamp(millis / 1000).ok()?;
    let offset = time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC);
    let time = time.to_offset(offset);
    Some(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute()
    ))
}

/// Node-driven items have a workflow; status-driven ones answer with an error,
/// which simply hides the workflow section.
fn parse_nodes(response: &serde_json::Value) -> Result<Vec<Node>> {
    if response.get("error").is_some_and(|error| !error.is_null()) {
        return Ok(Vec::new());
    }
    let nodes = response
        .pointer("/data/nodes")
        .or_else(|| response.get("nodes"))
        .or_else(|| response.get("data"))
        .and_then(|nodes| nodes.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(nodes
        .iter()
        .filter_map(|node| {
            let name = text(node, "/node_name").or_else(|| text(node, "/name"))?;
            let status = text(node, "/status")
                .or_else(|| text(node, "/node_status"))
                .unwrap_or_default()
                .to_lowercase();
            let state = if status.contains("pass")
                || status.contains("done")
                || status.contains("finish")
            {
                NodeState::Done
            } else if status.contains("progress")
                || status.contains("doing")
                || status.contains("current")
            {
                NodeState::Current
            } else {
                NodeState::Pending
            };
            Some(Node {
                name: name.into(),
                state,
            })
        })
        .collect())
}

/// `https://<issuer host>/<simple_name>/<type>/detail/<id>`. Scheme and host come
/// from the signed-in account; Meegle data only fills single, encoded path segments.
fn detail_url(issuer: &Url, simple_name: &str, type_key: &str, id: u64) -> Result<Url> {
    if issuer.scheme() != "https" {
        return Err(anyhow!("Meegle links must use https"));
    }
    let id = id.to_string();
    let segments = [simple_name, type_key, "detail", id.as_str()];
    for segment in segments {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(anyhow!("invalid Meegle link segment {segment:?}"));
        }
    }
    let mut url = issuer.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.path_segments_mut()
        .map_err(|_| anyhow!("issuer cannot be a base URL"))?
        .clear()
        .extend(segments);
    Ok(url)
}

/// Up to nine page numbers centred on the current page.
fn page_window(current: usize, pages: usize) -> std::ops::Range<usize> {
    let start = current.saturating_sub(4).min(pages.saturating_sub(9));
    start..(start + 9).min(pages)
}

fn today() -> String {
    let offset = time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC);
    let date = time::OffsetDateTime::now_utc().to_offset(offset).date();
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

fn relative_time(time: SystemTime) -> String {
    let seconds = SystemTime::now()
        .duration_since(time)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    match seconds {
        0..60 => "just now".into(),
        60..3600 => format!("{} min ago", seconds / 60),
        3600..86400 => format!("{} h ago", seconds / 3600),
        _ => format!("{} d ago", seconds / 86400),
    }
}

fn initials(name: &str) -> String {
    name.split_whitespace()
        .filter_map(|part| part.chars().next())
        .take(2)
        .collect::<String>()
        .to_uppercase()
}

// Column widths (logical px), VS Code-like density.
const COLUMN_ID: Pixels = px(100.);
const COLUMN_TYPE: Pixels = px(120.);
const COLUMN_SPACE: Pixels = px(120.);
const COLUMN_STATUS: Pixels = px(150.);
const COLUMN_OWNER: Pixels = px(170.);
const COLUMN_CREATOR: Pixels = px(140.);
const COLUMN_DUE: Pixels = px(100.);
const COLUMN_ACTIONS: Pixels = px(80.);

impl MyWorkPage {
    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let store = self.store.read(cx);
        let account = store.account().cloned();
        let needs_sign_in = store.needs_sign_in();
        let subtitle = match (self.loading, self.updated_at) {
            (true, _) => "Refreshing…".to_string(),
            (false, Some(updated_at)) => format!("Updated {}", relative_time(updated_at)),
            (false, None) => "Your Meegle work items".to_string(),
        };
        h_flex()
            .w_full()
            .justify_between()
            .items_start()
            .gap_6()
            .child(
                v_flex()
                    .gap_1()
                    .child(Headline::new("My Work").size(HeadlineSize::Large))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Label::new(subtitle).color(Color::Muted))
                            .child(
                                Button::new("my-work-refresh", "Refresh")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Subtle)
                                    .disabled(self.loading || account.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                            ),
                    ),
            )
            .when_some(account, |this, account| {
                let host = Url::parse(&account.issuer)
                    .ok()
                    .and_then(|issuer| issuer.host_str().map(str::to_string))
                    .unwrap_or_default();
                let home = Url::parse(&account.issuer).ok();
                let store = self.store.clone();
                this.child(
                    h_flex()
                        .gap_3()
                        .p_3()
                        .min_w(px(360.))
                        .rounded_lg()
                        .border_1()
                        .border_color(cx.theme().colors().border)
                        .bg(cx.theme().colors().element_background)
                        .child(match account.avatar_url.clone() {
                            Some(avatar) => Avatar::new(avatar).size(px(40.)).into_any_element(),
                            None => div()
                                .size(px(40.))
                                .rounded_full()
                                .bg(cx.theme().colors().text_accent.opacity(0.35))
                                .flex()
                                .items_center()
                                .justify_center()
                                .child(Label::new(initials(&account.display_name)))
                                .into_any_element(),
                        })
                        .child(
                            v_flex()
                                .flex_1()
                                .child(Label::new(account.display_name.clone()))
                                .when_some(account.email, |this, email| {
                                    this.child(
                                        Label::new(email)
                                            .size(LabelSize::Small)
                                            .color(Color::Muted),
                                    )
                                })
                                .child(
                                    Label::new(if needs_sign_in {
                                        format!("Needs sign-in · Meegle · {host}")
                                    } else {
                                        format!("Connected · Meegle · {host}")
                                    })
                                    .size(LabelSize::Small)
                                    .color(if needs_sign_in {
                                        Color::Warning
                                    } else {
                                        Color::Success
                                    }),
                                ),
                        )
                        .child(
                            v_flex()
                                .gap_1()
                                .child(
                                    Button::new("my-work-open-meegle", "Open Meegle")
                                        .label_size(LabelSize::Small)
                                        .style(ButtonStyle::Outlined)
                                        .on_click(move |_, _, cx| {
                                            if let Some(home) = &home {
                                                cx.open_url(home.as_str());
                                            }
                                        }),
                                )
                                .child(if needs_sign_in {
                                    Button::new("my-work-sign-in-again", "Sign In Again")
                                        .label_size(LabelSize::Small)
                                        .style(ButtonStyle::Filled)
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.open_sign_in(window, cx)
                                        }))
                                } else {
                                    Button::new("my-work-sign-out", "Sign Out")
                                        .label_size(LabelSize::Small)
                                        .style(ButtonStyle::Subtle)
                                        .on_click(move |_, _, cx| {
                                            store.update(cx, |store, cx| store.sign_out(cx));
                                        })
                                }),
                        ),
                )
            })
    }

    fn render_counts(&self, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .w_full()
            .gap_3()
            .children(WorkList::ALL.map(|list| {
                let active = self.list == list;
                let count = self
                    .counts
                    .get(&list)
                    .map_or_else(|| "–".to_string(), |count| count.to_string());
                let colors = cx.theme().colors();
                v_flex()
                    .id(list.action())
                    .flex_1()
                    .p_3()
                    .rounded_lg()
                    .border_1()
                    .border_color(if active {
                        colors.text_accent
                    } else {
                        colors.border
                    })
                    .bg(colors.element_background)
                    .cursor_pointer()
                    .hover(|style| style.bg(colors.element_hover))
                    .on_click(cx.listener(move |this, _, _, cx| this.select_list(list, cx)))
                    .child(Headline::new(count).size(HeadlineSize::Large).color(
                        if list == WorkList::Overdue {
                            Color::Error
                        } else {
                            Color::Default
                        },
                    ))
                    .child(Label::new(list.label()).color(Color::Muted))
            }))
    }

    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let items = self
            .lists
            .get(&self.list)
            .map(|state| state.items.clone())
            .unwrap_or_default();
        let mut types: Vec<(String, SharedString)> = Vec::new();
        let mut spaces: Vec<(String, SharedString)> = Vec::new();
        for item in &items {
            if !types.iter().any(|(key, _)| *key == item.type_key) {
                let name = self
                    .type_name(item)
                    .unwrap_or_else(|| item.type_key.clone().into());
                types.push((item.type_key.clone(), name));
            }
            if !spaces.iter().any(|(key, _)| *key == item.project_key) {
                spaces.push((item.project_key.clone(), item.project_name.clone()));
            }
        }
        let type_label = self
            .type_filter
            .as_ref()
            .and_then(|key| types.iter().find(|(candidate, _)| candidate == key))
            .map_or_else(|| SharedString::from("All types"), |(_, name)| name.clone());
        let space_label = self
            .space_filter
            .as_ref()
            .and_then(|key| spaces.iter().find(|(candidate, _)| candidate == key))
            .map_or_else(
                || SharedString::from("All spaces"),
                |(_, name)| name.clone(),
            );
        let page = cx.entity().downgrade();
        let type_menu = filter_menu(
            "my-work-type-filter",
            type_label,
            types,
            page.clone(),
            |this, key| this.type_filter = key,
        );
        let space_menu = filter_menu(
            "my-work-space-filter",
            space_label,
            spaces,
            page,
            |this, key| this.space_filter = key,
        );

        h_flex()
            .w_full()
            .gap_2()
            .child(
                h_flex()
                    .w(px(320.))
                    .h(px(28.))
                    .px_2()
                    .rounded_md()
                    .border_1()
                    .border_color(cx.theme().colors().border)
                    .bg(cx.theme().colors().editor_background)
                    .child(
                        Icon::new(IconName::MagnifyingGlass)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(div().flex_1().pl_2().child(self.filter_editor.clone())),
            )
            .child(type_menu)
            .child(space_menu)
            .child(div().flex_1())
            .child(
                Label::new("Rows per page")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .children([20usize, 50].map(|size| {
                Button::new(("my-work-page-size", size), size.to_string())
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .toggle_state(self.page_size == size)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.page_size = size;
                        this.page = 0;
                        this.enrich_visible(cx);
                        cx.notify();
                    }))
            }))
    }

    fn type_name(&self, item: &WorkItem) -> Option<SharedString> {
        self.briefs
            .get(&item.id)
            .map(|brief| brief.type_name.clone())
            .filter(|name| !name.is_empty())
            .or_else(|| {
                self.lists
                    .values()
                    .flat_map(|state| state.items.iter())
                    .filter(|other| other.type_key == item.type_key)
                    .find_map(|other| self.briefs.get(&other.id))
                    .map(|brief| brief.type_name.clone())
                    .filter(|name| !name.is_empty())
            })
    }

    fn render_table(&self, rows: &[WorkItem], cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors();
        let today = today();
        let cell = |width: Pixels| div().w(width).flex_none().min_w_0().overflow_hidden();
        let header = |label: &'static str, width: Option<Pixels>| {
            let base = match width {
                Some(width) => cell(width),
                None => div().flex_1().min_w(px(200.)),
            };
            base.child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
        };
        v_flex()
            .id("my-work-table")
            .w_full()
            .overflow_x_scroll()
            .child(
                v_flex()
                    .min_w(TABLE_MIN_WIDTH)
                    .rounded_lg()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.panel_background)
                    .child(
                        h_flex()
                            .px_3()
                            .py_2()
                            .gap_2()
                            .bg(colors.element_background)
                            .border_b_1()
                            .border_color(colors.border)
                            .child(header("ID", Some(COLUMN_ID)))
                            .child(header("Type", Some(COLUMN_TYPE)))
                            .child(header("Title", None))
                            .child(header("Space", Some(COLUMN_SPACE)))
                            .child(header("Status", Some(COLUMN_STATUS)))
                            .child(header("Current owner", Some(COLUMN_OWNER)))
                            .child(header("Creator", Some(COLUMN_CREATOR)))
                            .child(header("Due", Some(COLUMN_DUE)))
                            .child(cell(COLUMN_ACTIONS)),
                    )
                    .children(rows.iter().map(|item| {
                        let brief = self.briefs.get(&item.id);
                        let failed = self.brief_failures.contains(&item.id);
                        let detail_text = |value: Option<SharedString>| -> AnyElement {
                            match (value, failed) {
                                (Some(value), _) if !value.is_empty() => Label::new(value)
                                    .size(LabelSize::Small)
                                    .truncate()
                                    .into_any_element(),
                                (_, true) => Label::new("couldn’t load")
                                    .size(LabelSize::Small)
                                    .color(Color::Warning)
                                    .into_any_element(),
                                _ if brief.is_some() => Label::new("—")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted)
                                    .into_any_element(),
                                _ => div()
                                    .h(px(10.))
                                    .w(px(70.))
                                    .rounded_sm()
                                    .bg(colors.element_hover)
                                    .into_any_element(),
                            }
                        };
                        let status = brief
                            .map(|brief| brief.status.clone())
                            .filter(|status| !status.is_empty())
                            .unwrap_or_else(|| item.state.clone());
                        let owners = brief
                            .map(|brief| brief.owners.join(", "))
                            .map(SharedString::from);
                        let creator = brief.and_then(|brief| brief.creator.clone());
                        let late = item
                            .due
                            .as_ref()
                            .is_some_and(|due| due.get(..10).unwrap_or(due) < today.as_str())
                            && self.list != WorkList::Done;
                        let selected = self.selected == Some(item.id);
                        let open = item.clone();
                        let select = item.clone();
                        h_flex()
                            .id(("my-work-row", item.id))
                            .px_3()
                            .py_2()
                            .gap_2()
                            .border_b_1()
                            .border_color(colors.border_variant)
                            .cursor_pointer()
                            .when(selected, |this| {
                                this.bg(colors.element_selected)
                                    .border_l_2()
                                    .border_color(colors.text_accent)
                            })
                            .hover(|style| style.bg(colors.element_hover))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.open_drawer(select.clone(), cx)
                            }))
                            .child(
                                cell(COLUMN_ID).child(
                                    Label::new(format!("#{}", item.id))
                                        .size(LabelSize::Small)
                                        .color(Color::Muted),
                                ),
                            )
                            .child(cell(COLUMN_TYPE).child(detail_text(self.type_name(item))))
                            .child(
                                div()
                                    .flex_1()
                                    .min_w(px(200.))
                                    .overflow_hidden()
                                    .child(Label::new(item.name.clone()).truncate()),
                            )
                            .child(
                                cell(COLUMN_SPACE).child(
                                    Label::new(item.project_name.clone())
                                        .size(LabelSize::Small)
                                        .truncate(),
                                ),
                            )
                            .child(
                                cell(COLUMN_STATUS)
                                    .child(Label::new(status).size(LabelSize::Small).truncate()),
                            )
                            .child(cell(COLUMN_OWNER).child(detail_text(owners)))
                            .child(cell(COLUMN_CREATOR).child(detail_text(creator)))
                            .child(
                                cell(COLUMN_DUE).child(
                                    Label::new(item.due.clone().map_or_else(
                                        || "—".to_string(),
                                        |due| {
                                            if late { format!("{due} · late") } else { due }
                                        },
                                    ))
                                    .size(LabelSize::Small)
                                    .color(if late {
                                        Color::Error
                                    } else {
                                        Color::Default
                                    }),
                                ),
                            )
                            .child(
                                cell(COLUMN_ACTIONS).child(
                                    Button::new(("my-work-open", item.id), "Open ↗")
                                        .label_size(LabelSize::Small)
                                        .style(ButtonStyle::Subtle)
                                        .disabled(
                                            brief
                                                .and_then(|brief| brief.simple_name.as_ref())
                                                .is_none(),
                                        )
                                        .tooltip(Tooltip::text("Open in Meegle"))
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            cx.stop_propagation();
                                            this.open_in_meegle(&open, cx);
                                        })),
                                ),
                            )
                    })),
            )
            .into_any_element()
    }

    fn render_pager(&self, filtered: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.lists.get(&self.list);
        let loaded = state.map_or(0, |state| state.items.len());
        let total = state.map_or(0, |state| state.total);
        let pages = filtered.div_ceil(self.page_size).max(1);
        let first = (self.page * self.page_size + 1).min(filtered);
        let last = ((self.page + 1) * self.page_size).min(filtered);
        let filtering = filtered != loaded;
        let can_load_more = loaded < total && self.page + 1 >= pages;
        let summary = if filtering {
            format!("Showing {first}–{last} of {filtered} matching · {loaded} loaded of {total}")
        } else {
            format!(
                "Showing {first}–{last} · {loaded} loaded of {total} {}",
                self.list.label()
            )
        };
        h_flex()
            .w_full()
            .justify_between()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Label::new(summary)
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .when(filtering, |this| {
                        this.child(
                            Label::new("· filters apply to loaded items")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .when(can_load_more, |this| {
                        this.child(
                            Button::new(
                                "my-work-load-more",
                                if self.more_task.is_some() {
                                    "Loading…"
                                } else {
                                    "Load more"
                                },
                            )
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .disabled(self.more_task.is_some())
                            .on_click(cx.listener(|this, _, _, cx| this.load_more(cx))),
                        )
                    }),
            )
            .child(
                h_flex()
                    .gap_1()
                    .child(
                        IconButton::new("my-work-previous-page", IconName::ChevronLeft)
                            .disabled(self.page == 0)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.page = this.page.saturating_sub(1);
                                this.enrich_visible(cx);
                                cx.notify();
                            })),
                    )
                    .children(page_window(self.page, pages).map(|index| {
                        Button::new(("my-work-page", index), (index + 1).to_string())
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .toggle_state(index == self.page)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.page = index;
                                this.enrich_visible(cx);
                                cx.notify();
                            }))
                    }))
                    .child(
                        IconButton::new("my-work-next-page", IconName::ChevronRight)
                            .disabled(self.page + 1 >= pages)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.page = (this.page + 1).min(pages - 1);
                                this.enrich_visible(cx);
                                cx.notify();
                            })),
                    ),
            )
    }

    fn render_drawer(&self, drawer: &Drawer, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors();
        let item = drawer.item.clone();
        let brief = self.briefs.get(&item.id).cloned();
        let url = self.item_url(&item, cx);
        let section = |title: &'static str| {
            v_flex()
                .px_4()
                .py_3()
                .gap_2()
                .border_b_1()
                .border_color(colors.border_variant)
                .child(
                    Label::new(title)
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                )
        };
        let row = |label: &'static str, value: SharedString| {
            h_flex()
                .gap_3()
                .child(
                    div()
                        .w(px(110.))
                        .flex_none()
                        .child(Label::new(label).size(LabelSize::Small).color(Color::Muted)),
                )
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .child(Label::new(value).size(LabelSize::Small)),
                )
        };
        let tab = |id: &'static str, label: &'static str, tab: DrawerTab| {
            Button::new(id, label)
                .label_size(LabelSize::Small)
                .style(ButtonStyle::Subtle)
                .toggle_state(drawer.tab == tab)
                .on_click(cx.listener(move |this, _, _, cx| this.select_drawer_tab(tab, cx)))
        };

        let body: AnyElement = match drawer.tab {
            DrawerTab::Overview => {
                let workflow = match &drawer.nodes {
                    Some(Fetch::Loaded(nodes)) if !nodes.is_empty() => Some(
                        section("WORKFLOW").child(h_flex().flex_wrap().gap_1().children(
                            nodes.iter().map(|node| {
                                let (prefix, color) = match node.state {
                                    NodeState::Done => ("✓ ", Color::Success),
                                    NodeState::Current => ("● ", Color::Warning),
                                    NodeState::Pending => ("", Color::Muted),
                                };
                                div()
                                    .px_2()
                                    .py_0p5()
                                    .rounded_md()
                                    .bg(colors.element_background)
                                    .child(
                                        Label::new(format!("{prefix}{}", node.name))
                                            .size(LabelSize::Small)
                                            .color(color),
                                    )
                            }),
                        )),
                    ),
                    _ => None,
                };
                v_flex()
                    .children(workflow)
                    .child(match &brief {
                        Some(brief) => section("DETAILS")
                            .child(row("Type", brief.type_name.clone()))
                            .child(row("Status", brief.status.clone()))
                            .child(row("Current owner", brief.owners.join(", ").into()))
                            .child(row("Creator", brief.creator.clone().unwrap_or_default()))
                            .child(row("Created", brief.created.clone().unwrap_or_default()))
                            .child(row(
                                "Updated",
                                match (&brief.updated, &brief.updated_by) {
                                    (Some(time), Some(by)) => format!("{time} by {by}").into(),
                                    (Some(time), None) => time.clone(),
                                    _ => SharedString::default(),
                                },
                            ))
                            .child(row("Template", brief.template.clone().unwrap_or_default()))
                            .child(row("Due", item.due.clone().unwrap_or_default().into()))
                            .into_any_element(),
                        None if self.brief_failures.contains(&item.id) => section("DETAILS")
                            .child(
                                h_flex()
                                    .gap_2()
                                    .child(
                                        Label::new("Couldn’t load the details.")
                                            .size(LabelSize::Small)
                                            .color(Color::Warning),
                                    )
                                    .child({
                                        let id = item.id;
                                        Button::new("my-work-drawer-retry", "Retry")
                                            .label_size(LabelSize::Small)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.brief_failures.remove(&id);
                                                this.enrich_item(id, cx);
                                            }))
                                    }),
                            )
                            .into_any_element(),
                        None => section("DETAILS")
                            .child(
                                Label::new("Loading…")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .into_any_element(),
                    })
                    .when_some(
                        brief.as_ref().filter(|brief| !brief.roles.is_empty()),
                        |this, brief| {
                            this.child(section("ROLES").children(brief.roles.iter().map(
                                |(role, members)| {
                                    row_owned(role.clone(), members.join(", ").into())
                                },
                            )))
                        },
                    )
                    .into_any_element()
            }
            DrawerTab::Comments => match &drawer.comments {
                Some(Fetch::Loaded(comments)) if comments.is_empty() => section("COMMENTS")
                    .child(
                        Label::new("No comments yet.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .into_any_element(),
                Some(Fetch::Loaded(comments)) => section("COMMENTS")
                    .children(comments.iter().map(|comment| {
                        v_flex()
                            .gap_0p5()
                            .p_2()
                            .rounded_md()
                            .bg(colors.element_background)
                            .child(
                                Label::new(match &comment.time {
                                    Some(time) => format!("{} · {time}", comment.author),
                                    None => comment.author.to_string(),
                                })
                                .size(LabelSize::XSmall)
                                .color(Color::Muted),
                            )
                            .child(Label::new(comment.text.clone()).size(LabelSize::Small))
                    }))
                    .into_any_element(),
                Some(fetch) => section("COMMENTS")
                    .children(fetch_status(fetch))
                    .into_any_element(),
                None => div().into_any_element(),
            },
            DrawerTab::History => match &drawer.history {
                Some(Fetch::Loaded(history)) if history.is_empty() => section("HISTORY")
                    .child(
                        Label::new("No history.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .into_any_element(),
                Some(Fetch::Loaded(history)) => section("HISTORY")
                    .children(history.iter().map(|entry| {
                        Label::new(match &entry.time {
                            Some(time) => format!("{} · {} · {time}", entry.operator, entry.action),
                            None => format!("{} · {}", entry.operator, entry.action),
                        })
                        .size(LabelSize::Small)
                        .color(Color::Muted)
                    }))
                    .into_any_element(),
                Some(fetch) => section("HISTORY")
                    .children(fetch_status(fetch))
                    .into_any_element(),
                None => div().into_any_element(),
            },
        };

        let copy_url = url.clone();
        let open_url = url;
        v_flex()
            .id("my-work-drawer")
            .occlude()
            .w(DRAWER_WIDTH)
            .flex_none()
            .h_full()
            .overflow_y_scroll()
            .rounded_lg()
            .border_1()
            .border_color(colors.border)
            .bg(colors.elevated_surface_background)
            .child(
                v_flex()
                    .p_4()
                    .gap_2()
                    .border_b_1()
                    .border_color(colors.border_variant)
                    .child(
                        h_flex()
                            .justify_between()
                            .child(
                                Label::new(format!(
                                    "{} · #{} · {}",
                                    self.type_name(&item).unwrap_or_default(),
                                    item.id,
                                    item.project_name
                                ))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                            )
                            .child(
                                IconButton::new("my-work-close-drawer", IconName::Close)
                                    .icon_size(IconSize::Small)
                                    .tooltip(Tooltip::text("Close (Esc)"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.drawer = None;
                                        window.focus(&this.focus_handle, cx);
                                        cx.notify();
                                    })),
                            ),
                    )
                    .child(Label::new(item.name).size(LabelSize::Large))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new("my-work-drawer-open", "Open in Meegle ↗")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Outlined)
                                    .disabled(open_url.is_none())
                                    .on_click(move |_, _, cx| {
                                        if let Some(url) = &open_url {
                                            cx.open_url(url.as_str());
                                        }
                                    }),
                            )
                            .child(
                                Button::new("my-work-drawer-copy", "Copy Link")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Outlined)
                                    .disabled(copy_url.is_none())
                                    .on_click(move |_, _, cx| {
                                        if let Some(url) = &copy_url {
                                            cx.write_to_clipboard(ClipboardItem::new_string(
                                                url.to_string(),
                                            ));
                                        }
                                    }),
                            ),
                    ),
            )
            .child(
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_1()
                    .border_b_1()
                    .border_color(colors.border_variant)
                    .child(tab("my-work-tab-overview", "Overview", DrawerTab::Overview))
                    .child(tab("my-work-tab-comments", "Comments", DrawerTab::Comments))
                    .child(tab("my-work-tab-history", "History", DrawerTab::History)),
            )
            .child(body)
            .into_any_element()
    }

    fn render_message(
        &self,
        message: impl Into<SharedString>,
        action: Option<(&'static str, &'static str)>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_3()
            .child(Label::new(message.into()).color(Color::Muted))
            .when_some(action, |this, (id, label)| {
                this.child(
                    Button::new(id, label)
                        .style(ButtonStyle::Filled)
                        .on_click(cx.listener(move |this, _, window, cx| {
                            if id == "my-work-retry" {
                                this.refresh(cx);
                            } else {
                                this.open_sign_in(window, cx);
                            }
                        })),
                )
            })
            .into_any_element()
    }
}

fn fetch_status<T>(fetch: &Fetch<T>) -> Option<AnyElement> {
    match fetch {
        Fetch::Loading => Some(
            Label::new("Loading…")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element(),
        ),
        Fetch::Failed(error) => Some(
            Label::new(format!("Couldn’t load: {error}"))
                .size(LabelSize::Small)
                .color(Color::Warning)
                .into_any_element(),
        ),
        Fetch::Loaded(_) => None,
    }
}

fn row_owned(label: SharedString, value: SharedString) -> impl IntoElement {
    h_flex()
        .gap_3()
        .child(
            div()
                .w(px(110.))
                .flex_none()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .child(Label::new(value).size(LabelSize::Small)),
        )
}

fn filter_menu(
    id: &'static str,
    label: SharedString,
    options: Vec<(String, SharedString)>,
    page: WeakEntity<MyWorkPage>,
    apply: fn(&mut MyWorkPage, Option<String>),
) -> impl IntoElement {
    PopoverMenu::new(id)
        .trigger(
            Button::new(SharedString::from(format!("{id}-trigger")), label)
                .label_size(LabelSize::Small)
                .style(ButtonStyle::Outlined)
                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
        )
        .menu(move |window, cx| {
            let page = page.clone();
            let options = options.clone();
            Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                let all = page.clone();
                menu = menu.entry("All", None, move |_, cx| {
                    all.update(cx, |this, cx| {
                        apply(this, None);
                        this.page = 0;
                        this.enrich_visible(cx);
                        cx.notify();
                    })
                    .ok();
                });
                for (key, name) in options {
                    let page = page.clone();
                    menu = menu.entry(name, None, move |_, cx| {
                        let key = key.clone();
                        page.update(cx, |this, cx| {
                            apply(this, Some(key));
                            this.page = 0;
                            this.enrich_visible(cx);
                            cx.notify();
                        })
                        .ok();
                    });
                }
                menu
            }))
        })
}

impl Render for MyWorkPage {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let background = cx.theme().colors().editor_background;
        let signed_in = self.store.read(cx).account().is_some();
        let filtered = self.filtered(cx);
        let rows = filtered
            .iter()
            .skip(self.page * self.page_size)
            .take(self.page_size)
            .cloned()
            .collect::<Vec<_>>();
        let overlay_drawer = self.page_width < DRAWER_OVERLAY_BELOW;
        let this = cx.entity();
        // Measures the module's real width (the window may also hold sidebars).
        let measure = gpui::canvas(
            move |bounds, _, cx| {
                this.update(cx, |this, cx| {
                    if this.page_width != bounds.size.width {
                        this.page_width = bounds.size.width;
                        cx.notify();
                    }
                })
            },
            |_, _, _, _| {},
        )
        .absolute()
        .size_full();

        let content: AnyElement = if !signed_in {
            self.render_message(
                "Sign in with Meegle to see your work items here.",
                Some(("my-work-sign-in", "Sign In with Meegle")),
                cx,
            )
        } else if !self.lists.contains_key(&self.list) {
            match self.error.as_ref().or(self.list_errors.get(&self.list)) {
                Some(error) => self.render_message(
                    format!("Couldn’t load your work: {error}"),
                    Some(("my-work-retry", "Retry")),
                    cx,
                ),
                None if self.loading => self.render_message("Loading your work…", None, cx),
                None => self.render_message(
                    "This list hasn’t loaded.",
                    Some(("my-work-retry", "Retry")),
                    cx,
                ),
            }
        } else {
            let table: AnyElement = if rows.is_empty() {
                v_flex()
                    .p_6()
                    .items_center()
                    .gap_2()
                    .child(Label::new("Nothing matches.").color(Color::Muted))
                    .child(
                        Button::new("my-work-clear-filters", "Clear filters")
                            .style(ButtonStyle::Subtle)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.type_filter = None;
                                this.space_filter = None;
                                this.filter_editor
                                    .update(cx, |editor, cx| editor.set_text("", window, cx));
                                cx.notify();
                            })),
                    )
                    .into_any_element()
            } else {
                self.render_table(&rows, cx)
            };
            v_flex()
                .w_full()
                .gap_4()
                .child(self.render_counts(cx))
                .child(self.render_toolbar(cx))
                .when_some(self.error.clone(), |this, error| {
                    this.child(
                        h_flex()
                            .gap_2()
                            .child(
                                Label::new(format!("Couldn’t refresh: {error}"))
                                    .size(LabelSize::Small)
                                    .color(Color::Warning),
                            )
                            .child(
                                Button::new("my-work-retry-inline", "Retry")
                                    .label_size(LabelSize::Small)
                                    .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                            ),
                    )
                })
                .child(table)
                .child(self.render_pager(filtered.len(), cx))
                .into_any_element()
        };

        let drawer = self
            .drawer
            .as_ref()
            .map(|drawer| self.render_drawer(drawer, cx));
        // Narrow pages show the drawer over the table instead of beside it.
        let (side_drawer, overlay) = if overlay_drawer {
            (
                None,
                drawer.map(|drawer| {
                    div()
                        .absolute()
                        .top_0()
                        .right_0()
                        .h_full()
                        .p_4()
                        .child(drawer)
                }),
            )
        } else {
            (drawer, None)
        };

        v_flex()
            .key_context("MyWork")
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(Self::handle_key))
            .relative()
            .size_full()
            .bg(background)
            .child(measure)
            .child(
                h_flex()
                    .size_full()
                    .items_start()
                    .gap_5()
                    .p_8()
                    .child(
                        v_flex()
                            .id("my-work-main")
                            .flex_1()
                            .min_w_0()
                            .h_full()
                            .overflow_y_scroll()
                            .gap_6()
                            .child(self.render_header(cx))
                            .when(signed_in, |this| this.child(Divider::horizontal()))
                            .child(content),
                    )
                    .when(!overlay_drawer, |this| this.children(side_drawer)),
            )
            .children(overlay)
    }
}

impl Focusable for MyWorkPage {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_work_items() {
        let response = serde_json::json!({
            "total": 7,
            "list": [
                {
                    "work_item_info": { "work_item_id": 1001, "work_item_name": "Fix login", "work_item_type_key": "story" },
                    "project_key": "p1",
                    "project_name": "Space A",
                    "state_info": { "start_state_key_name": "Pending Approval" },
                    "node_info": { "node_name": "" },
                    "schedule": { "end_time": "2026-01-22" }
                },
                { "work_item_info": { "work_item_name": "missing id" } }
            ]
        });
        let (items, total) = parse_work_items(&response).unwrap();
        assert_eq!(total, 7);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].state.as_ref(), "Pending Approval");
        assert_eq!(items[0].due.as_deref(), Some("2026-01-22"));
        assert!(parse_work_items(&serde_json::json!({ "error": "x" })).is_err());
    }

    #[test]
    fn test_parse_brief() {
        let response = serde_json::json!({
            "work_item_attribute": {
                "work_item_type": { "name": "Story" },
                "work_item_status": { "name": "In Development" },
                "create_by": { "name": "Pat Lee" },
                "create_time": "2026-04-09T02:15:36Z",
                "owned_project": { "simple_name": "spaces" },
                "role_members": [{ "name": "QA", "members": [{ "name": "Sam Wu" }] }]
            },
            "work_item_fields": [
                { "key": "current_status_operator", "value": [{ "name": "Alex Doe" }, { "name": "Robin Ho" }] }
            ]
        });
        let brief = parse_brief(&response).unwrap();
        assert_eq!(brief.type_name.as_ref(), "Story");
        assert_eq!(
            brief.owners,
            vec![SharedString::from("Alex Doe"), "Robin Ho".into()]
        );
        assert_eq!(brief.creator.as_deref(), Some("Pat Lee"));
        // Shown in local time, so only the shape is fixed here.
        assert!(brief.created.as_deref().is_some_and(|created| created.len() == 16));
        assert_eq!(brief.simple_name.as_deref(), Some("spaces"));
        assert_eq!(brief.roles.len(), 1);
    }

    #[test]
    fn test_parse_nodes_tolerates_status_driven_items() {
        let error = serde_json::json!({ "data": null, "error": { "code": "x", "message": "y" } });
        assert!(parse_nodes(&error).unwrap().is_empty());
    }

    #[test]
    fn test_detail_url_encodes_segments_and_keeps_host() {
        let issuer = Url::parse("https://project.example.com").unwrap();
        assert_eq!(
            detail_url(&issuer, "spaces", "story", 42).unwrap().as_str(),
            "https://project.example.com/spaces/story/detail/42"
        );
        let hostile = detail_url(&issuer, "evil.com/x?y#z", "a b", 1).unwrap();
        assert_eq!(hostile.host_str(), Some("project.example.com"));
        assert!(detail_url(&issuer, "..", "story", 1).is_err());
    }
}
