use anyhow::{Context as _, Result, anyhow};
use collections::HashMap;
use gpui::{
    Action, App, AsyncApp, AsyncWindowContext, Entity, EventEmitter, FocusHandle, Focusable,
    Pixels, Subscription, Task, WeakEntity, Window, actions, prelude::*, px,
};
use ui::{Button, ButtonStyle, IconButton, IconName, ListItem, Tooltip, prelude::*};
use url::Url;
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
};

use crate::{AccountEvent, AccountStore, WelcomeModal, meegle};

actions!(
    vilo_account,
    [
        /// Opens the Meegle My Work view in the side bar.
        ToggleMyWork
    ]
);

const PANEL_KEY: &str = "MyWorkPanel";
const HEADER_HEIGHT: Pixels = px(35.);

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action(|workspace, _: &ToggleMyWork, window, cx| {
            workspace.focus_panel::<MyWorkPanel>(window, cx);
        });
    })
    .detach();
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Todo,
    ThisWeek,
    Overdue,
}

impl Tab {
    const ALL: [Tab; 3] = [Tab::Todo, Tab::ThisWeek, Tab::Overdue];

    fn label(self) -> &'static str {
        match self {
            Tab::Todo => "To Do",
            Tab::ThisWeek => "This Week",
            Tab::Overdue => "Overdue",
        }
    }

    fn action(self) -> &'static str {
        match self {
            Tab::Todo => "todo",
            Tab::ThisWeek => "this_week",
            Tab::Overdue => "overdue",
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
    due: Option<SharedString>,
}

#[derive(Clone, Copy)]
enum MessageAction {
    SignIn,
    SignInAgain,
    Retry,
}

enum LoadState {
    NotLoaded,
    Loading,
    Loaded,
    Failed(SharedString),
}

/// Vilo: the signed-in user's Meegle work items (to do, this week, overdue).
/// Read-only; items open in the browser.
pub struct MyWorkPanel {
    workspace: WeakEntity<Workspace>,
    store: Entity<AccountStore>,
    focus_handle: FocusHandle,
    position: DockPosition,
    tab: Tab,
    items: Vec<WorkItem>,
    total: usize,
    page: u32,
    load_state: LoadState,
    /// Replacing the task cancels the previous request, so a slower answer for an
    /// older tab or page can never land in the list.
    load_task: Option<Task<()>>,
    open_task: Option<Task<()>>,
    simple_names: HashMap<String, String>,
    active: bool,
    _subscriptions: Vec<Subscription>,
}

impl MyWorkPanel {
    pub async fn load(
        workspace: WeakEntity<Workspace>,
        mut cx: AsyncWindowContext,
    ) -> Result<Entity<Self>> {
        let store = cx
            .update(|_, cx| AccountStore::global(cx))?
            .context("vilo_account::init has not run")?;
        workspace.update_in(&mut cx, |workspace, _, cx| {
            let workspace = workspace.weak_handle();
            cx.new(|cx| {
                let subscriptions = vec![
                    cx.observe(&store, |_, _, cx| cx.notify()),
                    cx.subscribe(&store, |this: &mut Self, _, event, cx| match event {
                        AccountEvent::SignedIn => this.reload(cx),
                        AccountEvent::SignedOut => this.clear(cx),
                    }),
                ];
                Self {
                    workspace,
                    store,
                    focus_handle: cx.focus_handle(),
                    position: DockPosition::Left,
                    tab: Tab::Todo,
                    items: Vec::new(),
                    total: 0,
                    page: 0,
                    load_state: LoadState::NotLoaded,
                    load_task: None,
                    open_task: None,
                    simple_names: HashMap::default(),
                    active: false,
                    _subscriptions: subscriptions,
                }
            })
        })
    }

    fn clear(&mut self, cx: &mut Context<Self>) {
        self.load_task = None;
        self.open_task = None;
        self.items.clear();
        self.total = 0;
        self.page = 0;
        self.simple_names.clear();
        self.load_state = LoadState::NotLoaded;
        cx.notify();
    }

    fn reload(&mut self, cx: &mut Context<Self>) {
        self.clear(cx);
        if self.active {
            self.fetch_page(1, cx);
        }
    }

    fn select_tab(&mut self, tab: Tab, cx: &mut Context<Self>) {
        if self.tab != tab {
            self.tab = tab;
            self.reload(cx);
        }
    }

    fn fetch_page(&mut self, page: u32, cx: &mut Context<Self>) {
        if self.store.read(cx).account().is_none() {
            return;
        }
        let store = self.store.clone();
        let tab = self.tab;
        let epoch = store.read(cx).epoch();
        self.load_state = LoadState::Loading;
        self.load_task = Some(cx.spawn(async move |this, cx| {
            let arguments = serde_json::json!({ "action": tab.action(), "page_num": page });
            let result = call_meegle(&store, "list_todo", arguments, cx)
                .await
                .and_then(|response| parse_work_items(&response));
            this.update(cx, |this, cx| {
                // Discard answers for an account that has since changed.
                if this.store.read(cx).epoch() != epoch || this.tab != tab {
                    return;
                }
                match result {
                    Ok((items, total)) => {
                        if page == 1 {
                            this.items.clear();
                        }
                        this.items.extend(items);
                        this.total = total;
                        this.page = page;
                        this.load_state = LoadState::Loaded;
                    }
                    Err(error) => {
                        log::warn!("loading Meegle work items failed: {error:#}");
                        this.load_state = LoadState::Failed(format!("{error:#}").into());
                    }
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_item(&mut self, item: &WorkItem, cx: &mut Context<Self>) {
        let Some(issuer) = self
            .store
            .read(cx)
            .account()
            .and_then(|account| Url::parse(&account.issuer).ok())
        else {
            return;
        };
        let store = self.store.clone();
        let cached = self.simple_names.get(&item.project_key).cloned();
        let item = item.clone();
        self.open_task = Some(cx.spawn(async move |this, cx| {
            let simple_name = match cached {
                Some(simple_name) => simple_name,
                None => {
                    let arguments = serde_json::json!({ "project_key": item.project_key });
                    match call_meegle(&store, "search_project_info", arguments, cx)
                        .await
                        .and_then(|response| parse_simple_name(&response, &item.project_key))
                    {
                        Ok(simple_name) => {
                            this.update(cx, |this, _| {
                                this.simple_names
                                    .insert(item.project_key.clone(), simple_name.clone());
                            })
                            .ok();
                            simple_name
                        }
                        Err(error) => {
                            log::warn!("resolving the Meegle space failed: {error:#}");
                            return;
                        }
                    }
                }
            };
            match detail_url(&issuer, &simple_name, &item.type_key, item.id) {
                Ok(url) => cx.update(|cx| cx.open_url(url.as_str())),
                Err(error) => log::warn!("cannot link to Meegle work item: {error:#}"),
            }
        }));
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

    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let loading = matches!(self.load_state, LoadState::Loading);
        h_flex()
            .h(HEADER_HEIGHT)
            .px_3()
            .justify_between()
            .child(
                Label::new("MY WORK")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .child(
                IconButton::new("my-work-refresh", IconName::RotateCw)
                    .icon_size(IconSize::Small)
                    .disabled(loading || self.store.read(cx).account().is_none())
                    .tooltip(Tooltip::text("Refresh"))
                    .on_click(cx.listener(|this, _, _, cx| this.reload_now(cx))),
            )
    }

    fn reload_now(&mut self, cx: &mut Context<Self>) {
        self.load_task = None;
        self.fetch_page(1, cx);
    }

    fn render_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .px_2()
            .pb_1()
            .gap_1()
            .children(Tab::ALL.into_iter().map(|tab| {
                Button::new(tab.action(), tab.label())
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .toggle_state(self.tab == tab)
                    .on_click(cx.listener(move |this, _, _, cx| this.select_tab(tab, cx)))
            }))
    }

    fn render_message(
        &self,
        message: impl Into<SharedString>,
        action: Option<MessageAction>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        v_flex()
            .p_4()
            .gap_2()
            .child(
                Label::new(message.into())
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .when_some(action, |this, action| {
                let label = match action {
                    MessageAction::SignIn => "Sign In with Meegle",
                    MessageAction::SignInAgain => "Sign In Again",
                    MessageAction::Retry => "Retry",
                };
                this.child(
                    Button::new("my-work-message-action", label)
                        .style(ButtonStyle::Filled)
                        .full_width()
                        .on_click(cx.listener(move |this, _, window, cx| match action {
                            MessageAction::Retry => this.reload_now(cx),
                            MessageAction::SignIn | MessageAction::SignInAgain => {
                                this.open_sign_in(window, cx)
                            }
                        })),
                )
            })
            .into_any_element()
    }

    fn render_items(&self, cx: &mut Context<Self>) -> AnyElement {
        let overdue = self.tab == Tab::Overdue;
        let has_more = self.items.len() < self.total;
        let loading = matches!(self.load_state, LoadState::Loading);
        v_flex()
            .id("my-work-items")
            .flex_1()
            .overflow_y_scroll()
            .children(self.items.iter().enumerate().map(|(index, item)| {
                let mut details = vec![item.project_name.to_string()];
                if !item.state.is_empty() {
                    details.push(item.state.to_string());
                }
                let due = item.due.clone();
                let open = item.clone();
                ListItem::new(("my-work-item", index))
                    .spacing(ui::ListItemSpacing::Sparse)
                    .child(
                        v_flex()
                            .w_full()
                            .min_w_0()
                            .child(
                                Label::new(item.name.clone())
                                    .size(LabelSize::Small)
                                    .truncate(),
                            )
                            .child(
                                h_flex()
                                    .gap_1()
                                    .child(
                                        Label::new(details.join(" · "))
                                            .size(LabelSize::XSmall)
                                            .color(Color::Muted)
                                            .truncate(),
                                    )
                                    .when_some(due, |this, due| {
                                        this.child(
                                            Label::new(format!("· due {due}"))
                                                .size(LabelSize::XSmall)
                                                .color(if overdue {
                                                    Color::Error
                                                } else {
                                                    Color::Muted
                                                }),
                                        )
                                    }),
                            ),
                    )
                    .tooltip(Tooltip::text("Open in Meegle"))
                    .on_click(cx.listener(move |this, _, _, cx| this.open_item(&open, cx)))
            }))
            .when(has_more, |this| {
                let next_page = self.page + 1;
                this.child(
                    h_flex().p_2().child(
                        Button::new(
                            "my-work-more",
                            if loading { "Loading…" } else { "Load more" },
                        )
                        .full_width()
                        .style(ButtonStyle::Subtle)
                        .disabled(loading)
                        .on_click(
                            cx.listener(move |this, _, _, cx| this.fetch_page(next_page, cx)),
                        ),
                    ),
                )
            })
            .into_any_element()
    }
}

/// Calls a read-only Meegle tool with the account's token. A rejected token is
/// dropped and the call retried once with a refreshed one.
async fn call_meegle(
    store: &Entity<AccountStore>,
    name: &str,
    arguments: serde_json::Value,
    cx: &mut AsyncApp,
) -> Result<serde_json::Value> {
    let resource = store
        .read_with(cx, |store, _| store.resource())
        .context("Sign in to Meegle first")?;
    let token = store
        .update(cx, |store, cx| store.access_token(cx))
        .await
        .map_err(|error| anyhow!(error))?;
    match meegle::call_tool(&resource, &token, name, arguments.clone(), cx).await {
        Err(error) if error.is::<meegle::TokenRejected>() => {
            store.update(cx, |store, _| store.invalidate_access_token(&token));
            let token = store
                .update(cx, |store, cx| store.access_token(cx))
                .await
                .map_err(|error| anyhow!(error))?;
            let result = meegle::call_tool(&resource, &token, name, arguments, cx).await;
            if result
                .as_ref()
                .is_err_and(|error| error.is::<meegle::TokenRejected>())
            {
                store.update(cx, |store, cx| store.reject_access_token(&token, cx));
            }
            result
        }
        result => result,
    }
}

fn parse_work_items(response: &serde_json::Value) -> Result<(Vec<WorkItem>, usize)> {
    let list = response
        .get("list")
        .and_then(|list| list.as_array())
        .context("Meegle returned an unexpected work item list")?;
    let text = |value: &serde_json::Value, path: &[&str]| -> String {
        path.iter()
            .try_fold(value, |value, key| value.get(*key))
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string()
    };
    let items = list
        .iter()
        .filter_map(|entry| {
            let id = entry.pointer("/work_item_info/work_item_id")?.as_u64()?;
            let node = text(entry, &["node_info", "node_name"]);
            let state = if node.is_empty() {
                text(entry, &["state_info", "start_state_key_name"])
            } else {
                node
            };
            let due = text(entry, &["schedule", "end_time"]);
            Some(WorkItem {
                id,
                name: text(entry, &["work_item_info", "work_item_name"]).into(),
                type_key: text(entry, &["work_item_info", "work_item_type_key"]),
                project_key: text(entry, &["project_key"]),
                project_name: text(entry, &["project_name"]).into(),
                state: state.into(),
                due: (!due.is_empty()).then(|| due.into()),
            })
        })
        .collect::<Vec<_>>();
    let total = response
        .get("total")
        .and_then(|total| total.as_u64())
        .map_or(items.len(), |total| total as usize);
    Ok((items, total))
}

fn parse_simple_name(response: &serde_json::Value, project_key: &str) -> Result<String> {
    response
        .get("projects")
        .and_then(|projects| projects.as_array())
        .into_iter()
        .flatten()
        .find(|project| {
            project.get("project_key").and_then(|key| key.as_str()) == Some(project_key)
        })
        .and_then(|project| project.get("simple_name")?.as_str())
        .filter(|simple_name| !simple_name.is_empty())
        .map(str::to_string)
        .context("Meegle did not return the space for this work item")
}

/// `https://<issuer host>/<simple_name>/<type>/detail/<id>`. Scheme and host come
/// from the signed-in account; tool output only fills single, encoded path segments.
fn detail_url(issuer: &Url, simple_name: &str, type_key: &str, id: u64) -> Result<Url> {
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

impl Render for MyWorkPanel {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let store = self.store.read(cx);
        let body = if store.account().is_none() {
            self.render_message(
                "Sign in with Meegle to see your work items here.",
                Some(MessageAction::SignIn),
                cx,
            )
        } else if store.needs_sign_in() {
            self.render_message(
                "Your Meegle session needs to be renewed.",
                Some(MessageAction::SignInAgain),
                cx,
            )
        } else {
            match &self.load_state {
                LoadState::Failed(error) if self.items.is_empty() => self.render_message(
                    format!("Could not load work items: {error}"),
                    Some(MessageAction::Retry),
                    cx,
                ),
                LoadState::NotLoaded | LoadState::Loading if self.items.is_empty() => {
                    self.render_message("Loading…", None, cx)
                }
                LoadState::Loaded if self.items.is_empty() => {
                    self.render_message("Nothing here.", None, cx)
                }
                LoadState::Failed(error) => v_flex()
                    .flex_1()
                    .child(self.render_message(
                        format!("Could not refresh: {error}"),
                        Some(MessageAction::Retry),
                        cx,
                    ))
                    .child(self.render_items(cx))
                    .into_any_element(),
                _ => self.render_items(cx),
            }
        };
        v_flex()
            .key_context("MyWorkPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .child(self.render_header(cx))
            .child(self.render_tabs(cx))
            .child(body)
    }
}

impl Focusable for MyWorkPanel {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for MyWorkPanel {}

impl Panel for MyWorkPanel {
    fn persistent_name() -> &'static str {
        "My Work Panel"
    }

    fn panel_key() -> &'static str {
        PANEL_KEY
    }

    fn position(&self, _: &Window, _: &App) -> DockPosition {
        self.position
    }

    fn position_is_valid(&self, position: DockPosition) -> bool {
        // shortcut: left side only, like the search panel; moving it needs a persisted setting.
        position == DockPosition::Left
    }

    fn set_position(&mut self, position: DockPosition, _: &mut Window, cx: &mut Context<Self>) {
        self.position = position;
        cx.notify();
    }

    fn default_size(&self, _: &Window, _: &App) -> Pixels {
        px(320.)
    }

    fn icon(&self, _: &Window, _: &App) -> Option<IconName> {
        Some(IconName::ListTodo)
    }

    fn icon_tooltip(&self, _: &Window, _: &App) -> Option<&'static str> {
        Some("My Work")
    }

    fn toggle_action(&self) -> Box<dyn Action> {
        Box::new(ToggleMyWork)
    }

    fn activation_priority(&self) -> u32 {
        8
    }

    fn set_active(&mut self, active: bool, _: &mut Window, cx: &mut Context<Self>) {
        self.active = active;
        if active && matches!(self.load_state, LoadState::NotLoaded) {
            self.fetch_page(1, cx);
        }
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
                    "work_item_info": { "work_item_id": 11431316, "work_item_name": "Fix login", "work_item_type_key": "story" },
                    "project_key": "p1",
                    "project_name": "Master",
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
        assert_eq!(items[0].id, 11431316);
        assert_eq!(items[0].state.as_ref(), "Pending Approval");
        assert_eq!(items[0].due.as_deref(), Some("2026-01-22"));
        assert!(parse_work_items(&serde_json::json!({ "error": "x" })).is_err());
    }

    #[test]
    fn test_detail_url_encodes_segments_and_keeps_host() {
        let issuer = Url::parse("https://project.larksuite.com").unwrap();
        assert_eq!(
            detail_url(&issuer, "spaces", "story", 42).unwrap().as_str(),
            "https://project.larksuite.com/spaces/story/detail/42"
        );
        let hostile = detail_url(&issuer, "evil.com/x?y#z", "a b", 1).unwrap();
        assert_eq!(hostile.host_str(), Some("project.larksuite.com"));
        assert_eq!(hostile.path(), "/evil.com%2Fx%3Fy%23z/a%20b/detail/1");
        assert!(detail_url(&issuer, "..", "story", 1).is_err());
        assert!(detail_url(&issuer, "spaces", "", 1).is_err());
    }

    #[test]
    fn test_parse_simple_name_matches_project() {
        let response = serde_json::json!({
            "projects": [
                { "project_key": "other", "simple_name": "nope" },
                { "project_key": "p1", "simple_name": "spaces" }
            ]
        });
        assert_eq!(parse_simple_name(&response, "p1").unwrap(), "spaces");
        assert!(parse_simple_name(&response, "missing").is_err());
    }
}
