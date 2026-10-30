//! A MongoDB collection opened in a tab: its query, page, count, view and expansion state, and how it renders.

use std::sync::Arc;

use anyhow::anyhow;
use collections::{HashMap, HashSet};
use editor::Editor;
use gpui::{ClipboardItem, Entity, ListHorizontalSizingBehavior, Task, UniformListScrollHandle, Window, prelude::*, uniform_list};
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, ContextMenu, IconButton, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::{
    bson::{Bson, Document},
    display::{Shown, ValueKind, human_date, show, to_canonical_json},
    mongo::{FindRequest, to_display_json},
    shell::parse_query,
};

use crate::{COUNT_TIME_LIMIT, DatabaseView, Fetch, PAGE_SIZES, QUERY_TIME_LIMIT, boxed, editor, fetched, render_lines, run};

/// Top-level fields shown per document before "Show N more fields", as Compass does.
const FIELDS_SHOWN: usize = 25;
/// Array items shown when an array is opened, then more on request.
const ITEMS_SHOWN: usize = 100;
const ROW_HEIGHT: f32 = 22.;
const HISTORY_PER_COLLECTION: usize = 30;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum DocumentView {
    List,
    Json,
    Table,
}

pub(crate) struct MongoPage {
    documents: Arc<Vec<Document>>,
    /// The JSON view's lines, made once off the UI thread.
    json_lines: Vec<SharedString>,
    has_more: bool,
}

#[derive(Clone, PartialEq)]
enum Total {
    Counting,
    Exact(u64),
    /// An empty filter uses the collection's metadata count, labelled as an estimate.
    Estimated(u64),
    Unknown,
}

/// A path into a document: keys and array positions as separate steps, so a key containing a dot stays one step.
type FieldPath = Vec<String>;

pub(crate) struct MongoTab {
    pub(crate) database: String,
    pub(crate) collection: String,
    filter: Entity<Editor>,
    projection: Entity<Editor>,
    sort: Entity<Editor>,
    skip_editor: Entity<Editor>,
    show_options: bool,
    view: DocumentView,
    page_size: u32,
    skip: u64,
    page: Option<Fetch<MongoPage>>,
    total: Option<Total>,
    /// The filter the total belongs to, so paging does not count again.
    counted_filter: Option<Document>,
    estimated: Option<u64>,
    expanded: HashSet<(usize, FieldPath)>,
    show_all: HashSet<usize>,
    items_shown: HashMap<(usize, FieldPath), usize>,
    scroll: UniformListScrollHandle,
    /// Bumped on every query; answers for an older one are dropped.
    generation: u64,
    _tasks: Vec<Task<()>>,
}

impl MongoTab {
    pub(crate) fn new(database: String, collection: String, window: &mut Window, cx: &mut gpui::App) -> Self {
        Self {
            database,
            collection,
            filter: editor("Type a query: { field: 'value' }", "", false, window, cx),
            projection: editor("{ field: 0 }", "", false, window, cx),
            sort: editor("{ field: -1 }", "", false, window, cx),
            skip_editor: editor("0", "", false, window, cx),
            show_options: false,
            view: DocumentView::List,
            page_size: PAGE_SIZES[0],
            skip: 0,
            page: None,
            total: None,
            counted_filter: None,
            estimated: None,
            expanded: HashSet::default(),
            show_all: HashSet::default(),
            items_shown: HashMap::default(),
            scroll: UniformListScrollHandle::new(),
            generation: 0,
            _tasks: Vec::new(),
        }
    }

    pub(crate) fn title(&self) -> String {
        self.collection.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct SavedQuery {
    connection: String,
    database: String,
    collection: String,
    filter: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    sort: String,
    #[serde(default)]
    skip: String,
    #[serde(default)]
    favorite: bool,
}

impl SavedQuery {
    fn label(&self) -> String {
        let mut label = if self.filter.trim().is_empty() { "{}".to_string() } else { self.filter.trim().to_string() };
        for (name, value) in [("project", &self.project), ("sort", &self.sort), ("skip", &self.skip)] {
            if !value.trim().is_empty() {
                label.push_str(&format!(" · {name} {}", value.trim()));
            }
        }
        label.chars().take(90).collect()
    }

    fn same_query(&self, other: &SavedQuery) -> bool {
        (&self.connection, &self.database, &self.collection, &self.filter, &self.project, &self.sort, &self.skip)
            == (&other.connection, &other.database, &other.collection, &other.filter, &other.project, &other.sort, &other.skip)
    }
}

/// Recent and starred queries; queries can hold player ids, so the user can clear them.
pub(crate) fn load_history() -> Vec<SavedQuery> {
    std::fs::read(history_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn history_path() -> std::path::PathBuf {
    paths::config_dir().join("database_query_history.json")
}

/// Walks a document by path; positions index arrays.
fn value_at<'a>(document: &'a Document, path: &[String]) -> Option<&'a Bson> {
    let (first, rest) = path.split_first()?;
    let mut value = document.get(first)?;
    for step in rest {
        value = match value {
            Bson::Document(inner) => inner.get(step)?,
            Bson::Array(items) => items.get(step.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}

/// What one line of the list view shows. Every row has the same height so the list can be virtual.
#[derive(Clone)]
enum Row {
    /// The top of a document's card, with its copy button.
    Top(usize),
    Field { document: usize, path: FieldPath, depth: usize },
    MoreFields { document: usize, hidden: usize },
    MoreItems { document: usize, path: FieldPath, depth: usize, hidden: usize },
    Bottom,
}

fn kind_color(kind: ValueKind) -> Color {
    match kind {
        ValueKind::String => Color::Success,
        ValueKind::Number | ValueKind::Date => Color::Accent,
        ValueKind::ObjectId => Color::Error,
        ValueKind::Boolean => Color::Warning,
        ValueKind::Null => Color::Muted,
        ValueKind::Other => Color::Default,
    }
}

impl MongoTab {
    fn push_rows(&self, rows: &mut Vec<Row>, document: usize, path: FieldPath, value: &Bson, depth: usize) {
        rows.push(Row::Field { document, path: path.clone(), depth });
        if !self.expanded.contains(&(document, path.clone())) {
            return;
        }
        match value {
            Bson::Document(inner) => {
                for (key, value) in inner {
                    let mut child = path.clone();
                    child.push(key.clone());
                    self.push_rows(rows, document, child, value, depth + 1);
                }
            }
            Bson::Array(items) => {
                let shown = self.items_shown.get(&(document, path.clone())).copied().unwrap_or(ITEMS_SHOWN);
                for (position, value) in items.iter().enumerate().take(shown) {
                    let mut child = path.clone();
                    child.push(position.to_string());
                    self.push_rows(rows, document, child, value, depth + 1);
                }
                if items.len() > shown {
                    rows.push(Row::MoreItems { document, path, depth: depth + 1, hidden: items.len() - shown });
                }
            }
            _ => {}
        }
    }

    fn list_rows(&self, documents: &[Document]) -> Vec<Row> {
        let mut rows = Vec::new();
        for (index, document) in documents.iter().enumerate() {
            rows.push(Row::Top(index));
            let all = self.show_all.contains(&index);
            for (key, value) in document.iter().take(if all { usize::MAX } else { FIELDS_SHOWN }) {
                self.push_rows(&mut rows, index, vec![key.clone()], value, 0);
            }
            let hidden = document.len().saturating_sub(FIELDS_SHOWN);
            if hidden > 0 && !all {
                rows.push(Row::MoreFields { document: index, hidden });
            }
            rows.push(Row::Bottom);
        }
        rows
    }
}

/// Columns for the table view: top-level keys of the page in the order they first appear.
fn table_columns(documents: &[Document]) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    for document in documents {
        for key in document.keys() {
            if !columns.contains(key) {
                columns.push(key.clone());
            }
        }
    }
    columns
}

fn cell_text(value: Option<&Bson>) -> (String, Color) {
    match value.map(show) {
        None => ("—".into(), Color::Disabled),
        Some(Shown::Scalar { text, kind }) => (text, kind_color(kind)),
        Some(Shown::Object(count)) => (format!("Object ({count})"), Color::Muted),
        Some(Shown::Array(count)) => (format!("Array ({count})"), Color::Muted),
    }
}

impl DatabaseView {
    pub(crate) fn tab(&self) -> Option<&MongoTab> {
        self.active_tab.and_then(|index| self.tabs.get(index))
    }

    fn tab_mut(&mut self) -> Option<&mut MongoTab> {
        self.active_tab.and_then(|index| self.tabs.get_mut(index))
    }

    pub(crate) fn open_collection(&mut self, database: String, collection: String, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(index) = self.tabs.iter().position(|tab| tab.database == database && tab.collection == collection) {
            self.active_tab = Some(index);
            cx.notify();
            return;
        }
        let mut tab = MongoTab::new(database.clone(), collection.clone(), window, cx);
        if let Some(session) = self.mongo() {
            let connection = self.generation;
            let task = run(cx, async move { session.estimated_count(&database, &collection).await });
            let (tab_database, tab_collection) = (tab.database.clone(), tab.collection.clone());
            tab._tasks.push(cx.spawn(async move |this, cx| {
                let result = task.await;
                this.update(cx, |this, cx| {
                    if this.generation != connection {
                        return;
                    }
                    if let Some(tab) = this.tabs.iter_mut().find(|tab| tab.database == tab_database && tab.collection == tab_collection) {
                        tab.estimated = result.ok();
                        cx.notify();
                    }
                })
                .ok();
            }));
        }
        self.tabs.push(tab);
        self.active_tab = Some(self.tabs.len() - 1);
        self.find(cx);
    }

    pub(crate) fn close_tab(&mut self, index: usize, cx: &mut Context<Self>) {
        if index >= self.tabs.len() {
            return;
        }
        self.tabs.remove(index);
        self.active_tab = match self.active_tab {
            _ if self.tabs.is_empty() => None,
            Some(active) if active > index => Some(active - 1),
            Some(active) if active == index => Some(index.min(self.tabs.len() - 1)),
            other => other,
        };
        cx.notify();
    }

    fn reset_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tab_mut() else {
            return;
        };
        for editor in [&tab.filter, &tab.projection, &tab.sort, &tab.skip_editor] {
            editor.update(cx, |editor, cx| editor.set_text("", window, cx));
        }
        tab.skip = 0;
        self.find(cx);
    }

    fn current_query(&self, cx: &App) -> Option<SavedQuery> {
        let tab = self.tab()?;
        let text = |editor: &Entity<Editor>| editor.read(cx).text(cx).trim().to_string();
        Some(SavedQuery {
            connection: self.active.clone()?,
            database: tab.database.clone(),
            collection: tab.collection.clone(),
            filter: text(&tab.filter),
            project: text(&tab.projection),
            sort: text(&tab.sort),
            skip: text(&tab.skip_editor),
            favorite: false,
        })
    }

    fn remember(&mut self, query: SavedQuery, cx: &mut Context<Self>) {
        let favorite = self.history.iter().any(|saved| saved.favorite && saved.same_query(&query));
        self.history.retain(|saved| !saved.same_query(&query));
        self.history.insert(0, SavedQuery { favorite, ..query.clone() });
        // Keep every favourite; cap the recent ones per collection.
        let mut recent = 0;
        self.history.retain(|saved| {
            if saved.favorite || (&saved.connection, &saved.database, &saved.collection) != (&query.connection, &query.database, &query.collection) {
                return true;
            }
            recent += 1;
            recent <= HISTORY_PER_COLLECTION
        });
        self.save_history(cx);
    }

    fn save_history(&self, cx: &mut Context<Self>) {
        let history = self.history.clone();
        cx.background_spawn(async move {
            let path = history_path();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = path.with_extension("json.tmp");
            std::fs::write(&temporary, serde_json::to_vec_pretty(&history)?)?;
            std::fs::rename(&temporary, &path)?;
            anyhow::Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn toggle_favorite(&mut self, cx: &mut Context<Self>) {
        let Some(query) = self.current_query(cx) else {
            return;
        };
        match self.history.iter_mut().find(|saved| saved.same_query(&query)) {
            Some(saved) => saved.favorite = !saved.favorite,
            None => self.history.insert(0, SavedQuery { favorite: true, ..query }),
        }
        self.save_history(cx);
        cx.notify();
    }

    fn apply_saved(&mut self, query: SavedQuery, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tab_mut() else {
            return;
        };
        for (editor, text) in [(&tab.filter, &query.filter), (&tab.projection, &query.project), (&tab.sort, &query.sort), (&tab.skip_editor, &query.skip)] {
            editor.update(cx, |editor, cx| editor.set_text(text.clone(), window, cx));
        }
        tab.show_options |= !(query.project.is_empty() && query.sort.is_empty() && query.skip.is_empty());
        tab.skip = 0;
        self.find(cx);
    }

    pub(crate) fn find(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let query = self.current_query(cx);
        let connection = self.generation;
        let Some(tab) = self.tab_mut() else {
            return;
        };
        let parse = |editor: &Entity<Editor>, label: &str, cx: &App| {
            let text = editor.read(cx).text(cx);
            parse_query(&text).map_err(|error| anyhow!("{label}: {error:#}"))
        };
        let base_skip = tab.skip_editor.read(cx).text(cx).trim().to_string();
        let parsed = (|| {
            let filter = parse(&tab.filter, "Filter", cx)?;
            let projection = parse(&tab.projection, "Project", cx)?;
            let sort = parse(&tab.sort, "Sort", cx)?;
            let base_skip = if base_skip.is_empty() { 0 } else { base_skip.parse::<u64>().map_err(|_| anyhow!("Skip must be a whole number"))? };
            anyhow::Ok((filter, projection, sort, base_skip))
        })();
        let (filter, projection, sort, base_skip) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                tab.page = Some(Fetch::Failed(format!("{error:#}").into()));
                cx.notify();
                return;
            }
        };
        tab.generation += 1;
        let generation = tab.generation;
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        // Paging keeps the total; only a different filter counts again.
        if tab.counted_filter.as_ref() != Some(&filter) {
            tab.counted_filter = Some(filter.clone());
            if filter.is_empty() {
                tab.total = tab.estimated.map(Total::Estimated);
            } else {
                tab.total = Some(Total::Counting);
                let task = {
                    let (session, database, collection, filter) = (session.clone(), database.clone(), collection.clone(), filter.clone());
                    run(cx, async move { session.count(&database, &collection, filter, COUNT_TIME_LIMIT).await })
                };
                let counted = filter.clone();
                tab._tasks.push(cx.spawn(async move |this, cx| {
                    let total = task.await;
                    this.update(cx, |this, cx| {
                        if this.generation != connection {
                            return;
                        }
                        if let Some(tab) = this.tabs.iter_mut().find(|tab| tab.database == database && tab.collection == collection)
                            && tab.counted_filter.as_ref() == Some(&counted)
                        {
                            tab.total = Some(total.map_or(Total::Unknown, Total::Exact));
                            cx.notify();
                        }
                    })
                    .ok();
                }));
            }
        }
        let request = FindRequest {
            database: tab.database.clone(),
            collection: tab.collection.clone(),
            filter,
            projection: (!projection.is_empty()).then_some(projection),
            sort: (!sort.is_empty()).then_some(sort),
            skip: base_skip + tab.skip,
            page_size: tab.page_size,
            max_time: Some(QUERY_TIME_LIMIT),
        };
        tab.page = Some(Fetch::Loading);
        tab.expanded.clear();
        tab.show_all.clear();
        tab.items_shown.clear();
        let task = run(cx, async move {
            let page = session.find_page(&request).await?;
            let mut json_lines = Vec::new();
            for document in &page.documents {
                json_lines.extend(to_display_json(document).lines().map(|line| SharedString::from(line.to_string())));
                json_lines.push(SharedString::default());
            }
            Ok(MongoPage { documents: Arc::new(page.documents), json_lines, has_more: page.has_more })
        });
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        tab._tasks.push(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(tab) = this.tabs.iter_mut().find(|tab| tab.database == database && tab.collection == collection)
                    && tab.generation == generation
                {
                    tab.page = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        // Only the latest few loads matter.
        if tab._tasks.len() > 8 {
            tab._tasks.drain(..tab._tasks.len() - 4);
        }
        if let Some(query) = query {
            self.remember(query, cx);
        }
        cx.notify();
    }

    fn recount(&mut self, cx: &mut Context<Self>) {
        if let Some(tab) = self.tab_mut() {
            tab.counted_filter = None;
        }
        self.find(cx);
    }

    fn render_list(&self, tab: &MongoTab, page: &MongoPage, cx: &mut Context<Self>) -> AnyElement {
        let rows = Arc::new(tab.list_rows(&page.documents));
        let documents = page.documents.clone();
        let expanded = tab.expanded.clone();
        let local_dates = self.local_dates;
        let view = cx.entity().downgrade();
        let colors = cx.theme().colors().clone();
        let count = rows.len();
        uniform_list("database-documents", count, move |range, _, cx| {
            range
                .filter_map(|index| rows.get(index).cloned().map(|row| (index, row)))
                .map(|(index, row)| {
                    let card = h_flex()
                        .id(("database-row", index))
                        .h(px(ROW_HEIGHT))
                        .px_3()
                        .gap_1()
                        .border_l_1()
                        .border_r_1()
                        .border_color(colors.border)
                        .bg(colors.editor_background);
                    match row {
                        Row::Top(document) => {
                            let documents = documents.clone();
                            card.border_t_1()
                                .rounded_t_md()
                                .child(div().flex_1())
                                .child(
                                    IconButton::new(("database-copy", document), IconName::Copy)
                                        .icon_size(IconSize::XSmall)
                                        .tooltip(Tooltip::text("Copy document (Extended JSON)"))
                                        .on_click(move |_, _, cx| {
                                            if let Some(document) = documents.get(document) {
                                                cx.write_to_clipboard(ClipboardItem::new_string(to_canonical_json(document)));
                                            }
                                        }),
                                )
                                .into_any_element()
                        }
                        Row::Bottom => div().h(px(ROW_HEIGHT)).child(
                            div().h(px(ROW_HEIGHT / 2.)).border_l_1().border_r_1().border_b_1().rounded_b_md().border_color(colors.border).bg(colors.editor_background),
                        )
                        .into_any_element(),
                        Row::MoreFields { document, hidden } => {
                            let view = view.clone();
                            card.child(
                                Button::new(("database-more", document), format!("Show {hidden} more field{}", if hidden == 1 { "" } else { "s" }))
                                    .label_size(LabelSize::Small)
                                    .color(Color::Accent)
                                    .start_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                                    .on_click(move |_, _, cx| {
                                        view.update(cx, |this, cx| {
                                            if let Some(tab) = this.tab_mut() {
                                                tab.show_all.insert(document);
                                            }
                                            cx.notify();
                                        })
                                        .ok();
                                    }),
                            )
                            .into_any_element()
                        }
                        Row::MoreItems { document, path, depth, hidden } => {
                            let view = view.clone();
                            card.child(div().w(px(depth as f32 * 18.)))
                                .child(
                                    Button::new(("database-more-items", index), format!("Show {} more of {hidden} items", hidden.min(ITEMS_SHOWN)))
                                        .label_size(LabelSize::Small)
                                        .color(Color::Accent)
                                        .on_click(move |_, _, cx| {
                                            let key = (document, path.clone());
                                            view.update(cx, |this, cx| {
                                                if let Some(tab) = this.tab_mut() {
                                                    *tab.items_shown.entry(key).or_insert(ITEMS_SHOWN) += ITEMS_SHOWN;
                                                }
                                                cx.notify();
                                            })
                                            .ok();
                                        }),
                                )
                                .into_any_element()
                        }
                        Row::Field { document, path, depth } => {
                            let Some(value) = documents.get(document).and_then(|item| value_at(item, &path)) else {
                                return card.into_any_element();
                            };
                            let key = path.last().cloned().unwrap_or_default();
                            let shown = show(value);
                            let expandable = matches!(shown, Shown::Object(_) | Shown::Array(_));
                            let open = expanded.contains(&(document, path.clone()));
                            let (text, color) = match &shown {
                                Shown::Scalar { text, kind } => (text.clone(), kind_color(*kind)),
                                Shown::Object(count) => (format!("Object ({count})"), Color::Muted),
                                Shown::Array(count) => (format!("Array ({count})"), Color::Muted),
                            };
                            let human = match value {
                                Bson::DateTime(date) => human_date(date, local_dates),
                                _ => None,
                            };
                            let view = view.clone();
                            let copy = text.clone();
                            card.cursor_pointer()
                                .hover(|style| style.bg(colors.element_hover))
                                .tooltip(Tooltip::text(if expandable { "Click to open or close" } else { "Click to copy the value" }))
                                .on_click(move |_, _, cx| {
                                    if expandable {
                                        let key = (document, path.clone());
                                        view.update(cx, |this, cx| {
                                            if let Some(tab) = this.tab_mut()
                                                && !tab.expanded.remove(&key)
                                            {
                                                tab.expanded.insert(key);
                                            }
                                            cx.notify();
                                        })
                                        .ok();
                                    } else {
                                        cx.write_to_clipboard(ClipboardItem::new_string(copy.clone()));
                                    }
                                })
                                .child(div().w(px(depth as f32 * 18.)).flex_none())
                                .child(div().w(px(12.)).flex_none().when(expandable, |this| {
                                    this.child(Icon::new(if open { IconName::ChevronDown } else { IconName::ChevronRight }).size(IconSize::XSmall).color(Color::Muted))
                                }))
                                .child(Label::new(format!("{key}:")).buffer_font(cx).weight(gpui::FontWeight::SEMIBOLD).single_line())
                                .child(Label::new(text).buffer_font(cx).color(color).single_line().truncate())
                                .when_some(human, |this, human| this.child(Label::new(human).size(LabelSize::Small).color(Color::Muted).underline().single_line()))
                                .into_any_element()
                        }
                    }
                })
                .collect::<Vec<_>>()
        })
        .track_scroll(&tab.scroll)
        .flex_1()
        .into_any_element()
    }

    fn render_table(&self, tab: &MongoTab, page: &MongoPage, cx: &mut Context<Self>) -> AnyElement {
        const COLUMN_WIDTH: f32 = 200.;
        let columns = Arc::new(table_columns(&page.documents));
        let documents = page.documents.clone();
        let colors = cx.theme().colors().clone();
        let row_count = documents.len() + 1;
        let list = uniform_list("database-table", row_count, move |range, _, cx| {
            range
                .map(|index| {
                    let mut row = h_flex().id(("database-table-row", index)).h(px(ROW_HEIGHT)).border_b_1().border_color(colors.border_variant);
                    if index == 0 {
                        // The header scrolls with the rows; it keeps columns aligned while scrolling sideways.
                        row = row.bg(colors.element_background);
                        for column in columns.iter() {
                            row = row.child(
                                div().w(px(COLUMN_WIDTH)).flex_none().px_2().child(Label::new(column.clone()).size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD).single_line().truncate()),
                            );
                        }
                        return row.into_any_element();
                    }
                    let Some(document) = documents.get(index - 1) else {
                        return row.into_any_element();
                    };
                    for (column_index, column) in columns.iter().enumerate() {
                        let (text, color) = cell_text(document.get(column));
                        let copy = text.clone();
                        row = row.child(
                            div()
                                .id(("database-cell", index * 10_000 + column_index))
                                .w(px(COLUMN_WIDTH))
                                .flex_none()
                                .px_2()
                                .border_r_1()
                                .border_color(colors.border_variant)
                                .cursor_pointer()
                                .hover(|style| style.bg(colors.element_hover))
                                .on_click(move |_, _, cx| cx.write_to_clipboard(ClipboardItem::new_string(copy.clone())))
                                .child(Label::new(text).size(LabelSize::Small).buffer_font(cx).color(color).single_line().truncate()),
                        );
                    }
                    row.into_any_element()
                })
                .collect::<Vec<_>>()
        })
        .with_horizontal_sizing_behavior(ListHorizontalSizingBehavior::Unconstrained)
        .track_scroll(&tab.scroll)
        .flex_1();
        v_flex()
            .flex_1()
            .min_h_0()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .bg(colors.editor_background)
            .child(list)
            .into_any_element()
    }

    pub(crate) fn render_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let mut strip = h_flex().id("database-tabs").overflow_x_scroll().border_b_1().border_color(colors.border);
        for (index, tab) in self.tabs.iter().enumerate() {
            let selected = self.active_tab == Some(index);
            strip = strip.child(
                h_flex()
                    .id(("database-tab", index))
                    .px_3()
                    .py_1p5()
                    .gap_2()
                    .border_r_1()
                    .border_color(colors.border_variant)
                    .cursor_pointer()
                    .when(selected, |this| this.bg(colors.tab_active_background).border_b_2().border_color(colors.text_accent))
                    .when(!selected, |this| this.bg(colors.tab_inactive_background))
                    .tooltip(Tooltip::text(format!("{}.{}", tab.database, tab.collection)))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.active_tab = Some(index);
                        cx.notify();
                    }))
                    .child(Icon::new(IconName::Folder).size(IconSize::Small).color(Color::Muted))
                    .child(Label::new(tab.title()).size(LabelSize::Small).color(if selected { Color::Default } else { Color::Muted }))
                    .child(
                        IconButton::new(("database-tab-close", index), IconName::Close)
                            .icon_size(IconSize::XSmall)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                cx.stop_propagation();
                                this.close_tab(index, cx);
                            })),
                    ),
            );
        }
        strip
    }

    pub(crate) fn render_mongo(&self, connection: &crate::Connection, cx: &mut Context<Self>) -> AnyElement {
        let Some(tab) = self.tab() else {
            return v_flex()
                .gap_3()
                .child(self.render_header(connection, connection.name.clone(), cx))
                .child(Label::new("Pick a collection on the left.").color(Color::Muted))
                .into_any_element();
        };
        let page_button = |size: u32, cx: &mut Context<Self>| {
            Button::new(("database-page-size", size as usize), size.to_string())
                .label_size(LabelSize::Small)
                .style(if tab.page_size == size { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(tab) = this.tab_mut() {
                        tab.page_size = size;
                        tab.skip = 0;
                    }
                    this.find(cx);
                }))
        };
        let view_button = |id: &'static str, label: &'static str, view: DocumentView, cx: &mut Context<Self>| {
            Button::new(id, label)
                .label_size(LabelSize::Small)
                .style(if tab.view == view { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(tab) = this.tab_mut() {
                        tab.view = view;
                    }
                    cx.notify();
                }))
        };
        let (shown, has_more) = match &tab.page {
            Some(Fetch::Loaded(page)) => (page.documents.len(), page.has_more),
            _ => (0, false),
        };
        let skip = tab.skip;
        let page_size = u64::from(tab.page_size);
        let total = match &tab.total {
            Some(Total::Exact(total)) => format!(" of {total}"),
            Some(Total::Estimated(total)) => format!(" of ≈{total}"),
            Some(Total::Counting) => " of …".into(),
            Some(Total::Unknown) | None => String::new(),
        };
        let status = match &tab.page {
            Some(Fetch::Loaded(_)) if shown == 0 => "No documents match.".to_string(),
            Some(Fetch::Loaded(_)) => format!("{} – {}{total}", skip + 1, skip + shown as u64),
            Some(Fetch::Loading) => "Running…".into(),
            _ => String::new(),
        };
        let body = match &tab.page {
            Some(Fetch::Failed(message)) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Error).into_any_element(),
            Some(Fetch::Loaded(page)) if !page.documents.is_empty() => match tab.view {
                DocumentView::List => self.render_list(tab, page, cx),
                DocumentView::Json => render_lines("database-documents-json", page.json_lines.clone(), &tab.scroll, cx),
                DocumentView::Table => self.render_table(tab, page, cx),
            },
            _ => div().into_any_element(),
        };
        let option = |label: &'static str, editor: &Entity<Editor>, width: f32, cx: &App| {
            h_flex().gap_2().child(Label::new(label).size(LabelSize::Small).color(Color::Muted)).child(div().w(px(width)).child(boxed(editor.clone(), cx)))
        };
        let current = self.current_query(cx);
        let starred = current.as_ref().is_some_and(|query| self.history.iter().any(|saved| saved.favorite && saved.same_query(query)));
        let saved = self
            .history
            .iter()
            .filter(|saved| {
                Some(&saved.connection) == self.active.as_ref() && saved.database == tab.database && saved.collection == tab.collection
            })
            .cloned()
            .collect::<Vec<_>>();
        let view = cx.entity().downgrade();
        let history = PopoverMenu::new("database-history")
            .trigger(IconButton::new("database-history-button", IconName::HistoryRerun).icon_size(IconSize::Small).tooltip(Tooltip::text("Recent and starred queries")))
            .menu(move |window, cx| {
                let saved = saved.clone();
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    if saved.is_empty() {
                        return menu.label("No queries yet");
                    }
                    let (favorites, recent): (Vec<_>, Vec<_>) = saved.iter().cloned().partition(|saved| saved.favorite);
                    for (title, group) in [("Starred", favorites), ("Recent", recent)] {
                        if group.is_empty() {
                            continue;
                        }
                        menu = menu.header(title);
                        for query in group {
                            let view = view.clone();
                            menu = menu.entry(query.label(), None, move |window, cx| {
                                let query = query.clone();
                                view.update(cx, |this, cx| this.apply_saved(query, window, cx)).ok();
                            });
                        }
                    }
                    menu.separator().entry("Clear recent queries", None, move |_, cx| {
                        view.update(cx, |this, cx| {
                            this.history.retain(|saved| saved.favorite);
                            this.save_history(cx);
                            cx.notify();
                        })
                        .ok();
                    })
                }))
            });
        let title = format!("{} › {} › {}", connection.name, tab.database, tab.collection);
        v_flex()
            .size_full()
            .gap_2()
            .child(self.render_tabs(cx))
            .child(self.render_header(connection, title, cx))
            .when_some(tab.estimated, |this, estimated| this.child(Label::new(format!("Documents  ≈{estimated}")).size(LabelSize::Small).color(Color::Muted)))
            .child(
                h_flex()
                    .gap_2()
                    .child(history)
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| {
                                if let Some(tab) = this.tab_mut() {
                                    tab.skip = 0;
                                }
                                this.find(cx);
                            }))
                            .child(boxed(tab.filter.clone(), cx)),
                    )
                    .child(
                        IconButton::new("database-star", if starred { IconName::StarFilled } else { IconName::Star })
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text(if starred { "Remove from starred" } else { "Star this query" }))
                            .on_click(cx.listener(|this, _, _, cx| this.toggle_favorite(cx))),
                    )
                    .child(Button::new("database-reset", "Reset").label_size(LabelSize::Small).on_click(cx.listener(|this, _, window, cx| this.reset_query(window, cx))))
                    .child(Button::new("database-find", "Find").style(ButtonStyle::Filled).on_click(cx.listener(|this, _, _, cx| {
                        if let Some(tab) = this.tab_mut() {
                            tab.skip = 0;
                        }
                        this.find(cx);
                    })))
                    .child(
                        Button::new("database-options", "Options")
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .end_icon(Icon::new(if tab.show_options { IconName::ChevronDown } else { IconName::ChevronRight }).size(IconSize::XSmall))
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(tab) = this.tab_mut() {
                                    tab.show_options = !tab.show_options;
                                }
                                cx.notify();
                            })),
                    ),
            )
            .when(tab.show_options, |this| {
                this.child(
                    h_flex()
                        .gap_4()
                        .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| {
                            if let Some(tab) = this.tab_mut() {
                                tab.skip = 0;
                            }
                            this.find(cx);
                        }))
                        .child(option("Project", &tab.projection, 260., cx))
                        .child(option("Sort", &tab.sort, 220., cx))
                        .child(option("Skip", &tab.skip_editor, 80., cx)),
                )
            })
            .child(
                h_flex()
                    .gap_1()
                    .children(PAGE_SIZES.map(|size| page_button(size, cx)))
                    .child(div().w(px(12.)))
                    .child(Label::new(status).size(LabelSize::Small).color(Color::Muted))
                    .when(tab.total == Some(Total::Unknown), |this| {
                        this.child(
                            Button::new("database-recount", "total unknown · count again")
                                .label_size(LabelSize::Small)
                                .color(Color::Accent)
                                .tooltip(Tooltip::text("Counting took longer than 5 seconds or failed"))
                                .on_click(cx.listener(|this, _, _, cx| this.recount(cx))),
                        )
                    })
                    .child(IconButton::new("database-refresh", IconName::RotateCw).icon_size(IconSize::Small).tooltip(Tooltip::text("Run the query again")).on_click(cx.listener(|this, _, _, cx| this.find(cx))))
                    .child(IconButton::new("database-previous", IconName::ChevronLeft).icon_size(IconSize::Small).disabled(skip == 0).on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(tab) = this.tab_mut() {
                            tab.skip = skip.saturating_sub(page_size);
                        }
                        this.find(cx);
                    })))
                    .child(IconButton::new("database-next", IconName::ChevronRight).icon_size(IconSize::Small).disabled(!has_more).on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(tab) = this.tab_mut() {
                            tab.skip = skip + page_size;
                        }
                        this.find(cx);
                    })))
                    .child(div().flex_1())
                    .child(
                        Button::new("database-dates", if self.local_dates { "Local time" } else { "UTC" })
                            .label_size(LabelSize::Small)
                            .tooltip(Tooltip::text("Show readable dates in UTC or this Mac's time zone"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.local_dates = !this.local_dates;
                                cx.notify();
                            })),
                    )
                    .child(view_button("database-view-list", "List", DocumentView::List, cx))
                    .child(view_button("database-view-json", "JSON", DocumentView::Json, cx))
                    .child(view_button("database-view-table", "Table", DocumentView::Table, cx)),
            )
            .child(body)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vilo_database_core::bson::doc;

    #[test]
    fn test_paths_keep_dotted_keys_whole() {
        let document = doc! { "a.b": 1, "a": { "b": 2 }, "list": [ { "x": "y" } ] };
        assert_eq!(value_at(&document, &["a.b".to_string()]), Some(&Bson::Int32(1)));
        assert_eq!(value_at(&document, &["a".to_string(), "b".to_string()]), Some(&Bson::Int32(2)));
        assert_eq!(value_at(&document, &["list".to_string(), "0".to_string(), "x".to_string()]), Some(&Bson::String("y".into())));
        assert_eq!(value_at(&document, &["list".to_string(), "9".to_string()]), None);
        assert_eq!(table_columns(&[doc! { "b": 1, "a": 2 }, doc! { "c": 3, "a": 4 }]), vec!["b", "a", "c"]);
        assert_eq!(cell_text(None).0, "—");
        assert_eq!(cell_text(Some(&Bson::Null)).0, "null");
    }
}
