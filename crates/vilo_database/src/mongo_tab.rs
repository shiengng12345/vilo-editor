//! A MongoDB collection opened in a tab, laid out like MongoDB Compass: breadcrumb, sub tabs, query bar,
//! toolbar, and documents as cards, a JSON listing or a table.

use std::{
    io::Write as _,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use collections::{HashMap, HashSet};
use editor::Editor;
use gpui::{
    ClipboardItem, ElementId, Entity, ListHorizontalSizingBehavior, Task, UniformListScrollHandle,
    Window, prelude::*, rgb, uniform_list,
};
use serde::{Deserialize, Serialize};
use ui::{
    Button, ButtonStyle, ContextMenu, IconButton, PopoverMenu, TintColor, Tooltip, prelude::*,
};
use vilo_database_core::{
    bson::{Bson, Document},
    display::{Shown, ValueKind, human_date, show, to_canonical_json},
    mongo::{ExplainSummary, FindRequest, Hint, summarize_explain, to_display_json},
    shell::{parse_pipeline, parse_query},
};

use crate::{
    COUNT_TIME_LIMIT, DatabaseView, Fetch, QUERY_TIME_LIMIT, boxed, editor, fetched, render_lines,
    run,
};

/// Top-level fields shown per document before "Show N more fields", as Compass does.
const FIELDS_SHOWN: usize = 25;
/// Array items shown when an array is opened, then more on request.
const ITEMS_SHOWN: usize = 100;
const ROW_HEIGHT: f32 = 20.;
const PAGE_SIZES: [u32; 6] = [10, 20, 25, 50, 75, 100];
const HISTORY_PER_COLLECTION: usize = 30;
/// Compass's Max Time MS default.
const DEFAULT_MAX_TIME: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum DocumentView {
    List,
    Json,
    Table,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum SubTab {
    Documents,
    Aggregations,
    Schema,
    Indexes,
    Validation,
}

/// What the Documents tab shows in place of the results.
#[derive(Clone, Copy, PartialEq)]
enum Panel {
    Results,
    Explain,
    Code,
}

#[derive(Clone, Copy, PartialEq)]
enum CodeLanguage {
    Shell,
    Node,
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

struct ExplainResult {
    summary: ExplainSummary,
    raw: Vec<SharedString>,
}

#[derive(Clone, Copy, PartialEq)]
enum ExportFormat {
    Json,
    Csv,
}

struct Export {
    file: PathBuf,
    written: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
    outcome: Option<Result<u64, SharedString>>,
    _task: Task<()>,
}

/// A path into a document: keys and array positions as separate steps, so a key containing a dot stays one step.
type FieldPath = Vec<String>;

pub(crate) struct MongoTab {
    pub(crate) database: String,
    pub(crate) collection: String,
    sub_tab: SubTab,
    panel: Panel,
    code_language: CodeLanguage,
    filter: Entity<Editor>,
    projection: Entity<Editor>,
    sort: Entity<Editor>,
    skip_editor: Entity<Editor>,
    collation: Entity<Editor>,
    hint: Entity<Editor>,
    max_time: Entity<Editor>,
    limit: Entity<Editor>,
    pipeline: Entity<Editor>,
    show_options: bool,
    view: DocumentView,
    page_size: u32,
    skip: u64,
    page: Option<Fetch<MongoPage>>,
    aggregation_skip: u64,
    aggregation: Option<Fetch<MongoPage>>,
    total: Option<Total>,
    /// The filter the total belongs to, so paging does not count again.
    counted_filter: Option<Document>,
    estimated: Option<u64>,
    explain: Option<Fetch<ExplainResult>>,
    explain_raw: bool,
    indexes: Option<Fetch<Vec<Document>>>,
    options: Option<Fetch<Option<Document>>>,
    export: Option<Export>,
    expanded: HashSet<(usize, FieldPath)>,
    show_all: HashSet<usize>,
    items_shown: HashMap<(usize, FieldPath), usize>,
    scroll: UniformListScrollHandle,
    /// Bumped on every query; answers for an older one are dropped.
    generation: u64,
    _tasks: Vec<Task<()>>,
}

impl MongoTab {
    pub(crate) fn new(
        database: String,
        collection: String,
        window: &mut Window,
        cx: &mut gpui::App,
    ) -> Self {
        let pipeline = cx.new(|cx| {
            let mut editor = Editor::auto_height(4, 16, window, cx);
            editor.set_placeholder_text("[ { $match: { status: 'Success' } }, { $group: { _id: '$type', count: { $sum: 1 } } } ]", window, cx);
            editor
        });
        Self {
            database,
            collection,
            sub_tab: SubTab::Documents,
            panel: Panel::Results,
            code_language: CodeLanguage::Shell,
            filter: editor("Type a query: { field: 'value' }", "", false, window, cx),
            projection: editor("{ field: 0 }", "", false, window, cx),
            sort: editor("{ field: -1 }", "", false, window, cx),
            skip_editor: editor("0", "", false, window, cx),
            collation: editor("{ locale: 'simple' }", "", false, window, cx),
            hint: editor("{ field: -1 } or \"indexName\"", "", false, window, cx),
            max_time: editor("60000", "", false, window, cx),
            limit: editor("0", "", false, window, cx),
            pipeline,
            show_options: false,
            view: DocumentView::List,
            page_size: 25,
            skip: 0,
            page: None,
            aggregation_skip: 0,
            aggregation: None,
            total: None,
            counted_filter: None,
            estimated: None,
            explain: None,
            explain_raw: false,
            indexes: None,
            options: None,
            export: None,
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

    fn query_editors(&self) -> [&Entity<Editor>; 8] {
        [
            &self.filter,
            &self.projection,
            &self.sort,
            &self.skip_editor,
            &self.collation,
            &self.hint,
            &self.max_time,
            &self.limit,
        ]
    }

    fn keep(&mut self, task: Task<()>) {
        self._tasks.push(task);
        // Only the latest few loads matter.
        if self._tasks.len() > 12 {
            self._tasks.drain(..self._tasks.len() - 6);
        }
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
    collation: String,
    #[serde(default)]
    hint: String,
    #[serde(default)]
    max_time: String,
    #[serde(default)]
    limit: String,
    #[serde(default)]
    favorite: bool,
}

impl SavedQuery {
    fn label(&self) -> String {
        let mut label = if self.filter.trim().is_empty() {
            "{}".to_string()
        } else {
            self.filter.trim().to_string()
        };
        for (name, value) in [
            ("project", &self.project),
            ("sort", &self.sort),
            ("skip", &self.skip),
            ("collation", &self.collation),
            ("hint", &self.hint),
            ("max time", &self.max_time),
            ("limit", &self.limit),
        ] {
            if !value.trim().is_empty() {
                label.push_str(&format!(" · {name} {}", value.trim()));
            }
        }
        label.chars().take(90).collect()
    }

    fn same_query(&self, other: &SavedQuery) -> bool {
        let unstarred = |query: &SavedQuery| SavedQuery {
            favorite: false,
            ..query.clone()
        };
        unstarred(self) == unstarred(other)
    }
}

/// Recent and starred queries; queries can hold player ids, so the user can clear them.
pub(crate) fn load_history() -> Vec<SavedQuery> {
    std::fs::read(history_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn history_path() -> PathBuf {
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

#[derive(Clone)]
enum RowKind {
    Field {
        path: FieldPath,
        depth: usize,
    },
    MoreFields {
        hidden: usize,
    },
    MoreItems {
        path: FieldPath,
        depth: usize,
        hidden: usize,
    },
    /// Space between two cards.
    Gap,
}

/// One line of the list view. Every row has the same height so the list can be virtual; a card is drawn
/// by its rows' side borders, with the top and bottom on its first and last rows.
#[derive(Clone)]
struct Row {
    document: usize,
    kind: RowKind,
    first: bool,
    last: bool,
}

fn kind_color(kind: ValueKind) -> Color {
    match kind {
        ValueKind::String => Color::Success,
        ValueKind::Number | ValueKind::Date => Color::Accent,
        ValueKind::ObjectId => Color::Error,
        ValueKind::Boolean => Color::Warning,
        ValueKind::Null => Color::Default,
        ValueKind::Other => Color::Default,
    }
}

impl MongoTab {
    fn push_rows(
        &self,
        rows: &mut Vec<Row>,
        document: usize,
        path: FieldPath,
        value: &Bson,
        depth: usize,
    ) {
        rows.push(Row {
            document,
            kind: RowKind::Field {
                path: path.clone(),
                depth,
            },
            first: false,
            last: false,
        });
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
                let shown = self
                    .items_shown
                    .get(&(document, path.clone()))
                    .copied()
                    .unwrap_or(ITEMS_SHOWN);
                for (position, value) in items.iter().enumerate().take(shown) {
                    let mut child = path.clone();
                    child.push(position.to_string());
                    self.push_rows(rows, document, child, value, depth + 1);
                }
                if items.len() > shown {
                    rows.push(Row {
                        document,
                        kind: RowKind::MoreItems {
                            path,
                            depth: depth + 1,
                            hidden: items.len() - shown,
                        },
                        first: false,
                        last: false,
                    });
                }
            }
            _ => {}
        }
    }

    fn list_rows(&self, documents: &[Document]) -> Vec<Row> {
        let mut rows = Vec::new();
        for (index, document) in documents.iter().enumerate() {
            if index > 0 {
                rows.push(Row {
                    document: index,
                    kind: RowKind::Gap,
                    first: false,
                    last: false,
                });
            }
            let start = rows.len();
            let all = self.show_all.contains(&index);
            for (key, value) in document
                .iter()
                .take(if all { usize::MAX } else { FIELDS_SHOWN })
            {
                self.push_rows(&mut rows, index, vec![key.clone()], value, 0);
            }
            let hidden = document.len().saturating_sub(FIELDS_SHOWN);
            if hidden > 0 && !all {
                rows.push(Row {
                    document: index,
                    kind: RowKind::MoreFields { hidden },
                    first: false,
                    last: false,
                });
            }
            if rows.len() > start {
                rows[start].first = true;
                let end = rows.len() - 1;
                rows[end].last = true;
            }
        }
        rows
    }

    fn top_level_containers(&self, document: &Document, index: usize) -> Vec<(usize, FieldPath)> {
        document
            .iter()
            .filter(|(_, value)| matches!(value, Bson::Document(_) | Bson::Array(_)))
            .map(|(key, _)| (index, vec![key.clone()]))
            .collect()
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

/// A CSV field: text as is, other values in their Compass form, quoted when needed.
fn csv_field(value: Option<&Bson>) -> String {
    let text = match value {
        None => String::new(),
        Some(Bson::String(text)) => text.clone(),
        Some(value @ (Bson::Document(_) | Bson::Array(_))) => {
            value.clone().into_canonical_extjson().to_string()
        }
        Some(value) => match show(value) {
            Shown::Scalar { text, .. } => text,
            _ => String::new(),
        },
    };
    if text.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text
    }
}

fn index_type(keys: &Document) -> &'static str {
    let values = keys.values().filter_map(Bson::as_str).collect::<Vec<_>>();
    if keys.keys().any(|key| key.contains("$**")) {
        "WILDCARD"
    } else if values.contains(&"text") {
        "TEXT"
    } else if values.iter().any(|value| value.starts_with("2d")) {
        "GEOSPATIAL"
    } else if values.contains(&"hashed") {
        "HASHED"
    } else {
        "REGULAR"
    }
}

fn index_properties(index: &Document) -> Vec<String> {
    let mut properties = Vec::new();
    if index.get_bool("unique").unwrap_or(false) {
        properties.push("UNIQUE".to_string());
    }
    if index.get_bool("sparse").unwrap_or(false) {
        properties.push("SPARSE".to_string());
    }
    if index.contains_key("partialFilterExpression") {
        properties.push("PARTIAL".to_string());
    }
    if let Some(seconds) = index.get("expireAfterSeconds") {
        properties.push(format!("TTL {seconds}s"));
    }
    if index.get_bool("hidden").unwrap_or(false) {
        properties.push("HIDDEN".to_string());
    }
    if index.contains_key("collation") {
        properties.push("COLLATION".to_string());
    }
    properties
}

/// Compass's solid green button (Find, ADD DATA); the theme's buttons have no solid accent fill.
fn compass_primary(
    id: impl Into<ElementId>,
    label: &'static str,
    icon: Option<IconName>,
    disabled: bool,
) -> gpui::Stateful<gpui::Div> {
    let text = Color::Custom(rgb(0xFFFFFF).into());
    h_flex()
        .id(id)
        .h(px(26.))
        .px_2p5()
        .gap_1()
        .rounded_md()
        .border_1()
        .border_color(rgb(0x00ED64))
        .bg(rgb(0x00684A))
        .when(disabled, |this| this.opacity(0.5))
        .when(!disabled, |this| {
            this.cursor_pointer().hover(|style| style.bg(rgb(0x00593F)))
        })
        .children(icon.map(|icon| Icon::new(icon).size(IconSize::XSmall).color(text)))
        .child(
            Label::new(label)
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(text),
        )
}

fn compass_button(id: &'static str, label: &'static str, icon: IconName) -> Button {
    Button::new(id, label)
        .label_size(LabelSize::Small)
        .style(ButtonStyle::Outlined)
        .start_icon(Icon::new(icon).size(IconSize::XSmall))
}

impl DatabaseView {
    pub(crate) fn tab(&self) -> Option<&MongoTab> {
        self.active_tab.and_then(|index| self.tabs.get(index))
    }

    fn tab_mut(&mut self) -> Option<&mut MongoTab> {
        self.active_tab.and_then(|index| self.tabs.get_mut(index))
    }

    fn tab_at(&mut self, database: &str, collection: &str) -> Option<&mut MongoTab> {
        self.tabs
            .iter_mut()
            .find(|tab| tab.database == database && tab.collection == collection)
    }

    pub(crate) fn open_collection(
        &mut self,
        database: String,
        collection: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(index) = self
            .tabs
            .iter()
            .position(|tab| tab.database == database && tab.collection == collection)
        {
            self.active_tab = Some(index);
            cx.notify();
            return;
        }
        let mut tab = MongoTab::new(database.clone(), collection.clone(), window, cx);
        if let Some(session) = self.mongo() {
            let connection = self.generation;
            let task = run(cx, async move {
                session.estimated_count(&database, &collection).await
            });
            let (key_database, key_collection) = (tab.database.clone(), tab.collection.clone());
            tab.keep(cx.spawn(async move |this, cx| {
                let result = task.await;
                this.update(cx, |this, cx| {
                    if this.generation != connection {
                        return;
                    }
                    if let Some(tab) = this.tab_at(&key_database, &key_collection) {
                        tab.estimated = result.ok();
                        if tab.counted_filter.as_ref().is_some_and(Document::is_empty) {
                            tab.total = tab.estimated.map(Total::Estimated);
                        }
                        cx.notify();
                    }
                })
                .ok();
            }));
        }
        self.tabs.push(tab);
        self.active_tab = Some(self.tabs.len() - 1);
        self.find(cx);
        self.load_indexes(cx);
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
        for editor in tab.query_editors() {
            editor.update(cx, |editor, cx| editor.set_text("", window, cx));
        }
        tab.skip = 0;
        tab.panel = Panel::Results;
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
            collation: text(&tab.collation),
            hint: text(&tab.hint),
            max_time: text(&tab.max_time),
            limit: text(&tab.limit),
            favorite: false,
        })
    }

    fn remember(&mut self, query: SavedQuery, cx: &mut Context<Self>) {
        let favorite = self
            .history
            .iter()
            .any(|saved| saved.favorite && saved.same_query(&query));
        self.history.retain(|saved| !saved.same_query(&query));
        self.history.insert(
            0,
            SavedQuery {
                favorite,
                ..query.clone()
            },
        );
        // Keep every favourite; cap the recent ones per collection.
        let mut recent = 0;
        self.history.retain(|saved| {
            if saved.favorite
                || (&saved.connection, &saved.database, &saved.collection)
                    != (&query.connection, &query.database, &query.collection)
            {
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
        match self
            .history
            .iter_mut()
            .find(|saved| saved.same_query(&query))
        {
            Some(saved) => saved.favorite = !saved.favorite,
            None => self.history.insert(
                0,
                SavedQuery {
                    favorite: true,
                    ..query
                },
            ),
        }
        self.save_history(cx);
        cx.notify();
    }

    fn apply_saved(&mut self, query: SavedQuery, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.tab_mut() else {
            return;
        };
        for (editor, text) in [
            (&tab.filter, &query.filter),
            (&tab.projection, &query.project),
            (&tab.sort, &query.sort),
            (&tab.skip_editor, &query.skip),
            (&tab.collation, &query.collation),
            (&tab.hint, &query.hint),
            (&tab.max_time, &query.max_time),
            (&tab.limit, &query.limit),
        ] {
            editor.update(cx, |editor, cx| editor.set_text(text.clone(), window, cx));
        }
        tab.show_options |= [
            &query.project,
            &query.sort,
            &query.skip,
            &query.collation,
            &query.hint,
            &query.max_time,
            &query.limit,
        ]
        .iter()
        .any(|text| !text.is_empty());
        tab.skip = 0;
        self.find(cx);
    }

    /// The query as typed, without paging; `None` with the error shown when it does not parse.
    fn request(&mut self, cx: &mut Context<Self>) -> Option<FindRequest> {
        let tab = self.tab_mut()?;
        let parse = |editor: &Entity<Editor>, label: &str, cx: &App| {
            parse_query(&editor.read(cx).text(cx)).map_err(|error| anyhow!("{label}: {error:#}"))
        };
        let number = |editor: &Entity<Editor>, label: &str, cx: &App| {
            let text = editor.read(cx).text(cx).trim().to_string();
            if text.is_empty() {
                return Ok(None);
            }
            text.parse::<u64>()
                .map(Some)
                .map_err(|_| anyhow!("{label} must be a whole number"))
        };
        let parsed = (|| {
            let filter = parse(&tab.filter, "Filter", cx)?;
            let projection = parse(&tab.projection, "Project", cx)?;
            let sort = parse(&tab.sort, "Sort", cx)?;
            let collation = parse(&tab.collation, "Collation", cx)?;
            let hint = tab.hint.read(cx).text(cx).trim().to_string();
            let hint = match hint.strip_prefix(['"', '\'']) {
                Some(name) => Some(Hint::Name(name.trim_end_matches(['"', '\'']).to_string())),
                None => Some(parse(&tab.hint, "Index Hint", cx)?)
                    .filter(|keys| !keys.is_empty())
                    .map(Hint::Keys),
            };
            let base_skip = number(&tab.skip_editor, "Skip", cx)?.unwrap_or(0);
            let max_time = number(&tab.max_time, "Max Time MS", cx)?
                .map_or(DEFAULT_MAX_TIME, Duration::from_millis);
            // Compass treats Limit 0 as no limit.
            let limit = number(&tab.limit, "Limit", cx)?.filter(|limit| *limit > 0);
            anyhow::Ok(FindRequest {
                database: tab.database.clone(),
                collection: tab.collection.clone(),
                filter,
                projection: (!projection.is_empty()).then_some(projection),
                sort: (!sort.is_empty()).then_some(sort),
                skip: base_skip,
                page_size: tab.page_size,
                max_time: Some(max_time),
                collation: (!collation.is_empty()).then_some(collation),
                hint,
                limit,
            })
        })();
        match parsed {
            Ok(request) => Some(request),
            Err(error) => {
                tab.panel = Panel::Results;
                tab.page = Some(Fetch::Failed(format!("{error:#}").into()));
                cx.notify();
                None
            }
        }
    }

    pub(crate) fn find(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let query = self.current_query(cx);
        let connection = self.generation;
        let Some(mut request) = self.request(cx) else {
            return;
        };
        let Some(tab) = self.tab_mut() else {
            return;
        };
        tab.panel = Panel::Results;
        tab.generation += 1;
        let generation = tab.generation;
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        // Paging keeps the total; only a different filter counts again.
        if tab.counted_filter.as_ref() != Some(&request.filter) {
            tab.counted_filter = Some(request.filter.clone());
            if request.filter.is_empty() {
                tab.total = tab.estimated.map(Total::Estimated);
            } else {
                tab.total = Some(Total::Counting);
                let task = {
                    let (session, database, collection, filter) = (
                        session.clone(),
                        database.clone(),
                        collection.clone(),
                        request.filter.clone(),
                    );
                    run(cx, async move {
                        session
                            .count(&database, &collection, filter, COUNT_TIME_LIMIT)
                            .await
                    })
                };
                let counted = request.filter.clone();
                let (database, collection) = (database.clone(), collection.clone());
                tab.keep(cx.spawn(async move |this, cx| {
                    let total = task.await;
                    this.update(cx, |this, cx| {
                        if this.generation != connection {
                            return;
                        }
                        if let Some(tab) = this.tab_at(&database, &collection)
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
        request.skip += tab.skip;
        request.limit = request.limit.map(|limit| limit.saturating_sub(tab.skip));
        tab.page = Some(Fetch::Loading);
        tab.expanded.clear();
        tab.show_all.clear();
        tab.items_shown.clear();
        let task = run(cx, async move {
            let page = session.find_page(&request).await?;
            Ok(page_view(page.documents, page.has_more))
        });
        tab.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(tab) = this.tab_at(&database, &collection)
                    && tab.generation == generation
                {
                    tab.page = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
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

    fn explain(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let connection = self.generation;
        let Some(request) = self.request(cx) else {
            return;
        };
        let Some(tab) = self.tab_mut() else {
            return;
        };
        tab.panel = Panel::Explain;
        tab.explain = Some(Fetch::Loading);
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        let task = run(cx, async move {
            let explain = session.explain(&request).await?;
            let raw = to_display_json(&explain)
                .lines()
                .map(|line| SharedString::from(line.to_string()))
                .collect();
            Ok(ExplainResult {
                summary: summarize_explain(&explain),
                raw,
            })
        });
        tab.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(tab) = this.tab_at(&database, &collection)
                {
                    tab.explain = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn run_aggregation(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let connection = self.generation;
        let Some(tab) = self.tab_mut() else {
            return;
        };
        let pipeline = match parse_pipeline(&tab.pipeline.read(cx).text(cx)) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                tab.aggregation = Some(Fetch::Failed(format!("{error:#}").into()));
                cx.notify();
                return;
            }
        };
        tab.generation += 1;
        let generation = tab.generation;
        tab.aggregation = Some(Fetch::Loading);
        tab.expanded.clear();
        tab.show_all.clear();
        let (database, collection, skip, page_size) = (
            tab.database.clone(),
            tab.collection.clone(),
            tab.aggregation_skip,
            tab.page_size,
        );
        let task = {
            let (database, collection) = (database.clone(), collection.clone());
            run(cx, async move {
                let page = session
                    .aggregate_page(
                        &database,
                        &collection,
                        &pipeline,
                        skip,
                        page_size,
                        QUERY_TIME_LIMIT,
                    )
                    .await?;
                Ok(page_view(page.documents, page.has_more))
            })
        };
        tab.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(tab) = this.tab_at(&database, &collection)
                    && tab.generation == generation
                {
                    tab.aggregation = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn load_indexes(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let connection = self.generation;
        let Some(tab) = self.tab_mut() else {
            return;
        };
        tab.indexes = Some(Fetch::Loading);
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        let task = {
            let (database, collection) = (database.clone(), collection.clone());
            run(
                cx,
                async move { session.indexes(&database, &collection).await },
            )
        };
        tab.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(tab) = this.tab_at(&database, &collection)
                {
                    tab.indexes = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
    }

    fn load_options(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let connection = self.generation;
        let Some(tab) = self.tab_mut() else {
            return;
        };
        tab.options = Some(Fetch::Loading);
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        let task = {
            let (database, collection) = (database.clone(), collection.clone());
            run(cx, async move {
                session.collection_options(&database, &collection).await
            })
        };
        tab.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(tab) = this.tab_at(&database, &collection)
                {
                    tab.options = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
    }

    fn set_sub_tab(&mut self, sub_tab: SubTab, cx: &mut Context<Self>) {
        let Some(tab) = self.tab_mut() else {
            return;
        };
        tab.sub_tab = sub_tab;
        let (needs_indexes, needs_options) = (tab.indexes.is_none(), tab.options.is_none());
        match sub_tab {
            SubTab::Indexes if needs_indexes => self.load_indexes(cx),
            SubTab::Validation if needs_options => self.load_options(cx),
            _ => {}
        }
        cx.notify();
    }

    /// The current query in mongosh or Node.js. Node gets Extended JSON so every BSON type survives.
    fn query_code(&self, language: CodeLanguage, cx: &App) -> Result<String> {
        let tab = self.tab().ok_or_else(|| anyhow!("No collection is open"))?;
        let text = |editor: &Entity<Editor>| editor.read(cx).text(cx).trim().to_string();
        let (filter, project, sort, skip) = (
            text(&tab.filter),
            text(&tab.projection),
            text(&tab.sort),
            text(&tab.skip_editor),
        );
        let (collation, hint, max_time, limit) = (
            text(&tab.collation),
            text(&tab.hint),
            text(&tab.max_time),
            text(&tab.limit),
        );
        let or_empty = |text: &str| {
            if text.is_empty() {
                "{}".to_string()
            } else {
                text.to_string()
            }
        };
        Ok(match language {
            CodeLanguage::Shell => {
                let mut code = format!(
                    "db.getCollection('{}').find(\n  {}",
                    tab.collection,
                    or_empty(&filter)
                );
                if !project.is_empty() {
                    code.push_str(&format!(",\n  {project}"));
                }
                code.push_str("\n)");
                if !sort.is_empty() {
                    code.push_str(&format!(".sort({sort})"));
                }
                for (method, value) in [
                    ("skip", &skip),
                    ("limit", &limit),
                    ("collation", &collation),
                    ("hint", &hint),
                    ("maxTimeMS", &max_time),
                ] {
                    if !value.is_empty() {
                        code.push_str(&format!(".{method}({value})"));
                    }
                }
                code
            }
            CodeLanguage::Node => {
                let json = |text: &str| -> Result<String> {
                    let document = parse_query(text)?;
                    Ok(serde_json::to_string(
                        &Bson::Document(document).into_canonical_extjson(),
                    )?)
                };
                let mut options = Vec::new();
                if !project.is_empty() {
                    options.push(format!(
                        "projection: BSON.EJSON.parse('{}')",
                        json(&project)?
                    ));
                }
                if !sort.is_empty() {
                    options.push(format!("sort: BSON.EJSON.parse('{}')", json(&sort)?));
                }
                if !collation.is_empty() {
                    options.push(format!(
                        "collation: BSON.EJSON.parse('{}')",
                        json(&collation)?
                    ));
                }
                if hint.starts_with(['"', '\'']) {
                    options.push(format!("hint: {hint}"));
                } else if !hint.is_empty() {
                    options.push(format!("hint: BSON.EJSON.parse('{}')", json(&hint)?));
                }
                for (name, value) in [("skip", &skip), ("limit", &limit), ("maxTimeMS", &max_time)]
                {
                    if !value.is_empty() {
                        options.push(format!("{name}: {value}"));
                    }
                }
                format!(
                    "import {{ MongoClient, BSON }} from 'mongodb';\n\nconst client = new MongoClient(process.env.MONGODB_URI);\nconst filter = BSON.EJSON.parse('{}');\nconst cursor = client\n  .db('{}')\n  .collection('{}')\n  .find(filter{});\nconst result = await cursor.toArray();\nawait client.close();",
                    json(&filter)?,
                    tab.database,
                    tab.collection,
                    if options.is_empty() {
                        String::new()
                    } else {
                        format!(", {{ {} }}", options.join(", "))
                    }
                )
            }
        })
    }

    fn start_export(
        &mut self,
        format: ExportFormat,
        whole: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.mongo() else {
            return;
        };
        let Some(request) = self.request(cx) else {
            return;
        };
        let Some(tab) = self.tab_mut() else {
            return;
        };
        if tab
            .export
            .as_ref()
            .is_some_and(|export| export.outcome.is_none())
        {
            return;
        }
        let page = match &tab.page {
            Some(Fetch::Loaded(page)) => Some(page.documents.clone()),
            _ => None,
        };
        // shortcut: CSV columns are the current page's top-level keys; keys that only appear later are not exported.
        let columns = page
            .as_ref()
            .map(|documents| table_columns(documents))
            .unwrap_or_default();
        let extension = match format {
            ExportFormat::Json => "json",
            ExportFormat::Csv => "csv",
        };
        let name = format!("{}.{}.{extension}", tab.database, tab.collection);
        let (database, collection) = (tab.database.clone(), tab.collection.clone());
        let chosen = cx.prompt_for_new_path(&paths::home_dir().join("Downloads"), Some(&name));
        let task = cx.spawn(async move |this, cx| {
            let Ok(Ok(Some(file))) = chosen.await else {
                return;
            };
            let written = Arc::new(AtomicU64::new(0));
            let cancel = Arc::new(AtomicBool::new(false));
            let job = {
                let (written, cancel, file, columns) = (
                    written.clone(),
                    cancel.clone(),
                    file.clone(),
                    columns.clone(),
                );
                cx.update(|cx| {
                    run(cx, async move {
                        let mut out = std::io::BufWriter::new(std::fs::File::create(&file)?);
                        let mut write = |document: Document| -> Result<()> {
                            let index = written.fetch_add(1, Ordering::Relaxed);
                            match format {
                                ExportFormat::Json => {
                                    out.write_all(if index == 0 { b"[\n" } else { b",\n" })?;
                                    out.write_all(to_canonical_json(&document).as_bytes())?;
                                }
                                ExportFormat::Csv => {
                                    if index == 0 {
                                        writeln!(
                                            out,
                                            "{}",
                                            columns
                                                .iter()
                                                .map(|column| csv_field(Some(&Bson::String(
                                                    column.clone()
                                                ))))
                                                .collect::<Vec<_>>()
                                                .join(",")
                                        )?;
                                    }
                                    writeln!(
                                        out,
                                        "{}",
                                        columns
                                            .iter()
                                            .map(|column| csv_field(document.get(column)))
                                            .collect::<Vec<_>>()
                                            .join(",")
                                    )?;
                                }
                            }
                            Ok(())
                        };
                        match (whole, page) {
                            (false, Some(documents)) => {
                                for document in documents.iter() {
                                    write(document.clone())?;
                                }
                            }
                            _ => {
                                session
                                    .for_each_match(&request, None, &cancel, &mut write)
                                    .await?
                            }
                        }
                        if format == ExportFormat::Json {
                            out.write_all(if written.load(Ordering::Relaxed) == 0 {
                                b"[]\n"
                            } else {
                                b"\n]\n"
                            })?;
                        }
                        out.flush()?;
                        Ok(written.load(Ordering::Relaxed))
                    })
                })
            };
            this.update(cx, |this, cx| {
                if let Some(tab) = this.tab_at(&database, &collection) {
                    tab.export = Some(Export {
                        file: file.clone(),
                        written: written.clone(),
                        cancel: cancel.clone(),
                        outcome: None,
                        _task: Task::ready(()),
                    });
                    cx.notify();
                }
            })
            .ok();
            // Redraw the count while the export runs.
            let ticker = {
                let this = this.clone();
                let written = written.clone();
                cx.spawn(async move |cx| {
                    let mut last = 0;
                    loop {
                        cx.background_executor()
                            .timer(Duration::from_millis(300))
                            .await;
                        let now = written.load(Ordering::Relaxed);
                        if now != last {
                            last = now;
                            if this.update(cx, |_, cx| cx.notify()).is_err() {
                                return;
                            }
                        }
                    }
                })
            };
            let result = job.await;
            drop(ticker);
            this.update(cx, |this, cx| {
                if let Some(tab) = this.tab_at(&database, &collection)
                    && let Some(export) = &mut tab.export
                {
                    export.outcome = Some(result.map_err(|error| format!("{error:#}").into()));
                    cx.notify();
                }
            })
            .ok();
        });
        tab.keep(task);
    }

    fn open_shell(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (Some(connection), Some(tab)) = (self.active_connection().cloned(), self.tab()) else {
            return;
        };
        let database = tab.database.clone();
        let workspace = self.workspace.clone();
        cx.spawn_in(window, async move |_, cx| {
            let credential = cx
                .update(|_, cx| cx.read_credentials(&connection.credential_url()))?
                .await?;
            let (_, uri) = credential.ok_or_else(|| anyhow!("No connection string saved"))?;
            let uri = String::from_utf8(uri)?;
            // The connection string goes through the environment, never the command line, so `ps` does not show the password.
            let database = serde_json::to_string(&database)?;
            let spawn = task::SpawnInTerminal {
                label: format!("mongosh · {}", connection.name),
                full_label: format!("mongosh · {}", connection.name),
                command_label: format!("mongosh · {}", connection.name),
                command: Some("mongosh".into()),
                args: vec![
                    "--nodb".into(),
                    "--shell".into(),
                    "--quiet".into(),
                    "--eval".into(),
                    format!("db = connect(process.env.VILO_MONGO_URI).getSiblingDB({database})"),
                ],
                env: [("VILO_MONGO_URI".to_string(), uri)].into_iter().collect(),
                use_new_terminal: true,
                allow_concurrent_runs: true,
                ..Default::default()
            };
            workspace.update_in(cx, |workspace, window, cx| {
                workspace.spawn_in_terminal(spawn, window, cx).detach()
            })?;
            anyhow::Ok(())
        })
        .detach_and_log_err(cx);
    }
}

fn page_view(documents: Vec<Document>, has_more: bool) -> MongoPage {
    let mut json_lines = Vec::new();
    for document in &documents {
        json_lines.extend(
            to_display_json(document)
                .lines()
                .map(|line| SharedString::from(line.to_string())),
        );
        json_lines.push(SharedString::default());
    }
    MongoPage {
        documents: Arc::new(documents),
        json_lines,
        has_more,
    }
}

impl DatabaseView {
    fn render_list(&self, tab: &MongoTab, page: &MongoPage, cx: &mut Context<Self>) -> AnyElement {
        let rows = Arc::new(tab.list_rows(&page.documents));
        let documents = page.documents.clone();
        let expanded = tab.expanded.clone();
        let opened_documents = (0..documents.len())
            .filter(|index| {
                documents.get(*index).is_some_and(|document| {
                    tab.top_level_containers(document, *index)
                        .iter()
                        .any(|key| expanded.contains(key))
                })
            })
            .collect::<HashSet<_>>();
        let containers = documents
            .iter()
            .enumerate()
            .map(|(index, document)| tab.top_level_containers(document, index))
            .collect::<Vec<_>>();
        let local_dates = self.local_dates;
        let view = cx.entity().downgrade();
        let colors = cx.theme().colors().clone();
        let count = rows.len();
        uniform_list("database-documents", count, move |range, _, cx| {
            range
                .filter_map(|index| rows.get(index).cloned().map(|row| (index, row)))
                .map(|(index, row)| {
                    if matches!(row.kind, RowKind::Gap) {
                        return div()
                            .id(("database-gap", index))
                            .h(px(ROW_HEIGHT))
                            .w_full()
                            .into_any_element();
                    }
                    let group = SharedString::from(format!("database-card-row-{index}"));
                    let document = row.document;
                    let mut card = h_flex()
                        .id(("database-row", index))
                        .group(group.clone())
                        .w_full()
                        .h(px(ROW_HEIGHT))
                        .pr_3()
                        .gap_1()
                        .border_l_1()
                        .border_r_1()
                        .border_color(colors.border)
                        .bg(colors.editor_background)
                        .when(row.first, |this| this.border_t_1().rounded_t_md())
                        .when(row.last, |this| this.border_b_1().rounded_b_md());
                    // The card's expand toggle sits left of its first line, as in Compass.
                    // Compass shows it only while the pointer is on the document.
                    card = card.child(
                        div()
                            .w(px(36.))
                            .flex_none()
                            .flex()
                            .justify_center()
                            .visible_on_hover(group.clone())
                            .when(row.first, |this| {
                                let opened = opened_documents.contains(&document);
                                let keys = containers.get(document).cloned().unwrap_or_default();
                                let view = view.clone();
                                this.child(
                                    IconButton::new(
                                        ("database-expand-all", index),
                                        if opened {
                                            IconName::ChevronDown
                                        } else {
                                            IconName::ChevronRight
                                        },
                                    )
                                    .icon_size(IconSize::XSmall)
                                    .style(ButtonStyle::Outlined)
                                    .tooltip(Tooltip::text(if opened {
                                        "Collapse all"
                                    } else {
                                        "Expand all"
                                    }))
                                    .on_click(
                                        move |_, _, cx| {
                                            let keys = keys.clone();
                                            view.update(cx, |this, cx| {
                                                if let Some(tab) = this.tab_mut() {
                                                    for key in keys {
                                                        if opened {
                                                            tab.expanded.remove(&key);
                                                        } else {
                                                            tab.expanded.insert(key);
                                                        }
                                                    }
                                                }
                                                cx.notify();
                                            })
                                            .ok();
                                        },
                                    ),
                                )
                            }),
                    );
                    let content = match row.kind {
                        RowKind::Gap => div().into_any_element(),
                        RowKind::MoreFields { hidden } => {
                            let view = view.clone();
                            Button::new(
                                ("database-more", document),
                                format!(
                                    "Show {hidden} more field{}",
                                    if hidden == 1 { "" } else { "s" }
                                ),
                            )
                            .label_size(LabelSize::Small)
                            .color(Color::Accent)
                            .start_icon(
                                Icon::new(IconName::ChevronDown)
                                    .size(IconSize::XSmall)
                                    .color(Color::Accent),
                            )
                            .on_click(move |_, _, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(tab) = this.tab_mut() {
                                        tab.show_all.insert(document);
                                    }
                                    cx.notify();
                                })
                                .ok();
                            })
                            .into_any_element()
                        }
                        RowKind::MoreItems {
                            path,
                            depth,
                            hidden,
                        } => {
                            let view = view.clone();
                            h_flex()
                                .child(div().w(px(depth as f32 * 18.)))
                                .child(
                                    Button::new(
                                        ("database-more-items", index),
                                        format!(
                                            "Show {} more of {hidden} items",
                                            hidden.min(ITEMS_SHOWN)
                                        ),
                                    )
                                    .label_size(LabelSize::Small)
                                    .color(Color::Accent)
                                    .on_click(
                                        move |_, _, cx| {
                                            let key = (document, path.clone());
                                            view.update(cx, |this, cx| {
                                                if let Some(tab) = this.tab_mut() {
                                                    *tab.items_shown
                                                        .entry(key)
                                                        .or_insert(ITEMS_SHOWN) += ITEMS_SHOWN;
                                                }
                                                cx.notify();
                                            })
                                            .ok();
                                        },
                                    ),
                                )
                                .into_any_element()
                        }
                        RowKind::Field { path, depth } => {
                            let Some(value) = documents
                                .get(document)
                                .and_then(|item| value_at(item, &path))
                            else {
                                return card.into_any_element();
                            };
                            let key = path.last().cloned().unwrap_or_default();
                            let shown = show(value);
                            let expandable = matches!(shown, Shown::Object(_) | Shown::Array(_));
                            let open = expanded.contains(&(document, path.clone()));
                            let (text, color) = match &shown {
                                Shown::Scalar { text, kind } => (text.clone(), kind_color(*kind)),
                                Shown::Object(count) => {
                                    (format!("Object ({count})"), Color::Default)
                                }
                                Shown::Array(count) => (format!("Array ({count})"), Color::Default),
                            };
                            let human = match value {
                                Bson::DateTime(date) => human_date(date, local_dates),
                                _ => None,
                            };
                            let view = view.clone();
                            let copy = text.clone();
                            h_flex()
                                .id(("database-field", index))
                                .flex_1()
                                .min_w_0()
                                .gap_1()
                                .cursor_pointer()
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
                                        cx.write_to_clipboard(ClipboardItem::new_string(
                                            copy.clone(),
                                        ));
                                    }
                                })
                                .child(div().w(px(depth as f32 * 18.)).flex_none())
                                .child(div().w(px(10.)).flex_none().when(expandable, |this| {
                                    this.child(
                                        Icon::new(if open {
                                            IconName::ChevronDown
                                        } else {
                                            IconName::ChevronRight
                                        })
                                        .size(IconSize::XSmall)
                                        .color(Color::Default),
                                    )
                                }))
                                .child(
                                    Label::new(format!("{key}:"))
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .weight(gpui::FontWeight::BOLD)
                                        .single_line(),
                                )
                                .child(
                                    Label::new(text)
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .color(color)
                                        .single_line()
                                        .truncate(),
                                )
                                .when_some(human, |this, human| {
                                    this.child(
                                        Label::new(human)
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .color(Color::Default)
                                            .underline()
                                            .single_line(),
                                    )
                                })
                                .into_any_element()
                        }
                    };
                    card = card.child(div().flex_1().min_w_0().child(content));
                    if row.first {
                        let documents = documents.clone();
                        card = card.child(
                            div().visible_on_hover(group).child(
                                IconButton::new(("database-copy", document), IconName::Copy)
                                    .icon_size(IconSize::Small)
                                    .style(ButtonStyle::Outlined)
                                    .tooltip(Tooltip::text("Copy document"))
                                    .on_click(move |_, _, cx| {
                                        if let Some(document) = documents.get(document) {
                                            cx.write_to_clipboard(ClipboardItem::new_string(
                                                to_canonical_json(document),
                                            ));
                                        }
                                    }),
                            ),
                        );
                    }
                    card.into_any_element()
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
                    let mut row = h_flex()
                        .id(("database-table-row", index))
                        .h(px(ROW_HEIGHT + 4.))
                        .border_b_1()
                        .border_color(colors.border_variant);
                    if index == 0 {
                        row = row.bg(colors.element_background);
                        row = row.child(div().w(px(48.)).flex_none().px_2());
                        for column in columns.iter() {
                            row = row.child(
                                div()
                                    .w(px(COLUMN_WIDTH))
                                    .flex_none()
                                    .px_2()
                                    .border_r_1()
                                    .border_color(colors.border_variant)
                                    .child(
                                        Label::new(column.clone())
                                            .size(LabelSize::Small)
                                            .weight(gpui::FontWeight::SEMIBOLD)
                                            .single_line()
                                            .truncate(),
                                    ),
                            );
                        }
                        return row.into_any_element();
                    }
                    let Some(document) = documents.get(index - 1) else {
                        return row.into_any_element();
                    };
                    row = row.child(
                        div().w(px(48.)).flex_none().px_2().child(
                            Label::new(index.to_string())
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        ),
                    );
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
                                .tooltip(Tooltip::text("Click to copy"))
                                .on_click(move |_, _, cx| {
                                    cx.write_to_clipboard(ClipboardItem::new_string(copy.clone()))
                                })
                                .child(
                                    Label::new(text)
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .color(color)
                                        .single_line()
                                        .truncate(),
                                ),
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

    fn render_results(
        &self,
        tab: &MongoTab,
        page: Option<&Fetch<MongoPage>>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match page {
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .size(LabelSize::Small)
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loading) => Label::new("Loading documents…")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Loaded(page)) if page.documents.is_empty() => v_flex()
                .pt_8()
                .items_center()
                .child(Label::new("No results").color(Color::Muted))
                .into_any_element(),
            Some(Fetch::Loaded(page)) => match tab.view {
                DocumentView::List => self.render_list(tab, page, cx),
                DocumentView::Json => render_lines(
                    "database-documents-json",
                    page.json_lines.clone(),
                    &tab.scroll,
                    cx,
                ),
                DocumentView::Table => self.render_table(tab, page, cx),
            },
            None => div().into_any_element(),
        }
    }

    pub(crate) fn render_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let mut strip = h_flex()
            .id("database-tabs")
            .w_full()
            .overflow_x_scroll()
            .border_b_1()
            .border_color(colors.border)
            .bg(colors.tab_bar_background);
        for (index, tab) in self.tabs.iter().enumerate() {
            let selected = self.active_tab == Some(index);
            strip =
                strip.child(
                    h_flex()
                        .id(("database-tab", index))
                        .group(SharedString::from(format!("database-tab-{index}")))
                        .h(px(36.))
                        .px_3()
                        .gap_2()
                        .border_r_1()
                        .border_color(colors.border_variant)
                        .cursor_pointer()
                        .bg(if selected {
                            colors.tab_active_background
                        } else {
                            colors.tab_inactive_background
                        })
                        .tooltip(Tooltip::text(format!(
                            "{}.{}",
                            tab.database, tab.collection
                        )))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.active_tab = Some(index);
                            cx.notify();
                        }))
                        .child(Icon::new(IconName::Folder).size(IconSize::Small).color(
                            if selected {
                                Color::Default
                            } else {
                                Color::Muted
                            },
                        ))
                        .child(Label::new(tab.title()).color(if selected {
                            Color::Default
                        } else {
                            Color::Muted
                        }))
                        .child(
                            div()
                                .when(!selected, |this| {
                                    this.visible_on_hover(SharedString::from(format!(
                                        "database-tab-{index}"
                                    )))
                                })
                                .child(
                                    IconButton::new(("database-tab-close", index), IconName::Close)
                                        .icon_size(IconSize::XSmall)
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            cx.stop_propagation();
                                            this.close_tab(index, cx);
                                        })),
                                ),
                        ),
                );
        }
        strip
    }

    fn render_pager(
        &self,
        tab: &MongoTab,
        aggregation: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let page = if aggregation {
            &tab.aggregation
        } else {
            &tab.page
        };
        let (shown, has_more) = match page {
            Some(Fetch::Loaded(page)) => (page.documents.len(), page.has_more),
            _ => (0, false),
        };
        let skip = if aggregation {
            tab.aggregation_skip
        } else {
            tab.skip
        };
        let page_size = u64::from(tab.page_size);
        let total = match (&tab.total, aggregation) {
            (_, true) => String::new(),
            (Some(Total::Exact(total)), _) => format!(" of {total}"),
            (Some(Total::Estimated(total)), _) => format!(" of ≈{total}"),
            (Some(Total::Counting), _) => " of …".into(),
            _ => String::new(),
        };
        let status = match page {
            Some(Fetch::Loaded(_)) if shown == 0 => "0 – 0".to_string(),
            Some(Fetch::Loaded(_)) => format!("{} – {}{total}", skip + 1, skip + shown as u64),
            _ => String::new(),
        };
        let rerun = move |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if aggregation {
                this.run_aggregation(cx)
            } else {
                this.find(cx)
            }
        };
        let set_skip = move |this: &mut DatabaseView, value: u64| {
            if let Some(tab) = this.tab_mut() {
                if aggregation {
                    tab.aggregation_skip = value;
                } else {
                    tab.skip = value;
                }
            }
        };
        let view = cx.entity().downgrade();
        let size_menu = PopoverMenu::new(if aggregation {
            "database-page-size-aggregation"
        } else {
            "database-page-size"
        })
        .trigger(
            Button::new("database-page-size-button", tab.page_size.to_string())
                .label_size(LabelSize::Small)
                .style(ButtonStyle::Outlined)
                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
        )
        .menu(move |window, cx| {
            let view = view.clone();
            Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                for size in PAGE_SIZES {
                    let view = view.clone();
                    menu = menu.entry(size.to_string(), None, move |_, cx| {
                        view.update(cx, |this, cx| {
                            if let Some(tab) = this.tab_mut() {
                                tab.page_size = size;
                            }
                            set_skip(this, 0);
                            rerun(this, cx);
                        })
                        .ok();
                    });
                }
                menu
            }))
        });
        let view_icon = |id: &'static str,
                         icon: IconName,
                         tip: &'static str,
                         view: DocumentView,
                         cx: &mut Context<Self>| {
            IconButton::new(id, icon)
                .icon_size(IconSize::Small)
                .style(if tab.view == view {
                    ButtonStyle::Filled
                } else {
                    ButtonStyle::Subtle
                })
                .tooltip(Tooltip::text(tip))
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(tab) = this.tab_mut() {
                        tab.view = view;
                    }
                    cx.notify();
                }))
        };
        let more_view = cx.entity().downgrade();
        let local_dates = self.local_dates;
        let unknown = tab.total == Some(Total::Unknown) && !aggregation;
        h_flex()
            .gap_1p5()
            .child(size_menu)
            .child(Label::new(status).size(LabelSize::Small))
            .when(unknown, |this| {
                this.child(
                    Button::new("database-recount", "count unknown · retry")
                        .label_size(LabelSize::Small)
                        .color(Color::Accent)
                        .tooltip(Tooltip::text(
                            "Counting took longer than 5 seconds or failed",
                        ))
                        .on_click(cx.listener(|this, _, _, cx| this.recount(cx))),
                )
            })
            .child(
                IconButton::new("database-refresh", IconName::RotateCw)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Refresh"))
                    .on_click(cx.listener(move |this, _, _, cx| rerun(this, cx))),
            )
            .child(
                IconButton::new("database-previous", IconName::ChevronLeft)
                    .icon_size(IconSize::Small)
                    .disabled(skip == 0)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        set_skip(this, skip.saturating_sub(page_size));
                        rerun(this, cx);
                    })),
            )
            .child(
                IconButton::new("database-next", IconName::ChevronRight)
                    .icon_size(IconSize::Small)
                    .disabled(!has_more)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        set_skip(this, skip + page_size);
                        rerun(this, cx);
                    })),
            )
            .child(
                PopoverMenu::new("database-view-options")
                    .trigger(
                        IconButton::new("database-view-options-button", IconName::ChevronDown)
                            .icon_size(IconSize::Small)
                            .style(ButtonStyle::Outlined),
                    )
                    .menu(move |window, cx| {
                        let view = more_view.clone();
                        Some(ContextMenu::build(window, cx, move |menu, _, _| {
                            let toggle = view.clone();
                            let collapse = view.clone();
                            menu.entry(
                                if local_dates {
                                    "Show dates in UTC"
                                } else {
                                    "Show dates in local time"
                                },
                                None,
                                move |_, cx| {
                                    toggle
                                        .update(cx, |this, cx| {
                                            this.local_dates = !this.local_dates;
                                            cx.notify();
                                        })
                                        .ok();
                                },
                            )
                            .entry(
                                "Collapse all documents",
                                None,
                                move |_, cx| {
                                    collapse
                                        .update(cx, |this, cx| {
                                            if let Some(tab) = this.tab_mut() {
                                                tab.expanded.clear();
                                            }
                                            cx.notify();
                                        })
                                        .ok();
                                },
                            )
                        }))
                    }),
            )
            .child(
                h_flex()
                    .p_0p5()
                    .rounded_md()
                    .border_1()
                    .border_color(cx.theme().colors().border)
                    .child(view_icon(
                        "database-view-list",
                        IconName::Menu,
                        "List view",
                        DocumentView::List,
                        cx,
                    ))
                    .child(view_icon(
                        "database-view-json",
                        IconName::Code,
                        "JSON view",
                        DocumentView::Json,
                        cx,
                    ))
                    .child(view_icon(
                        "database-view-table",
                        IconName::Table,
                        "Table view",
                        DocumentView::Table,
                        cx,
                    )),
            )
    }

    fn render_query_bar(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let current = self.current_query(cx);
        let starred = current.as_ref().is_some_and(|query| {
            self.history
                .iter()
                .any(|saved| saved.favorite && saved.same_query(query))
        });
        let saved = self
            .history
            .iter()
            .filter(|saved| {
                Some(&saved.connection) == self.active.as_ref()
                    && saved.database == tab.database
                    && saved.collection == tab.collection
            })
            .cloned()
            .collect::<Vec<_>>();
        let view = cx.entity().downgrade();
        let history = PopoverMenu::new("database-history")
            .trigger(
                Button::new("database-history-button", "")
                    .start_icon(Icon::new(IconName::HistoryRerun).size(IconSize::Small))
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                    .tooltip(Tooltip::text("Query history")),
            )
            .menu(move |window, cx| {
                let saved = saved.clone();
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    if saved.is_empty() {
                        return menu.label("No queries yet");
                    }
                    let (favorites, recent): (Vec<_>, Vec<_>) =
                        saved.iter().cloned().partition(|saved| saved.favorite);
                    for (title, group) in [("Favorites", favorites), ("Recent", recent)] {
                        if group.is_empty() {
                            continue;
                        }
                        menu = menu.header(title);
                        for query in group {
                            let view = view.clone();
                            menu = menu.entry(query.label(), None, move |window, cx| {
                                let query = query.clone();
                                view.update(cx, |this, cx| this.apply_saved(query, window, cx))
                                    .ok();
                            });
                        }
                    }
                    menu.separator()
                        .entry("Clear recent queries", None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                this.history.retain(|saved| saved.favorite);
                                this.save_history(cx);
                                cx.notify();
                            })
                            .ok();
                        })
                }))
            });
        // Compass lays the options out as labelled lines, with the numbers boxed on the right.
        let option = |label: &'static str, editor: &Entity<Editor>| {
            h_flex()
                .flex_1()
                .min_w_0()
                .h(px(28.))
                .gap_3()
                .child(
                    div().w(px(88.)).flex_none().child(
                        Label::new(label)
                            .size(LabelSize::Small)
                            .weight(gpui::FontWeight::SEMIBOLD),
                    ),
                )
                .child(div().flex_1().min_w_0().child(editor.clone()))
        };
        let number = |label: &'static str, editor: &Entity<Editor>, cx: &App| {
            h_flex()
                .flex_none()
                .gap_2()
                .child(
                    Label::new(label)
                        .size(LabelSize::Small)
                        .weight(gpui::FontWeight::SEMIBOLD),
                )
                .child(div().w(px(180.)).child(boxed(editor.clone(), cx)))
        };
        let untouched = tab
            .query_editors()
            .iter()
            .all(|editor| editor.read(cx).text(cx).trim().is_empty());
        let find = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if let Some(tab) = this.tab_mut() {
                tab.skip = 0;
            }
            this.find(cx);
        };
        v_flex()
            .p_3()
            .gap_2()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .bg(colors.editor_background)
            .on_action(cx.listener(move |this, _: &menu::Confirm, _, cx| find(this, cx)))
            .child(
                h_flex()
                    .gap_2()
                    .child(history)
                    .child(div().flex_1().min_w_0().child(tab.filter.clone()))
                    .child(
                        IconButton::new(
                            "database-star",
                            if starred {
                                IconName::StarFilled
                            } else {
                                IconName::Star
                            },
                        )
                        .icon_size(IconSize::Small)
                        .tooltip(Tooltip::text(if starred {
                            "Remove from favorites"
                        } else {
                            "Add to favorites"
                        }))
                        .on_click(cx.listener(|this, _, _, cx| this.toggle_favorite(cx))),
                    )
                    .child(
                        Button::new("database-explain", "Explain")
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, _, cx| this.explain(cx))),
                    )
                    .child(
                        Button::new("database-reset", "Reset")
                            .style(ButtonStyle::Filled)
                            .disabled(untouched)
                            .on_click(
                                cx.listener(|this, _, window, cx| this.reset_query(window, cx)),
                            ),
                    )
                    .child(
                        compass_primary("database-find", "Find", None, false)
                            .on_click(cx.listener(move |this, _, _, cx| find(this, cx))),
                    )
                    .child(
                        Button::new("database-options", "Options")
                            .color(Color::Accent)
                            .end_icon(
                                Icon::new(if tab.show_options {
                                    IconName::ChevronDown
                                } else {
                                    IconName::ChevronRight
                                })
                                .size(IconSize::XSmall)
                                .color(Color::Accent),
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(tab) = this.tab_mut() {
                                    tab.show_options = !tab.show_options;
                                }
                                cx.notify();
                            })),
                    ),
            )
            .when(tab.show_options, |this| {
                this.child(h_flex().child(option("Project", &tab.projection)))
                    .child(
                        h_flex()
                            .gap_4()
                            .child(option("Sort", &tab.sort))
                            .child(number("Max Time MS", &tab.max_time, cx)),
                    )
                    .child(
                        h_flex()
                            .gap_4()
                            .child(option("Collation", &tab.collation))
                            .child(number("Skip", &tab.skip_editor, cx))
                            .child(number("Limit", &tab.limit, cx)),
                    )
                    .child(h_flex().child(option("Index Hint", &tab.hint)))
            })
            .into_any_element()
    }

    fn render_toolbar(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let view = cx.entity().downgrade();
        let export = PopoverMenu::new("database-export")
            .trigger(
                compass_button("database-export-button", "EXPORT DATA", IconName::Download)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |menu, _, _| {
                    let entry = |view: gpui::WeakEntity<DatabaseView>,
                                 format: ExportFormat,
                                 whole: bool| {
                        move |window: &mut Window, cx: &mut App| {
                            view.update(cx, |this, cx| {
                                this.start_export(format, whole, window, cx)
                            })
                            .ok();
                        }
                    };
                    menu.entry(
                        "Export the full query result as JSON…",
                        None,
                        entry(view.clone(), ExportFormat::Json, true),
                    )
                    .entry(
                        "Export the full query result as CSV…",
                        None,
                        entry(view.clone(), ExportFormat::Csv, true),
                    )
                    .separator()
                    .entry(
                        "Export this page as JSON…",
                        None,
                        entry(view.clone(), ExportFormat::Json, false),
                    )
                    .entry(
                        "Export this page as CSV…",
                        None,
                        entry(view, ExportFormat::Csv, false),
                    )
                }))
            });
        let export_status = tab.export.as_ref().map(|export| {
            let name = export
                .file
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            match &export.outcome {
                None => h_flex()
                    .gap_1()
                    .child(
                        Label::new(format!(
                            "Exporting {}…",
                            export.written.load(Ordering::Relaxed)
                        ))
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                    )
                    .child({
                        let cancel = export.cancel.clone();
                        Button::new("database-export-cancel", "Cancel")
                            .label_size(LabelSize::Small)
                            .on_click(move |_, _, _| cancel.store(true, Ordering::Relaxed))
                    })
                    .into_any_element(),
                Some(Ok(count)) => Label::new(format!("Exported {count} documents to {name}"))
                    .size(LabelSize::Small)
                    .color(Color::Success)
                    .into_any_element(),
                Some(Err(message)) => Label::new(format!("Export stopped: {message}"))
                    .size(LabelSize::Small)
                    .color(Color::Error)
                    .into_any_element(),
            }
        });
        h_flex()
            .gap_2()
            .child(
                compass_primary("database-add-data", "ADD DATA", Some(IconName::Plus), true)
                    .child(
                        Icon::new(IconName::ChevronDown)
                            .size(IconSize::XSmall)
                            .color(Color::Custom(rgb(0xFFFFFF).into())),
                    )
                    .tooltip(Tooltip::text(
                        "Adding and editing documents comes with the next release",
                    )),
            )
            .child(
                compass_button("database-bulk", "BULK", IconName::Copy)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                    .disabled(true)
                    .tooltip(Tooltip::text(
                        "Bulk update and delete come with the next release",
                    )),
            )
            .child(export)
            .child(
                compass_button("database-export-code", "EXPORT CODE", IconName::Code).on_click(
                    cx.listener(|this, _, _, cx| {
                        if let Some(tab) = this.tab_mut() {
                            tab.panel = if tab.panel == Panel::Code {
                                Panel::Results
                            } else {
                                Panel::Code
                            };
                        }
                        cx.notify();
                    }),
                ),
            )
            .children(export_status)
            .child(div().flex_1())
            .child(self.render_pager(tab, false, cx))
            .into_any_element()
    }

    fn render_explain(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let header = h_flex()
            .gap_2()
            .child(Headline::new("Explain Plan").size(HeadlineSize::Small))
            .child(div().flex_1())
            .child(
                Button::new(
                    "database-explain-raw",
                    if tab.explain_raw {
                        "Visual"
                    } else {
                        "Raw output"
                    },
                )
                .style(ButtonStyle::Outlined)
                .on_click(cx.listener(|this, _, _, cx| {
                    if let Some(tab) = this.tab_mut() {
                        tab.explain_raw = !tab.explain_raw;
                    }
                    cx.notify();
                })),
            )
            .child(
                IconButton::new("database-explain-close", IconName::Close).on_click(cx.listener(
                    |this, _, _, cx| {
                        if let Some(tab) = this.tab_mut() {
                            tab.panel = Panel::Results;
                        }
                        cx.notify();
                    },
                )),
            );
        let body = match &tab.explain {
            None | Some(Fetch::Loading) => Label::new("Running explain…")
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loaded(result)) if tab.explain_raw => render_lines(
                "database-explain-lines",
                result.raw.clone(),
                &tab.scroll,
                cx,
            ),
            Some(Fetch::Loaded(result)) => {
                let summary = &result.summary;
                let metric = |label: &'static str, value: String| {
                    v_flex()
                        .p_3()
                        .gap_1()
                        .min_w(px(180.))
                        .rounded_md()
                        .border_1()
                        .border_color(colors.border)
                        .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                        .child(Headline::new(value).size(HeadlineSize::Small))
                };
                let collection_scan = summary.stages.iter().any(|stage| stage == "COLLSCAN");
                v_flex()
                    .gap_3()
                    .child(
                        h_flex()
                            .gap_3()
                            .flex_wrap()
                            .child(metric("Documents Returned", summary.returned.to_string()))
                            .child(metric("Index Keys Examined", summary.keys_examined.to_string()))
                            .child(metric("Documents Examined", summary.documents_examined.to_string()))
                            .child(metric("Actual Query Execution Time (ms)", summary.millis.to_string())),
                    )
                    .child(if summary.indexes.is_empty() {
                        Label::new(if collection_scan { "No index available for this query: it scans the whole collection." } else { "No index used." })
                            .color(if collection_scan { Color::Error } else { Color::Muted })
                            .into_any_element()
                    } else {
                        h_flex()
                            .gap_2()
                            .child(Label::new("Query used the following index:"))
                            .children(summary.indexes.iter().map(|index| Label::new(index.clone()).buffer_font(cx).color(Color::Success)))
                            .into_any_element()
                    })
                    .child(Label::new(format!("Plan: {}", summary.stages.join(" → "))).buffer_font(cx).color(Color::Muted))
                    .child(Label::new("Execution time is measured on the server; it does not include the network.").size(LabelSize::Small).color(Color::Muted))
                    .into_any_element()
            }
        };
        v_flex()
            .flex_1()
            .min_h_0()
            .p_4()
            .gap_3()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .child(header)
            .child(body)
            .into_any_element()
    }

    fn render_code(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let code = self.query_code(tab.code_language, cx);
        let language = |id: &'static str,
                        label: &'static str,
                        language: CodeLanguage,
                        cx: &mut Context<Self>| {
            Button::new(id, label)
                .style(if tab.code_language == language {
                    ButtonStyle::Filled
                } else {
                    ButtonStyle::Subtle
                })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(tab) = this.tab_mut() {
                        tab.code_language = language;
                    }
                    cx.notify();
                }))
        };
        let copy = code.as_ref().ok().cloned();
        v_flex()
            .flex_1()
            .min_h_0()
            .p_4()
            .gap_3()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .child(
                h_flex()
                    .gap_2()
                    .child(Headline::new("Export Query To Language").size(HeadlineSize::Small))
                    .child(div().flex_1())
                    .child(
                        IconButton::new("database-code-close", IconName::Close).on_click(
                            cx.listener(|this, _, _, cx| {
                                if let Some(tab) = this.tab_mut() {
                                    tab.panel = Panel::Results;
                                }
                                cx.notify();
                            }),
                        ),
                    ),
            )
            .child(
                h_flex()
                    .gap_1()
                    .child(language(
                        "database-code-shell",
                        "mongosh",
                        CodeLanguage::Shell,
                        cx,
                    ))
                    .child(language(
                        "database-code-node",
                        "Node.js",
                        CodeLanguage::Node,
                        cx,
                    )),
            )
            .child(match code {
                Ok(code) => render_lines(
                    "database-code-lines",
                    code.lines()
                        .map(|line| SharedString::from(line.to_string()))
                        .collect(),
                    &tab.scroll,
                    cx,
                ),
                Err(error) => Label::new(format!("{error:#}"))
                    .color(Color::Error)
                    .into_any_element(),
            })
            .child(
                h_flex().child(
                    Button::new("database-code-copy", "Copy")
                        .style(ButtonStyle::Filled)
                        .start_icon(Icon::new(IconName::Copy).size(IconSize::Small))
                        .on_click(move |_, _, cx| {
                            if let Some(code) = &copy {
                                cx.write_to_clipboard(ClipboardItem::new_string(code.clone()));
                            }
                        }),
                ),
            )
            .into_any_element()
    }

    fn render_indexes(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let indexes = match &tab.indexes {
            None | Some(Fetch::Loading) => {
                return Label::new("Loading indexes…")
                    .color(Color::Muted)
                    .into_any_element();
            }
            Some(Fetch::Failed(message)) => {
                return Label::new(message.clone())
                    .color(Color::Error)
                    .into_any_element();
            }
            Some(Fetch::Loaded(indexes)) => indexes,
        };
        let header = |text: &'static str, width: Option<f32>| {
            let label = Label::new(text)
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD);
            match width {
                Some(width) => div().w(px(width)).flex_none().child(label),
                None => div().flex_1().child(label),
            }
        };
        let mut table = v_flex()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .child(
                h_flex()
                    .px_3()
                    .py_2()
                    .gap_3()
                    .bg(colors.element_background)
                    .child(header("Name & Definition", None))
                    .child(header("Type", Some(120.)))
                    .child(header("Properties", Some(260.))),
            );
        for index in indexes {
            let keys = index.get_document("key").cloned().unwrap_or_default();
            let definition = keys
                .iter()
                .map(|(key, value)| format!("{key} {value}"))
                .collect::<Vec<_>>()
                .join(", ");
            table = table.child(
                h_flex()
                    .px_3()
                    .py_2()
                    .gap_3()
                    .border_t_1()
                    .border_color(colors.border_variant)
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .child(
                                Label::new(index.get_str("name").unwrap_or_default().to_string())
                                    .buffer_font(cx),
                            )
                            .child(
                                Label::new(definition)
                                    .size(LabelSize::Small)
                                    .buffer_font(cx)
                                    .color(Color::Muted),
                            ),
                    )
                    .child(
                        div()
                            .w(px(120.))
                            .flex_none()
                            .child(Label::new(index_type(&keys)).size(LabelSize::Small)),
                    )
                    .child(
                        div().w(px(260.)).flex_none().child(
                            Label::new(index_properties(index).join(" · "))
                                .size(LabelSize::Small)
                                .color(Color::Accent),
                        ),
                    ),
            );
        }
        v_flex()
            .id("database-indexes")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .child(table)
            .into_any_element()
    }

    fn render_validation(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        match &tab.options {
            None | Some(Fetch::Loading) => Label::new("Loading…")
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loaded(None)) => Label::new("The collection no longer exists.")
                .color(Color::Warning)
                .into_any_element(),
            Some(Fetch::Loaded(Some(options))) => match options.get_document("validator") {
                Ok(validator) => {
                    let mut lines = vec![SharedString::from(format!(
                        "// validationLevel: {} · validationAction: {}",
                        options.get_str("validationLevel").unwrap_or("strict"),
                        options.get_str("validationAction").unwrap_or("error")
                    ))];
                    lines.extend(
                        to_display_json(validator)
                            .lines()
                            .map(|line| SharedString::from(line.to_string())),
                    );
                    render_lines("database-validation", lines, &tab.scroll, cx)
                }
                Err(_) => v_flex()
                    .pt_8()
                    .items_center()
                    .gap_2()
                    .child(Headline::new("No validation rules").size(HeadlineSize::Small))
                    .child(
                        Label::new(
                            "Documents in this collection are not checked against a schema.",
                        )
                        .color(Color::Muted),
                    )
                    .into_any_element(),
            },
        }
    }

    fn render_aggregations(&self, tab: &MongoTab, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        v_flex()
            .flex_1()
            .min_h_0()
            .gap_3()
            .child(
                v_flex()
                    .p_3()
                    .gap_2()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_background)
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Label::new("Pipeline").weight(gpui::FontWeight::SEMIBOLD))
                            .child(
                                Label::new("Read-only: $out and $merge are refused.")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(div().flex_1())
                            .child(
                                Button::new("database-run-pipeline", "Run")
                                    .style(ButtonStyle::Tinted(TintColor::Success))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        if let Some(tab) = this.tab_mut() {
                                            tab.aggregation_skip = 0;
                                        }
                                        this.run_aggregation(cx);
                                    })),
                            ),
                    )
                    .child(tab.pipeline.clone()),
            )
            .child(
                h_flex()
                    .child(div().flex_1())
                    .child(self.render_pager(tab, true, cx)),
            )
            .child(self.render_results(tab, tab.aggregation.as_ref(), cx))
            .into_any_element()
    }

    pub(crate) fn render_mongo(
        &self,
        connection: &crate::Connection,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let Some(tab) = self.tab() else {
            return v_flex()
                .size_full()
                .items_center()
                .justify_center()
                .gap_2()
                .child(Headline::new(connection.name.clone()).size(HeadlineSize::Small))
                .child(
                    Label::new(
                        self.status
                            .clone()
                            .unwrap_or_else(|| "Pick a collection on the left.".into()),
                    )
                    .color(Color::Muted),
                )
                .into_any_element();
        };
        let sub_tab = |id: &'static str,
                       label: &'static str,
                       badge: Option<String>,
                       target: SubTab,
                       cx: &mut Context<Self>| {
            let selected = tab.sub_tab == target;
            h_flex()
                .id(id)
                .h(px(40.))
                .px_4()
                .gap_2()
                .cursor_pointer()
                .border_b_2()
                .border_color(if selected {
                    colors.text_accent
                } else {
                    gpui::transparent_black()
                })
                .on_click(cx.listener(move |this, _, _, cx| this.set_sub_tab(target, cx)))
                .child(
                    Label::new(label)
                        .weight(if selected {
                            gpui::FontWeight::BOLD
                        } else {
                            gpui::FontWeight::NORMAL
                        })
                        .color(if selected {
                            Color::Default
                        } else {
                            Color::Muted
                        }),
                )
                .when_some(badge, |this, badge| {
                    this.child(
                        div()
                            .px_2()
                            .rounded_full()
                            .border_1()
                            .border_color(colors.border)
                            .bg(colors.element_background)
                            .child(
                                Label::new(badge)
                                    .size(LabelSize::XSmall)
                                    .weight(gpui::FontWeight::SEMIBOLD),
                            ),
                    )
                })
        };
        let documents_badge = tab.estimated.map(short_count);
        let indexes_badge = match &tab.indexes {
            Some(Fetch::Loaded(indexes)) => Some(indexes.len().to_string()),
            _ => None,
        };
        let body = match tab.sub_tab {
            SubTab::Documents => v_flex()
                .flex_1()
                .min_h_0()
                .gap_3()
                .child(self.render_query_bar(tab, cx))
                .child(self.render_toolbar(tab, cx))
                .child(match tab.panel {
                    Panel::Results => self.render_results(tab, tab.page.as_ref(), cx),
                    Panel::Explain => self.render_explain(tab, cx),
                    Panel::Code => self.render_code(tab, cx),
                })
                .into_any_element(),
            SubTab::Aggregations => self.render_aggregations(tab, cx),
            SubTab::Schema => v_flex()
                .pt_8()
                .items_center()
                .gap_2()
                .child(Headline::new("Schema analysis").size(HeadlineSize::Small))
                .child(
                    Label::new(
                        "Not in Vilo yet. Use the Table view to compare fields across documents.",
                    )
                    .color(Color::Muted),
                )
                .into_any_element(),
            SubTab::Indexes => self.render_indexes(tab, cx),
            SubTab::Validation => self.render_validation(tab, cx),
        };
        let crumb = |text: String, accent: bool| {
            Label::new(text)
                .weight(gpui::FontWeight::BOLD)
                .color(if accent { Color::Success } else { Color::Muted })
        };
        v_flex()
            .size_full()
            .child(self.render_tabs(cx))
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .px_6()
                    .pt_4()
                    .pb_2()
                    .gap_2()
                    .child(
                        h_flex()
                            .gap_2()
                            .child(crumb(connection.name.clone(), true))
                            .child(Icon::new(IconName::ChevronRight).size(IconSize::Small).color(Color::Muted))
                            .child(crumb(tab.database.clone(), true))
                            .child(Icon::new(IconName::ChevronRight).size(IconSize::Small).color(Color::Muted))
                            .child(crumb(tab.collection.clone(), false))
                            .child(
                                Label::new(connection.environment.clone())
                                    .size(LabelSize::XSmall)
                                    .color(if connection.production() { Color::Warning } else { Color::Muted }),
                            )
                            .child(div().flex_1())
                            .child(
                                Button::new("database-open-shell", "Open MongoDB shell")
                                    .style(ButtonStyle::Outlined)
                                    .start_icon(Icon::new(IconName::Terminal).size(IconSize::Small))
                                    .tooltip(Tooltip::text(if connection.production() {
                                        "Opens mongosh in a Vilo terminal. mongosh can change data — this is production."
                                    } else {
                                        "Opens mongosh in a Vilo terminal. mongosh can change data."
                                    }))
                                    .on_click(cx.listener(|this, _, window, cx| this.open_shell(window, cx))),
                            ),
                    )
                    .child(
                        h_flex()
                            .border_b_1()
                            .border_color(colors.border)
                            .child(sub_tab("database-sub-documents", "Documents", documents_badge, SubTab::Documents, cx))
                            .child(sub_tab("database-sub-aggregations", "Aggregations", None, SubTab::Aggregations, cx))
                            .child(sub_tab("database-sub-schema", "Schema", None, SubTab::Schema, cx))
                            .child(sub_tab("database-sub-indexes", "Indexes", indexes_badge, SubTab::Indexes, cx))
                            .child(sub_tab("database-sub-validation", "Validation", None, SubTab::Validation, cx)),
                    )
                    .child(div().h(px(4.)))
                    .child(body),
            )
            .into_any_element()
    }
}

/// "10M", "4.1M", "56K" like Compass's badges.
fn short_count(count: u64) -> String {
    match count {
        0..1_000 => count.to_string(),
        1_000..1_000_000 => format!("{:.1}K", count as f64 / 1_000.).replace(".0K", "K"),
        _ => format!("{:.1}M", count as f64 / 1_000_000.).replace(".0M", "M"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vilo_database_core::bson::doc;

    #[test]
    fn test_paths_columns_and_cells() {
        let document = doc! { "a.b": 1, "a": { "b": 2 }, "list": [ { "x": "y" } ] };
        assert_eq!(
            value_at(&document, &["a.b".to_string()]),
            Some(&Bson::Int32(1))
        );
        assert_eq!(
            value_at(&document, &["a".to_string(), "b".to_string()]),
            Some(&Bson::Int32(2))
        );
        assert_eq!(
            value_at(
                &document,
                &["list".to_string(), "0".to_string(), "x".to_string()]
            ),
            Some(&Bson::String("y".into()))
        );
        assert_eq!(
            value_at(&document, &["list".to_string(), "9".to_string()]),
            None
        );
        assert_eq!(
            table_columns(&[doc! { "b": 1, "a": 2 }, doc! { "c": 3, "a": 4 }]),
            vec!["b", "a", "c"]
        );
        assert_eq!(cell_text(None).0, "—");
        assert_eq!(cell_text(Some(&Bson::Null)).0, "null");
    }

    #[test]
    fn test_csv_indexes_and_counts() {
        assert_eq!(
            csv_field(Some(&Bson::String("a,b \"c\"".into()))),
            "\"a,b \"\"c\"\"\""
        );
        assert_eq!(csv_field(None), "");
        assert_eq!(csv_field(Some(&Bson::Int32(3))), "3");
        assert_eq!(
            index_type(&doc! { "status": 1, "createTime": -1 }),
            "REGULAR"
        );
        assert_eq!(index_type(&doc! { "name": "text" }), "TEXT");
        assert_eq!(index_type(&doc! { "$**": 1 }), "WILDCARD");
        assert_eq!(
            index_properties(&doc! { "unique": true, "expireAfterSeconds": 60 }),
            vec!["UNIQUE", "TTL 60s"]
        );
        assert_eq!(short_count(10_000_000), "10M");
        assert_eq!(short_count(4_055_771), "4.1M");
        assert_eq!(short_count(56), "56");
    }
}
