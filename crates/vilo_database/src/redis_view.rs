//! Redis, laid out like RedisInsight: Browser (keys as a list or a tree, key details by type), Workbench and Analysis.

use std::{collections::BTreeMap, sync::Arc};

use collections::{HashMap, HashSet};
use gpui::{ClipboardItem, Task, UniformListScrollHandle, prelude::*, uniform_list};
use ui::{
    Button, ButtonStyle, ContextMenu, IconButton, PopoverMenu, TintColor, Tooltip, prelude::*,
};
use vilo_database_core::redis::{
    Bytes, KeyInfo, ScanNode, StreamEntry, format_reply, quote_bytes, split_command,
};

use crate::{Connection, DatabaseView, Fetch, fetched, render_lines, run};

const SCAN_COUNT: u32 = 500;
const PAGE: u32 = 200;
const ROW_HEIGHT: f32 = 24.;
const KEY_TYPES: [(&str, &str); 8] = [
    ("All key types", ""),
    ("String", "string"),
    ("Hash", "hash"),
    ("List", "list"),
    ("Set", "set"),
    ("Sorted Set", "zset"),
    ("Stream", "stream"),
    ("JSON", "ReJSON-RL"),
];

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum RedisTab {
    Browser,
    Workbench,
    Analysis,
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Text,
    Hex,
    Json,
}

#[derive(Clone, Copy, PartialEq)]
enum KeyView {
    List,
    Tree,
}

#[derive(Clone, Default)]
struct KeyMeta {
    kind: String,
    ttl_ms: i64,
    memory: Option<u64>,
}

/// One node's walk: `None` once its cursor came back to 0.
struct NodeScan {
    node: ScanNode,
    cursor: Option<String>,
    scanned: u64,
}

enum Value {
    String(Option<Bytes>),
    Hash {
        fields: Vec<(Bytes, Bytes)>,
        cursor: Option<String>,
    },
    List {
        items: Vec<Bytes>,
        next: Option<i64>,
    },
    Set {
        members: Vec<Bytes>,
        cursor: Option<String>,
    },
    SortedSet {
        members: Vec<(Bytes, String)>,
        next: Option<i64>,
    },
    Stream {
        entries: Vec<StreamEntry>,
        groups: Vec<Vec<(String, String)>>,
        more: bool,
    },
    Json(Option<String>),
    Other,
}

struct Detail {
    key: Bytes,
    info: KeyInfo,
    length: Option<u64>,
    memory: Option<u64>,
    value: Value,
}

pub(crate) struct RedisState {
    tab: RedisTab,
    key_view: KeyView,
    delimiter: String,
    kind_filter: &'static str,
    nodes: Vec<NodeScan>,
    keys: Vec<Bytes>,
    seen: HashSet<Bytes>,
    meta: HashMap<Bytes, KeyMeta>,
    show_memory: bool,
    expanded: HashSet<Bytes>,
    scanning: bool,
    scan_error: Option<SharedString>,
    selected: Option<Bytes>,
    detail: Option<Fetch<Detail>>,
    format: Format,
    workbench: Vec<(String, Result<Vec<String>, String>)>,
    info: Option<Fetch<String>>,
    keys_scroll: UniformListScrollHandle,
    value_scroll: UniformListScrollHandle,
    generation: u64,
    _tasks: Vec<Task<()>>,
}

impl RedisState {
    pub(crate) fn new() -> Self {
        Self {
            tab: RedisTab::Browser,
            key_view: KeyView::Tree,
            delimiter: ":".into(),
            kind_filter: "",
            nodes: Vec::new(),
            keys: Vec::new(),
            seen: HashSet::default(),
            meta: HashMap::default(),
            show_memory: false,
            expanded: HashSet::default(),
            scanning: false,
            scan_error: None,
            selected: None,
            detail: None,
            format: Format::Text,
            workbench: Vec::new(),
            info: None,
            keys_scroll: UniformListScrollHandle::new(),
            value_scroll: UniformListScrollHandle::new(),
            generation: 0,
            _tasks: Vec::new(),
        }
    }

    fn keep(&mut self, task: Task<()>) {
        self._tasks.push(task);
        if self._tasks.len() > 16 {
            self._tasks.drain(..self._tasks.len() - 8);
        }
    }
}

/// Printable ASCII as is, everything else as `\xNN`, so different bytes never look the same.
pub(crate) fn escape_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b' '..=b'~' if *byte != b'\\' => (*byte as char).to_string(),
            b'\\' => "\\\\".to_string(),
            _ => format!("\\x{byte:02x}"),
        })
        .collect()
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn describe_ttl(ttl_ms: i64) -> String {
    match ttl_ms {
        -1 => "No limit".into(),
        -2 => "Gone".into(),
        ms if ms < 1000 => format!("{ms} ms"),
        ms if ms < 60_000 => format!("{} s", ms / 1000),
        ms if ms < 3_600_000 => format!("{} min", ms / 60_000),
        ms if ms < 86_400_000 => format!("{} h", ms / 3_600_000),
        ms => format!("{} d", ms / 86_400_000),
    }
}

fn describe_size(bytes: u64) -> String {
    match bytes {
        0..1024 => format!("{bytes} B"),
        1024..1_048_576 => format!("{:.1} KB", bytes as f64 / 1024.),
        _ => format!("{:.1} MB", bytes as f64 / 1_048_576.),
    }
}

/// RedisInsight's type badges: a short name and a colour per type.
fn type_badge(kind: &str) -> (&'static str, Color) {
    match kind {
        "string" => ("STRING", Color::Accent),
        "hash" => ("HASH", Color::Info),
        "list" => ("LIST", Color::Success),
        "set" => ("SET", Color::Warning),
        "zset" => ("ZSET", Color::Error),
        "stream" => ("STREAM", Color::Modified),
        "ReJSON-RL" => ("JSON", Color::Created),
        "none" => ("GONE", Color::Disabled),
        _ => ("…", Color::Muted),
    }
}

/// A value shown in the chosen format; JSON pretty-prints only when the bytes are valid JSON.
fn formatted(bytes: &[u8], format: Format) -> String {
    match format {
        Format::Hex => hex_bytes(bytes),
        Format::Json => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| {
                String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| escape_bytes(bytes))
            }),
        Format::Text => String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| escape_bytes(bytes)),
    }
}

/// A line of the key list: a folder of keys sharing a prefix, or a key.
#[derive(Clone)]
enum KeyRow {
    Folder {
        prefix: Bytes,
        label: String,
        count: usize,
        depth: usize,
        open: bool,
    },
    Key {
        key: Bytes,
        label: String,
        depth: usize,
    },
}

#[derive(Default)]
struct Folder {
    folders: BTreeMap<Bytes, Folder>,
    keys: Vec<Bytes>,
    count: usize,
}

/// Groups keys into folders by the delimiter, as RedisInsight's tree does; only opened folders list their content.
fn tree_rows(keys: &[Bytes], delimiter: &[u8], expanded: &HashSet<Bytes>) -> Vec<KeyRow> {
    let mut root = Folder::default();
    for key in keys {
        let mut folder = &mut root;
        let mut start = 0;
        folder.count += 1;
        while !delimiter.is_empty()
            && let Some(position) = key[start..]
                .windows(delimiter.len())
                .position(|window| window == delimiter)
        {
            let end = start + position + delimiter.len();
            folder = folder.folders.entry(key[..end].to_vec()).or_default();
            folder.count += 1;
            start = end;
        }
        folder.keys.push(key.clone());
    }
    let mut rows = Vec::new();
    fn walk(
        folder: &Folder,
        depth: usize,
        parent: usize,
        delimiter: &[u8],
        expanded: &HashSet<Bytes>,
        rows: &mut Vec<KeyRow>,
    ) {
        for (prefix, child) in &folder.folders {
            let open = expanded.contains(prefix);
            let label = escape_bytes(&prefix[parent..prefix.len() - delimiter.len()]);
            rows.push(KeyRow::Folder {
                prefix: prefix.clone(),
                label,
                count: child.count,
                depth,
                open,
            });
            if open {
                walk(child, depth + 1, prefix.len(), delimiter, expanded, rows);
            }
        }
        let mut keys = folder.keys.clone();
        keys.sort();
        for key in keys {
            rows.push(KeyRow::Key {
                label: escape_bytes(&key[parent..]),
                key,
                depth,
            });
        }
    }
    walk(&root, 0, 0, delimiter, expanded, &mut rows);
    rows
}

fn info_sections(info: &str) -> Vec<(String, Vec<(String, String)>)> {
    let mut sections: Vec<(String, Vec<(String, String)>)> = Vec::new();
    for line in info.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Some(title) = line.strip_prefix("# ") {
            sections.push((title.to_string(), Vec::new()));
        } else if let Some((key, value)) = line.split_once(':')
            && let Some((_, entries)) = sections.last_mut()
        {
            entries.push((key.to_string(), value.to_string()));
        }
    }
    sections
}

impl DatabaseView {
    fn redis_state(&mut self) -> Option<&mut RedisState> {
        self.redis_state.as_mut()
    }

    pub(crate) fn scan(&mut self, restart: bool, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let pattern = self.pattern.read(cx).text(cx);
        let pattern = if pattern.trim().is_empty() {
            "*".to_string()
        } else if pattern.contains(['*', '?', '[']) {
            pattern
        } else {
            format!("*{pattern}*")
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        if restart {
            state.generation += 1;
            state.keys.clear();
            state.seen.clear();
            state.meta.clear();
            state.nodes = session
                .scan_nodes()
                .into_iter()
                .map(|node| NodeScan {
                    node,
                    cursor: Some("0".into()),
                    scanned: 0,
                })
                .collect();
        }
        // Walk nodes one at a time, the first that still has a cursor.
        let Some(index) = state.nodes.iter().position(|node| node.cursor.is_some()) else {
            return;
        };
        let node = state.nodes[index].node.clone();
        let cursor = state.nodes[index].cursor.clone().unwrap_or_default();
        let kind = (!state.kind_filter.is_empty()).then(|| state.kind_filter.to_string());
        let show_memory = state.show_memory;
        state.scanning = true;
        state.scan_error = None;
        let generation = state.generation;
        let task = run(cx, async move {
            let page = session
                .scan(&node, &cursor, &pattern, SCAN_COUNT, kind.as_deref())
                .await?;
            let mut meta = Vec::new();
            for key in &page.keys {
                let info = session.key_info(key).await?;
                let memory = if show_memory {
                    session.memory_usage(key).await.ok().flatten()
                } else {
                    None
                };
                meta.push((
                    key.clone(),
                    KeyMeta {
                        kind: info.kind,
                        ttl_ms: info.ttl_ms,
                        memory,
                    },
                ));
            }
            Ok((page, meta))
        });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                let Some(state) = this.redis_state() else {
                    return;
                };
                if state.generation != generation {
                    return;
                }
                state.scanning = false;
                match result {
                    Ok((page, meta)) => {
                        if let Some(node) = state.nodes.get_mut(index) {
                            node.scanned += page.keys.len() as u64;
                            node.cursor = (page.cursor != "0").then_some(page.cursor);
                        }
                        // SCAN may return a key again on a later page.
                        for key in page.keys {
                            if state.seen.insert(key.clone()) {
                                state.keys.push(key);
                            }
                        }
                        state.meta.extend(meta);
                    }
                    Err(error) => state.scan_error = Some(format!("{error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_key(&mut self, key: Bytes, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.selected = Some(key.clone());
        state.detail = Some(Fetch::Loading);
        let task = run(cx, {
            let key = key.clone();
            async move {
                let info = session.key_info(&key).await?;
                let length = session.length(&key, &info.kind).await.ok().flatten();
                let memory = session.memory_usage(&key).await.ok().flatten();
                let value = match info.kind.as_str() {
                    "string" => Value::String(session.get_string(&key).await?),
                    "hash" => {
                        let (fields, cursor) = session.hash_page(&key, "0", PAGE).await?;
                        Value::Hash {
                            fields,
                            cursor: (cursor != "0").then_some(cursor),
                        }
                    }
                    "list" => {
                        let items = session.list_range(&key, 0, i64::from(PAGE) - 1).await?;
                        let next = (items.len() == PAGE as usize).then_some(i64::from(PAGE));
                        Value::List { items, next }
                    }
                    "set" => {
                        let (members, cursor) = session.set_page(&key, "0", PAGE).await?;
                        Value::Set {
                            members,
                            cursor: (cursor != "0").then_some(cursor),
                        }
                    }
                    "zset" => {
                        let members = session.zset_range(&key, 0, i64::from(PAGE) - 1).await?;
                        let next = (members.len() == PAGE as usize).then_some(i64::from(PAGE));
                        Value::SortedSet { members, next }
                    }
                    "stream" => {
                        let entries = session.stream_range(&key, None, PAGE).await?;
                        let groups = session.stream_groups(&key).await.unwrap_or_default();
                        let more = entries.len() == PAGE as usize;
                        Value::Stream {
                            entries,
                            groups,
                            more,
                        }
                    }
                    "ReJSON-RL" => Value::Json(session.json_get(&key).await?),
                    _ => Value::Other,
                };
                Ok(Detail {
                    key,
                    info,
                    length,
                    memory,
                    value,
                })
            }
        });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(state) = this.redis_state()
                    && state.selected.as_ref() == Some(&key)
                {
                    state.detail = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    /// The next page of the open key's value: another HSCAN / SSCAN step, LRANGE / ZRANGE window or XRANGE batch.
    fn load_more_value(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        let Some(Fetch::Loaded(detail)) = &state.detail else {
            return;
        };
        let key = detail.key.clone();
        enum Next {
            Hash(String),
            List(i64),
            Set(String),
            SortedSet(i64),
            Stream(String),
        }
        let next = match &detail.value {
            Value::Hash {
                cursor: Some(cursor),
                ..
            } => Next::Hash(cursor.clone()),
            Value::List {
                next: Some(start), ..
            } => Next::List(*start),
            Value::Set {
                cursor: Some(cursor),
                ..
            } => Next::Set(cursor.clone()),
            Value::SortedSet {
                next: Some(start), ..
            } => Next::SortedSet(*start),
            Value::Stream {
                entries,
                more: true,
                ..
            } => match entries.last() {
                Some(last) => Next::Stream(last.id.clone()),
                None => return,
            },
            _ => return,
        };
        enum More {
            Hash(Vec<(Bytes, Bytes)>, String),
            List(Vec<Bytes>, i64),
            Set(Vec<Bytes>, String),
            SortedSet(Vec<(Bytes, String)>, i64),
            Stream(Vec<StreamEntry>),
        }
        let task = run(cx, {
            let key = key.clone();
            async move {
                Ok(match next {
                    Next::Hash(cursor) => {
                        let (fields, cursor) = session.hash_page(&key, &cursor, PAGE).await?;
                        More::Hash(fields, cursor)
                    }
                    Next::List(start) => More::List(
                        session
                            .list_range(&key, start, start + i64::from(PAGE) - 1)
                            .await?,
                        start,
                    ),
                    Next::Set(cursor) => {
                        let (members, cursor) = session.set_page(&key, &cursor, PAGE).await?;
                        More::Set(members, cursor)
                    }
                    Next::SortedSet(start) => More::SortedSet(
                        session
                            .zset_range(&key, start, start + i64::from(PAGE) - 1)
                            .await?,
                        start,
                    ),
                    Next::Stream(after) => {
                        More::Stream(session.stream_range(&key, Some(&after), PAGE).await?)
                    }
                })
            }
        });
        state.keep(cx.spawn(async move |this, cx| {
            let Ok(more) = task.await else {
                return;
            };
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                let Some(state) = this.redis_state() else {
                    return;
                };
                let Some(Fetch::Loaded(detail)) = &mut state.detail else {
                    return;
                };
                if detail.key != key {
                    return;
                }
                match (&mut detail.value, more) {
                    (Value::Hash { fields, cursor }, More::Hash(page, next)) => {
                        for field in page {
                            if !fields.contains(&field) {
                                fields.push(field);
                            }
                        }
                        *cursor = (next != "0").then_some(next);
                    }
                    (Value::List { items, next }, More::List(page, start)) => {
                        *next = (page.len() == PAGE as usize).then_some(start + i64::from(PAGE));
                        items.extend(page);
                    }
                    (Value::Set { members, cursor }, More::Set(page, next)) => {
                        for member in page {
                            if !members.contains(&member) {
                                members.push(member);
                            }
                        }
                        *cursor = (next != "0").then_some(next);
                    }
                    (Value::SortedSet { members, next }, More::SortedSet(page, start)) => {
                        *next = (page.len() == PAGE as usize).then_some(start + i64::from(PAGE));
                        members.extend(page);
                    }
                    (Value::Stream { entries, more, .. }, More::Stream(page)) => {
                        *more = page.len() == PAGE as usize;
                        entries.extend(page);
                    }
                    _ => {}
                }
                cx.notify();
            })
            .ok();
        }));
    }

    fn run_workbench(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let line = self.workbench_input.read(cx).text(cx).trim().to_string();
        if line.is_empty() {
            return;
        }
        self.workbench_input
            .update(cx, |editor, cx| editor.set_text("", window, cx));
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        let index = state.workbench.len();
        state.workbench.push((line.clone(), Ok(vec!["…".into()])));
        let task = run(cx, async move {
            let words = split_command(&line)?;
            Ok(format_reply(&session.run_read_command(words).await?))
        });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(state) = this.redis_state()
                    && let Some(entry) = state.workbench.get_mut(index)
                {
                    entry.1 = result.map_err(|error| format!("{error:#}"));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn load_info(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.info = Some(Fetch::Loading);
        let task = run(cx, async move { session.info().await });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(state) = this.redis_state()
                {
                    state.info = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn render_key_list(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let rows: Arc<Vec<KeyRow>> = Arc::new(match state.key_view {
            KeyView::Tree => tree_rows(&state.keys, state.delimiter.as_bytes(), &state.expanded),
            KeyView::List => {
                let mut keys = state.keys.clone();
                keys.sort();
                keys.into_iter()
                    .map(|key| KeyRow::Key {
                        label: escape_bytes(&key),
                        key,
                        depth: 0,
                    })
                    .collect()
            }
        });
        let meta = Arc::new(state.meta.clone());
        let selected = state.selected.clone();
        let view = cx.entity().downgrade();
        let show_memory = state.show_memory;
        let colors = cx.theme().colors().clone();
        uniform_list("database-redis-keys", rows.len(), move |range, _, cx| {
            range
                .filter_map(|index| rows.get(index).cloned().map(|row| (index, row)))
                .map(|(index, row)| {
                    let view = view.clone();
                    let base = h_flex()
                        .id(("database-redis-row", index))
                        .w_full()
                        .h(px(ROW_HEIGHT))
                        .px_2()
                        .gap_2()
                        .cursor_pointer()
                        .hover(|style| style.bg(colors.element_hover));
                    match row {
                        KeyRow::Folder {
                            prefix,
                            label,
                            count,
                            depth,
                            open,
                        } => base
                            .on_click(move |_, _, cx| {
                                let prefix = prefix.clone();
                                view.update(cx, |this, cx| {
                                    if let Some(state) = this.redis_state()
                                        && !state.expanded.remove(&prefix)
                                    {
                                        state.expanded.insert(prefix);
                                    }
                                    cx.notify();
                                })
                                .ok();
                            })
                            .child(div().w(px(depth as f32 * 16.)).flex_none())
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
                                Icon::new(IconName::Folder)
                                    .size(IconSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(
                                Label::new(label)
                                    .size(LabelSize::Small)
                                    .buffer_font(cx)
                                    .single_line()
                                    .truncate(),
                            )
                            .child(div().flex_1())
                            .child(
                                Label::new(format!("{count}"))
                                    .size(LabelSize::XSmall)
                                    .color(Color::Muted),
                            )
                            .into_any_element(),
                        KeyRow::Key { key, label, depth } => {
                            let info = meta.get(&key).cloned().unwrap_or_default();
                            let (badge, color) = type_badge(&info.kind);
                            let is_selected = selected.as_ref() == Some(&key);
                            base.when(is_selected, |this| this.bg(colors.element_selected))
                                .on_click(move |_, _, cx| {
                                    let key = key.clone();
                                    view.update(cx, |this, cx| this.open_key(key, cx)).ok();
                                })
                                .child(div().w(px(depth as f32 * 16. + 14.)).flex_none())
                                .child(
                                    div().w(px(56.)).flex_none().child(
                                        Label::new(badge)
                                            .size(LabelSize::XSmall)
                                            .weight(gpui::FontWeight::BOLD)
                                            .color(color),
                                    ),
                                )
                                .child(
                                    div().flex_1().min_w_0().child(
                                        Label::new(label)
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .single_line()
                                            .truncate(),
                                    ),
                                )
                                .child(
                                    div().w(px(64.)).flex_none().child(
                                        Label::new(describe_ttl(info.ttl_ms))
                                            .size(LabelSize::XSmall)
                                            .color(Color::Muted),
                                    ),
                                )
                                .when(show_memory, |this| {
                                    this.child(
                                        div().w(px(64.)).flex_none().child(
                                            Label::new(
                                                info.memory.map(describe_size).unwrap_or_default(),
                                            )
                                            .size(LabelSize::XSmall)
                                            .color(Color::Muted),
                                        ),
                                    )
                                })
                                .into_any_element()
                        }
                    }
                })
                .collect::<Vec<_>>()
        })
        .track_scroll(&state.keys_scroll)
        .flex_1()
        .into_any_element()
    }

    fn value_lines(&self, detail: &Detail, format: Format) -> Vec<SharedString> {
        let show = |bytes: &[u8]| formatted(bytes, format);
        let mut lines = Vec::new();
        let mut push_block = |prefix: String, text: String| {
            let mut iter = text.lines();
            lines.push(SharedString::from(format!(
                "{prefix}{}",
                iter.next().unwrap_or_default()
            )));
            let pad = " ".repeat(prefix.chars().count());
            for line in iter {
                lines.push(SharedString::from(format!("{pad}{line}")));
            }
        };
        match &detail.value {
            Value::String(None) => push_block(String::new(), "(the key no longer exists)".into()),
            Value::String(Some(bytes)) => push_block(String::new(), show(bytes)),
            Value::Hash { fields, .. } => {
                for (field, value) in fields {
                    push_block(format!("{} → ", escape_bytes(field)), show(value));
                }
            }
            Value::List { items, .. } => {
                for (index, item) in items.iter().enumerate() {
                    push_block(format!("{index:>5}  "), show(item));
                }
            }
            Value::Set { members, .. } => {
                for member in members {
                    push_block(String::new(), show(member));
                }
            }
            Value::SortedSet { members, .. } => {
                for (member, score) in members {
                    push_block(format!("{score:>12}  "), show(member));
                }
            }
            Value::Stream { entries, .. } => {
                for entry in entries {
                    push_block(String::new(), entry.id.clone());
                    for (field, value) in &entry.fields {
                        push_block(format!("    {} → ", escape_bytes(field)), show(value));
                    }
                }
            }
            Value::Json(Some(json)) => {
                push_block(String::new(), formatted(json.as_bytes(), Format::Json))
            }
            Value::Json(None) => push_block(String::new(), "(no value)".into()),
            Value::Other => push_block(
                String::new(),
                format!(
                    "Vilo has no viewer for {} keys yet; use the Workbench.",
                    detail.info.kind
                ),
            ),
        }
        lines
    }

    fn render_detail(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let detail = match &state.detail {
            None => {
                return v_flex()
                    .size_full()
                    .items_center()
                    .justify_center()
                    .gap_1()
                    .child(Label::new("Select the key from the list on the left to see the details of the key.").color(Color::Muted))
                    .into_any_element();
            }
            Some(Fetch::Loading) => {
                return Label::new("Loading…")
                    .color(Color::Muted)
                    .into_any_element();
            }
            Some(Fetch::Failed(message)) => {
                return Label::new(message.clone())
                    .color(Color::Error)
                    .into_any_element();
            }
            Some(Fetch::Loaded(detail)) => detail,
        };
        let (badge, color) = type_badge(&detail.info.kind);
        let more = match &detail.value {
            Value::Hash { cursor, .. } | Value::Set { cursor, .. } => cursor.is_some(),
            Value::List { next, .. } | Value::SortedSet { next, .. } => next.is_some(),
            Value::Stream { more, .. } => *more,
            _ => false,
        };
        let loaded = match &detail.value {
            Value::Hash { fields, .. } => Some(fields.len()),
            Value::List { items, .. } => Some(items.len()),
            Value::Set { members, .. } => Some(members.len()),
            Value::SortedSet { members, .. } => Some(members.len()),
            Value::Stream { entries, .. } => Some(entries.len()),
            _ => None,
        };
        let format_button =
            |id: &'static str, label: &'static str, format: Format, cx: &mut Context<Self>| {
                Button::new(id, label)
                    .label_size(LabelSize::Small)
                    .style(if state.format == format {
                        ButtonStyle::Filled
                    } else {
                        ButtonStyle::Subtle
                    })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(state) = this.redis_state() {
                            state.format = format;
                        }
                        cx.notify();
                    }))
            };
        let key_text = escape_bytes(&detail.key);
        let copy_key = quote_bytes(&detail.key);
        let groups = match &detail.value {
            Value::Stream { groups, .. } if !groups.is_empty() => Some(groups.clone()),
            _ => None,
        };
        v_flex()
            .size_full()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .px_1p5()
                            .rounded_sm()
                            .border_1()
                            .border_color(colors.border)
                            .child(
                                Label::new(badge)
                                    .size(LabelSize::XSmall)
                                    .weight(gpui::FontWeight::BOLD)
                                    .color(color),
                            ),
                    )
                    .child(
                        Label::new(key_text)
                            .buffer_font(cx)
                            .weight(gpui::FontWeight::SEMIBOLD)
                            .single_line()
                            .truncate(),
                    )
                    .child(
                        IconButton::new("database-redis-copy-key", IconName::Copy)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Copy key name"))
                            .on_click(move |_, _, cx| {
                                cx.write_to_clipboard(ClipboardItem::new_string(copy_key.clone()))
                            }),
                    )
                    .child(div().flex_1())
                    .child(
                        IconButton::new("database-redis-reload-key", IconName::RotateCw)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Refresh"))
                            .on_click({
                                let key = detail.key.clone();
                                cx.listener(move |this, _, _, cx| this.open_key(key.clone(), cx))
                            }),
                    ),
            )
            .child(
                h_flex()
                    .gap_4()
                    .child(
                        Label::new(format!("TTL: {}", describe_ttl(detail.info.ttl_ms)))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .when_some(detail.length, |this, length| {
                        this.child(
                            Label::new(format!("Length: {length}"))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .when_some(detail.memory, |this, memory| {
                        this.child(
                            Label::new(format!("Key Size: {}", describe_size(memory)))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .when_some(loaded, |this, loaded| {
                        this.child(
                            Label::new(format!("Loaded: {loaded}"))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .child(div().flex_1())
                    .child(format_button(
                        "database-redis-format-text",
                        "Unicode",
                        Format::Text,
                        cx,
                    ))
                    .child(format_button(
                        "database-redis-format-json",
                        "JSON",
                        Format::Json,
                        cx,
                    ))
                    .child(format_button(
                        "database-redis-format-hex",
                        "HEX",
                        Format::Hex,
                        cx,
                    )),
            )
            .when_some(groups, |this, groups| {
                this.child(
                    Label::new("Consumer groups")
                        .size(LabelSize::Small)
                        .weight(gpui::FontWeight::SEMIBOLD),
                )
                .children(groups.into_iter().map(|group| {
                    Label::new(
                        group
                            .into_iter()
                            .map(|(name, value)| format!("{name}: {value}"))
                            .collect::<Vec<_>>()
                            .join(" · "),
                    )
                    .size(LabelSize::Small)
                    .buffer_font(cx)
                }))
            })
            .child(render_lines(
                "database-redis-value",
                self.value_lines(detail, state.format),
                &state.value_scroll,
                cx,
            ))
            .when(more, |this| {
                this.child(
                    h_flex().child(
                        Button::new("database-redis-more", "Load more")
                            .style(ButtonStyle::Outlined)
                            .on_click(cx.listener(|this, _, _, cx| this.load_more_value(cx))),
                    ),
                )
            })
            .into_any_element()
    }

    fn render_browser(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let kind_label = KEY_TYPES
            .iter()
            .find(|(_, kind)| *kind == state.kind_filter)
            .map_or("All key types", |(label, _)| label);
        let types = PopoverMenu::new("database-redis-types")
            .trigger(
                Button::new("database-redis-types-button", kind_label)
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Outlined)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for (label, kind) in KEY_TYPES {
                        let view = view.clone();
                        menu = menu.entry(label, None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.kind_filter = kind;
                                }
                                this.scan(true, cx);
                            })
                            .ok();
                        });
                    }
                    menu
                }))
            });
        let delimiter_view = cx.entity().downgrade();
        let delimiter = PopoverMenu::new("database-redis-delimiter")
            .trigger(
                IconButton::new("database-redis-delimiter-button", IconName::Settings)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Tree view delimiter and columns")),
            )
            .menu(move |window, cx| {
                let view = delimiter_view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    menu = menu.header("Delimiter");
                    for delimiter in [":", "/", ".", "-", "_"] {
                        let view = view.clone();
                        menu = menu.entry(delimiter, None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.delimiter = delimiter.to_string();
                                    state.expanded.clear();
                                }
                                cx.notify();
                            })
                            .ok();
                        });
                    }
                    let view = view.clone();
                    menu.separator()
                        .entry("Show key size (MEMORY USAGE)", None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.show_memory = !state.show_memory;
                                }
                                this.scan(true, cx);
                            })
                            .ok();
                        })
                }))
            });
        let view_icon = |id: &'static str,
                         icon: IconName,
                         tip: &'static str,
                         key_view: KeyView,
                         cx: &mut Context<Self>| {
            IconButton::new(id, icon)
                .icon_size(IconSize::Small)
                .style(if state.key_view == key_view {
                    ButtonStyle::Filled
                } else {
                    ButtonStyle::Subtle
                })
                .tooltip(Tooltip::text(tip))
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(state) = this.redis_state() {
                        state.key_view = key_view;
                    }
                    cx.notify();
                }))
        };
        let scanned: u64 = state.nodes.iter().map(|node| node.scanned).sum();
        let done = state.nodes.iter().all(|node| node.cursor.is_none());
        let coverage = if state.nodes.len() > 1 {
            format!(
                " · {} of {} nodes fully scanned",
                state
                    .nodes
                    .iter()
                    .filter(|node| node.cursor.is_none())
                    .count(),
                state.nodes.len()
            )
        } else {
            String::new()
        };
        h_flex()
            .size_full()
            .gap_0()
            .child(
                v_flex()
                    .w(px(520.))
                    .flex_none()
                    .h_full()
                    .border_r_1()
                    .border_color(colors.border)
                    .child(
                        h_flex()
                            .p_2()
                            .gap_2()
                            .child(types)
                            .child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| {
                                        this.scan(true, cx)
                                    }))
                                    .child(crate::boxed(self.pattern.clone(), cx)),
                            )
                            .child(
                                IconButton::new("database-redis-scan", IconName::MagnifyingGlass)
                                    .icon_size(IconSize::Small)
                                    .tooltip(Tooltip::text("Search"))
                                    .on_click(cx.listener(|this, _, _, cx| this.scan(true, cx))),
                            )
                            .child(delimiter)
                            .child(view_icon(
                                "database-redis-list",
                                IconName::Menu,
                                "List view",
                                KeyView::List,
                                cx,
                            ))
                            .child(view_icon(
                                "database-redis-tree",
                                IconName::ListTree,
                                "Tree view",
                                KeyView::Tree,
                                cx,
                            )),
                    )
                    .child(
                        h_flex()
                            .px_2()
                            .pb_1()
                            .gap_2()
                            .child(
                                Label::new(format!(
                                    "Results: {} · Scanned {scanned}{coverage}{}",
                                    state.keys.len(),
                                    if done { " · all scanned" } else { "" }
                                ))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                            )
                            .child(div().flex_1())
                            .when(!done, |this| {
                                this.child(
                                    Button::new(
                                        "database-redis-scan-more",
                                        if state.scanning {
                                            "Scanning…"
                                        } else {
                                            "Scan more"
                                        },
                                    )
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Outlined)
                                    .disabled(state.scanning)
                                    .on_click(cx.listener(|this, _, _, cx| this.scan(false, cx))),
                                )
                            }),
                    )
                    .when_some(state.scan_error.clone(), |this, message| {
                        this.child(
                            div().px_2().child(
                                Label::new(message)
                                    .size(LabelSize::Small)
                                    .color(Color::Error),
                            ),
                        )
                    })
                    .child(self.render_key_list(state, cx)),
            )
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .p_3()
                    .child(self.render_detail(state, cx)),
            )
            .into_any_element()
    }

    fn render_workbench(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let mut lines = Vec::new();
        for (command, result) in &state.workbench {
            lines.push(SharedString::from(format!("> {command}")));
            match result {
                Ok(reply) => {
                    lines.extend(reply.iter().map(|line| SharedString::from(line.clone())))
                }
                Err(error) => lines.push(SharedString::from(format!("(error) {error}"))),
            }
            lines.push(SharedString::default());
        }
        v_flex()
            .size_full()
            .p_3()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .on_action(cx.listener(|this, _: &menu::Confirm, window, cx| this.run_workbench(window, cx)))
                    .child(div().flex_1().child(crate::boxed(self.workbench_input.clone(), cx)))
                    .child(Button::new("database-redis-run", "Run").style(ButtonStyle::Tinted(TintColor::Success)).on_click(cx.listener(|this, _, window, cx| this.run_workbench(window, cx)))),
            )
            .child(Label::new("Read commands only in this release. Writes, KEYS, FLUSH*, CONFIG, MONITOR and scripts are refused before sending.").size(LabelSize::Small).color(Color::Muted))
            .child(if lines.is_empty() {
                Label::new("Run a command such as HGETALL user:1 or XINFO GROUPS orders.").color(Color::Muted).into_any_element()
            } else {
                render_lines("database-redis-workbench", lines, &state.value_scroll, cx)
            })
            .into_any_element()
    }

    fn render_analysis(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let body = match &state.info {
            None | Some(Fetch::Loading) => Label::new("Loading…")
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loaded(info)) => {
                let mut sections = v_flex().gap_3();
                for (title, entries) in info_sections(info) {
                    let mut table = v_flex()
                        .rounded_md()
                        .border_1()
                        .border_color(colors.border)
                        .child(
                            div().px_3().py_1().bg(colors.element_background).child(
                                Label::new(title)
                                    .size(LabelSize::Small)
                                    .weight(gpui::FontWeight::SEMIBOLD),
                            ),
                        );
                    for (key, value) in entries {
                        table = table.child(
                            h_flex()
                                .px_3()
                                .py_0p5()
                                .gap_3()
                                .border_t_1()
                                .border_color(colors.border_variant)
                                .child(div().w(px(260.)).flex_none().child(
                                    Label::new(key).size(LabelSize::Small).color(Color::Muted),
                                ))
                                .child(div().flex_1().min_w_0().child(
                                    Label::new(value).size(LabelSize::Small).buffer_font(cx),
                                )),
                        );
                    }
                    sections = sections.child(table);
                }
                sections.into_any_element()
            }
        };
        v_flex()
            .id("database-redis-analysis")
            .size_full()
            .p_3()
            .gap_2()
            .overflow_y_scroll()
            .child(
                h_flex().child(div().flex_1()).child(
                    Button::new("database-redis-info", "Refresh")
                        .style(ButtonStyle::Outlined)
                        .on_click(cx.listener(|this, _, _, cx| this.load_info(cx))),
                ),
            )
            .child(body)
            .into_any_element()
    }

    pub(crate) fn render_redis(
        &self,
        connection: &Connection,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let Some(state) = &self.redis_state else {
            return div().into_any_element();
        };
        let tab =
            |id: &'static str, label: &'static str, target: RedisTab, cx: &mut Context<Self>| {
                let selected = state.tab == target;
                h_flex()
                    .id(id)
                    .h(px(38.))
                    .px_4()
                    .cursor_pointer()
                    .border_b_2()
                    .border_color(if selected {
                        colors.text_accent
                    } else {
                        gpui::transparent_black()
                    })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        let needs_info = target == RedisTab::Analysis
                            && this
                                .redis_state
                                .as_ref()
                                .is_some_and(|state| state.info.is_none());
                        if let Some(state) = this.redis_state() {
                            state.tab = target;
                        }
                        if needs_info {
                            this.load_info(cx);
                        }
                        cx.notify();
                    }))
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
            };
        let body = match state.tab {
            RedisTab::Browser => self.render_browser(state, cx),
            RedisTab::Workbench => self.render_workbench(state, cx),
            RedisTab::Analysis => self.render_analysis(state, cx),
        };
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .px_3()
                    .border_b_1()
                    .border_color(colors.border)
                    .child(tab(
                        "database-redis-browser",
                        "Browser",
                        RedisTab::Browser,
                        cx,
                    ))
                    .child(tab(
                        "database-redis-workbench-tab",
                        "Workbench",
                        RedisTab::Workbench,
                        cx,
                    ))
                    .child(tab(
                        "database-redis-analysis-tab",
                        "Analysis",
                        RedisTab::Analysis,
                        cx,
                    ))
                    .child(div().flex_1())
                    .child(Label::new(connection.name.clone()).weight(gpui::FontWeight::SEMIBOLD))
                    .child(div().w(px(8.)))
                    .child(
                        Label::new(connection.environment.clone())
                            .size(LabelSize::XSmall)
                            .color(if connection.production() {
                                Color::Warning
                            } else {
                                Color::Muted
                            }),
                    ),
            )
            .child(div().flex_1().min_h_0().child(body))
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_groups_by_delimiter() {
        let keys = vec![
            b"user:1".to_vec(),
            b"user:2".to_vec(),
            b"user:vip:9".to_vec(),
            b"plain".to_vec(),
        ];
        let collapsed = tree_rows(&keys, b":", &HashSet::default());
        let labels = collapsed
            .iter()
            .map(|row| match row {
                KeyRow::Folder { label, count, .. } => format!("{label}/{count}"),
                KeyRow::Key { label, .. } => label.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["user/3", "plain"]);
        let open = tree_rows(&keys, b":", &[b"user:".to_vec()].into_iter().collect());
        let labels = open
            .iter()
            .map(|row| match row {
                KeyRow::Folder { label, .. } => format!("[{label}]"),
                KeyRow::Key { label, .. } => label.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["[user]", "[vip]", "1", "2", "plain"]);
    }

    #[test]
    fn test_formats_and_info() {
        assert_eq!(escape_bytes(b"a\\x00"), "a\\\\x00");
        assert_ne!(escape_bytes(b"a\\x00"), escape_bytes(b"a\x00"));
        assert_eq!(formatted(br#"{"a":1}"#, Format::Json), "{\n  \"a\": 1\n}");
        assert_eq!(formatted(b"\xff", Format::Text), "\\xff");
        assert_eq!(formatted(b"\x01\xab", Format::Hex), "01 ab");
        assert_eq!(describe_ttl(-1), "No limit");
        assert_eq!(describe_ttl(90_000), "1 min");
        let sections = info_sections(
            "# Server\r\nredis_version:7.4.0\r\n\r\n# Clients\r\nconnected_clients:3\r\n",
        );
        assert_eq!(
            sections[1],
            (
                "Clients".to_string(),
                vec![("connected_clients".to_string(), "3".to_string())]
            )
        );
    }
}
