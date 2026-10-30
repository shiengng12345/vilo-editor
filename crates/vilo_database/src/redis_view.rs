//! Redis, laid out like RedisInsight: a top bar with live server stats, Browse / Search / Workbench / Analyze /
//! Pub/Sub tabs, keys as a tree or a list with type badges, TTL and size, key details by type, and a CLI bar.
//! This release only reads; the write entry points are shown but switched off.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use collections::{HashMap, HashSet};
use gpui::{
    ClipboardItem, Entity, Focusable as _, Hsla, Task, UniformListScrollHandle, prelude::*, rgb,
    uniform_list,
};
use ui::{Button, ButtonStyle, ContextMenu, IconButton, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::redis::{
    Bytes, ClusterNode, KeyInfo, ScanNode, StreamEntry, format_reply, quote_bytes, split_command,
};

use crate::{Connection, DatabaseView, Fetch, fetched, render_lines, run};

/// Keys asked for per SCAN call, and keys walked per "Scan more", as RedisInsight does.
const SCAN_COUNT: u32 = 200;
const SCAN_ROUND: u64 = 10_000;
/// Keys whose type, TTL and size are fetched together.
const META_BATCH: usize = 200;
const PAGE: u32 = 200;
const ROW_HEIGHT: f32 = 28.;
const STATS_EVERY: Duration = Duration::from_secs(10);
const KEY_TYPES: [(&str, &str); 8] = [
    ("All Key Types", ""),
    ("Hash", "hash"),
    ("List", "list"),
    ("Set", "set"),
    ("Sorted Set", "zset"),
    ("String", "string"),
    ("JSON", "ReJSON-RL"),
    ("Stream", "stream"),
];

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum RedisTab {
    Browse,
    Workbench,
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Unicode,
    Ascii,
    Hex,
    Binary,
    Json,
}

impl Format {
    fn label(self) -> &'static str {
        match self {
            Format::Unicode => "Unicode",
            Format::Ascii => "ASCII",
            Format::Hex => "HEX",
            Format::Binary => "Binary",
            Format::Json => "JSON",
        }
    }
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
    size: Option<u64>,
}

/// One node's walk: `None` once its cursor came back to 0.
struct NodeScan {
    node: ScanNode,
    cursor: Option<String>,
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
    size: Option<u64>,
    value: Value,
}

/// What the right panel shows.
enum RightPanel {
    Empty,
    Key,
    Info,
    Topology,
}

enum Topology {
    Cluster(Vec<ClusterNode>),
    /// INFO replication of a single server.
    Single(Vec<(String, String)>),
}

#[derive(Clone, Default)]
struct Stats {
    cpu_percent: Option<f64>,
    ops_per_second: Option<u64>,
    memory: Option<String>,
    keys: Option<u64>,
    clients: Option<u64>,
    /// The last CPU sample (seconds used, when), to turn totals into a percentage.
    cpu_sample: Option<(f64, Instant)>,
}

/// The tree or list built for one set of keys, reused until the keys, view or opened folders change.
type RowsKey = (usize, usize, &'static str, u64, String, bool);

pub(crate) struct RedisState {
    tab: RedisTab,
    key_view: KeyView,
    delimiter: String,
    kind_filter: &'static str,
    nodes: Vec<NodeScan>,
    /// Keys walked so far, counted as RedisInsight does from each SCAN's COUNT: an estimate of the work done.
    scanned: u64,
    total: Option<u64>,
    keys: Vec<Bytes>,
    seen: HashSet<Bytes>,
    /// Shared so drawing the list does not copy it.
    meta: Arc<HashMap<Bytes, KeyMeta>>,
    /// The type filter the current list was scanned with; "" means every type, which lets the
    /// filter change on the client without scanning again.
    listed_kind: &'static str,
    /// The previous list stays on screen until a new scan returns its first keys.
    stale: bool,
    expanded: HashSet<Bytes>,
    expanded_version: u64,
    rows: RefCell<Option<(RowsKey, Arc<Vec<KeyRow>>)>>,
    scanning: bool,
    scan_error: Option<SharedString>,
    last_refresh: Option<Instant>,
    selected: Option<Bytes>,
    panel: RightPanel,
    detail: Option<Fetch<Detail>>,
    format: Format,
    cli_open: bool,
    workbench: Vec<(String, Result<Vec<String>, String>)>,
    info: Option<Fetch<String>>,
    topology: Option<Fetch<Topology>>,
    stats: Stats,
    keys_scroll: UniformListScrollHandle,
    value_scroll: UniformListScrollHandle,
    cli_scroll: UniformListScrollHandle,
    generation: u64,
    _stats: Option<Task<()>>,
    _tasks: Vec<Task<()>>,
    /// From CONFIG GET databases; `None` when the server does not allow CONFIG.
    database_count: Option<u32>,
    /// Databases with keys, from INFO keyspace.
    keyspace: Vec<(u32, u64)>,
    /// Why key counts per database are missing, when INFO keyspace failed.
    keyspace_error: Option<SharedString>,
    /// The database being connected to while a switch is in progress.
    switching: Option<u8>,
    pub(crate) switch_error: Option<SharedString>,
    /// A database number typed in when the menu cannot know how many there are.
    database_input: Option<Entity<editor::Editor>>,
}

impl RedisState {
    pub(crate) fn switching_database(&self) -> bool {
        self.switching.is_some()
    }

    pub(crate) fn new() -> Self {
        Self {
            tab: RedisTab::Browse,
            key_view: KeyView::Tree,
            delimiter: ":".into(),
            kind_filter: "",
            nodes: Vec::new(),
            scanned: 0,
            total: None,
            keys: Vec::new(),
            seen: HashSet::default(),
            meta: Arc::default(),
            listed_kind: "",
            stale: false,
            expanded: HashSet::default(),
            expanded_version: 0,
            rows: RefCell::new(None),
            scanning: false,
            scan_error: None,
            last_refresh: None,
            selected: None,
            panel: RightPanel::Empty,
            detail: None,
            format: Format::Unicode,
            cli_open: false,
            workbench: Vec::new(),
            info: None,
            topology: None,
            stats: Stats::default(),
            keys_scroll: UniformListScrollHandle::new(),
            value_scroll: UniformListScrollHandle::new(),
            cli_scroll: UniformListScrollHandle::new(),
            generation: 0,
            _stats: None,
            _tasks: Vec::new(),
            database_count: None,
            keyspace: Vec::new(),
            keyspace_error: None,
            switching: None,
            switch_error: None,
            database_input: None,
        }
    }

    fn keep(&mut self, task: Task<()>) {
        self._tasks.push(task);
        if self._tasks.len() > 24 {
            self._tasks.drain(..self._tasks.len() - 12);
        }
    }

    /// Keys the list shows: the scanned keys, narrowed by type here when the scan took every type.
    fn visible_keys(&self) -> Vec<Bytes> {
        if self.kind_filter.is_empty() || !self.listed_kind.is_empty() {
            return self.keys.clone();
        }
        self.keys
            .iter()
            .filter(|key| {
                self.meta
                    .get(*key)
                    .is_some_and(|meta| meta.kind == self.kind_filter)
            })
            .cloned()
            .collect()
    }

    /// Whether every key of every type is listed with its type, so a type filter needs no scan.
    fn can_filter_locally(&self) -> bool {
        self.listed_kind.is_empty()
            && !self.stale
            && self.nodes.iter().all(|node| node.cursor.is_none())
            && self.keys.iter().all(|key| self.meta.contains_key(key))
    }

    fn rows(&self) -> Arc<Vec<KeyRow>> {
        let key = (
            self.keys.len(),
            self.meta.len(),
            self.kind_filter,
            self.expanded_version,
            self.delimiter.clone(),
            self.key_view == KeyView::Tree,
        );
        if let Some((cached, rows)) = self.rows.borrow().as_ref()
            && *cached == key
        {
            return rows.clone();
        }
        let keys = self.visible_keys();
        let rows = Arc::new(match self.key_view {
            KeyView::Tree => tree_rows(&keys, self.delimiter.as_bytes(), &self.expanded),
            KeyView::List => {
                let mut keys = keys;
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
        *self.rows.borrow_mut() = Some((key, rows.clone()));
        rows
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

/// RedisInsight's TTL wording: "No limit", or the largest whole unit left.
pub(crate) fn describe_ttl(ttl_ms: i64) -> String {
    match ttl_ms {
        -1 => "No limit".into(),
        -2 => "—".into(),
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
        1024..1_048_576 => format!("{:.0} KB", bytes as f64 / 1024.),
        _ => format!("{:.1} MB", bytes as f64 / 1_048_576.),
    }
}

/// "36 300" → "36 K", as RedisInsight's header.
fn short_number(number: u64) -> String {
    match number {
        0..1_000 => number.to_string(),
        1_000..1_000_000 => format!("{} K", number / 1_000),
        _ => format!("{:.1} M", number as f64 / 1_000_000.),
    }
}

fn group_thousands(number: u64) -> String {
    let digits = number.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(digit);
    }
    out
}

/// Type badges: a tint of the type's colour with the name in that colour, like the index pills.
fn type_badge(kind: &str) -> (&'static str, Hsla) {
    let (label, color) = match kind {
        "hash" => ("HASH", 0x5C8DFF),
        "list" => ("LIST", 0x00C78A),
        "set" => ("SET", 0xFF8C42),
        "zset" => ("ZSET", 0xFF5FA2),
        "string" => ("STRING", 0xB45AF2),
        "ReJSON-RL" => ("JSON", 0xFFC010),
        "stream" => ("STREAM", 0xC2D34A),
        "none" => ("GONE", 0x889397),
        "" => ("…", 0x889397),
        _ => ("OTHER", 0x889397),
    };
    (label, rgb(color).into())
}

fn badge(kind: &str) -> AnyElement {
    // Until its type arrives a key shows no badge rather than an empty one.
    if kind.is_empty() {
        return div().w(px(58.)).flex_none().into_any_element();
    }
    let (label, color) = type_badge(kind);
    // One width for every type, so the key names line up.
    h_flex()
        .w(px(58.))
        .flex_none()
        .justify_center()
        .rounded_md()
        .border_1()
        .border_color(color.opacity(0.45))
        .bg(color.opacity(0.14))
        .child(
            Label::new(label)
                .size(LabelSize::XSmall)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(Color::Custom(color)),
        )
        .into_any_element()
}

/// A value in the chosen format; JSON pretty-prints only when the bytes are valid JSON.
fn formatted(bytes: &[u8], format: Format) -> String {
    let text = || String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| escape_bytes(bytes));
    match format {
        Format::Hex => hex_bytes(bytes),
        Format::Binary => bytes
            .iter()
            .map(|byte| format!("{byte:08b}"))
            .collect::<Vec<_>>()
            .join(" "),
        Format::Ascii => escape_bytes(bytes),
        Format::Json => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(text),
        Format::Unicode => text(),
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
/// Works on bytes, so a delimiter byte inside binary data splits exactly where Redis has it.
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

fn info_value<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    info.lines().find_map(|line| {
        line.trim()
            .strip_prefix(field)
            .and_then(|rest| rest.strip_prefix(':'))
    })
}

impl DatabaseView {
    fn redis_state(&mut self) -> Option<&mut RedisState> {
        self.redis_state.as_mut()
    }

    pub(crate) fn load_redis_databases(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        if session.is_clustered() {
            return;
        }
        let connection = self.generation;
        let task = run(cx, async move {
            let count = session.database_count().await;
            Ok((count, session.keyspace().await))
        });
        cx.spawn(async move |this, cx| {
            let Ok((count, keyspace)) = task.await else {
                return;
            };
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(state) = this.redis_state() {
                    state.database_count = count;
                    match keyspace {
                        Ok(keyspace) => state.keyspace = keyspace,
                        Err(error) => {
                            state.keyspace_error =
                                Some(format!("Key counts unavailable: {error:#}").into())
                        }
                    }
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
    }

    /// SELECT is per connection, so a switch is a new session on the other database; the old one
    /// stays until the new one is up, and everything read or pending on the old one is dropped.
    fn switch_redis_database(&mut self, database: u8, cx: &mut Context<Self>) {
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        // A write must finish or be closed first; its result belongs to the database it was sent to.
        let editing = self.redis_write.is_some();
        let Some(state) = self.redis_state() else {
            return;
        };
        if editing {
            state.switch_error = Some("Close the open edit before switching databases.".into());
            cx.notify();
            return;
        }
        if state.switching.is_some() {
            state.switch_error = Some("A database switch is already in progress.".into());
            cx.notify();
            return;
        }
        state.switching = Some(database);
        state.switch_error = None;
        let generation = self.generation;
        let read = cx.read_credentials(&connection.credential_url());
        cx.spawn(async move |this, cx| {
            let result = async {
                let (_, uri) = read
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("No connection string saved."))?;
                let uri = String::from_utf8(uri)?;
                let task = cx.update(|cx| {
                    run(cx, async move {
                        vilo_database_core::redis::RedisSession::connect_to_database(
                            &uri,
                            Some(database),
                        )
                        .await
                    })
                });
                task.await
            }
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                match result {
                    Ok(session) => {
                        this.generation += 1;
                        this.session = Some(crate::Session::Redis(Arc::new(session)));
                        this.redis_write = None;
                        this.redis_state = Some(RedisState::new());
                        this.start_redis_stats(cx);
                        this.scan(true, cx);
                        this.load_redis_databases(cx);
                    }
                    Err(error) => {
                        if let Some(state) = this.redis_state() {
                            state.switching = None;
                            state.switch_error =
                                Some(format!("Could not switch to db{database}: {error:#}").into());
                        }
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn close_redis_key(&mut self) {
        if let Some(state) = self.redis_state() {
            state.panel = RightPanel::Empty;
            state.selected = None;
            state.detail = None;
        }
    }

    pub(crate) fn reopen_redis_key(&mut self, key: Bytes, cx: &mut Context<Self>) {
        self.open_key(key, cx);
    }

    /// Polls INFO and DBSIZE every few seconds while the Redis connection is open, for the top bar.
    pub(crate) fn start_redis_stats(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let task = cx.spawn(async move |this, cx| {
            loop {
                let poll = {
                    let session = session.clone();
                    cx.update(|cx| {
                        run(cx, async move {
                            let info = session.info().await?;
                            let mut keys = 0;
                            for node in session.scan_nodes() {
                                keys += session.dbsize(&node).await.unwrap_or(0);
                            }
                            Ok((info, keys))
                        })
                    })
                };
                let result = poll.await;
                let alive = this
                    .update(cx, |this, cx| {
                        if this.generation != connection {
                            return false;
                        }
                        let Some(state) = this.redis_state() else {
                            return false;
                        };
                        if let Ok((info, keys)) = result {
                            let stats = &mut state.stats;
                            stats.ops_per_second = info_value(&info, "instantaneous_ops_per_sec")
                                .and_then(|value| value.trim().parse().ok());
                            stats.memory = info_value(&info, "used_memory_human")
                                .map(|value| value.trim().to_string());
                            stats.clients = info_value(&info, "connected_clients")
                                .and_then(|value| value.trim().parse().ok());
                            stats.keys = Some(keys);
                            state.total = Some(keys);
                            // CPU is the change in seconds used between two samples over the wall time between them.
                            let used = ["used_cpu_sys", "used_cpu_user"]
                                .iter()
                                .filter_map(|field| {
                                    info_value(&info, field)?.trim().parse::<f64>().ok()
                                })
                                .sum::<f64>();
                            let now = Instant::now();
                            if let Some((previous, at)) = stats.cpu_sample {
                                let elapsed = now.duration_since(at).as_secs_f64();
                                if elapsed > 0. && used >= previous {
                                    stats.cpu_percent = Some(100. * (used - previous) / elapsed);
                                }
                            }
                            stats.cpu_sample = Some((used, now));
                        }
                        cx.notify();
                        true
                    })
                    .unwrap_or(false);
                if !alive {
                    return;
                }
                cx.background_executor().timer(STATS_EVERY).await;
            }
        });
        if let Some(state) = self.redis_state() {
            state._stats = Some(task);
        }
    }

    pub(crate) fn scan(&mut self, restart: bool, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let pattern = self.pattern.read(cx).text(cx);
        // A plain word finds keys containing it; a glob is used as typed.
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
            // Types do not change with the filter, so known ones are kept.
            state.seen.clear();
            state.stale = true;
            state.listed_kind = state.kind_filter;
            state.scanned = 0;
            state.nodes = session
                .scan_nodes()
                .into_iter()
                .map(|node| NodeScan {
                    node,
                    cursor: Some("0".into()),
                })
                .collect();
        }
        if state.nodes.iter().all(|node| node.cursor.is_none()) {
            return;
        }
        let nodes = state
            .nodes
            .iter()
            .map(|node| (node.node.clone(), node.cursor.clone()))
            .collect::<Vec<_>>();
        let kind = (!state.kind_filter.is_empty()).then(|| state.kind_filter.to_string());
        state.scanning = true;
        state.scan_error = None;
        let generation = state.generation;
        // One round walks up to SCAN_ROUND keys, node after node, as RedisInsight's "Scan more" does.
        let task = run(cx, async move {
            let mut cursors = nodes;
            let mut found = Vec::new();
            let mut walked = 0;
            while walked < SCAN_ROUND {
                let Some(index) = cursors.iter().position(|(_, cursor)| cursor.is_some()) else {
                    break;
                };
                let cursor = cursors[index].1.clone().unwrap_or_default();
                let page = session
                    .scan(
                        &cursors[index].0,
                        &cursor,
                        &pattern,
                        SCAN_COUNT,
                        kind.as_deref(),
                    )
                    .await?;
                walked += u64::from(SCAN_COUNT);
                found.extend(page.keys);
                cursors[index].1 = (page.cursor != "0").then_some(page.cursor);
            }
            Ok((found, cursors, walked))
        });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            let new_keys = this
                .update(cx, |this, cx| {
                    if this.generation != connection {
                        return Vec::new();
                    }
                    let Some(state) = this.redis_state() else {
                        return Vec::new();
                    };
                    if state.generation != generation {
                        return Vec::new();
                    }
                    state.scanning = false;
                    state.last_refresh = Some(Instant::now());
                    let mut new_keys = Vec::new();
                    match result {
                        Ok((found, cursors, walked)) => {
                            if state.stale {
                                state.keys.clear();
                                state.stale = false;
                            }
                            state.scanned += walked;
                            for (node, (_, cursor)) in state.nodes.iter_mut().zip(cursors) {
                                node.cursor = cursor;
                            }
                            // SCAN may return a key again on a later page.
                            for key in found {
                                if state.seen.insert(key.clone()) {
                                    state.keys.push(key.clone());
                                    new_keys.push(key);
                                }
                            }
                            if state.nodes.iter().all(|node| node.cursor.is_none()) {
                                // The walk is complete; the estimate gives way to the true total.
                                state.scanned = state
                                    .total
                                    .unwrap_or(state.scanned)
                                    .max(state.keys.len() as u64);
                            }
                        }
                        Err(error) => state.scan_error = Some(format!("{error:#}").into()),
                    }
                    cx.notify();
                    new_keys
                })
                .unwrap_or_default();
            // Names show at once; type, TTL and size follow in batches.
            for batch in new_keys.chunks(META_BATCH) {
                let Ok(Some(session)) = this.update(cx, |this, _| this.redis()) else {
                    return;
                };
                let keys = batch.to_vec();
                let lookup = cx.update(|cx| {
                    run(cx, async move {
                        let meta = session.key_meta(&keys, true).await;
                        Ok(keys.into_iter().zip(meta).collect::<Vec<_>>())
                    })
                });
                let Ok(meta) = lookup.await else {
                    return;
                };
                let still_current = this
                    .update(cx, |this, cx| {
                        let Some(state) = this.redis_state() else {
                            return false;
                        };
                        if state.generation != generation {
                            return false;
                        }
                        let known = Arc::make_mut(&mut state.meta);
                        for (key, (info, size)) in meta {
                            known.insert(
                                key,
                                KeyMeta {
                                    kind: info.kind,
                                    ttl_ms: info.ttl_ms,
                                    size,
                                },
                            );
                        }
                        cx.notify();
                        true
                    })
                    .unwrap_or(false);
                if !still_current {
                    return;
                }
            }
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
        state.panel = RightPanel::Key;
        state.detail = Some(Fetch::Loading);
        let task = run(cx, {
            let key = key.clone();
            async move {
                let info = session.key_info(&key).await?;
                let length = session.length(&key, &info.kind).await.ok().flatten();
                let size = session.memory_usage(&key).await.ok().flatten();
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
                    size,
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
                // A fresh read shows what an unknown write did, so the key's lock can go.
                if result.is_ok() {
                    this.clear_redis_lock(&key);
                    this.write_notice = None;
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

    fn show_info(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.panel = RightPanel::Info;
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

    fn show_topology(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.panel = RightPanel::Topology;
        state.topology = Some(Fetch::Loading);
        let task = run(cx, async move {
            Ok(match session.cluster_nodes().await? {
                Some(nodes) => Topology::Cluster(nodes),
                None => Topology::Single(session.replication().await?),
            })
        });
        state.keep(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation == connection
                    && let Some(state) = this.redis_state()
                {
                    state.topology = Some(fetched(result));
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn render_topology(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let cell = |text: String, width: Option<f32>, color: Color| {
            let label = Label::new(text)
                .size(LabelSize::Small)
                .buffer_font(cx)
                .color(color);
            match width {
                Some(width) => div().w(px(width)).flex_none().child(label),
                None => div().flex_1().min_w_0().child(label),
            }
        };
        let row = |colors: &theme::ThemeColors| {
            h_flex()
                .px_3()
                .py_1()
                .gap_3()
                .border_t_1()
                .border_color(colors.border_variant)
        };
        match &state.topology {
            None | Some(Fetch::Loading) => Label::new("Loading…")
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loaded(Topology::Single(pairs))) => {
                let mut table = v_flex().rounded_md().border_1().border_color(colors.border);
                for (key, value) in pairs {
                    table = table.child(
                        row(&colors)
                            .child(cell(key.clone(), Some(240.), Color::Muted))
                            .child(cell(value.clone(), None, Color::Default)),
                    );
                }
                v_flex()
                    .gap_2()
                    .child(
                        Label::new("Single server (not a cluster). Replication:")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(table)
                    .into_any_element()
            }
            Some(Fetch::Loaded(Topology::Cluster(nodes))) => {
                let mut primaries = nodes.iter().filter(|node| node.primary).collect::<Vec<_>>();
                primaries.sort_by_key(|node| {
                    node.slots
                        .first()
                        .and_then(|slot| slot.split('-').next()?.parse::<u32>().ok())
                        .unwrap_or(u32::MAX)
                });
                let covered: u32 = primaries.iter().map(|node| node.slot_count).sum();
                let health = |node: &ClusterNode| {
                    if !node.connected || !node.problems.is_empty() {
                        (
                            format!(
                                "{} {}",
                                if node.connected {
                                    "connected"
                                } else {
                                    "disconnected"
                                },
                                node.problems.join(",")
                            ),
                            Color::Error,
                        )
                    } else {
                        ("ok".to_string(), Color::Success)
                    }
                };
                let mut table = v_flex()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .child(
                        h_flex()
                            .px_3()
                            .py_1()
                            .gap_3()
                            .bg(colors.element_background)
                            .child(cell("Node".into(), Some(200.), Color::Default))
                            .child(cell("Role".into(), Some(80.), Color::Default))
                            .child(cell("Slots".into(), None, Color::Default))
                            .child(cell("Health".into(), Some(160.), Color::Default)),
                    );
                for primary in &primaries {
                    let (state_text, state_color) = health(primary);
                    table = table.child(
                        row(&colors)
                            .child(cell(primary.address.clone(), Some(200.), Color::Default))
                            .child(cell("primary".into(), Some(80.), Color::Accent))
                            .child(cell(
                                format!(
                                    "{} ({} slots)",
                                    primary.slots.join(" "),
                                    primary.slot_count
                                ),
                                None,
                                Color::Default,
                            ))
                            .child(cell(state_text, Some(160.), state_color)),
                    );
                    for replica in nodes
                        .iter()
                        .filter(|node| node.primary_id.as_deref() == Some(primary.id.as_str()))
                    {
                        let (state_text, state_color) = health(replica);
                        table = table.child(
                            row(&colors)
                                .child(cell(
                                    format!("  └ {}", replica.address),
                                    Some(200.),
                                    Color::Muted,
                                ))
                                .child(cell("replica".into(), Some(80.), Color::Muted))
                                .child(cell(String::new(), None, Color::Muted))
                                .child(cell(state_text, Some(160.), state_color)),
                        );
                    }
                }
                // Nodes with no known primary (mid-failover, handshake, noaddr) are listed too.
                let others = nodes
                    .iter()
                    .filter(|node| {
                        !node.primary
                            && !primaries.iter().any(|primary| {
                                Some(primary.id.as_str()) == node.primary_id.as_deref()
                            })
                    })
                    .collect::<Vec<_>>();
                let orphans = others.len();
                for node in &others {
                    let (state_text, state_color) = match health(node) {
                        (_, Color::Error) => health(node),
                        _ => ("no known primary".to_string(), Color::Warning),
                    };
                    table = table.child(
                        row(&colors)
                            .child(cell(node.address.clone(), Some(200.), Color::Warning))
                            .child(cell(
                                if node.primary_id.is_some() {
                                    "replica"
                                } else {
                                    "unknown"
                                }
                                .into(),
                                Some(80.),
                                Color::Warning,
                            ))
                            .child(cell(
                                match &node.primary_id {
                                    Some(primary) => format!("primary {primary} not in the list"),
                                    None => "no role reported".to_string(),
                                },
                                None,
                                Color::Muted,
                            ))
                            .child(cell(state_text, Some(160.), state_color)),
                    );
                }
                v_flex()
                    .gap_2()
                    .child(
                        Label::new(format!(
                            "{} primaries, {} replicas · {covered} of 16384 slots covered{}",
                            primaries.len(),
                            nodes
                                .iter()
                                .filter(|node| !node.primary)
                                .count()
                                .saturating_sub(orphans),
                            if orphans > 0 {
                                format!(" · {orphans} nodes without a known primary, listed last")
                            } else {
                                String::new()
                            }
                        ))
                        .size(LabelSize::Small)
                        .color(if covered == 16384 {
                            Color::Muted
                        } else {
                            Color::Error
                        }),
                    )
                    .child(table)
                    .into_any_element()
            }
        }
    }

    fn render_key_list(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let rows = state.rows();
        let meta = state.meta.clone();
        let selected = state.selected.clone();
        let view = cx.entity().downgrade();
        let colors = cx.theme().colors().clone();
        let loaded = state.keys.len().max(1);
        uniform_list("database-redis-keys", rows.len(), move |range, _, _cx| {
            range
                .filter_map(|index| rows.get(index).cloned().map(|row| (index, row)))
                .map(|(index, row)| {
                    let view = view.clone();
                    let zebra = if index % 2 == 1 {
                        colors.element_background
                    } else {
                        colors.editor_background
                    };
                    let base = h_flex()
                        .id(("database-redis-row", index))
                        .w_full()
                        .h(px(ROW_HEIGHT))
                        .px_3()
                        .gap_2()
                        .bg(zebra)
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
                                    if let Some(state) = this.redis_state() {
                                        if !state.expanded.remove(&prefix) {
                                            state.expanded.insert(prefix);
                                        }
                                        state.expanded_version += 1;
                                    }
                                    cx.notify();
                                })
                                .ok();
                            })
                            .child(div().w(px(depth as f32 * 18.)).flex_none())
                            .child(
                                Icon::new(if open {
                                    IconName::ChevronDown
                                } else {
                                    IconName::ChevronRight
                                })
                                .size(IconSize::Small)
                                .color(Color::Muted),
                            )
                            .child(
                                Icon::new(IconName::Folder)
                                    .size(IconSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .child(Label::new(label).single_line().truncate()),
                            )
                            .child(div().w(px(64.)).flex_none().flex().justify_end().child(
                                Label::new({
                                    let share = count * 100 / loaded;
                                    if share == 0 {
                                        "<1%".to_string()
                                    } else {
                                        format!("{share}%")
                                    }
                                }),
                            ))
                            .child(
                                div()
                                    .w(px(72.))
                                    .flex_none()
                                    .flex()
                                    .justify_end()
                                    .child(Label::new(count.to_string())),
                            )
                            .into_any_element(),
                        KeyRow::Key { key, label, depth } => {
                            let info = meta.get(&key).cloned().unwrap_or_default();
                            let is_selected = selected.as_ref() == Some(&key);
                            base.when(is_selected, |this| this.bg(colors.element_selected))
                                .on_click(move |_, _, cx| {
                                    let key = key.clone();
                                    view.update(cx, |this, cx| this.open_key(key, cx)).ok();
                                })
                                .child(div().w(px(depth as f32 * 18. + 4.)).flex_none())
                                .child(h_flex().w(px(64.)).flex_none().child(badge(&info.kind)))
                                .child(
                                    div()
                                        .flex_1()
                                        .min_w_0()
                                        .child(Label::new(label).single_line().truncate()),
                                )
                                .child(div().w(px(64.)).flex_none().flex().justify_end().child(
                                    Label::new(if info.kind.is_empty() {
                                        String::new()
                                    } else {
                                        describe_ttl(info.ttl_ms)
                                    }),
                                ))
                                .child(
                                    div().w(px(72.)).flex_none().flex().justify_end().child(
                                        Label::new(
                                            info.size.map(describe_size).unwrap_or_default(),
                                        )
                                        .size(LabelSize::Small),
                                    ),
                                )
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
                    push_block(format!("{:<28} ", escape_bytes(field)), show(value));
                }
            }
            Value::List { items, .. } => {
                for (index, item) in items.iter().enumerate() {
                    push_block(format!("{index:<8} "), show(item));
                }
            }
            Value::Set { members, .. } => {
                for member in members {
                    push_block(String::new(), show(member));
                }
            }
            Value::SortedSet { members, .. } => {
                for (member, score) in members {
                    push_block(format!("{score:<16} "), show(member));
                }
            }
            Value::Stream { entries, .. } => {
                for entry in entries {
                    push_block(String::new(), entry.id.clone());
                    for (field, value) in &entry.fields {
                        push_block(format!("    {:<24} ", escape_bytes(field)), show(value));
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
                    "Vilo has no viewer for {} keys yet; use the CLI.",
                    detail.info.kind
                ),
            ),
        }
        lines
    }

    fn render_detail(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let close = IconButton::new("database-redis-close-detail", IconName::Close).on_click(
            cx.listener(|this, _, _, cx| {
                if let Some(state) = this.redis_state() {
                    state.panel = RightPanel::Empty;
                    state.selected = None;
                }
                cx.notify();
            }),
        );
        match state.panel {
            RightPanel::Empty => {
                return v_flex()
                    .size_full()
                    .child(h_flex().justify_end().child(close))
                    .child(
                        v_flex().flex_1().items_center().justify_center().child(
                            Label::new("Select the key from the list on the left to see the details of the key.").color(Color::Muted),
                        ),
                    )
                    .into_any_element();
            }
            RightPanel::Info => {
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
                                        .child(
                                            div().w(px(240.)).flex_none().child(
                                                Label::new(key)
                                                    .size(LabelSize::Small)
                                                    .color(Color::Muted),
                                            ),
                                        )
                                        .child(
                                            div().flex_1().min_w_0().child(
                                                Label::new(value)
                                                    .size(LabelSize::Small)
                                                    .buffer_font(cx),
                                            ),
                                        ),
                                );
                            }
                            sections = sections.child(table);
                        }
                        sections.into_any_element()
                    }
                };
                return v_flex()
                    .size_full()
                    .gap_2()
                    .child(
                        h_flex()
                            .child(Headline::new("Database information").size(HeadlineSize::Small))
                            .child(div().flex_1())
                            .child(close),
                    )
                    .child(
                        v_flex()
                            .id("database-redis-info")
                            .flex_1()
                            .min_h_0()
                            .overflow_y_scroll()
                            .child(body),
                    )
                    .into_any_element();
            }
            RightPanel::Topology => {
                return v_flex()
                    .size_full()
                    .gap_2()
                    .child(
                        h_flex()
                            .child(Headline::new("Topology").size(HeadlineSize::Small))
                            .child(div().flex_1())
                            .child(
                                IconButton::new(
                                    "database-redis-topology-refresh",
                                    IconName::RotateCw,
                                )
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Refresh"))
                                .on_click(cx.listener(|this, _, _, cx| this.show_topology(cx))),
                            )
                            .child(close),
                    )
                    .child(
                        v_flex()
                            .id("database-redis-topology")
                            .flex_1()
                            .min_h_0()
                            .overflow_y_scroll()
                            .child(self.render_topology(state, cx)),
                    )
                    .into_any_element();
            }
            RightPanel::Key => {}
        }
        let detail = match &state.detail {
            None | Some(Fetch::Loading) => {
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
        let more = match &detail.value {
            Value::Hash { cursor, .. } | Value::Set { cursor, .. } => cursor.is_some(),
            Value::List { next, .. } | Value::SortedSet { next, .. } => next.is_some(),
            Value::Stream { more, .. } => *more,
            _ => false,
        };
        let header = match &detail.value {
            Value::Hash { .. } => Some(("Field", "Value")),
            Value::List { .. } => Some(("Index", "Element")),
            Value::Set { .. } => Some(("Member", "")),
            Value::SortedSet { .. } => Some(("Score", "Member")),
            Value::Stream { .. } => Some(("Entry ID", "Fields")),
            _ => None,
        };
        let view = cx.entity().downgrade();
        let current_format = state.format;
        let formats = PopoverMenu::new("database-redis-format")
            .trigger(
                Button::new("database-redis-format-button", current_format.label())
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Outlined)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for format in [
                        Format::Unicode,
                        Format::Ascii,
                        Format::Binary,
                        Format::Hex,
                        Format::Json,
                    ] {
                        let view = view.clone();
                        menu = menu.entry(format.label(), None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.format = format;
                                }
                                cx.notify();
                            })
                            .ok();
                        });
                    }
                    for unavailable in [
                        "Msgpack",
                        "PHP serialized",
                        "Java serialized",
                        "Pickle",
                        "Vector 32-bit",
                        "Vector 64-bit",
                    ] {
                        menu = menu.label(format!("{unavailable} (not in Vilo yet)"));
                    }
                    menu
                }))
            });
        let copy_key = quote_bytes(&detail.key);
        let groups = match &detail.value {
            Value::Stream { groups, .. } if !groups.is_empty() => Some(groups.clone()),
            _ => None,
        };
        let key = detail.key.clone();
        let writable = self
            .active_connection()
            .is_some_and(|connection| self.writes_allowed(connection));
        v_flex()
            .size_full()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(badge(&detail.info.kind))
                    .child(
                        Label::new(escape_bytes(&detail.key))
                            .size(LabelSize::Large)
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
                    .child({
                        let view = cx.entity().downgrade();
                        let key = detail.key.clone();
                        let current = match &detail.value {
                            Value::String(value) => value.clone(),
                            _ => None,
                        };
                        let ops = crate::redis_write::RedisOp::for_kind(&detail.info.kind);
                        PopoverMenu::new("database-redis-edit")
                            .trigger(
                                Button::new("database-redis-edit-button", "Edit")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Outlined)
                                    .start_icon(Icon::new(IconName::Pencil).size(IconSize::XSmall))
                                    .end_icon(
                                        Icon::new(IconName::ChevronDown).size(IconSize::XSmall),
                                    )
                                    .disabled(!writable)
                                    .tooltip(Tooltip::text(if writable {
                                        "Change this key"
                                    } else {
                                        "Switch writes on (top bar) to change keys"
                                    })),
                            )
                            .menu(move |window, cx| {
                                let (view, key, current, ops) =
                                    (view.clone(), key.clone(), current.clone(), ops.clone());
                                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                    for op in ops {
                                        if op == crate::redis_write::RedisOp::Rename {
                                            menu = menu.separator();
                                        }
                                        let (view, key, current) =
                                            (view.clone(), key.clone(), current.clone());
                                        menu = menu.entry(op.label(), None, move |window, cx| {
                                            let (key, current) = (key.clone(), current.clone());
                                            view.update(cx, |this, cx| {
                                                this.start_redis_write(op, key, current, window, cx)
                                            })
                                            .ok();
                                        });
                                    }
                                    menu
                                }))
                            })
                    })
                    .child(
                        IconButton::new("database-redis-reload-key", IconName::RotateCw)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Refresh"))
                            .on_click(
                                cx.listener(move |this, _, _, cx| this.open_key(key.clone(), cx)),
                            ),
                    )
                    .child(close),
            )
            .child(
                h_flex()
                    .gap_4()
                    .when_some(detail.size, |this, size| {
                        this.child(
                            Label::new(format!("Key Size: {}", describe_size(size)))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .when_some(detail.length, |this, length| {
                        this.child(
                            Label::new(format!("Length: {length}"))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .child(
                        Label::new(format!("TTL: {}", describe_ttl(detail.info.ttl_ms)))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(div().flex_1())
                    .child(formats),
            )
            .when_some(groups, |this, groups| {
                this.child(
                    Label::new("Consumer Groups")
                        .size(LabelSize::Small)
                        .weight(gpui::FontWeight::SEMIBOLD),
                )
                .children(groups.into_iter().map(|group| {
                    Label::new(
                        group
                            .into_iter()
                            .map(|(name, value)| format!("{name}: {value}"))
                            .collect::<Vec<_>>()
                            .join("  ·  "),
                    )
                    .size(LabelSize::Small)
                    .buffer_font(cx)
                }))
            })
            .when_some(header, |this, (left, right)| {
                this.child(
                    h_flex()
                        .px_2()
                        .py_1()
                        .bg(colors.element_background)
                        .rounded_sm()
                        .child(
                            div().w(px(220.)).flex_none().child(
                                Label::new(left)
                                    .size(LabelSize::Small)
                                    .weight(gpui::FontWeight::SEMIBOLD),
                            ),
                        )
                        .child(
                            Label::new(right)
                                .size(LabelSize::Small)
                                .weight(gpui::FontWeight::SEMIBOLD),
                        ),
                )
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

    fn render_browse(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let kind_label = KEY_TYPES
            .iter()
            .find(|(_, kind)| *kind == state.kind_filter)
            .map_or("All Key Types", |(label, _)| label);
        let types = PopoverMenu::new("database-redis-types")
            .trigger(
                Button::new("database-redis-types-button", kind_label)
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
                                let Some(state) = this.redis_state() else {
                                    return;
                                };
                                let local = state.can_filter_locally();
                                state.kind_filter = kind;
                                if local {
                                    cx.notify();
                                } else {
                                    this.scan(true, cx);
                                }
                            })
                            .ok();
                        });
                    }
                    menu
                }))
            });
        let settings_view = cx.entity().downgrade();
        let settings = PopoverMenu::new("database-redis-settings")
            .trigger(
                IconButton::new("database-redis-settings-button", IconName::Settings)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Delimiter")),
            )
            .menu(move |window, cx| {
                let view = settings_view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    menu = menu.header("Delimiter");
                    for delimiter in [":", "/", ".", "-", "_"] {
                        let view = view.clone();
                        menu = menu.entry(delimiter, None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.delimiter = delimiter.to_string();
                                    state.expanded.clear();
                                    state.expanded_version += 1;
                                }
                                cx.notify();
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
        let done = state.nodes.iter().all(|node| node.cursor.is_none());
        let total = state
            .total
            .map(group_thousands)
            .unwrap_or_else(|| "…".into());
        let last_refresh = match state.last_refresh.map(|at| at.elapsed().as_secs()) {
            None => String::new(),
            Some(0..60) => "now".into(),
            Some(seconds) if seconds < 3600 => format!("{} min", seconds / 60),
            Some(seconds) => format!("{} h", seconds / 3600),
        };
        let coverage = (state.nodes.len() > 1).then(|| {
            format!(
                "{} of {} nodes fully scanned",
                state
                    .nodes
                    .iter()
                    .filter(|node| node.cursor.is_none())
                    .count(),
                state.nodes.len()
            )
        });
        v_flex()
            .size_full()
            .p_3()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        h_flex()
                            .rounded_md()
                            .border_1()
                            .border_color(colors.border)
                            .child(IconButton::new("database-redis-filter-mode", IconName::Filter).style(ButtonStyle::Filled).tooltip(Tooltip::text("Filter by key name or pattern")))
                            .child(IconButton::new("database-redis-search-mode", IconName::MagnifyingGlass).disabled(true).tooltip(Tooltip::text("Full-text search (RediSearch) is not in Vilo yet"))),
                    )
                    .child(types)
                    .child(
                        h_flex()
                            .flex_1()
                            .min_w_0()
                            .px_2()
                            .py_1()
                            .gap_2()
                            .rounded_md()
                            .border_1()
                            .border_color(colors.border)
                            .bg(colors.editor_background)
                            .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| this.scan(true, cx)))
                            .child(div().flex_1().min_w_0().child(self.pattern.clone()))
                            .child(IconButton::new("database-redis-search", IconName::MagnifyingGlass).icon_size(IconSize::Small).on_click(cx.listener(|this, _, _, cx| this.scan(true, cx)))),
                    ),
            )
            .child(
                h_flex()
                    .flex_1()
                    .min_h_0()
                    .gap_3()
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .h_full()
                            .rounded_md()
                            .border_1()
                            .border_color(colors.border)
                            .child(
                                h_flex()
                                    .px_3()
                                    .py_1p5()
                                    .gap_3()
                                    .border_b_1()
                                    .border_color(colors.border)
                                    .child(Label::new(format!("Results: {}. Scanned {} / {total}", group_thousands(state.visible_keys().len() as u64), group_thousands(state.scanned))).size(LabelSize::Small))
                                    .when(!done, |this| {
                                        this.child(
                                            Button::new("database-redis-scan-more", if state.scanning { "Scanning…" } else { "Scan more" })
                                                .label_size(LabelSize::Small)
                                                .color(Color::Accent)
                                                .disabled(state.scanning)
                                                .tooltip(Tooltip::text("SCAN walks keys in rounds of 10 000; results may repeat and are not a snapshot"))
                                                .on_click(cx.listener(|this, _, _, cx| this.scan(false, cx))),
                                        )
                                    })
                                    .when_some(coverage, |this, coverage| this.child(Label::new(coverage).size(LabelSize::Small).color(Color::Muted)))
                                    .child(settings)
                                    .child(div().flex_1())
                                    .child(Label::new(format!("Last refresh: {last_refresh}")).size(LabelSize::Small).color(Color::Muted))
                                    .child(IconButton::new("database-redis-refresh", IconName::RotateCw).icon_size(IconSize::Small).tooltip(Tooltip::text("Refresh")).on_click(cx.listener(|this, _, _, cx| this.scan(true, cx))))
                                    .child(
                                        h_flex()
                                            .rounded_md()
                                            .border_1()
                                            .border_color(colors.border)
                                            .child(view_icon("database-redis-list", IconName::Menu, "List view", KeyView::List, cx))
                                            .child(view_icon("database-redis-tree", IconName::ListTree, "Tree view", KeyView::Tree, cx)),
                                    ),
                            )
                            .when_some(state.scan_error.clone(), |this, message| this.child(div().px_3().py_1().child(Label::new(message).size(LabelSize::Small).color(Color::Error))))
                            .child(self.render_key_list(state, cx)),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .h_full()
                            .p_3()
                            .rounded_md()
                            .border_1()
                            .border_color(colors.border)
                            .child(self.render_detail(state, cx)),
                    ),
            )
            .into_any_element()
    }

    fn workbench_lines(state: &RedisState) -> Vec<SharedString> {
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
        lines
    }

    fn render_command_input(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        h_flex()
            .gap_2()
            .px_2()
            .py_1()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .bg(colors.editor_background)
            .on_action(
                cx.listener(|this, _: &menu::Confirm, window, cx| this.run_workbench(window, cx)),
            )
            .child(Label::new(">").buffer_font(cx).color(Color::Muted))
            .child(div().flex_1().child(self.workbench_input.clone()))
            .child(
                Button::new("database-redis-run", "Run")
                    .style(ButtonStyle::Tinted(ui::TintColor::Accent))
                    .on_click(cx.listener(|this, _, window, cx| this.run_workbench(window, cx))),
            )
    }

    fn render_workbench(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let lines = Self::workbench_lines(state);
        v_flex()
            .size_full()
            .p_3()
            .gap_2()
            .child(self.render_command_input(cx))
            .child(
                Label::new("Read commands only in this release. Writes, KEYS, FLUSH*, CONFIG, MONITOR, scripts and risky subcommands are refused before sending.")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .child(if lines.is_empty() {
                Label::new("Run a command such as HGETALL user:1 or XINFO GROUPS orders.").color(Color::Muted).into_any_element()
            } else {
                render_lines("database-redis-workbench", lines, &state.value_scroll, cx)
            })
            .into_any_element()
    }

    fn render_database_picker(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let Some(session) = self.redis() else {
            return div().into_any_element();
        };
        let current = session.database();
        if session.is_clustered() {
            return Label::new("db0").into_any_element();
        }
        let label = match state.switching {
            Some(database) => format!("db{database}…"),
            None => format!("db{current}"),
        };
        // Without CONFIG the count is unknown; the usual 16 are offered, plus any with keys.
        let count = state
            .database_count
            .unwrap_or(16)
            .max(
                state
                    .keyspace
                    .iter()
                    .map(|(database, _)| database + 1)
                    .max()
                    .unwrap_or(0),
            )
            .min(256);
        let keyspace = state.keyspace.clone();
        let keyspace_error = state.keyspace_error.clone();
        let view = cx.entity().downgrade();
        h_flex()
            .gap_1()
            .child(
                PopoverMenu::new("database-redis-db")
                    .trigger(
                        Button::new("database-redis-db-button", label)
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                            .tooltip(Tooltip::text("Logical database")),
                    )
                    .menu(move |window, cx| {
                        let (view, keyspace, keyspace_error) =
                            (view.clone(), keyspace.clone(), keyspace_error.clone());
                        Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                            if let Some(error) = &keyspace_error {
                                menu = menu.label(error.clone());
                            }
                            for database in 0..count {
                                let Ok(database) = u8::try_from(database) else {
                                    break;
                                };
                                let keys = keyspace
                                    .iter()
                                    .find(|(known, _)| *known == u32::from(database))
                                    .map(|(_, keys)| *keys);
                                let label = match keys {
                                    Some(keys) => {
                                        format!("db{database} · {} keys", group_thousands(keys))
                                    }
                                    None => format!("db{database}"),
                                };
                                let view = view.clone();
                                menu = menu.toggleable_entry(
                                    label,
                                    database == current,
                                    ui::IconPosition::Start,
                                    None,
                                    move |_, cx| {
                                        view.update(cx, |this, cx| {
                                            if database != current {
                                                this.switch_redis_database(database, cx);
                                            }
                                        })
                                        .ok();
                                    },
                                );
                            }
                            // CONFIG may be off-limits, so any number the server accepts can be typed.
                            menu.separator()
                                .entry("Other database…", None, move |window, cx| {
                                    view.update(cx, |this, cx| {
                                        let input = crate::editor(
                                            "Database number, 0–255",
                                            "",
                                            false,
                                            window,
                                            cx,
                                        );
                                        window.focus(&input.focus_handle(cx), cx);
                                        if let Some(state) = this.redis_state() {
                                            state.database_input = Some(input);
                                        }
                                        cx.notify();
                                    })
                                    .ok();
                                })
                        }))
                    }),
            )
            .when_some(state.database_input.clone(), |this, input| {
                let submit = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
                    let Some(state) = this.redis_state() else {
                        return;
                    };
                    let Some(input) = state.database_input.clone() else {
                        return;
                    };
                    match input.read(cx).text(cx).trim().parse::<u8>() {
                        Ok(database) => {
                            state.database_input = None;
                            state.switch_error = None;
                            this.switch_redis_database(database, cx);
                        }
                        Err(_) => {
                            state.switch_error =
                                Some("Type a database number from 0 to 255.".into())
                        }
                    }
                    cx.notify();
                };
                this.child(
                    h_flex()
                        .w(px(120.))
                        .on_action(
                            cx.listener(move |this, _: &menu::Confirm, _, cx| submit(this, cx)),
                        )
                        .on_action(cx.listener(|this, _: &menu::Cancel, _, cx| {
                            if let Some(state) = this.redis_state() {
                                state.database_input = None;
                            }
                            cx.notify();
                        }))
                        .child(crate::boxed(input, cx)),
                )
            })
            .when_some(state.switch_error.clone(), |this, error| {
                this.child(Label::new(error).size(LabelSize::Small).color(Color::Error))
            })
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
        let tab = |id: &'static str,
                   label: &'static str,
                   target: Option<RedisTab>,
                   cx: &mut Context<Self>| {
            let selected = target == Some(state.tab);
            h_flex()
                .id(id)
                .h(px(44.))
                .px_4()
                .border_b_2()
                .border_color(if selected {
                    colors.text_accent
                } else {
                    gpui::transparent_black()
                })
                .when_some(target, |this, target| {
                    this.cursor_pointer()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            if let Some(state) = this.redis_state() {
                                state.tab = target;
                            }
                            cx.notify();
                        }))
                })
                .when(target.is_none(), |this| {
                    this.tooltip(Tooltip::text("Not in Vilo yet"))
                })
                .child(Label::new(label).size(LabelSize::Large).color(if selected {
                    Color::Default
                } else if target.is_none() {
                    Color::Disabled
                } else {
                    Color::Muted
                }))
        };
        let stat = |icon: IconName, text: String, tip: &'static str| {
            h_flex()
                .id(tip)
                .gap_1p5()
                .px_3()
                .border_r_1()
                .border_color(colors.border)
                .tooltip(Tooltip::text(tip))
                .child(Icon::new(icon).size(IconSize::Small).color(Color::Muted))
                .child(Label::new(text))
        };
        let stats = &state.stats;
        let body = match state.tab {
            RedisTab::Browse => self.render_browse(state, cx),
            RedisTab::Workbench => self.render_workbench(state, cx),
        };
        let cli_lines = Self::workbench_lines(state);
        v_flex()
            .size_full()
            .child(
                h_flex()
                    .h(px(52.))
                    .px_4()
                    .gap_2()
                    .border_b_1()
                    .border_color(colors.border)
                    .child(Label::new("Databases").color(Color::Accent).underline())
                    .child(Label::new("/").color(Color::Muted))
                    .child(Label::new(connection.name.clone()))
                    .child(
                        Label::new(format!(
                            "{} · {}",
                            connection.site_label(),
                            connection.environment
                        ))
                        .size(LabelSize::Small)
                        .color(if connection.production() {
                            Color::Warning
                        } else {
                            Color::Muted
                        }),
                    )
                    .child(div().w(px(8.)))
                    .child(self.render_database_picker(state, cx))
                    .child(
                        IconButton::new("database-redis-info-button", IconName::Info)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Database information (INFO)"))
                            .on_click(cx.listener(|this, _, _, cx| this.show_info(cx))),
                    )
                    .child(
                        IconButton::new("database-redis-topology-button", IconName::Server)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Topology: cluster nodes and slots, or replication"))
                            .on_click(cx.listener(|this, _, _, cx| this.show_topology(cx))),
                    )
                    .child({
                        let writable = self.writes_allowed(connection);
                        Button::new("database-redis-writes", if writable { "Writes on" } else { "Read only" })
                            .label_size(LabelSize::Small)
                            .style(if writable { ButtonStyle::Tinted(ui::TintColor::Warning) } else { ButtonStyle::Outlined })
                            .start_icon(Icon::new(if writable { IconName::LockOff } else { IconName::Lock }).size(IconSize::XSmall))
                            .tooltip(Tooltip::text(if crate::mongo_write::guarded(connection) {
                                "Production (or no environment): writes switch off again when Vilo restarts, and each write needs the connection name typed."
                            } else {
                                "Allow changing keys on this connection."
                            }))
                            .on_click(cx.listener(|this, _, _, cx| this.toggle_writes(cx)))
                    })
                    .child(div().flex_1())
                    .child(stat(
                        IconName::Sparkle,
                        stats
                            .cpu_percent
                            .map_or("Calculating…".into(), |cpu| format!("{cpu:.0} %")),
                        "CPU",
                    ))
                    .child(stat(
                        IconName::ArrowCircle,
                        stats
                            .ops_per_second
                            .map_or("…".into(), |ops| ops.to_string()),
                        "Commands per second",
                    ))
                    .child(stat(
                        IconName::DatabaseZap,
                        stats.memory.clone().unwrap_or_else(|| "…".into()),
                        "Memory used",
                    ))
                    .child(stat(
                        IconName::Hash,
                        stats.keys.map_or("…".into(), short_number),
                        "Total keys",
                    ))
                    .child(stat(
                        IconName::Person,
                        stats
                            .clients
                            .map_or("…".into(), |clients| clients.to_string()),
                        "Connected clients",
                    ))
                    .child(div().flex_1()),
            )
            .child(
                h_flex()
                    .px_4()
                    .border_b_1()
                    .border_color(colors.border)
                    .child(div().flex_1())
                    .child(tab(
                        "database-redis-tab-browse",
                        "Browse",
                        Some(RedisTab::Browse),
                        cx,
                    ))
                    .child(tab("database-redis-tab-search", "Search", None, cx))
                    .child(tab(
                        "database-redis-tab-workbench",
                        "Workbench",
                        Some(RedisTab::Workbench),
                        cx,
                    ))
                    .child(tab("database-redis-tab-analyze", "Analyze", None, cx))
                    .child(tab("database-redis-tab-pubsub", "Pub/Sub", None, cx))
                    .child(div().flex_1())
                    .child(
                        Button::new("database-redis-bulk", "Bulk actions")
                            .start_icon(Icon::new(IconName::Copy).size(IconSize::Small))
                            .disabled(true)
                            .tooltip(Tooltip::text(
                                "Bulk actions come with writes, in the next release",
                            )),
                    )
                    .child(
                        Button::new("database-redis-add-key", "Add key")
                            .style(ButtonStyle::Filled)
                            .disabled(true)
                            .tooltip(Tooltip::text(
                                "Adding keys comes with writes, in the next release",
                            )),
                    ),
            )
            .when_some(self.write_notice.clone(), |this, notice| {
                this.child(
                    h_flex()
                        .mx_4()
                        .mt_2()
                        .p_2()
                        .gap_2()
                        .rounded_md()
                        .bg(cx.theme().status().warning_background)
                        .child(Icon::new(IconName::Warning).size(IconSize::Small).color(Color::Warning))
                        .child(Label::new(notice).size(LabelSize::Small)),
                )
            })
            .child(div().flex_1().min_h_0().child(body))
            .when(state.cli_open, |this| {
                this.child(
                    v_flex()
                        .h(px(220.))
                        .p_2()
                        .gap_2()
                        .border_t_1()
                        .border_color(colors.border)
                        .child(if cli_lines.is_empty() {
                            Label::new("Read-only CLI. Try INFO server or HGETALL user:1.")
                                .size(LabelSize::Small)
                                .color(Color::Muted)
                                .into_any_element()
                        } else {
                            render_lines("database-redis-cli", cli_lines, &state.cli_scroll, cx)
                        })
                        .child(self.render_command_input(cx)),
                )
            })
            .child(
                h_flex()
                    .h(px(30.))
                    .px_3()
                    .gap_3()
                    .border_t_1()
                    .border_color(colors.border)
                    .child(
                        Button::new("database-redis-cli-toggle", "CLI")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::Terminal).size(IconSize::XSmall))
                            .style(if state.cli_open {
                                ButtonStyle::Filled
                            } else {
                                ButtonStyle::Subtle
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.cli_open = !state.cli_open;
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("database-redis-helper", "Command Helper")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::FileCode).size(IconSize::XSmall))
                            .disabled(true),
                    )
                    .child(
                        Button::new("database-redis-profiler", "Profiler")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::Rerun).size(IconSize::XSmall))
                            .disabled(true)
                            .tooltip(Tooltip::text(
                                "The profiler uses MONITOR, which loads the server; not in Vilo",
                            )),
                    ),
            )
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
    fn test_formats_numbers_and_info() {
        assert_eq!(escape_bytes(b"a\\x00"), "a\\\\x00");
        assert_ne!(escape_bytes(b"a\\x00"), escape_bytes(b"a\x00"));
        assert_eq!(formatted(br#"{"a":1}"#, Format::Json), "{\n  \"a\": 1\n}");
        assert_eq!(formatted(b"\xff", Format::Unicode), "\\xff");
        assert_eq!(formatted(b"\x01\xab", Format::Hex), "01 ab");
        assert_eq!(formatted(b"\x05", Format::Binary), "00000101");
        assert_eq!(describe_ttl(-1), "No limit");
        assert_eq!(describe_ttl(90_000), "1 min");
        assert_eq!(group_thousands(36_300), "36 300");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(short_number(36_300), "36 K");
        let info = "# Server\r\nredis_version:7.4.0\r\n\r\n# Clients\r\nconnected_clients:3\r\n";
        assert_eq!(
            info_sections(info)[1],
            (
                "Clients".to_string(),
                vec![("connected_clients".to_string(), "3".to_string())]
            )
        );
        assert_eq!(info_value(info, "connected_clients"), Some("3"));
    }
}
