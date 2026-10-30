//! Redis, laid out like RedisInsight: a top bar with live server stats, Browse / Search / Workbench / Analyze /
//! Pub/Sub tabs, keys as a tree or a list with type badges, TTL and size, key details by type, and a CLI bar.
//! Writes (Add key, the key's Edit menu, row edit and remove) go through redis_write's confirm step.

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
use vilo_database_core::formats::Decoder;
use vilo_database_core::redis::{
    Bytes, ClusterNode, KeyInfo, RedisWrite, ScanNode, StreamEntry, format_reply, quote_bytes,
    split_command,
};
use vilo_database_core::redis_docs::{self, CommandDoc};

use crate::{Connection, DatabaseView, Fetch, fetched, render_lines, run};
use util::ResultExt as _;

/// Keys asked for per SCAN call, and keys walked per "Scan more", as RedisInsight does.
const SCAN_COUNT: u32 = 200;
const SCAN_ROUND: u64 = 10_000;
/// Keys whose type, TTL and size are fetched together.
const META_BATCH: usize = 200;
const PAGE: u32 = 200;
const ROW_HEIGHT: f32 = 28.;
const STATS_EVERY: Duration = Duration::from_secs(10);
pub(crate) const KEY_TYPES: [(&str, &str); 8] = [
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
    Analyze,
    PubSub,
    Search,
}

/// One hash field, list item or member of the shown value, which its row can edit or remove.
#[derive(Clone)]
enum RowTarget {
    HashField { field: Bytes, value: Bytes },
    ListItem { index: i64, value: Bytes },
    SetMember(Bytes),
    ZSetMember { member: Bytes, score: String },
    StreamEntry(String),
}

/// An edit a row button starts: the operation, its prefilled inputs, and the list item shown.
type RowWrite = (crate::redis_write::RedisOp, Vec<Bytes>, Option<Bytes>);

impl RowTarget {
    fn edit(&self) -> Option<RowWrite> {
        use crate::redis_write::RedisOp;
        Some(match self {
            RowTarget::HashField { field, value } => {
                (RedisOp::HashSet, vec![field.clone(), value.clone()], None)
            }
            RowTarget::ListItem { index, value } => (
                RedisOp::ListSet { index: *index },
                vec![value.clone()],
                Some(value.clone()),
            ),
            // A member is its own identity; changing it is removing one and adding another.
            // A stream entry cannot be changed at all.
            RowTarget::SetMember(_) | RowTarget::StreamEntry(_) => return None,
            RowTarget::ZSetMember { member, score } => (
                RedisOp::ZSetScore,
                vec![member.clone(), score.clone().into_bytes()],
                None,
            ),
        })
    }

    /// The removal, built from the row's exact bytes; a hash field goes only if it still holds
    /// the value shown.
    fn remove(&self, key: Bytes) -> (crate::redis_write::RedisOp, RedisWrite) {
        use crate::redis_write::RedisOp;
        match self.clone() {
            RowTarget::HashField { field, value } => (
                RedisOp::HashDelete,
                RedisWrite::HashDelete {
                    key,
                    field,
                    expected: value,
                },
            ),
            RowTarget::ListItem { index, value } => (
                RedisOp::ListRemoveAt { index },
                RedisWrite::ListRemoveAt {
                    key,
                    index,
                    expected: value,
                },
            ),
            RowTarget::SetMember(member) => {
                (RedisOp::SetRemove, RedisWrite::SetRemove { key, member })
            }
            RowTarget::ZSetMember { member, .. } => {
                (RedisOp::ZSetRemove, RedisWrite::ZSetRemove { key, member })
            }
            RowTarget::StreamEntry(id) => {
                (RedisOp::StreamRemove, RedisWrite::StreamDelete { key, id })
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Unicode,
    Ascii,
    Hex,
    Binary,
    Json,
    Decoded(Decoder),
}

impl Format {
    fn label(self) -> &'static str {
        match self {
            Format::Unicode => "Unicode",
            Format::Ascii => "ASCII",
            Format::Hex => "HEX",
            Format::Binary => "Binary",
            Format::Json => "JSON",
            Format::Decoded(Decoder::Msgpack) => "Msgpack",
            Format::Decoded(Decoder::Php) => "PHP serialized",
            Format::Decoded(Decoder::Pickle) => "Pickle",
            Format::Decoded(Decoder::Java) => "Java serialized",
            Format::Decoded(Decoder::Float32Vector) => "Vector 32-bit",
            Format::Decoded(Decoder::Float64Vector) => "Vector 64-bit",
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
    /// A sorted set searched with ZSCAN MATCH, in the server's scan order.
    SortedSetMatch {
        members: Vec<(Bytes, String)>,
        cursor: Option<String>,
    },
    Stream {
        entries: Vec<StreamEntry>,
        /// The groups, or why they could not be read (shown, never taken as none).
        groups: Result<Vec<Vec<(String, String)>>, String>,
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
    /// The MATCH pattern a hash, set or sorted set was scanned with, across the whole key.
    pattern: Option<Bytes>,
    /// Text a list's loaded items are filtered by; lists have no server-side search.
    local_filter: Option<String>,
    /// Why the last "Load more" failed.
    more_error: Option<SharedString>,
    /// The load that produced this detail, so later pages join only their own search.
    request: u64,
    /// A "Load more" is running; auto refresh waits for it.
    loading_more: bool,
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
    pub(crate) tab: RedisTab,
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
    /// The key last opened and its search, kept through a failed load so a reload repeats it.
    detail_search: Option<(Bytes, Option<Bytes>, Option<String>)>,
    format: Format,
    cli_open: bool,
    helper_open: bool,
    profiler_open: bool,
    helper_group: Option<&'static str>,
    /// The command picked from the helper's list; with none, the helper follows what is typed.
    helper_selected: Option<&'static CommandDoc>,
    helper_scroll: UniformListScrollHandle,
    pub(crate) workbench: Vec<(String, Result<Vec<String>, String>)>,
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
    /// Bumped by every key load; a load or "Load more" answering an older one is dropped.
    detail_request: u64,
    /// Seconds between automatic refreshes of the key list and the open key; `None` is off.
    auto_refresh: Option<u64>,
    _auto_refresh: Option<Task<()>>,
    pub(crate) analyze: crate::redis_analyze::AnalyzeState,
    pub(crate) pubsub: crate::redis_pubsub::PubSubState,
    pub(crate) search: crate::redis_search::SearchState,
    pub(crate) profiler: crate::redis_profiler::ProfilerState,
    /// The consumer group opened under a stream key.
    pub(crate) stream_group: Option<crate::redis_stream::StreamGroupView>,
    /// A database number typed in when the menu cannot know how many there are.
    database_input: Option<Entity<editor::Editor>>,
}

impl RedisState {
    pub(crate) fn switching_database(&self) -> bool {
        self.switching.is_some()
    }

    pub(crate) fn delimiter(&self) -> &str {
        &self.delimiter
    }

    pub(crate) fn clear_helper_pick(&mut self) {
        self.helper_selected = None;
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
            detail_search: None,
            format: Format::Unicode,
            cli_open: false,
            helper_open: false,
            profiler_open: false,
            helper_group: None,
            helper_selected: None,
            helper_scroll: UniformListScrollHandle::new(),
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
            detail_request: 0,
            auto_refresh: None,
            _auto_refresh: None,
            analyze: Default::default(),
            pubsub: Default::default(),
            search: Default::default(),
            profiler: Default::default(),
            stream_group: None,
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

pub(crate) fn describe_size(bytes: u64) -> String {
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

pub(crate) fn badge(kind: &str) -> AnyElement {
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

/// The next place in a history of `count` commands: older from the newest, newer back to empty.
fn history_position(position: Option<usize>, count: usize, older: bool) -> Option<usize> {
    let last = count.checked_sub(1)?;
    match (position, older) {
        (None, true) => Some(last),
        (None, false) => None,
        (Some(position), true) => Some(position.min(last).saturating_sub(1)),
        (Some(position), false) if position < last => Some(position + 1),
        (Some(_), false) => None,
    }
}

/// Commands kept in the Workbench history.
const COMMAND_HISTORY: usize = 200;

fn command_history_path() -> std::path::PathBuf {
    paths::config_dir().join("database_redis_history.json")
}

/// The saved history; a missing file is an empty history, an unreadable one is an error.
pub(crate) fn load_command_history() -> anyhow::Result<Vec<String>> {
    match std::fs::read(command_history_path()) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

/// Appends one command to the history file and returns the file's history: read, add, write,
/// all under a file lock, so windows and other Vilo processes add to each other's history
/// instead of replacing it.
fn append_history(path: &std::path::Path, line: &str, limit: usize) -> anyhow::Result<Vec<String>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // A separate lock file, because the history file itself is replaced by the rename.
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    let mut history: Vec<String> = match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    if history.last().map(String::as_str) != Some(line) {
        history.push(line.to_string());
    }
    let excess = history.len().saturating_sub(limit);
    history.drain(..excess);
    let temporary = path.with_extension("tmp");
    let written = std::fs::write(&temporary, serde_json::to_vec_pretty(&history)?)
        .and_then(|()| std::fs::rename(&temporary, path));
    if let Err(error) = written {
        std::fs::remove_file(&temporary).log_err();
        return Err(error.into());
    }
    Ok(history)
}

/// Completes the command name (the first word) from the read commands the Workbench accepts.
pub(crate) struct CommandCompletions;

/// Command names starting with what was typed, case ignored, upper case as Redis writes them.
fn command_candidates(typed: &str) -> Vec<String> {
    let typed = typed.to_uppercase();
    vilo_database_core::redis::read_commands()
        .iter()
        .map(|command| command.to_uppercase())
        .filter(|command| command.starts_with(&typed))
        .collect()
}

impl editor::CompletionProvider for CommandCompletions {
    fn completions(
        &self,
        buffer: &gpui::Entity<language::Buffer>,
        buffer_position: language::Anchor,
        _trigger: editor::CompletionContext,
        _window: &mut gpui::Window,
        cx: &mut Context<editor::Editor>,
    ) -> Task<anyhow::Result<Vec<project::CompletionResponse>>> {
        use language::ToOffset as _;
        let buffer = buffer.read(cx);
        let offset = buffer_position.to_offset(buffer);
        let before = buffer.text_for_range(0..offset).collect::<String>();
        // Only the command name is completed; arguments are free text.
        if before.trim_start().contains(char::is_whitespace) {
            return Task::ready(Ok(Vec::new()));
        }
        let word = before.trim_start();
        let replace_range = buffer.anchor_before(offset - word.len())..buffer_position;
        Task::ready(Ok(vec![project::CompletionResponse {
            completions: command_candidates(word)
                .into_iter()
                .map(|text| project::Completion {
                    replace_range: replace_range.clone(),
                    label: language::CodeLabel::plain(text.clone(), None),
                    new_text: text,
                    documentation: None,
                    source: project::CompletionSource::Custom,
                    icon_path: None,
                    icon_color: None,
                    match_start: None,
                    snippet_deduplication_key: None,
                    insert_text_mode: None,
                    confirm: None,
                    group: None,
                })
                .collect(),
            display_options: project::CompletionDisplayOptions::default(),
            is_incomplete: false,
        }]))
    }

    fn is_completion_trigger(
        &self,
        _buffer: &gpui::Entity<language::Buffer>,
        _position: language::Anchor,
        text: &str,
        _trigger_in_words: bool,
        _cx: &mut Context<editor::Editor>,
    ) -> bool {
        text.chars()
            .last()
            .is_some_and(|character| character.is_ascii_alphabetic())
    }
}

/// Commands that recreate the key as loaded, for pasting into redis-cli, and whether only part
/// of the key is in them. `None` for types Vilo cannot rebuild or an empty value.
fn rebuild_command(key: &[u8], value: &Value, ttl_ms: i64) -> Option<(String, bool)> {
    let key = quote_bytes(key);
    let join = |items: Vec<String>| items.join(" ");
    let (command, more) = match value {
        Value::String(Some(bytes)) => (format!("SET {key} {}", quote_bytes(bytes)), false),
        Value::Hash { fields, cursor } if !fields.is_empty() => (
            format!(
                "HSET {key} {}",
                join(
                    fields
                        .iter()
                        .map(|(field, value)| format!(
                            "{} {}",
                            quote_bytes(field),
                            quote_bytes(value)
                        ))
                        .collect()
                )
            ),
            cursor.is_some(),
        ),
        Value::List { items, next } if !items.is_empty() => (
            format!(
                "RPUSH {key} {}",
                join(items.iter().map(|item| quote_bytes(item)).collect())
            ),
            next.is_some(),
        ),
        Value::Set { members, cursor } if !members.is_empty() => (
            format!(
                "SADD {key} {}",
                join(members.iter().map(|member| quote_bytes(member)).collect())
            ),
            cursor.is_some(),
        ),
        Value::SortedSet { members, next } if !members.is_empty() => (
            format!(
                "ZADD {key} {}",
                join(
                    members
                        .iter()
                        .map(|(member, score)| format!("{score} {}", quote_bytes(member)))
                        .collect()
                )
            ),
            next.is_some(),
        ),
        Value::SortedSetMatch { members, cursor } if !members.is_empty() => (
            format!(
                "ZADD {key} {}",
                join(
                    members
                        .iter()
                        .map(|(member, score)| format!("{score} {}", quote_bytes(member)))
                        .collect()
                )
            ),
            cursor.is_some(),
        ),
        _ => return None,
    };
    let mut text = command;
    if ttl_ms > 0 {
        text.push_str(&format!("\nPEXPIRE {key} {ttl_ms}"));
    }
    Some((text, more))
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
        // Read for its structure only; a value in another form says why and shows as text.
        Format::Decoded(decoder) => vilo_database_core::formats::decode(decoder, bytes)
            .unwrap_or_else(|error| {
                // The reason and the text after it are bounded like a decoded value.
                const SHOWN: usize = 1_000_000;
                let shown = &bytes[..bytes.len().min(SHOWN)];
                let mut text =
                    String::from_utf8(shown.to_vec()).unwrap_or_else(|_| escape_bytes(shown));
                let mut cut = bytes.len() > SHOWN;
                if text.len() > SHOWN {
                    let end = (0..=SHOWN)
                        .rev()
                        .find(|end| text.is_char_boundary(*end))
                        .unwrap_or(0);
                    text.truncate(end);
                    cut = true;
                }
                let reason = error.chars().take(500).collect::<String>();
                let rest = if cut {
                    format!("\n… cut at {} KB", SHOWN / 1000)
                } else {
                    String::new()
                };
                format!("({reason})\n{text}{rest}")
            }),
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
        let editing = self.redis_write.is_some()
            || self.admin_write.is_some()
            || self.bulk_delete.is_some()
            || self.workbench_run.is_some();
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
                        // A run keeps its own session; anything not yet sent belongs to the old one.
                        if this
                            .bulk_delete
                            .as_ref()
                            .is_some_and(|form| form.closable())
                        {
                            this.bulk_delete = None;
                        }
                        if this
                            .workbench_run
                            .as_ref()
                            .is_some_and(|run| run.closable())
                        {
                            this.workbench_run = None;
                        }
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

    /// Searches the open key: hash, set and sorted set across the whole key on the server; a list
    /// only among its loaded items, as lists have no server-side search.
    fn search_key_values(&mut self, cx: &mut Context<Self>) {
        let text = self.value_search.read(cx).text(cx);
        let Some(state) = self.redis_state() else {
            return;
        };
        let Some(Fetch::Loaded(detail)) = &mut state.detail else {
            return;
        };
        let key = detail.key.clone();
        if detail.info.kind == "list" {
            detail.local_filter = (!text.trim().is_empty()).then(|| text.trim().to_string());
            detail.more_error = None;
            state.detail_search = Some((key, None, detail.local_filter.clone()));
            cx.notify();
            return;
        }
        let pattern = vilo_database_core::redis::search_pattern(&text);
        self.open_key_matching(key, pattern, None, cx);
    }

    /// Refreshes the key list and the open key every `seconds`, skipping a round while an edit is
    /// open, a scan or database switch is running, or the open key is being searched.
    fn set_auto_refresh(&mut self, seconds: Option<u64>, cx: &mut Context<Self>) {
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.auto_refresh = seconds;
        state._auto_refresh = seconds.map(|seconds| {
            cx.spawn(async move |this, cx| {
                loop {
                    cx.background_executor()
                        .timer(std::time::Duration::from_secs(seconds))
                        .await;
                    let alive = this
                        .update(cx, |this, cx| {
                            if this.generation != connection {
                                return false;
                            }
                            let busy = this.redis_write.is_some()
                                || this.admin_write.is_some()
                                || this.bulk_delete.is_some()
                                || this.workbench_run.is_some();
                            let Some(state) = this.redis_state() else {
                                return false;
                            };
                            if busy || state.scanning || state.switching.is_some() {
                                return true;
                            }
                            // Only the key still on screen reloads, with its search, and not while a
                            // load or a page is running; a failed load is tried again.
                            let reload = matches!(state.panel, RightPanel::Key)
                                && match &state.detail {
                                    Some(Fetch::Loaded(detail)) => !detail.loading_more,
                                    Some(Fetch::Failed(_)) => true,
                                    _ => false,
                                };
                            let open = state.selected.clone().filter(|_| reload);
                            this.scan(true, cx);
                            if let Some(key) = open {
                                this.open_key(key, cx);
                            }
                            true
                        })
                        .unwrap_or(false);
                    if !alive {
                        break;
                    }
                }
            })
        });
        cx.notify();
    }

    pub(crate) fn close_redis_key(&mut self) {
        if let Some(state) = self.redis_state() {
            state.panel = RightPanel::Empty;
            state.selected = None;
            state.detail = None;
            state.detail_search = None;
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

    /// Opens the key, repeating its current search when it is the key already open.
    fn open_key(&mut self, key: Bytes, cx: &mut Context<Self>) {
        let search = self
            .redis_state()
            .and_then(|state| state.detail_search.clone())
            .filter(|(open, _, _)| *open == key);
        match search {
            Some((key, pattern, local_filter)) => {
                self.open_key_matching(key, pattern, local_filter, cx)
            }
            None => self.open_key_matching(key, None, None, cx),
        }
    }

    fn open_key_matching(
        &mut self,
        key: Bytes,
        pattern: Option<Bytes>,
        local_filter: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(state) = self.redis_state() else {
            return;
        };
        state.selected = Some(key.clone());
        state.panel = RightPanel::Key;
        state.detail_search = Some((key.clone(), pattern.clone(), local_filter.clone()));
        state.detail = Some(Fetch::Loading);
        state.detail_request += 1;
        let request = state.detail_request;
        let task = run(cx, {
            let key = key.clone();
            async move {
                let info = session.key_info(&key).await?;
                let length = session.length(&key, &info.kind).await.ok().flatten();
                let size = session.memory_usage(&key).await.ok().flatten();
                let value = match info.kind.as_str() {
                    "string" => Value::String(session.get_string(&key).await?),
                    "hash" => {
                        let (fields, cursor) = session
                            .hash_page(&key, "0", PAGE, pattern.as_deref())
                            .await?;
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
                        let (members, cursor) = session
                            .set_page(&key, "0", PAGE, pattern.as_deref())
                            .await?;
                        Value::Set {
                            members,
                            cursor: (cursor != "0").then_some(cursor),
                        }
                    }
                    "zset" if pattern.is_some() => {
                        let (members, cursor) = session
                            .zset_scan(&key, "0", PAGE, pattern.as_deref())
                            .await?;
                        Value::SortedSetMatch {
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
                        let groups = session
                            .stream_groups(&key)
                            .await
                            .map_err(|error| format!("{error:#}"));
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
                    pattern,
                    local_filter,
                    more_error: None,
                    request,
                    loading_more: false,
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
                    && state.detail_request == request
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
        let Some(Fetch::Loaded(detail)) = &mut state.detail else {
            return;
        };
        if detail.loading_more {
            return;
        }
        let key = detail.key.clone();
        let request = detail.request;
        enum Next {
            Hash(String),
            List(i64),
            Set(String),
            SortedSet(i64),
            SortedSetMatch(String),
            Stream(String),
        }
        let pattern = detail.pattern.clone();
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
            Value::SortedSetMatch {
                cursor: Some(cursor),
                ..
            } => Next::SortedSetMatch(cursor.clone()),
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
        detail.loading_more = true;
        enum More {
            Hash(Vec<(Bytes, Bytes)>, String),
            List(Vec<Bytes>, i64),
            Set(Vec<Bytes>, String),
            SortedSet(Vec<(Bytes, String)>, i64),
            SortedSetMatch(Vec<(Bytes, String)>, String),
            Stream(Vec<StreamEntry>),
        }
        let task = run(cx, {
            let key = key.clone();
            async move {
                Ok(match next {
                    Next::Hash(cursor) => {
                        let (fields, cursor) = session
                            .hash_page(&key, &cursor, PAGE, pattern.as_deref())
                            .await?;
                        More::Hash(fields, cursor)
                    }
                    Next::List(start) => More::List(
                        session
                            .list_range(&key, start, start + i64::from(PAGE) - 1)
                            .await?,
                        start,
                    ),
                    Next::Set(cursor) => {
                        let (members, cursor) = session
                            .set_page(&key, &cursor, PAGE, pattern.as_deref())
                            .await?;
                        More::Set(members, cursor)
                    }
                    Next::SortedSet(start) => More::SortedSet(
                        session
                            .zset_range(&key, start, start + i64::from(PAGE) - 1)
                            .await?,
                        start,
                    ),
                    Next::SortedSetMatch(cursor) => {
                        let (members, cursor) = session
                            .zset_scan(&key, &cursor, PAGE, pattern.as_deref())
                            .await?;
                        More::SortedSetMatch(members, cursor)
                    }
                    Next::Stream(after) => {
                        More::Stream(session.stream_range(&key, Some(&after), PAGE).await?)
                    }
                })
            }
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
                let Some(Fetch::Loaded(detail)) = &mut state.detail else {
                    return;
                };
                if detail.key != key || detail.request != request {
                    return;
                }
                detail.loading_more = false;
                let more = match result {
                    Ok(more) => {
                        detail.more_error = None;
                        more
                    }
                    Err(error) => {
                        detail.more_error = Some(format!("Could not load more: {error:#}").into());
                        cx.notify();
                        return;
                    }
                };
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
                    (
                        Value::SortedSetMatch { members, cursor },
                        More::SortedSetMatch(page, next),
                    ) => {
                        for member in page {
                            if !members.contains(&member) {
                                members.push(member);
                            }
                        }
                        *cursor = (next != "0").then_some(next);
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
        use vilo_database_core::redis_workbench::{Planned, plan};
        let planned = split_command(&line).and_then(plan);
        // Only reads are kept: history is saved to disk, and a write's values may be secrets.
        if matches!(planned, Ok(Planned::Read(_))) {
            self.remember_command(&line, cx);
        }
        match planned {
            Ok(Planned::Read(_)) => {}
            Ok(write @ Planned::Write(_)) => {
                let writable = self
                    .active_connection()
                    .is_some_and(|connection| self.writes_allowed(connection));
                if writable {
                    self.open_workbench_run(
                        "the Workbench".into(),
                        vec![(1, line, write)],
                        window,
                        cx,
                    );
                } else if let Some(state) = self.redis_state() {
                    state.workbench.push((
                        line,
                        Err("This is a write. Switch writes on (top bar) to run it.".into()),
                    ));
                    cx.notify();
                }
                return;
            }
            Err(error) => {
                if let Some(state) = self.redis_state() {
                    state.workbench.push((line, Err(format!("{error:#}"))));
                    cx.notify();
                }
                return;
            }
        }
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
                        (text, Color::Error) => (text, Color::Error),
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
                                .filter(|node| {
                                    !node.primary
                                        && primaries.iter().any(|primary| {
                                            Some(primary.id.as_str()) == node.primary_id.as_deref()
                                        })
                                })
                                .count(),
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
                                .on_click(move |_, window, cx| {
                                    let key = key.clone();
                                    view.update(cx, |this, cx| {
                                        // Another key's search would otherwise sit in the box and apply on Enter.
                                        if !is_selected {
                                            this.value_search.update(cx, |editor, cx| {
                                                editor.set_text("", window, cx)
                                            });
                                        }
                                        this.open_key(key, cx)
                                    })
                                    .ok();
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

    /// The value as text lines; the first line of each hash field, list item or member carries
    /// the row it starts, for the row's edit and remove buttons.
    fn value_lines(
        &self,
        detail: &Detail,
        format: Format,
    ) -> Vec<(SharedString, Option<RowTarget>)> {
        let show = |bytes: &[u8]| formatted(bytes, format);
        let mut lines = Vec::new();
        let mut push_row = |prefix: String, text: String, target: Option<RowTarget>| {
            let mut iter = text.lines();
            lines.push((
                SharedString::from(format!("{prefix}{}", iter.next().unwrap_or_default())),
                target,
            ));
            let pad = " ".repeat(prefix.chars().count());
            for line in iter {
                lines.push((SharedString::from(format!("{pad}{line}")), None));
            }
        };
        match &detail.value {
            Value::String(None) => {
                push_row(String::new(), "(the key no longer exists)".into(), None)
            }
            Value::String(Some(bytes)) => push_row(String::new(), show(bytes), None),
            Value::Hash { fields, .. } => {
                for (field, value) in fields {
                    push_row(
                        format!("{:<28} ", escape_bytes(field)),
                        show(value),
                        Some(RowTarget::HashField {
                            field: field.clone(),
                            value: value.clone(),
                        }),
                    );
                }
            }
            Value::List { items, .. } => {
                let filter = detail.local_filter.as_ref().map(|text| text.to_lowercase());
                for (index, item) in items.iter().enumerate() {
                    let matches = filter.as_ref().is_none_or(|filter| {
                        String::from_utf8_lossy(item)
                            .to_lowercase()
                            .contains(filter.as_str())
                    });
                    if matches {
                        push_row(
                            format!("{index:<8} "),
                            show(item),
                            Some(RowTarget::ListItem {
                                index: index as i64,
                                value: item.clone(),
                            }),
                        );
                    }
                }
            }
            Value::Set { members, .. } => {
                for member in members {
                    push_row(
                        String::new(),
                        show(member),
                        Some(RowTarget::SetMember(member.clone())),
                    );
                }
            }
            Value::SortedSet { members, .. } | Value::SortedSetMatch { members, .. } => {
                for (member, score) in members {
                    push_row(
                        format!("{score:<16} "),
                        show(member),
                        Some(RowTarget::ZSetMember {
                            member: member.clone(),
                            score: score.clone(),
                        }),
                    );
                }
            }
            Value::Stream { entries, .. } => {
                for entry in entries {
                    push_row(
                        String::new(),
                        entry.id.clone(),
                        Some(RowTarget::StreamEntry(entry.id.clone())),
                    );
                    for (field, value) in &entry.fields {
                        push_row(
                            format!("    {:<24} ", escape_bytes(field)),
                            show(value),
                            None,
                        );
                    }
                }
            }
            Value::Json(Some(json)) => push_row(
                String::new(),
                formatted(json.as_bytes(), Format::Json),
                None,
            ),
            Value::Json(None) => push_row(String::new(), "(no value)".into(), None),
            Value::Other => push_row(
                String::new(),
                format!(
                    "Vilo has no viewer for {} keys yet; use the CLI.",
                    detail.info.kind
                ),
                None,
            ),
        }
        lines
    }

    /// The value's lines, with edit and remove buttons on a row's first line when writes are on.
    fn render_value_rows(
        &self,
        key: Bytes,
        lines: Vec<(SharedString, Option<RowTarget>)>,
        writable: bool,
        scroll: &UniformListScrollHandle,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if !writable || lines.iter().all(|(_, target)| target.is_none()) {
            let lines = lines.into_iter().map(|(line, _)| line).collect();
            return render_lines("database-redis-value", lines, scroll, cx);
        }
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let widest = lines
            .iter()
            .enumerate()
            .max_by_key(|(_, (line, _))| line.len())
            .map(|(index, _)| index);
        let button = move |id: (&'static str, usize),
                           icon: IconName,
                           tip: &'static str,
                           target: RowTarget,
                           edit: bool,
                           key: Bytes,
                           view: gpui::WeakEntity<Self>| {
            IconButton::new(id, icon)
                .icon_size(IconSize::XSmall)
                .tooltip(Tooltip::text(tip))
                .visible_on_hover("database-redis-value-row")
                .on_click(move |_, window, cx| {
                    let (target, key) = (target.clone(), key.clone());
                    view.update(cx, |this, cx| match target.edit().filter(|_| edit) {
                        Some((op, prefill, shown)) => {
                            this.start_redis_write(op, key, prefill, shown, window, cx)
                        }
                        None => {
                            let (op, planned) = target.remove(key);
                            this.start_planned_redis_write(op, planned, window, cx)
                        }
                    })
                    .ok();
                })
        };
        let list =
            uniform_list("database-redis-value", lines.len(), move |range, _, cx| {
                range
                    .filter_map(|index| lines.get(index).cloned().map(|line| (index, line)))
                    .map(|(index, (line, target))| {
                        h_flex()
                            .group("database-redis-value-row")
                            .px_2()
                            .gap_1()
                            .whitespace_nowrap()
                            .hover(|style| style.bg(colors.element_hover))
                            .child(h_flex().w(px(44.)).flex_none().when_some(
                                target,
                                |this, target| {
                                    this.when(target.edit().is_some(), |this| {
                                        this.child(button(
                                            ("database-redis-row-edit", index),
                                            IconName::Pencil,
                                            "Edit",
                                            target.clone(),
                                            true,
                                            key.clone(),
                                            view.clone(),
                                        ))
                                    })
                                    .child(button(
                                        ("database-redis-row-remove", index),
                                        IconName::Trash,
                                        "Remove",
                                        target,
                                        false,
                                        key.clone(),
                                        view.clone(),
                                    ))
                                },
                            ))
                            .child(Label::new(line).size(LabelSize::Small).buffer_font(cx))
                            .into_any_element()
                    })
                    .collect::<Vec<_>>()
            })
            .with_horizontal_sizing_behavior(gpui::ListHorizontalSizingBehavior::Unconstrained)
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

    fn render_detail(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let close = IconButton::new("database-redis-close-detail", IconName::Close).on_click(
            cx.listener(|this, _, _, cx| {
                this.close_redis_key();
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
            Value::Hash { cursor, .. }
            | Value::Set { cursor, .. }
            | Value::SortedSetMatch { cursor, .. } => cursor.is_some(),
            Value::List { next, .. } | Value::SortedSet { next, .. } => next.is_some(),
            Value::Stream { more, .. } => *more,
            _ => false,
        };
        let header = match &detail.value {
            Value::Hash { .. } => Some(("Field", "Value")),
            Value::List { .. } => Some(("Index", "Element")),
            Value::Set { .. } => Some(("Member", "")),
            Value::SortedSet { .. } | Value::SortedSetMatch { .. } => Some(("Score", "Member")),
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
                        Format::Decoded(Decoder::Msgpack),
                        Format::Decoded(Decoder::Php),
                        Format::Decoded(Decoder::Pickle),
                        Format::Decoded(Decoder::Java),
                        Format::Decoded(Decoder::Float32Vector),
                        Format::Decoded(Decoder::Float64Vector),
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
            Value::Stream { groups, .. } => Some(groups.clone()),
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
                            Value::Json(Some(json)) => Some(
                                // Edited readable; the guard compares with the stored text, not this.
                                serde_json::from_str::<serde_json::Value>(json)
                                    .ok()
                                    .and_then(|value| serde_json::to_string_pretty(&value).ok())
                                    .unwrap_or_else(|| json.clone())
                                    .into_bytes(),
                            ),
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
                                                // Only a string or document edit starts from the value shown.
                                                let prefill = if matches!(
                                                    op,
                                                    crate::redis_write::RedisOp::EditString
                                                        | crate::redis_write::RedisOp::EditJson
                                                ) {
                                                    current.into_iter().collect()
                                                } else {
                                                    Vec::new()
                                                };
                                                this.start_redis_write(
                                                    op, key, prefill, None, window, cx,
                                                )
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
            .child(self.render_value_tools(detail, cx))
            .when_some(groups, |this, groups| {
                this.child(self.render_stream_groups(
                    &detail.key,
                    &groups,
                    state.stream_group.as_ref(),
                    writable,
                    cx,
                ))
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
            .child(self.render_value_rows(
                detail.key.clone(),
                self.value_lines(detail, state.format),
                writable,
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
            .when_some(detail.more_error.clone(), |this, error| {
                this.child(Label::new(error).size(LabelSize::Small).color(Color::Error))
            })
            .into_any_element()
    }

    fn render_auto_refresh(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let view = cx.entity().downgrade();
        let label = match state.auto_refresh {
            Some(seconds) => format!("Auto {seconds}s"),
            None => "Auto off".to_string(),
        };
        PopoverMenu::new("database-redis-auto-refresh")
            .trigger(
                Button::new("database-redis-auto-refresh-button", label)
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .tooltip(Tooltip::text(
                        "Rescan keys and reload the open key on a timer; each round is a full SCAN, so keep it off on large production databases",
                    )),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for (label, seconds) in [("Off", None), ("Every 5 seconds", Some(5)), ("Every 10 seconds", Some(10)), ("Every 30 seconds", Some(30))] {
                        let view = view.clone();
                        menu = menu.entry(label, None, move |_, cx| {
                            view.update(cx, |this, cx| this.set_auto_refresh(seconds, cx)).ok();
                        });
                    }
                    menu
                }))
            })
            .into_any_element()
    }

    /// Offline command documentation, as RedisInsight's Command Helper: search by name or
    /// summary, filter by group, and with nothing searched it shows the command being typed.
    fn render_command_helper(&self, state: &RedisState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let query = self.helper_search.read(cx).text(cx);
        let browsing = query.trim().is_empty() && state.helper_group.is_none();
        let shown = state.helper_selected.or_else(|| {
            browsing
                .then(|| redis_docs::command_for_line(&self.workbench_input.read(cx).text(cx)))
                .flatten()
        });
        let docs = redis_docs::command_docs();
        let group_menu = PopoverMenu::new("database-redis-helper-group")
            .trigger(
                Button::new(
                    "database-redis-helper-group-button",
                    state.helper_group.unwrap_or("All groups"),
                )
                .label_size(LabelSize::Small)
                .style(ButtonStyle::Subtle)
                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu({
                let view = view.clone();
                move |window, cx| {
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        for group in std::iter::once(None).chain(
                            docs.iter()
                                .flat_map(|docs| redis_docs::command_groups(docs))
                                .map(Some),
                        ) {
                            let view = view.clone();
                            menu = menu.entry(group.unwrap_or("All groups"), None, move |_, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(state) = this.redis_state() {
                                        state.helper_group = group;
                                        state.helper_selected = None;
                                    }
                                    cx.notify();
                                })
                                .ok();
                            });
                        }
                        menu
                    }))
                }
            });
        let body = match (docs, shown) {
            (Err(problem), _) => Label::new(problem)
                .size(LabelSize::Small)
                .color(Color::Error)
                .into_any_element(),
            (Ok(_), Some(doc)) => {
                Self::render_command_doc(doc, state.helper_selected.is_some(), view, cx)
            }
            (Ok(_), None) if browsing => Label::new(
                "Search for a command, pick a group, or type a command in the CLI to see its documentation.",
            )
            .size(LabelSize::Small)
            .color(Color::Muted)
            .into_any_element(),
            (Ok(docs), None) => {
                let found = redis_docs::search_commands(docs, &query, state.helper_group);
                if found.is_empty() {
                    Label::new("No command matches.")
                        .size(LabelSize::Small)
                        .color(Color::Muted)
                        .into_any_element()
                } else {
                    uniform_list("database-redis-helper-list", found.len(), move |range, _, cx| {
                        range
                            .filter_map(|index| found.get(index).map(|doc| (index, *doc)))
                            .map(|(index, doc)| {
                                let view = view.clone();
                                h_flex()
                                    .id(("database-redis-helper-row", index))
                                    .w_full()
                                    .h(px(ROW_HEIGHT))
                                    .px_2()
                                    .gap_2()
                                    .cursor_pointer()
                                    .hover(|style| style.bg(colors.element_hover))
                                    .on_click(move |_, _, cx| {
                                        view.update(cx, |this, cx| {
                                            if let Some(state) = this.redis_state() {
                                                state.helper_selected = Some(doc);
                                            }
                                            cx.notify();
                                        })
                                        .ok();
                                    })
                                    .child(
                                        Label::new(doc.name.clone())
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .color(Color::Accent),
                                    )
                                    .child(
                                        div().flex_1().min_w_0().child(
                                            Label::new(doc.summary.clone())
                                                .size(LabelSize::Small)
                                                .color(Color::Muted)
                                                .single_line()
                                                .truncate(),
                                        ),
                                    )
                            })
                            .collect()
                    })
                    .track_scroll(&state.helper_scroll)
                    .flex_1()
                    .into_any_element()
                }
            }
        };
        v_flex()
            .size_full()
            .p_2()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .flex_1()
                            .child(crate::boxed(self.helper_search.clone(), cx)),
                    )
                    .child(group_menu),
            )
            .child(v_flex().flex_1().min_h_0().child(body))
            .into_any_element()
    }

    fn render_command_doc(
        doc: &'static CommandDoc,
        picked: bool,
        view: gpui::WeakEntity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let words = doc
            .name
            .split_whitespace()
            .map(|word| Bytes::from(word.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let runs = vilo_database_core::redis::classify_words(&words)
            == vilo_database_core::redis::CommandKind::Read;
        let link = format!(
            "https://redis.io/docs/latest/commands/{}/",
            doc.name.to_lowercase().replace(' ', "-")
        );
        let facts = [
            ("Group", doc.group.as_str()),
            ("Since", doc.since.as_str()),
            ("Complexity", doc.complexity.as_str()),
        ];
        v_flex()
            .id("database-redis-helper-doc")
            .size_full()
            .gap_2()
            .overflow_y_scroll()
            .child(
                h_flex()
                    .gap_2()
                    .when(picked, |this| {
                        this.child(
                            IconButton::new("database-redis-helper-back", IconName::ArrowLeft)
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Back to the list"))
                                .on_click(move |_, _, cx| {
                                    view.update(cx, |this, cx| {
                                        if let Some(state) = this.redis_state() {
                                            state.helper_selected = None;
                                        }
                                        cx.notify();
                                    })
                                    .ok();
                                }),
                        )
                    })
                    .child(Label::new(doc.name.clone()).buffer_font(cx).color(Color::Accent))
                    .child(div().flex_1())
                    .child(
                        Button::new("database-redis-helper-link", "Read more")
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .end_icon(Icon::new(IconName::ArrowUpRight).size(IconSize::XSmall))
                            .on_click(move |_, _, cx| cx.open_url(&link)),
                    ),
            )
            .child(Label::new(doc.syntax.clone()).size(LabelSize::Small).buffer_font(cx))
            .child(Label::new(doc.summary.clone()).size(LabelSize::Small))
            .when_some(doc.deprecated_since.as_ref(), |this, since| {
                let replaced = doc
                    .replaced_by
                    .as_deref()
                    .filter(|replaced| !replaced.is_empty())
                    .map(|replaced| format!("; use {replaced} instead"))
                    .unwrap_or_default();
                this.child(
                    Label::new(format!("Deprecated since {since}{replaced}"))
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                )
            })
            .children(facts.into_iter().filter(|(_, value)| !value.is_empty()).map(
                |(name, value)| {
                    h_flex()
                        .gap_2()
                        .child(
                            Label::new(name)
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                        .child(Label::new(value.to_string()).size(LabelSize::Small))
                },
            ))
            // The docs cover every command; this says whether Vilo itself would send it.
            .child(
                Label::new(if runs {
                    "Vilo's CLI and Workbench run this read command."
                } else {
                    "Documentation only: Vilo's CLI and Workbench refuse this command (it writes or is risky)."
                })
                .size(LabelSize::Small)
                .color(if runs { Color::Success } else { Color::Muted }),
            )
            .into_any_element()
    }

    /// Value search and copying for the open key.
    fn render_value_tools(&self, detail: &Detail, cx: &mut Context<Self>) -> AnyElement {
        let searchable = matches!(detail.info.kind.as_str(), "hash" | "set" | "zset" | "list");
        // A list's filter only hides rows, so its command still holds every loaded item.
        let searched = detail.pattern.is_some();
        let rebuilt = rebuild_command(&detail.key, &detail.value, detail.info.ttl_ms).map(
            |(command, more)| {
                // A window that ends exactly at the end looks unfinished; the length says otherwise.
                let more = match (&detail.value, detail.length) {
                    (Value::List { items, .. }, Some(length)) => (items.len() as u64) < length,
                    (Value::SortedSet { members, .. }, Some(length)) => {
                        (members.len() as u64) < length
                    }
                    _ => more,
                };
                (command, searched, more)
            },
        );
        let filtered = detail.local_filter.is_some();
        let note = match (&detail.pattern, &detail.local_filter) {
            (Some(pattern), _) => Some(format!(
                "Matching {} across the whole key",
                quote_bytes(pattern)
            )),
            (None, Some(filter)) => Some(format!(
                "Showing loaded items containing “{filter}” (lists are filtered here, not on the server)"
            )),
            (None, None) => None,
        };
        // Bytes that are not text are copied escaped, never with replacement characters.
        let value_text = match &detail.value {
            Value::String(Some(bytes)) => Some(match String::from_utf8(bytes.clone()) {
                Ok(text) => ("Copy value", text),
                Err(_) => ("Copy value (escaped)", escape_bytes(bytes)),
            }),
            _ => None,
        };
        h_flex()
            .gap_2()
            .when(searchable, |this| {
                this.child(
                    div()
                        .w(px(300.))
                        .on_action(cx.listener(|this, _: &menu::Confirm, _, cx| {
                            this.search_key_values(cx)
                        }))
                        .child(crate::boxed(self.value_search.clone(), cx)),
                )
            })
            .when_some(note, |this, note| {
                this.child(Label::new(note).size(LabelSize::Small).color(Color::Muted))
            })
            .child(div().flex_1())
            .when_some(value_text, |this, (label, text)| {
                this.child(
                    Button::new("database-redis-copy-value", label)
                        .label_size(LabelSize::Small)
                        .start_icon(Icon::new(IconName::Copy).size(IconSize::XSmall))
                        .on_click(move |_, _, cx| {
                            cx.write_to_clipboard(ClipboardItem::new_string(text.clone()))
                        }),
                )
            })
            .when_some(rebuilt, |this, (command, searched, more)| {
                let (label, tip) = match (searched, more) {
                    (true, _) => (
                        "Copy as command (matching items only)",
                        "Only the items matching the search are in the command; it would not recreate the whole key",
                    ),
                    (false, true) => (
                        "Copy as command (loaded part only)",
                        "Only the items loaded so far are in the command; load the rest first for the whole key",
                    ),
                    (false, false) if filtered => (
                        "Copy as command (whole key, filter ignored)",
                        "The filter only hides rows; every item of the key is in the command",
                    ),
                    (false, false) => ("Copy as command", "Commands that recreate this key, for redis-cli"),
                };
                this.child(
                    Button::new("database-redis-copy-command", label)
                        .label_size(LabelSize::Small)
                        .start_icon(Icon::new(IconName::Copy).size(IconSize::XSmall))
                        .tooltip(Tooltip::text(tip))
                        .on_click(move |_, _, cx| {
                            cx.write_to_clipboard(ClipboardItem::new_string(command.clone()))
                        }),
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
                                    .child(self.render_auto_refresh(state, cx))
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

    /// Up and Down walk the command history; past the newest entry the input empties.
    fn step_history(&mut self, older: bool, window: &mut gpui::Window, cx: &mut Context<Self>) {
        if self.command_history.is_empty() {
            return;
        }
        let position = history_position(self.history_position, self.command_history.len(), older);
        self.history_position = position;
        let text = position
            .and_then(|position| self.command_history.get(position))
            .cloned()
            .unwrap_or_default();
        self.workbench_input
            .update(cx, |input, cx| input.set_text(text, window, cx));
    }

    fn remember_command(&mut self, line: &str, cx: &mut Context<Self>) {
        self.history_position = None;
        let line = line.to_string();
        let task = cx.background_spawn(async move {
            append_history(&command_history_path(), &line, COMMAND_HISTORY)
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                match result {
                    // The file holds every window's commands, so it becomes this window's history.
                    Ok(history) => {
                        this.command_history = history;
                        this.history_problem = None;
                    }
                    Err(error) => {
                        this.history_problem =
                            Some(format!("Command history not saved: {error:#}").into())
                    }
                }
                cx.notify();
            })
        })
        .detach_and_log_err(cx);
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
            .capture_action(
                cx.listener(|this, _: &zed_actions::editor::MoveUp, window, cx| {
                    this.step_history(true, window, cx)
                }),
            )
            .capture_action(
                cx.listener(|this, _: &zed_actions::editor::MoveDown, window, cx| {
                    this.step_history(false, window, cx)
                }),
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
            .when_some(self.history_problem.clone(), |this, problem| {
                this.child(Label::new(problem).size(LabelSize::Small).color(Color::Warning))
            })
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Label::new("Reads run at once. Writes from Vilo's reviewed list run after a confirm step, logged and sent once; KEYS, FLUSH*, CONFIG, MONITOR, scripts, functions and connection commands are refused before sending.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(div().flex_1())
                    .child(
                        Button::new("database-workbench-file", "Run a command file…")
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Outlined)
                            .on_click(cx.listener(|this, _, window, cx| this.pick_command_file(window, cx))),
                    ),
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
                        .on_click(cx.listener(move |this, _, window, cx| {
                            let analyze = this.redis_state().map(|state| {
                                if target != RedisTab::Analyze {
                                    state.analyze.leave_database_analysis();
                                }
                                if target != RedisTab::PubSub {
                                    state.pubsub.leave();
                                }
                                state.tab = target;
                                state.analyze.tab()
                            });
                            if let (RedisTab::Analyze, Some(analyze)) = (target, analyze) {
                                this.open_analyze_tab(analyze, window, cx);
                            }
                            if target == RedisTab::PubSub {
                                this.open_pubsub(window, cx);
                            }
                            if target == RedisTab::Search {
                                this.open_search(window, cx);
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
            RedisTab::Analyze => self.render_analyze(&state.analyze, cx),
            RedisTab::PubSub => self.render_pubsub(&state.pubsub, cx),
            RedisTab::Search => self.render_search(&state.search, cx),
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
                    .child(tab(
                        "database-redis-tab-search",
                        "Search",
                        Some(RedisTab::Search),
                        cx,
                    ))
                    .child(tab(
                        "database-redis-tab-workbench",
                        "Workbench",
                        Some(RedisTab::Workbench),
                        cx,
                    ))
                    .child(tab(
                        "database-redis-tab-analyze",
                        "Analyze",
                        Some(RedisTab::Analyze),
                        cx,
                    ))
                    .child(tab(
                        "database-redis-tab-pubsub",
                        "Pub/Sub",
                        Some(RedisTab::PubSub),
                        cx,
                    ))
                    .child(div().flex_1())
                    .child(
                        Button::new("database-redis-bulk", "Bulk delete")
                            .start_icon(Icon::new(IconName::Trash).size(IconSize::Small))
                            .disabled(!self.writes_allowed(connection) || state.switching_database())
                            .tooltip(Tooltip::text(if self.writes_allowed(connection) {
                                "Preview the keys a pattern matches, then delete exactly those"
                            } else {
                                "Switch writes on (top bar) to bulk delete"
                            }))
                            .on_click(cx.listener(|this, _, window, cx| this.open_bulk_delete(window, cx))),
                    )
                    .child({
                        let writable = self.writes_allowed(connection);
                        let view = cx.entity().downgrade();
                        PopoverMenu::new("database-redis-add-key")
                            .trigger(
                                Button::new("database-redis-add-key-button", "Add key")
                                    .style(ButtonStyle::Filled)
                                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                                    .disabled(!writable)
                                    .tooltip(Tooltip::text(if writable {
                                        "Create a key that does not exist yet"
                                    } else {
                                        "Switch writes on (top bar) to add keys"
                                    })),
                            )
                            .menu(move |window, cx| {
                                let view = view.clone();
                                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                    for (label, kind) in [
                                        ("String", "string"),
                                        ("Hash", "hash"),
                                        ("List", "list"),
                                        ("Set", "set"),
                                        ("Sorted Set", "zset"),
                                        ("JSON", "json"),
                                    ] {
                                        let view = view.clone();
                                        menu = menu.entry(label, None, move |window, cx| {
                                            view.update(cx, |this, cx| {
                                                this.start_redis_write(
                                                    crate::redis_write::RedisOp::AddKey(kind),
                                                    Bytes::new(),
                                                    Vec::new(),
                                                    None,
                                                    window,
                                                    cx,
                                                )
                                            })
                                            .ok();
                                        });
                                    }
                                    menu
                                }))
                            })
                    }),
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
            .when(state.cli_open || state.helper_open || state.profiler_open, |this| {
                this.child(
                    h_flex()
                        .h(px(220.))
                        .border_t_1()
                        .border_color(colors.border)
                        .when(state.cli_open, |this| {
                            this.child(
                                v_flex()
                                    .flex_1()
                                    .min_w_0()
                                    .h_full()
                                    .p_2()
                                    .gap_2()
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
                        .when(state.helper_open, |this| {
                            this.child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .h_full()
                                    .when(state.cli_open, |this| {
                                        this.border_l_1().border_color(colors.border)
                                    })
                                    .child(self.render_command_helper(state, cx)),
                            )
                        })
                        .when(state.profiler_open, |this| {
                            this.child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .h_full()
                                    .when(state.cli_open || state.helper_open, |this| {
                                        this.border_l_1().border_color(colors.border)
                                    })
                                    .child(self.render_profiler(&state.profiler, cx)),
                            )
                        }),
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
                            .style(if state.helper_open {
                                ButtonStyle::Filled
                            } else {
                                ButtonStyle::Subtle
                            })
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(state) = this.redis_state() {
                                    state.helper_open = !state.helper_open;
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("database-redis-profiler", "Profiler")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::Rerun).size(IconSize::XSmall))
                            .style(if state.profiler_open {
                                ButtonStyle::Filled
                            } else {
                                ButtonStyle::Subtle
                            })
                            .tooltip(Tooltip::text(
                                "MONITOR one node for a few seconds; closing the panel stops it",
                            ))
                            .on_click(cx.listener(|this, _, window, cx| {
                                let open = this.redis_state().map(|state| {
                                    state.profiler_open = !state.profiler_open;
                                    if !state.profiler_open {
                                        state.profiler.stop("Stopped: the panel was closed.");
                                    }
                                    state.profiler_open
                                });
                                if open == Some(true) {
                                    this.open_profiler(window, cx);
                                } else {
                                    this.clear_profiler_confirm(window, cx);
                                }
                                cx.notify();
                            })),
                    ),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_history_appends_from_every_window() {
        let folder = std::env::temp_dir().join(format!("vilo-history-{}", std::process::id()));
        std::fs::remove_dir_all(&folder).ok();
        let path = folder.join("history.json");
        // Two windows, each holding an older copy, add their own commands to the same file.
        assert_eq!(append_history(&path, "GET a", 3).unwrap(), ["GET a"]);
        assert_eq!(
            append_history(&path, "GET b", 3).unwrap(),
            ["GET a", "GET b"]
        );
        assert_eq!(
            append_history(&path, "GET b", 3).unwrap(),
            ["GET a", "GET b"],
            "repeats once"
        );
        append_history(&path, "GET c", 3).unwrap();
        assert_eq!(
            append_history(&path, "GET d", 3).unwrap(),
            ["GET b", "GET c", "GET d"]
        );
        std::fs::write(&path, b"not json").unwrap();
        assert!(
            append_history(&path, "GET e", 3).is_err(),
            "a damaged file is not overwritten"
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"not json");
        let leftovers = std::fs::read_dir(&folder)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "tmp")
            })
            .count();
        assert_eq!(leftovers, 0, "no temporary files are left");
        std::fs::remove_file(&path).unwrap();
        // Writers with their own file handles, as separate processes have, lose nothing.
        let writers = (0..8)
            .map(|writer| {
                let path = path.clone();
                std::thread::spawn(move || append_history(&path, &format!("GET {writer}"), 100))
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
        let history: Vec<String> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(history.len(), 8);
        std::fs::remove_dir_all(&folder).unwrap();
    }

    #[test]
    fn test_row_buttons_start_the_right_write() {
        use crate::redis_write::RedisOp;
        let item = RowTarget::ListItem {
            index: 3,
            value: b"a".to_vec(),
        };
        let (op, prefill, shown) = item.edit().unwrap();
        assert!(op == RedisOp::ListSet { index: 3 });
        assert_eq!((prefill, shown), (vec![b"a".to_vec()], Some(b"a".to_vec())));
        let (op, planned) = item.remove(b"k".to_vec());
        assert!(op == RedisOp::ListRemoveAt { index: 3 });
        assert_eq!(
            planned,
            RedisWrite::ListRemoveAt {
                key: b"k".to_vec(),
                index: 3,
                expected: b"a".to_vec(),
            }
        );
        assert!(RowTarget::SetMember(b"m".to_vec()).edit().is_none());
        let field = RowTarget::HashField {
            field: b"f".to_vec(),
            value: b"v".to_vec(),
        };
        assert!(field.edit().unwrap().0 == RedisOp::HashSet);
        assert_eq!(
            field.remove(b"k".to_vec()).1,
            RedisWrite::HashDelete {
                key: b"k".to_vec(),
                field: b"f".to_vec(),
                expected: b"v".to_vec(),
            },
            "a field goes only if it still holds the value shown"
        );
        let entry = RowTarget::StreamEntry("1-1".into());
        assert!(entry.edit().is_none(), "stream entries cannot be edited");
        assert_eq!(
            entry.remove(b"k".to_vec()).1,
            RedisWrite::StreamDelete {
                key: b"k".to_vec(),
                id: "1-1".into(),
            }
        );
        let binary = RowTarget::SetMember(b"\xff".to_vec());
        assert_eq!(
            binary.remove(b"k".to_vec()).1,
            RedisWrite::SetRemove {
                key: b"k".to_vec(),
                member: b"\xff".to_vec(),
            },
            "a member that is not text can still be removed"
        );
        let member = RowTarget::ZSetMember {
            member: b"m".to_vec(),
            score: "1.5".into(),
        };
        assert_eq!(
            member.edit().unwrap().1,
            vec![b"m".to_vec(), b"1.5".to_vec()]
        );
    }

    #[test]
    fn test_history_position() {
        assert_eq!(history_position(None, 0, true), None);
        assert_eq!(history_position(None, 3, true), Some(2));
        assert_eq!(history_position(Some(2), 3, true), Some(1));
        assert_eq!(history_position(Some(0), 3, true), Some(0));
        assert_eq!(history_position(Some(1), 3, false), Some(2));
        assert_eq!(history_position(Some(2), 3, false), None);
        assert_eq!(history_position(None, 3, false), None);
    }

    #[test]
    fn test_command_candidates() {
        let candidates = command_candidates("hge");
        assert!(candidates.contains(&"HGET".to_string()));
        assert!(candidates.contains(&"HGETALL".to_string()));
        assert!(candidates.iter().all(|command| command.starts_with("HGE")));
        // Writes are never offered.
        assert!(
            command_candidates("se")
                .iter()
                .all(|command| command != "SET")
        );
        assert!(command_candidates("fl").is_empty());
    }

    #[test]
    fn test_rebuild_command() {
        let (text, partial) =
            rebuild_command(b"k", &Value::String(Some(b"a b".to_vec())), 5000).unwrap();
        assert_eq!(text, "SET \"k\" \"a b\"\nPEXPIRE \"k\" 5000");
        assert!(!partial);
        let hash = Value::Hash {
            fields: vec![(b"f".to_vec(), b"v".to_vec())],
            cursor: Some("12".into()),
        };
        let (text, partial) = rebuild_command(b"h", &hash, -1).unwrap();
        assert_eq!(text, "HSET \"h\" \"f\" \"v\"");
        assert!(partial, "more pages remain");
        let zset = Value::SortedSet {
            members: vec![(b"m".to_vec(), "1.5".into())],
            next: None,
        };
        assert_eq!(
            rebuild_command(b"z", &zset, -1).unwrap(),
            ("ZADD \"z\" 1.5 \"m\"".into(), false)
        );
        assert_eq!(
            rebuild_command(
                b"l",
                &Value::List {
                    items: vec![b"x".to_vec()],
                    next: None
                },
                -1
            )
            .unwrap()
            .0,
            "RPUSH \"l\" \"x\""
        );
        assert!(
            rebuild_command(
                b"s",
                &Value::Set {
                    members: Vec::new(),
                    cursor: None
                },
                -1
            )
            .is_none()
        );
        assert!(rebuild_command(b"o", &Value::Other, -1).is_none());
    }

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
        assert_eq!(
            formatted(b"i:5;", Format::Decoded(Decoder::Php)),
            "5",
            "a decoder's text is shown"
        );
        assert!(
            formatted(b"plain", Format::Decoded(Decoder::Java)).ends_with(")\nplain"),
            "a value in another form says why, then shows as text"
        );
        let huge = vec![b'a'; 3_000_000];
        let shown = formatted(&huge, Format::Decoded(Decoder::Java));
        assert!(
            shown.len() < 1_100_000 && shown.ends_with("KB"),
            "the fallback text is bounded"
        );
        let escaped = formatted(&vec![0xff; 1_000_000], Format::Decoded(Decoder::Php));
        assert!(
            escaped.len() < 1_100_000 && escaped.ends_with("KB"),
            "escaping does not grow it past the limit"
        );
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
