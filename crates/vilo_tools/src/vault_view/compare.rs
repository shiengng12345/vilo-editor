use std::{collections::VecDeque, path::PathBuf, time::Duration};

use anyhow::{Context as _, Result};
use collections::{BTreeSet, HashSet};
use futures::{StreamExt as _, stream};
use gpui::{AsyncApp, Entity, Task, Window, prelude::*};
use editor::Editor;
use serde::{Deserialize, Serialize};
use ui::{
    Button, ButtonStyle, Checkbox, ContextMenu, IconButton, PopoverMenu, ToggleState, Tooltip,
    prelude::*,
};

use super::{Connection, VaultView, client_for, import::editor};
use crate::{
    vault::{Mount, SecretRead, VaultClient, VaultError, display_value},
    vault_compare::{Cell, ReportPath, ReportSide, Row, SideRead, cell_label, compare, markdown_report},
};

/// Folder scans stop here and report what they did not reach.
const MAX_PATHS: usize = 500;
const MAX_DEPTH: usize = 8;
const MAX_LISTS_PER_SIDE: usize = 300;
/// Requests in flight for one scan, across all sides.
const CONCURRENCY: usize = 4;
/// Spacing before each request, so a scan does not burst a shared Vault.
const PACING: Duration = Duration::from_millis(25);

/// A user's decision that a difference does not apply to one site and environment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Mark {
    site: String,
    environment: String,
    /// `mount/path/to/secret`.
    path: String,
    key: String,
}

struct CompareSide {
    connection_id: Option<String>,
    path: Entity<Editor>,
}

struct SideSummary {
    label: String,
    site: String,
    environment: String,
    address: String,
    /// What the user typed: a secret path, or a folder ending in `/`.
    input: String,
}

impl SideSummary {
    fn full_path(&self, relative: &str) -> String {
        if relative.is_empty() {
            self.input.clone()
        } else {
            format!("{}{relative}", self.input)
        }
    }
}

struct PathResult {
    /// Relative to each side's folder; empty when comparing single secrets.
    path: String,
    reads: Vec<SideRead>,
    versions: Vec<Option<u64>>,
    rows: Vec<Row>,
}

struct CompareResult {
    sides: Vec<SideSummary>,
    paths: Vec<PathResult>,
    coverage: Vec<String>,
    started_at: String,
    finished_at: String,
}

pub(super) struct CompareState {
    sides: Vec<CompareSide>,
    result: Option<CompareResult>,
    running: bool,
    only_differences: bool,
    expanded: HashSet<String>,
    marks: HashSet<Mark>,
    message: Option<SharedString>,
    generation: u64,
    _task: Option<Task<()>>,
}

fn marks_path() -> PathBuf {
    paths::config_dir().join("vault_not_applicable.json")
}

fn load_marks() -> HashSet<Mark> {
    std::fs::read(marks_path())
        .ok()
        .and_then(|bytes| {
            serde_json::from_slice::<Vec<Mark>>(&bytes)
                .map_err(|error| log::error!("ignoring unreadable not-applicable marks: {error}"))
                .ok()
        })
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn now() -> String {
    let now = time::OffsetDateTime::now_local().unwrap_or_else(|_| time::OffsetDateTime::now_utc());
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

/// Splits `mount/path` using the longest known mount. Unknown mounts are not
/// guessed: reading a KV v1 mount as v2 would report a misleading "missing".
fn resolve_path(mounts: &[Mount], input: &str, folder: bool) -> Option<(Mount, String)> {
    let trimmed = input.trim().trim_matches('/');
    let mount = mounts
        .iter()
        .filter(|mount| trimmed == mount.path || trimmed.starts_with(&format!("{}/", mount.path)))
        .max_by_key(|mount| mount.path.len())?
        .clone();
    let path = trimmed[mount.path.len()..].trim_matches('/').to_string();
    if path.is_empty() && !folder {
        return None;
    }
    let path = if folder && !path.is_empty() { format!("{path}/") } else { path };
    Some((mount, path))
}

struct PreparedSide {
    client: VaultClient,
    mount: Mount,
    /// Inside the mount: a secret path, or a folder ending in `/` (empty for the mount root).
    base: String,
}

async fn prepare_side(connection: Option<Connection>, input: &str, folder: bool, cx: &AsyncApp) -> Result<PreparedSide, String> {
    let connection = connection.ok_or("Pick a connection.")?;
    let client = client_for(&connection, cx).await?;
    let mounts = client
        .kv_mounts()
        .await
        .map_err(|error| format!("KV version unknown: could not list mounts ({error})"))?;
    let (mount, base) =
        resolve_path(&mounts, input, folder).ok_or("No KV mount matches this path on this Vault.")?;
    Ok(PreparedSide { client, mount, base })
}

/// Secret paths below the side's folder, relative to it, with notes on what was not reached.
async fn list_tree(side: &PreparedSide, cx: &AsyncApp) -> (Vec<String>, Vec<String>) {
    let mut found = Vec::new();
    let mut notes = Vec::new();
    let mut queue = VecDeque::from([(String::new(), 0)]);
    let mut lists = 0;
    while let Some((folder, depth)) = queue.pop_front() {
        if lists >= MAX_LISTS_PER_SIDE || found.len() >= MAX_PATHS {
            notes.push(format!("stopped after {lists} folders; {} folders not listed", queue.len() + 1));
            break;
        }
        lists += 1;
        cx.background_executor().timer(PACING).await;
        match side.client.list(&side.mount, &format!("{}{folder}", side.base)).await {
            Ok(children) => {
                for child in children {
                    if child.ends_with('/') {
                        if depth + 1 > MAX_DEPTH {
                            notes.push(format!("{folder}{child} is deeper than {MAX_DEPTH} levels; not listed"));
                        } else {
                            queue.push_back((format!("{folder}{child}"), depth + 1));
                        }
                    } else {
                        found.push(format!("{folder}{child}"));
                    }
                }
            }
            Err(VaultError::Forbidden) => notes.push(format!("no permission to list {}", if folder.is_empty() { "the folder" } else { &folder })),
            Err(error) => notes.push(format!("could not list {}: {error}", if folder.is_empty() { "the folder" } else { &folder })),
        }
    }
    (found, notes)
}

async fn read_path(side: &Result<PreparedSide, String>, relative: &str, cx: &AsyncApp) -> (Option<u64>, SideRead) {
    let side = match side {
        Ok(side) => side,
        Err(reason) => return (None, SideRead::Unreadable(reason.clone())),
    };
    cx.background_executor().timer(PACING).await;
    match side.client.read(&side.mount, &format!("{}{relative}", side.base), None).await {
        Ok(SecretRead::Found(secret)) => (
            secret.version,
            SideRead::Found {
                values: secret.values,
                version: secret.version,
            },
        ),
        Ok(SecretRead::Unavailable { version, state }) => (Some(version), SideRead::Unavailable(state)),
        // The exact path was read and Vault has nothing there.
        Err(VaultError::NotFound) => (None, SideRead::MissingSecret),
        Err(VaultError::Forbidden) => (None, SideRead::Unreadable("no permission".into())),
        Err(error) => (None, SideRead::Unreadable(error.to_string())),
    }
}

fn cell_color(cell: &Cell) -> Color {
    match cell {
        Cell::Present => Color::Default,
        Cell::Same => Color::Success,
        Cell::Different | Cell::Empty => Color::Warning,
        Cell::MissingKey | Cell::MissingSecret => Color::Error,
        Cell::Deleted | Cell::Destroyed | Cell::Unreadable(_) | Cell::NoBaseline => Color::Muted,
    }
}

impl VaultView {
    pub(super) fn open_compare(&mut self, start: Option<(String, String)>, window: &mut Window, cx: &mut Context<Self>) {
        let default_connection = start
            .as_ref()
            .map(|(id, _)| id.clone())
            .or_else(|| self.session.as_ref().map(|session| session.connection_id.clone()))
            .or_else(|| self.connections.first().map(|c| c.id.clone()));
        let (first_path, second_path) = match &start {
            Some((_, path)) => {
                let folder = path.rsplit_once('/').map(|(folder, _)| format!("{folder}/")).unwrap_or_default();
                (path.clone(), folder)
            }
            None => (String::new(), String::new()),
        };
        let placeholder = "mount/service/lane, or a folder ending in /";
        let sides = vec![
            CompareSide {
                connection_id: default_connection.clone(),
                path: editor(placeholder, &first_path, false, window, cx),
            },
            CompareSide {
                connection_id: default_connection,
                path: editor(placeholder, &second_path, false, window, cx),
            },
        ];
        self.form = None;
        self.import = None;
        self.revealed.clear();
        self.compare = Some(CompareState {
            sides,
            result: None,
            running: false,
            only_differences: true,
            expanded: HashSet::default(),
            marks: load_marks(),
            message: None,
            generation: 0,
            _task: None,
        });
        cx.notify();
    }

    fn run_compare(&mut self, cx: &mut Context<Self>) {
        let Some(state) = &mut self.compare else {
            return;
        };
        let jobs = state
            .sides
            .iter()
            .map(|side| {
                let connection = side
                    .connection_id
                    .as_ref()
                    .and_then(|id| self.connections.iter().find(|c| &c.id == id))
                    .cloned();
                (connection, side.path.read(cx).text(cx).trim().trim_start_matches('/').to_string())
            })
            .collect::<Vec<_>>();
        let folder = jobs.first().is_some_and(|(_, path)| path.ends_with('/'));
        if jobs.iter().any(|(_, path)| path.ends_with('/') != folder || path.is_empty()) {
            state.message = Some(
                "Give every side a path. Either all are secrets, or all are folders ending in /.".into(),
            );
            cx.notify();
            return;
        }
        state.generation += 1;
        let generation = state.generation;
        state.running = true;
        state.result = None;
        state.expanded.clear();
        state.message = Some("Preparing…".into());
        self.revealed.clear();
        let task = cx.spawn(async move |this, cx| {
            let started_at = now();
            let report = |this: &gpui::WeakEntity<VaultView>, cx: &mut AsyncApp, message: String| {
                this.update(cx, |this, cx| {
                    if let Some(state) = &mut this.compare
                        && state.generation == generation
                    {
                        state.message = Some(message.into());
                        cx.notify();
                    }
                })
                .ok();
            };
            let mut prepared = Vec::new();
            for (connection, path) in &jobs {
                prepared.push(prepare_side(connection.clone(), path, folder, cx).await);
            }
            let mut coverage = Vec::new();
            let relative_paths = if folder {
                report(&this, cx, "Listing folders…".into());
                let mut union = BTreeSet::new();
                for (index, side) in prepared.iter().enumerate() {
                    let role = if index == 0 { "baseline".to_string() } else { format!("side {index}") };
                    match side {
                        Ok(side) => {
                            let (paths, notes) = list_tree(side, cx).await;
                            union.extend(paths);
                            coverage.extend(notes.into_iter().map(|note| format!("{role}: {note}")));
                        }
                        Err(reason) => coverage.push(format!("{role}: not listed ({reason})")),
                    }
                }
                if union.len() > MAX_PATHS {
                    coverage.push(format!("{} secrets found; only the first {MAX_PATHS} were compared", union.len()));
                }
                union.into_iter().take(MAX_PATHS).collect::<Vec<_>>()
            } else {
                vec![String::new()]
            };

            let total = relative_paths.len() * prepared.len();
            let reads_in_order = relative_paths
                .iter()
                .enumerate()
                .flat_map(|(path_index, _)| (0..prepared.len()).map(move |side_index| (path_index, side_index)))
                .collect::<Vec<_>>();
            let mut results = vec![vec![(None, SideRead::Unreadable("not read".into())); prepared.len()]; relative_paths.len()];
            let reader = cx.clone();
            let mut stream = stream::iter(reads_in_order)
                .map(|(path_index, side_index)| {
                    let prepared = &prepared;
                    let relative_paths = &relative_paths;
                    let cx = reader.clone();
                    async move {
                        let result = read_path(&prepared[side_index], &relative_paths[path_index], &cx).await;
                        (path_index, side_index, result)
                    }
                })
                .buffer_unordered(CONCURRENCY);
            let mut done = 0;
            while let Some((path_index, side_index, result)) = stream.next().await {
                results[path_index][side_index] = result;
                done += 1;
                if done % 10 == 0 || done == total {
                    report(&this, cx, format!("Reading {done} of {total}…"));
                }
            }
            drop(stream);

            let paths = relative_paths
                .into_iter()
                .zip(results)
                .map(|(path, reads)| {
                    let (versions, reads): (Vec<_>, Vec<_>) = reads.into_iter().unzip();
                    let rows = compare(&reads);
                    PathResult { path, reads, versions, rows }
                })
                .collect::<Vec<_>>();
            let sides = jobs
                .iter()
                .map(|(connection, input)| SideSummary {
                    label: connection.as_ref().map_or("No connection".to_string(), Connection::label),
                    site: connection.as_ref().map(|c| c.site.clone()).unwrap_or_default(),
                    environment: connection.as_ref().map(|c| c.environment.clone()).unwrap_or_default(),
                    address: connection.as_ref().map(|c| c.address.clone()).unwrap_or_default(),
                    input: if folder { format!("{}/", input.trim_end_matches('/')) } else { input.clone() },
                })
                .collect();
            this.update(cx, |this, cx| {
                if let Some(state) = &mut this.compare
                    && state.generation == generation
                {
                    if paths.len() == 1 {
                        state.expanded.insert(paths[0].path.clone());
                    }
                    state.running = false;
                    state.message = None;
                    state.result = Some(CompareResult {
                        sides,
                        paths,
                        coverage,
                        started_at,
                        finished_at: now(),
                    });
                }
                cx.notify();
            })
            .ok();
        });
        if let Some(state) = &mut self.compare {
            state._task = Some(task);
        }
        cx.notify();
    }

    fn stop_compare(&mut self, cx: &mut Context<Self>) {
        if let Some(state) = &mut self.compare {
            // Dropping the task cancels the requests still queued.
            state._task = None;
            state.generation += 1;
            state.running = false;
            state.message = Some("Stopped. Nothing was compared.".into());
        }
        cx.notify();
    }

    fn mark_for(&self, side: usize, path: &str, key: &str) -> Option<Mark> {
        let result = self.compare.as_ref()?.result.as_ref()?;
        let summary = result.sides.get(side)?;
        Some(Mark {
            site: summary.site.clone(),
            environment: summary.environment.clone(),
            path: summary.full_path(path),
            key: key.to_string(),
        })
    }

    fn toggle_mark(&mut self, mark: Mark, cx: &mut Context<Self>) {
        let Some(state) = &mut self.compare else {
            return;
        };
        if !state.marks.remove(&mark) {
            state.marks.insert(mark);
        }
        let mut marks = state.marks.iter().cloned().collect::<Vec<_>>();
        marks.sort_by(|a, b| (&a.site, &a.environment, &a.path, &a.key).cmp(&(&b.site, &b.environment, &b.path, &b.key)));
        cx.background_spawn(async move {
            let path = marks_path();
            let temporary = path.with_extension("json.tmp");
            std::fs::write(&temporary, serde_json::to_vec_pretty(&marks)?)?;
            std::fs::rename(&temporary, &path).context("Could not save the not-applicable marks")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }

    fn export_report(&mut self, cx: &mut Context<Self>) {
        let Some(state) = &self.compare else {
            return;
        };
        let Some(result) = &state.result else {
            return;
        };
        let sides = result
            .sides
            .iter()
            .map(|side| ReportSide {
                label: side.label.clone(),
                address: side.address.clone(),
                path: side.input.clone(),
            })
            .collect::<Vec<_>>();
        let paths = result
            .paths
            .iter()
            .map(|path| ReportPath {
                path: &path.path,
                reads: &path.reads,
                versions: &path.versions,
                rows: &path.rows,
            })
            .collect::<Vec<_>>();
        let marks = &state.marks;
        let report = markdown_report(&result.started_at, &result.finished_at, &sides, &paths, &result.coverage, |path, side, key| {
            result.sides.get(side).is_some_and(|summary| {
                marks.contains(&Mark {
                    site: summary.site.clone(),
                    environment: summary.environment.clone(),
                    path: summary.full_path(path),
                    key: key.to_string(),
                })
            })
        });
        let target = cx.prompt_for_new_path(paths::home_dir(), Some("vault-comparison.md"));
        cx.spawn(async move |_, cx| -> Result<()> {
            let Ok(Ok(Some(path))) = target.await else {
                return Ok(());
            };
            cx.background_spawn(async move { std::fs::write(&path, report) })
                .await
                .context("Could not write the report")
        })
        .detach_and_log_err(cx);
    }

    fn render_rows(&self, state: &CompareState, result: &CompareResult, path: &PathResult, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let mut table = v_flex().border_t_1().border_color(colors.border_variant);
        let rows = path
            .rows
            .iter()
            .filter(|row| !state.only_differences || row.cells.iter().any(Cell::is_difference));
        for row in rows {
            let reveal_key = format!("compare:{}:{}", path.path, row.key);
            let revealed = self.revealed.contains(&reveal_key);
            let mark_options = row
                .cells
                .iter()
                .enumerate()
                .skip(1)
                .filter(|(_, cell)| cell.is_difference())
                .filter_map(|(index, _)| {
                    let mark = self.mark_for(index, &path.path, &row.key)?;
                    let marked = state.marks.contains(&mark);
                    let label = result.sides[index].label.clone();
                    Some((mark, marked, label))
                })
                .collect::<Vec<_>>();
            let view = cx.entity().downgrade();
            let mut line = h_flex()
                .px_3()
                .py_1()
                .gap_3()
                .border_t_1()
                .border_color(colors.border_variant)
                .child(
                    h_flex()
                        .w(px(280.))
                        .flex_none()
                        .gap_1()
                        .child(
                            IconButton::new(
                                SharedString::from(format!("compare-reveal-{reveal_key}")),
                                if revealed { IconName::EyeOff } else { IconName::Eye },
                            )
                            .icon_size(IconSize::XSmall)
                            .tooltip(Tooltip::text("Show the values read in this comparison"))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                if !this.revealed.remove(&reveal_key) {
                                    this.revealed.insert(reveal_key.clone());
                                }
                                cx.notify();
                            })),
                        )
                        .when(!mark_options.is_empty(), |this| {
                            this.child(
                                PopoverMenu::new(SharedString::from(format!("compare-mark-{}-{}", path.path, row.key)))
                                    .trigger(
                                        IconButton::new(
                                            SharedString::from(format!("compare-mark-button-{}-{}", path.path, row.key)),
                                            IconName::Ellipsis,
                                        )
                                        .icon_size(IconSize::XSmall),
                                    )
                                    .menu(move |window, cx| {
                                        let options = mark_options.clone();
                                        let view = view.clone();
                                        Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                            for (mark, marked, label) in options {
                                                let view = view.clone();
                                                let text = if marked {
                                                    format!("Undo “not applicable” on {label}")
                                                } else {
                                                    format!("Not applicable on {label}")
                                                };
                                                menu = menu.entry(text, None, move |_, cx| {
                                                    view.update(cx, |this, cx| this.toggle_mark(mark.clone(), cx)).ok();
                                                });
                                            }
                                            menu
                                        }))
                                    }),
                            )
                        })
                        .child(Label::new(row.key.clone()).size(LabelSize::Small).buffer_font(cx).truncate()),
                );
            for (index, cell) in row.cells.iter().enumerate() {
                let marked = index > 0
                    && self
                        .mark_for(index, &path.path, &row.key)
                        .is_some_and(|mark| state.marks.contains(&mark));
                // Values come from this comparison's reads, never a fresh fetch.
                let value = match &path.reads[index] {
                    SideRead::Found { values, .. } if revealed => values.get(&row.key).map(display_value),
                    _ => None,
                };
                let text = match (&value, marked) {
                    (Some(value), _) => value.clone(),
                    (None, true) => format!("n/a ({})", cell_label(cell)),
                    (None, false) => cell_label(cell).to_string(),
                };
                let color = if marked { Color::Muted } else { cell_color(cell) };
                let mut label = Label::new(text).size(LabelSize::Small).color(color).truncate();
                if value.is_some() {
                    label = label.buffer_font(cx);
                }
                line = line.child(div().flex_1().min_w_0().child(label));
            }
            table = table.child(line);
        }
        table.into_any_element()
    }

    pub(super) fn render_compare(&self, state: &CompareState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let mut sides = v_flex().gap_2();
        for (index, side) in state.sides.iter().enumerate() {
            let label = side
                .connection_id
                .as_ref()
                .and_then(|id| self.connections.iter().find(|c| &c.id == id))
                .map_or("Pick a connection".to_string(), Connection::label);
            let connections = self.connections.iter().map(|c| (c.id.clone(), c.label())).collect::<Vec<_>>();
            let view = cx.entity().downgrade();
            sides = sides.child(
                h_flex()
                    .gap_2()
                    .child(
                        div().w(px(70.)).flex_none().child(
                            Label::new(if index == 0 { "Baseline".to_string() } else { format!("Side {index}") })
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        ),
                    )
                    .child(
                        PopoverMenu::new(SharedString::from(format!("compare-connection-{index}")))
                            .trigger(
                                Button::new(SharedString::from(format!("compare-connection-button-{index}")), label)
                                    .label_size(LabelSize::Small)
                                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                            )
                            .menu(move |window, cx| {
                                let view = view.clone();
                                let connections = connections.clone();
                                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                    for (id, label) in &connections {
                                        let view = view.clone();
                                        let id = id.clone();
                                        menu = menu.entry(label.clone(), None, move |_, cx| {
                                            view.update(cx, |this, cx| {
                                                if let Some(side) = this.compare.as_mut().and_then(|state| state.sides.get_mut(index)) {
                                                    side.connection_id = Some(id.clone());
                                                }
                                                cx.notify();
                                            })
                                            .ok();
                                        });
                                    }
                                    menu
                                }))
                            }),
                    )
                    .child(
                        div()
                            .flex_1()
                            .px_2()
                            .py_1()
                            .rounded_sm()
                            .border_1()
                            .border_color(colors.border)
                            .child(side.path.clone()),
                    )
                    .when(state.sides.len() > 2, |this| {
                        this.child(
                            IconButton::new(SharedString::from(format!("compare-remove-{index}")), IconName::Close)
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Remove this side"))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(state) = &mut this.compare
                                        && index < state.sides.len()
                                    {
                                        state.sides.remove(index);
                                        state.result = None;
                                    }
                                    cx.notify();
                                })),
                        )
                    }),
            );
        }

        let result = state.result.as_ref().map(|result| {
            let mut sections = v_flex().gap_2();
            let differing = |path: &PathResult| path.rows.iter().any(|row| row.cells.iter().any(Cell::is_difference))
                || path.reads.iter().any(|read| !matches!(read, SideRead::Found { .. }));
            let mut shown = 0;
            for path in &result.paths {
                if state.only_differences && !differing(path) {
                    continue;
                }
                shown += 1;
                let expanded = state.expanded.contains(&path.path);
                let toggle = path.path.clone();
                let mut header = h_flex()
                    .id(SharedString::from(format!("compare-path-{}", path.path)))
                    .px_3()
                    .py_1p5()
                    .gap_3()
                    .bg(colors.element_background)
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(state) = &mut this.compare
                            && !state.expanded.remove(&toggle)
                        {
                            state.expanded.insert(toggle.clone());
                        }
                        cx.notify();
                    }))
                    .child(
                        h_flex()
                            .w(px(280.))
                            .flex_none()
                            .gap_1()
                            .child(Icon::new(if expanded { IconName::ChevronDown } else { IconName::ChevronRight }).size(IconSize::XSmall).color(Color::Muted))
                            .child(Label::new(if path.path.is_empty() { "Keys".to_string() } else { path.path.clone() }).size(LabelSize::Small).truncate()),
                    );
                for (index, read) in path.reads.iter().enumerate() {
                    let differences = path.rows.iter().filter(|row| row.cells.get(index).is_some_and(Cell::is_difference)).count();
                    let (text, color) = match read {
                        SideRead::Found { values, .. } if index == 0 => (format!("{} keys", values.len()), Color::Muted),
                        SideRead::Found { values, .. } if differences == 0 => (format!("{} keys · same", values.len()), Color::Success),
                        SideRead::Found { values, .. } => (format!("{} keys · {differences} differ", values.len()), Color::Warning),
                        SideRead::MissingSecret => ("missing secret".to_string(), Color::Error),
                        SideRead::Unavailable(_) => ("deleted version".to_string(), Color::Muted),
                        SideRead::Unreadable(reason) => (format!("unreadable: {reason}"), Color::Muted),
                    };
                    header = header.child(div().flex_1().min_w_0().child(Label::new(text).size(LabelSize::XSmall).color(color).truncate()));
                }
                let mut section = v_flex().border_1().border_color(colors.border).rounded_md().child(header);
                if expanded {
                    section = section.child(self.render_rows(state, result, path, cx));
                }
                sections = sections.child(section);
            }
            let mut side_header = h_flex().px_3().gap_3().child(div().w(px(280.)).flex_none());
            for (index, side) in result.sides.iter().enumerate() {
                side_header = side_header.child(
                    v_flex()
                        .flex_1()
                        .min_w_0()
                        .child(Label::new(if index == 0 { format!("Baseline · {}", side.label) } else { side.label.clone() }).size(LabelSize::Small).truncate())
                        .child(Label::new(side.input.clone()).size(LabelSize::XSmall).color(Color::Muted).truncate()),
                );
            }
            v_flex()
                .gap_2()
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            Label::new(format!(
                                "{} secrets compared · {shown} shown · read {} – {}",
                                result.paths.len(),
                                result.started_at,
                                result.finished_at
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                        )
                        .child(div().flex_1())
                        .child(
                            Button::new("compare-export", "Export report")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, _, cx| this.export_report(cx))),
                        ),
                )
                .children(result.coverage.iter().map(|note| {
                    Label::new(format!("Not covered: {note}")).size(LabelSize::XSmall).color(Color::Warning)
                }))
                .child(side_header)
                .child(sections)
        });

        v_flex()
            .id("vault-compare")
            .flex_1()
            .overflow_y_scroll()
            .gap_3()
            .child(Headline::new("Compare secrets").size(HeadlineSize::Small))
            .child(
                Label::new(
                    "Differences are evidence, not requirements. Lanes are compared as stored, not merged \
                     over their base lane. Unreadable sides are never counted as missing. End a path with / \
                     to compare every secret in that folder.",
                )
                .size(LabelSize::Small)
                .color(Color::Warning),
            )
            .child(sides)
            .child(
                h_flex()
                    .gap_2()
                    .child(Button::new("compare-add-side", "Add side").on_click(cx.listener(|this, _, window, cx| {
                        let connection_id = this
                            .compare
                            .as_ref()
                            .and_then(|state| state.sides.last())
                            .and_then(|side| side.connection_id.clone());
                        let path = editor("mount/service/lane", "", false, window, cx);
                        if let Some(state) = &mut this.compare {
                            state.sides.push(CompareSide { connection_id, path });
                        }
                        cx.notify();
                    })))
                    .child(if state.running {
                        Button::new("compare-stop", "Stop")
                            .on_click(cx.listener(|this, _, _, cx| this.stop_compare(cx)))
                    } else {
                        Button::new("compare-run", "Compare")
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, _, cx| this.run_compare(cx)))
                    })
                    .child(
                        Checkbox::new("compare-only-differences", ToggleState::from(state.only_differences))
                            .label("Only differences")
                            .on_click(cx.listener(|this, toggle: &ToggleState, _, cx| {
                                if let Some(state) = &mut this.compare {
                                    state.only_differences = toggle.selected();
                                }
                                cx.notify();
                            })),
                    )
                    .child(div().flex_1())
                    .child(Button::new("compare-close", "Close").on_click(cx.listener(|this, _, _, cx| {
                        // Dropping the state drops every value read for it.
                        this.compare = None;
                        this.revealed.clear();
                        cx.notify();
                    }))),
            )
            .when_some(state.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(Color::Muted))
            })
            .children(result)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::KvVersion;

    #[test]
    fn test_resolve_path_requires_a_known_mount() {
        let mounts = vec![
            Mount { path: "FPMS".into(), version: KvVersion::V1 },
            Mount { path: "FPMS-NT-V2".into(), version: KvVersion::V2 },
        ];
        let (mount, path) = resolve_path(&mounts, "/FPMS-NT-V2/auth/uat/", false).unwrap();
        assert_eq!((mount.path.as_str(), path.as_str()), ("FPMS-NT-V2", "auth/uat"));
        assert!(resolve_path(&mounts, "OTHER/auth/uat", false).is_none());
        assert!(resolve_path(&mounts, "FPMS-NT-V2", false).is_none());
        let (_, folder) = resolve_path(&mounts, "FPMS-NT-V2/auth/", true).unwrap();
        assert_eq!(folder, "auth/");
        let (mount, root) = resolve_path(&mounts, "FPMS/", true).unwrap();
        assert_eq!((mount.path.as_str(), root.as_str()), ("FPMS", ""));
    }
}
