//! Vilo: VS Code style search side bar.
//!
//! Runs the same `project.search` as the center Project Search view, but shows
//! results as a per-file tree in the left dock instead of a multibuffer.

use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use collections::HashSet;
use editor::{Editor, EditorEvent};
use futures::StreamExt as _;
use gpui::{
    Action, App, Context, Entity, EventEmitter, FocusHandle, Focusable, KeyDownEvent, Pixels,
    ScrollStrategy, Subscription, Task, UniformListScrollHandle, WeakEntity, Window, actions, px,
    uniform_list,
};
use language::{Buffer, Point, ToPoint as _};
use project::{
    Project, ProjectPath, WorktreeSettings,
    search::{SearchQuery, SearchResult},
};
use util::paths::PathStyle;
use util::rel_path::RelPath;

use crate::disk_search::{self, WalkProgress};
use settings::{Settings as _, SettingsLocation};
use text::Anchor;
use ui::{
    Button, IconButton, IconButtonShape, IndentGuideColors, ListItem, ListItemSpacing, Tooltip,
    prelude::*, rems_from_px,
};
use util::{ResultExt as _, paths::PathMatcher};
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
    pane::DeploySearch,
};

actions!(
    search_panel,
    [
        /// Opens the search side bar and focuses its query input.
        ToggleFocus
    ]
);

const SEARCH_PANEL_KEY: &str = "SearchPanel";
const SEARCH_ON_TYPE_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_LINE_PREVIEW_CHARS: usize = 200;
const MAX_MATCHES: usize = 20_000;
const MAX_BATCH_FILES: usize = 256;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const TIMER_TICK: std::time::Duration = std::time::Duration::from_millis(100);
/// Characters kept before a match; longer leading context is replaced by "…" so the match stays visible.
const MAX_CONTEXT_BEFORE_MATCH: usize = 24;
// VS Code workbench metrics (logical px), scoped to the side bar so the global UI size stays as is.
const ROW_HEIGHT: Pixels = px(22.);
const HEADER_HEIGHT: Pixels = px(35.);
const INPUT_HEIGHT: Pixels = px(28.);
const INDENT_SIZE: f32 = 16.;

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action(|workspace, _: &ToggleFocus, window, cx| {
            workspace.focus_panel::<SearchPanel>(window, cx);
        });
    })
    .detach();
}

#[derive(Clone)]
struct MatchLine {
    /// Present for open buffers, so the position follows later edits.
    anchor: Option<Anchor>,
    point: Point,
    line_number: u32,
    preview: SharedString,
    highlight: Range<usize>,
}

enum ResultTarget {
    Buffer(Entity<Buffer>),
    /// Found on disk without opening a buffer; opened only when clicked.
    Path(ProjectPath),
}

struct FileResult {
    target: ResultTarget,
    icon: Option<SharedString>,
    file_name: SharedString,
    directory: SharedString,
    matches: Vec<MatchLine>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Idle,
    WaitingForScan,
    Searching,
    Done,
    InvalidQuery,
}

#[derive(Clone, Copy)]
enum ReplaceScope {
    All,
    File(usize),
    Match(usize, usize),
}

#[derive(Clone, Copy)]
enum Row {
    File(usize),
    Match(usize, usize),
}

pub struct SearchPanel {
    workspace: WeakEntity<Workspace>,
    project: Entity<Project>,
    focus_handle: FocusHandle,
    query_editor: Entity<Editor>,
    include_editor: Entity<Editor>,
    exclude_editor: Entity<Editor>,
    replace_editor: Entity<Editor>,
    show_replace: bool,
    replace_status: Option<SharedString>,
    results_focus: FocusHandle,
    scroll_handle: UniformListScrollHandle,
    /// The query behind `results`; the input may already hold newer, unsearched text.
    results_query: Option<SearchQuery>,
    pending_replace: Option<Task<()>>,
    case_sensitive: bool,
    whole_word: bool,
    regex: bool,
    show_filters: bool,
    results: Vec<FileResult>,
    collapsed: HashSet<usize>,
    rows: Vec<Row>,
    selected_row: Option<usize>,
    status: Status,
    position: DockPosition,
    pending_debounce: Option<Task<()>>,
    pending_search: Option<Task<()>>,
    walk_progress: Option<Arc<WalkProgress>>,
    search_started: Option<Instant>,
    search_elapsed: Option<Duration>,
    _timer: Option<Task<()>>,
    /// Sticky for the whole search: a later status event must not hide that results were cut off.
    limit_reached: bool,
    _subscriptions: Vec<Subscription>,
}

impl SearchPanel {
    pub fn new(workspace: &Workspace, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let single_line = |placeholder: &str, window: &mut Window, cx: &mut Context<Self>| {
            cx.new(|cx| {
                let mut editor = Editor::single_line(window, cx);
                editor.set_placeholder_text(placeholder, window, cx);
                editor
            })
        };
        let query_editor = single_line("Search", window, cx);
        let include_editor = single_line("files to include", window, cx);
        let exclude_editor = single_line("files to exclude", window, cx);
        let replace_editor = single_line("Replace", window, cx);

        let subscriptions = [&query_editor, &include_editor, &exclude_editor]
            .into_iter()
            .map(|editor| {
                cx.subscribe_in(editor, window, |this, _, event, window, cx| {
                    if let EditorEvent::BufferEdited = event {
                        this.schedule_search(window, cx);
                    }
                })
            })
            .collect();

        Self {
            workspace: workspace.weak_handle(),
            project: workspace.project().clone(),
            focus_handle: cx.focus_handle(),
            query_editor,
            include_editor,
            exclude_editor,
            replace_editor,
            show_replace: false,
            replace_status: None,
            results_focus: cx.focus_handle(),
            scroll_handle: UniformListScrollHandle::new(),
            results_query: None,
            pending_replace: None,
            case_sensitive: false,
            whole_word: false,
            regex: false,
            show_filters: false,
            results: Vec::new(),
            collapsed: HashSet::default(),
            rows: Vec::new(),
            selected_row: None,
            status: Status::Idle,
            position: DockPosition::Left,
            pending_debounce: None,
            pending_search: None,
            walk_progress: None,
            search_started: None,
            search_elapsed: None,
            _timer: None,
            limit_reached: false,
            _subscriptions: subscriptions,
        }
    }

    pub async fn load(
        workspace: WeakEntity<Workspace>,
        mut cx: gpui::AsyncWindowContext,
    ) -> anyhow::Result<Entity<Self>> {
        workspace.update_in(&mut cx, |workspace, window, cx| {
            cx.new(|cx| SearchPanel::new(workspace, window, cx))
        })
    }

    fn schedule_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Replacing the task drops the previous one, so an older query can never
        // overwrite the results of a newer one.
        self.pending_debounce = Some(cx.spawn_in(window, async move |this, cx| {
            cx.background_executor()
                .timer(SEARCH_ON_TYPE_DEBOUNCE)
                .await;
            this.update(cx, |this, cx| this.run_search(cx)).log_err();
        }));
    }

    fn build_query(&self, cx: &App) -> Option<anyhow::Result<SearchQuery>> {
        let text = self.query_editor.read(cx).text(cx);
        if text.is_empty() {
            return None;
        }
        let path_style = self.project.read(cx).path_style(cx);
        let matcher = |editor: &Entity<Editor>| -> anyhow::Result<PathMatcher> {
            let globs = editor
                .read(cx)
                .text(cx)
                .split(',')
                .map(str::trim)
                .filter(|glob| !glob.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            Ok(PathMatcher::new(&globs, path_style)?)
        };
        let query = (|| {
            let include = matcher(&self.include_editor)?;
            let exclude = matcher(&self.exclude_editor)?;
            if self.regex {
                SearchQuery::regex(
                    text,
                    self.whole_word,
                    self.case_sensitive,
                    false,
                    false,
                    include,
                    exclude,
                    false,
                    None,
                )
            } else {
                SearchQuery::text(
                    text,
                    self.whole_word,
                    self.case_sensitive,
                    false,
                    include,
                    exclude,
                    false,
                    None,
                )
            }
        })();
        Some(query)
    }

    fn run_search(&mut self, cx: &mut Context<Self>) {
        self.results_query = None;
        self.replace_status = None;
        self.results.clear();
        self.collapsed.clear();
        self.selected_row = None;
        self.limit_reached = false;
        self.search_elapsed = None;
        if let Some(progress) = self.walk_progress.take() {
            progress.cancelled.store(true, Ordering::Relaxed);
        }
        self.rebuild_rows();
        let query = match self.build_query(cx) {
            None => {
                self.status = Status::Idle;
                self.pending_search = None;
                self._timer = None;
                cx.notify();
                return;
            }
            Some(Err(_)) => {
                self.status = Status::InvalidQuery;
                self.pending_search = None;
                self._timer = None;
                cx.notify();
                return;
            }
            Some(Ok(query)) => query,
        };
        self.results_query = Some(query.clone());
        self.status = Status::Searching;
        self.search_started = Some(Instant::now());
        self._timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(TIMER_TICK).await;
                let searching = this
                    .update(cx, |this, cx| {
                        cx.notify();
                        matches!(this.status, Status::Searching | Status::WaitingForScan)
                    })
                    .unwrap_or(false);
                if !searching {
                    break;
                }
            }
        }));
        let project = self.project.read(cx);
        // The in-memory test file system has nothing on disk to walk.
        if project.is_local() && !project.fs().is_fake() {
            self.run_disk_search(query, cx);
        } else {
            self.run_project_search(query, cx);
        }
        cx.notify();
    }

    fn finish_search(&mut self, cx: &mut Context<Self>) {
        self.status = Status::Done;
        self.search_elapsed = self.search_started.map(|started| started.elapsed());
        cx.notify();
    }

    /// Remote and collaborative projects have no local disk to walk, so they use the project search.
    fn run_project_search(&mut self, query: SearchQuery, cx: &mut Context<Self>) {
        let search = self
            .project
            .update(cx, |project, cx| project.search(query, cx));
        self.pending_search = Some(cx.spawn(async move |this, cx| {
            let _search_task = search.task_handle;
            let mut chunks = pin!(search.rx.ready_chunks(256));
            while let Some(chunk) = chunks.next().await {
                let updated = this.update(cx, |this, cx| {
                    for result in chunk {
                        match result {
                            SearchResult::Buffer { buffer, ranges } => {
                                this.push_buffer_result(buffer, ranges, cx)
                            }
                            SearchResult::LimitReached => this.limit_reached = true,
                            SearchResult::WaitingForScan => this.status = Status::WaitingForScan,
                            SearchResult::Searching => this.status = Status::Searching,
                        }
                    }
                    this.rebuild_rows();
                    cx.notify();
                });
                if updated.is_err() {
                    return;
                }
            }
            this.update(cx, |this, cx| this.finish_search(cx)).ok();
        }));
    }

    /// Searches local worktrees straight from disk instead of waiting for the worktree scan.
    fn run_disk_search(&mut self, query: SearchQuery, cx: &mut Context<Self>) {
        let project = self.project.read(cx);
        let path_style = project.path_style(cx);
        let roots = project
            .visible_worktrees(cx)
            .map(|worktree| {
                let worktree = worktree.read(cx);
                let location = SettingsLocation {
                    worktree_id: worktree.id(),
                    path: RelPath::empty(),
                };
                disk_search::DiskRoot {
                    worktree_id: worktree.id(),
                    abs_path: worktree.abs_path(),
                    scan_exclusions: WorktreeSettings::get(Some(location), cx)
                        .file_scan_exclusions
                        .clone(),
                }
            })
            .collect::<Vec<_>>();
        // Unsaved edits live only in memory: search those buffers directly and skip their files on disk.
        let dirty_buffers = project
            .opened_buffers(cx)
            .into_iter()
            .filter(|buffer| {
                let buffer = buffer.read(cx);
                // Same path filters as the disk walk; untitled buffers have no path to filter.
                buffer.is_dirty()
                    && buffer.file().is_none_or(|file| {
                        let location = SettingsLocation {
                            worktree_id: file.worktree_id(cx),
                            path: RelPath::empty(),
                        };
                        // is_path_excluded also checks ancestors, e.g. a file under an excluded folder.
                        query.match_path(file.path())
                            && !WorktreeSettings::get(Some(location), cx)
                                .is_path_excluded(file.path())
                    })
            })
            .collect::<Vec<_>>();
        let skip_abs_paths = dirty_buffers
            .iter()
            .filter_map(|buffer| {
                let file = buffer.read(cx).file()?;
                Some(file.as_local()?.abs_path(cx))
            })
            .collect::<HashSet<_>>();

        let progress = Arc::new(WalkProgress::default());
        self.walk_progress = Some(progress.clone());
        let candidates = disk_search::walk(
            roots,
            query.clone(),
            path_style,
            skip_abs_paths,
            progress.clone(),
        );

        self.pending_search = Some(cx.spawn(async move |this, cx| {
            for buffer in dirty_buffers {
                let snapshot = buffer.read_with(cx, |buffer, _| buffer.snapshot());
                let ranges = query.search(&snapshot, None).await;
                let anchors = ranges
                    .into_iter()
                    .map(|range| {
                        snapshot.anchor_before(range.start)..snapshot.anchor_after(range.end)
                    })
                    .collect::<Vec<_>>();
                if this
                    .update(cx, |this, cx| this.push_buffer_result(buffer, anchors, cx))
                    .is_err()
                {
                    return;
                }
            }

            while let Ok(first) = candidates.recv().await {
                // Batch by count and by bytes: each candidate holds a whole file.
                let mut batch_bytes = first.text.len();
                let mut chunk = vec![first];
                while chunk.len() < MAX_BATCH_FILES && batch_bytes < MAX_BATCH_BYTES {
                    let Ok(candidate) = candidates.try_recv() else {
                        break;
                    };
                    batch_bytes += candidate.text.len();
                    chunk.push(candidate);
                }
                // One foreground hop to reserve snapshots and one background task per batch:
                // handling candidates one at a time dominated search time on large folders.
                let Ok(snapshots) = this.update(cx, |_, cx| {
                    chunk
                        .into_iter()
                        .map(|candidate| {
                            let snapshot = Buffer::build_snapshot(
                                text::Rope::from(candidate.text.as_str()),
                                None,
                                None,
                                None,
                                cx,
                            );
                            let target = ProjectPath {
                                worktree_id: candidate.worktree_id,
                                path: candidate.rel_path,
                            };
                            (target, snapshot)
                        })
                        .collect::<Vec<_>>()
                }) else {
                    return;
                };
                let query = query.clone();
                let Ok(mut remaining) =
                    this.read_with(cx, |this, _| MAX_MATCHES.saturating_sub(this.match_count()))
                else {
                    return;
                };
                let files = cx
                    .background_spawn(async move {
                        let mut files = Vec::with_capacity(snapshots.len());
                        for (target, snapshot) in snapshots {
                            // Truncate per file so one file with huge match counts cannot blow past the limit.
                            if remaining == 0 {
                                break;
                            }
                            let snapshot = snapshot.await;
                            let mut ranges = query.search(&snapshot, None).await;
                            ranges.truncate(remaining);
                            remaining -= ranges.len();
                            let lines = build_match_lines(
                                &snapshot,
                                ranges.into_iter().map(|range| {
                                    (
                                        snapshot.offset_to_point(range.start),
                                        snapshot.offset_to_point(range.end),
                                        None,
                                    )
                                }),
                            );
                            files.push((target, lines));
                        }
                        files
                    })
                    .await;
                let reached_limit = this
                    .update(cx, |this, cx| {
                        for (target, lines) in files {
                            if this.match_count() >= MAX_MATCHES {
                                break;
                            }
                            this.push_path_result(target, lines, cx);
                        }
                        this.rebuild_rows();
                        cx.notify();
                        this.match_count() >= MAX_MATCHES
                    })
                    .unwrap_or(true);
                if reached_limit {
                    progress.cancelled.store(true, Ordering::Relaxed);
                    this.update(cx, |this, _| this.limit_reached = true).ok();
                    break;
                }
                if progress.cancelled.load(Ordering::Relaxed) {
                    break;
                }
            }
            this.update(cx, |this, cx| {
                this.rebuild_rows();
                this.finish_search(cx);
            })
            .ok();
        }));
    }

    fn push_buffer_result(&mut self, buffer: Entity<Buffer>, ranges: Vec<Range<Anchor>>, cx: &App) {
        if ranges.is_empty() {
            return;
        }
        let buffer_ref = buffer.read(cx);
        let snapshot = buffer_ref.snapshot();
        let icon = buffer_ref
            .file()
            .and_then(|file| file_icons::FileIcons::get_icon(file.path().as_std_path(), cx));
        let (file_name, directory) = match buffer_ref.file() {
            Some(file) => file_labels(file.path(), file.path_style(cx)),
            None => ("untitled".into(), SharedString::default()),
        };
        let matches = build_match_lines(
            &snapshot,
            ranges.into_iter().map(|range| {
                (
                    range.start.to_point(&snapshot),
                    range.end.to_point(&snapshot),
                    Some(range.start),
                )
            }),
        );
        self.results.push(FileResult {
            target: ResultTarget::Buffer(buffer),
            icon,
            file_name,
            directory,
            matches,
        });
    }

    fn push_path_result(&mut self, target: ProjectPath, matches: Vec<MatchLine>, cx: &App) {
        if matches.is_empty() {
            return;
        }
        let path_style = self.project.read(cx).path_style(cx);
        let icon = file_icons::FileIcons::get_icon(target.path.as_std_path(), cx);
        let (file_name, directory) = file_labels(&target.path, path_style);
        self.results.push(FileResult {
            target: ResultTarget::Path(target),
            icon,
            file_name,
            directory,
            matches,
        });
    }

    fn rebuild_rows(&mut self) {
        self.rows.clear();
        for (file_ix, file) in self.results.iter().enumerate() {
            self.rows.push(Row::File(file_ix));
            if !self.collapsed.contains(&file_ix) {
                self.rows
                    .extend((0..file.matches.len()).map(|match_ix| Row::Match(file_ix, match_ix)));
            }
        }
    }

    fn match_count(&self) -> usize {
        self.results.iter().map(|file| file.matches.len()).sum()
    }

    fn toggle_file(&mut self, file_ix: usize, cx: &mut Context<Self>) {
        if !self.collapsed.remove(&file_ix) {
            self.collapsed.insert(file_ix);
        }
        self.rebuild_rows();
        cx.notify();
    }

    fn open_match(
        &mut self,
        file_ix: usize,
        match_ix: usize,
        allow_preview: bool,
        focus_editor: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(file) = self.results.get(file_ix) else {
            return;
        };
        let Some(search_match) = file.matches.get(match_ix) else {
            return;
        };
        let Some(workspace) = self.workspace.upgrade() else {
            return;
        };
        match &file.target {
            ResultTarget::Buffer(buffer) => {
                let buffer = buffer.clone();
                // Anchors follow edits made after the search, so this lands on the current match position.
                let point = match search_match.anchor {
                    Some(anchor) => anchor.to_point(&buffer.read(cx).snapshot()),
                    None => search_match.point,
                };
                workspace.update(cx, |workspace, cx| {
                    // Reuse an editor already showing this buffer: path-based lookup misses untitled buffers.
                    let existing = workspace.items_of_type::<Editor>(cx).find(|editor| {
                        editor.read(cx).buffer().read(cx).as_singleton().as_ref() == Some(&buffer)
                    });
                    let editor = match existing {
                        Some(editor) => {
                            workspace.activate_item(&editor, true, focus_editor, window, cx);
                            if !allow_preview && let Some(pane) = workspace.pane_for(&editor) {
                                pane.update(cx, |pane, _| {
                                    pane.unpreview_item_if_preview(editor.entity_id())
                                });
                            }
                            editor
                        }
                        // keep_old_preview = true lets a single-click preview replace the previous one.
                        None => workspace.open_project_item::<Editor>(
                            None,
                            buffer,
                            true,
                            focus_editor,
                            true,
                            allow_preview,
                            window,
                            cx,
                        ),
                    };
                    editor.update(cx, |editor, cx| {
                        editor.go_to_singleton_buffer_point(point, window, cx)
                    });
                });
            }
            ResultTarget::Path(project_path) => {
                let point = search_match.point;
                let open_task = workspace.update(cx, |workspace, cx| {
                    workspace.open_path_preview(
                        project_path.clone(),
                        None,
                        focus_editor,
                        allow_preview,
                        true,
                        window,
                        cx,
                    )
                });
                cx.spawn_in(window, async move |_, cx| {
                    let item = open_task.await.log_err()?;
                    let editor = item.downcast::<Editor>()?;
                    editor
                        .update_in(cx, |editor, window, cx| {
                            editor.go_to_singleton_buffer_point(point, window, cx)
                        })
                        .log_err();
                    Some(())
                })
                .detach();
            }
        }
    }

    fn summary(&self) -> Option<SharedString> {
        let files = self.results.len();
        let matches = self.match_count();
        let found = format!(
            "{matches} result{} in {files} file{}",
            if matches == 1 { "" } else { "s" },
            if files == 1 { "" } else { "s" }
        );
        let text = match self.status {
            Status::Idle => return None,
            Status::InvalidQuery => "Invalid search query".to_string(),
            Status::Searching | Status::WaitingForScan => {
                let elapsed = self
                    .search_started
                    .map_or(0., |started| started.elapsed().as_secs_f32());
                let searched = self
                    .walk_progress
                    .as_ref()
                    .map(|progress| progress.files_searched.load(Ordering::Relaxed));
                match searched {
                    Some(searched) => {
                        format!("Searching… {elapsed:.1}s · {searched} files searched · {found}")
                    }
                    None => format!("Searching… {elapsed:.1}s · {found}"),
                }
            }
            Status::Done => {
                let elapsed = self
                    .search_elapsed
                    .map_or(0., |elapsed| elapsed.as_secs_f32());
                if self.limit_reached {
                    format!("{found} ({elapsed:.1}s, limit reached, refine the search)")
                } else if files == 0 {
                    format!("No results found ({elapsed:.1}s)")
                } else {
                    format!("{found} ({elapsed:.1}s)")
                }
            }
        };
        Some(text.into())
    }

    fn render_option(
        &self,
        id: &'static str,
        icon: IconName,
        tooltip: &'static str,
        active: bool,
        toggle: fn(&mut Self),
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        IconButton::new(id, icon)
            .shape(IconButtonShape::Square)
            .icon_size(IconSize::XSmall)
            .toggle_state(active)
            .tooltip(Tooltip::text(tooltip))
            .on_click(cx.listener(move |this, _, window, cx| {
                toggle(this);
                this.schedule_search(window, cx);
            }))
    }

    /// A compact single-line input box; `trailing` sits inside it on the right, as in VS Code.
    fn render_input(
        &self,
        editor: &Entity<Editor>,
        trailing: Vec<AnyElement>,
        window: &Window,
        cx: &App,
    ) -> impl IntoElement {
        let colors = cx.theme().colors();
        let focused = editor.focus_handle(cx).is_focused(window);
        h_flex()
            .w_full()
            .flex_none()
            .h(INPUT_HEIGHT)
            .pl_1p5()
            .pr_0p5()
            .gap_0p5()
            .rounded_sm()
            .border_1()
            .border_color(if focused {
                colors.border_focused
            } else {
                colors.border
            })
            .bg(colors.editor_background)
            .text_size(rems_from_px(13_f32))
            .child(div().flex_1().min_w_0().child(editor.clone()))
            .children(trailing)
    }

    fn render_row(
        &self,
        row_ix: usize,
        row: Row,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected = self.selected_row == Some(row_ix);
        let focused = selected && self.results_focus.is_focused(window);
        let show_replace = self.show_replace;
        let text_size = LabelSize::Custom(rems_from_px(13_f32));
        match row {
            Row::File(file_ix) => {
                let file = &self.results[file_ix];
                let expanded = !self.collapsed.contains(&file_ix);
                ListItem::new(("search-file", file_ix))
                    .height(ROW_HEIGHT)
                    .spacing(ListItemSpacing::Dense)
                    .indent_level(0)
                    .indent_step_size(px(INDENT_SIZE))
                    .toggle(expanded)
                    .on_toggle(cx.listener(move |this, _, _, cx| this.toggle_file(file_ix, cx)))
                    .toggle_state(selected)
                    .focused(focused)
                    .start_slot::<Icon>(file.icon.clone().map(|icon| {
                        Icon::from_path(icon)
                            .size(IconSize::Small)
                            .color(Color::Muted)
                    }))
                    .child(
                        h_flex()
                            .min_w_0()
                            .gap_1p5()
                            .child(
                                Label::new(file.file_name.clone())
                                    .size(text_size)
                                    .single_line(),
                            )
                            .child(
                                Label::new(file.directory.clone())
                                    .size(LabelSize::Small)
                                    .color(Color::Muted)
                                    .single_line()
                                    .truncate(),
                            ),
                    )
                    .when(show_replace, |this| {
                        this.end_slot_on_hover(
                            IconButton::new(("search-replace-file", file_ix), IconName::ReplaceAll)
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Replace All in File"))
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.replace(ReplaceScope::File(file_ix), window, cx)
                                })),
                        )
                    })
                    .end_slot(
                        div()
                            .px_1p5()
                            .h(px(16.))
                            .flex()
                            .items_center()
                            .rounded_full()
                            .bg(cx.theme().colors().element_background)
                            .child(
                                Label::new(file.matches.len().to_string()).size(LabelSize::XSmall),
                            ),
                    )
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.selected_row = Some(row_ix);
                        this.toggle_file(file_ix, cx);
                    }))
                    .into_any_element()
            }
            Row::Match(file_ix, match_ix) => {
                let search_match = &self.results[file_ix].matches[match_ix];
                let highlight_style = gpui::HighlightStyle {
                    background_color: Some(cx.theme().colors().search_match_background),
                    ..Default::default()
                };
                ListItem::new(ElementId::NamedInteger(
                    "search-match".into(),
                    ((file_ix as u64) << 32) | match_ix as u64,
                ))
                .height(ROW_HEIGHT)
                .spacing(ListItemSpacing::Dense)
                .indent_level(1)
                .indent_step_size(px(INDENT_SIZE))
                .toggle_state(selected)
                .focused(focused)
                .tooltip(Tooltip::text(format!("Line {}", search_match.line_number)))
                .when(show_replace, |this| {
                    this.end_slot(
                        IconButton::new(
                            ElementId::NamedInteger(
                                "search-replace-match".into(),
                                ((file_ix as u64) << 32) | match_ix as u64,
                            ),
                            IconName::Replace,
                        )
                        .icon_size(IconSize::Small)
                        .tooltip(Tooltip::text("Replace"))
                        .on_click(cx.listener(
                            move |this, _, window, cx| {
                                this.replace(ReplaceScope::Match(file_ix, match_ix), window, cx)
                            },
                        )),
                    )
                    .show_end_slot_on_hover()
                })
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .whitespace_nowrap()
                        .text_size(rems_from_px(13_f32))
                        .text_ellipsis()
                        .child(
                            gpui::StyledText::new(search_match.preview.clone()).with_highlights([
                                (search_match.highlight.clone(), highlight_style),
                            ]),
                        ),
                )
                .on_click(
                    cx.listener(move |this, event: &gpui::ClickEvent, window, cx| {
                        this.selected_row = Some(row_ix);
                        // Single click previews, double click pins the tab, as in VS Code.
                        let allow_preview = event.click_count() < 2;
                        this.open_match(file_ix, match_ix, allow_preview, true, window, cx);
                        cx.notify();
                    }),
                )
                .into_any_element()
            }
        }
    }

    /// Replaces matches in place, leaving buffers modified but unsaved (as Zed's project search
    /// does): one transaction per buffer, so Cmd+Z in a file restores it. Matches are recomputed
    /// on each buffer's current text right before editing, so stale positions are never edited.
    fn replace(&mut self, scope: ReplaceScope, window: &mut Window, cx: &mut Context<Self>) {
        if self.pending_replace.is_some() {
            return;
        }
        // Replace with the query that produced the visible results, not the possibly newer input text.
        let Some(query) = self.results_query.clone() else {
            return;
        };
        if matches!(scope, ReplaceScope::All)
            && (self.limit_reached || !matches!(self.status, Status::Done))
        {
            self.replace_status = Some(
                "Results are incomplete; wait for the search to finish or refine it before replacing all"
                    .into(),
            );
            cx.notify();
            return;
        }
        let query = query.with_replacement(self.replace_editor.read(cx).text(cx));
        let targets = self
            .results
            .iter()
            .enumerate()
            .filter_map(|(file_ix, file)| {
                let search_match = match scope {
                    ReplaceScope::All => None,
                    ReplaceScope::File(ix) if ix == file_ix => None,
                    ReplaceScope::Match(ix, match_ix) if ix == file_ix => {
                        Some(file.matches.get(match_ix)?)
                    }
                    _ => return None,
                };
                let target = match &file.target {
                    ResultTarget::Buffer(buffer) => ResultTarget::Buffer(buffer.clone()),
                    ResultTarget::Path(path) => ResultTarget::Path(path.clone()),
                };
                Some((search_match.map(|m| (m.anchor, m.point)), target))
            })
            .collect::<Vec<_>>();
        let project = self.project.clone();
        self.pending_replace = Some(cx.spawn_in(window, async move |this, cx| {
            let mut replaced = 0;
            let mut skipped = 0;
            let mut unsaved = 0;
            let mut failed_saves = Vec::new();
            for (search_match, target) in targets {
                let buffer = match target {
                    ResultTarget::Buffer(buffer) => buffer,
                    ResultTarget::Path(path) => {
                        let open = project.update(cx, |project, cx| project.open_buffer(path, cx));
                        match open.await {
                            Ok(buffer) => buffer,
                            Err(error) => {
                                log::warn!(
                                    "search panel could not open a file to replace: {error:#}"
                                );
                                skipped += 1;
                                continue;
                            }
                        }
                    }
                };
                let (snapshot, read_only) =
                    buffer.read_with(cx, |buffer, _| (buffer.snapshot(), buffer.read_only()));
                if read_only {
                    skipped += 1;
                    continue;
                }
                // Anchors track edits made since the search, so a single replace hits the clicked match.
                let point = search_match.map(|(anchor, point)| {
                    anchor.map_or(point, |anchor| anchor.to_point(&snapshot))
                });
                let ranges = query.search(&snapshot, None).await;
                let edits = ranges
                    .into_iter()
                    .filter(|range| {
                        point.is_none_or(|point| snapshot.offset_to_point(range.start) == point)
                    })
                    .take(if point.is_some() { 1 } else { usize::MAX })
                    .filter_map(|range| {
                        let start = snapshot.offset_to_point(range.start);
                        let end = snapshot.offset_to_point(range.end);
                        let line_start = snapshot.point_to_offset(Point::new(start.row, 0));
                        let line = snapshot
                            .text_for_range(
                                line_start
                                    ..snapshot.point_to_offset(Point::new(
                                        end.row,
                                        snapshot.line_len(end.row),
                                    )),
                            )
                            .collect::<String>();
                        let hit = range.start - line_start..range.end - line_start;
                        let replacement = query.replacement_for(&line, hit)?.into_owned();
                        Some((
                            snapshot.anchor_before(range.start)..snapshot.anchor_after(range.end),
                            replacement,
                        ))
                    })
                    .collect::<Vec<_>>();
                if edits.is_empty() {
                    continue;
                }
                replaced += edits.len();
                // Dirty state is read at edit time: the user may have typed while the search awaited.
                let was_dirty = buffer.update(cx, |buffer, cx| {
                    let was_dirty = buffer.is_dirty();
                    buffer.edit(edits, None, cx);
                    was_dirty
                });
                // Like VS Code, save files that were clean: a buffer only held by the result list
                // would be dropped with its edits once the results refresh.
                if was_dirty {
                    unsaved += 1;
                } else {
                    let save =
                        project.update(cx, |project, cx| project.save_buffer(buffer.clone(), cx));
                    if let Err(error) = save.await {
                        log::warn!("search panel could not save a replaced file: {error:#}");
                        failed_saves.push(buffer);
                    }
                }
            }
            this.update_in(cx, |this, window, cx| {
                // Keep unsaved edits reachable by opening them in an editor.
                if let Some(workspace) = this.workspace.upgrade() {
                    for buffer in &failed_saves {
                        workspace.update(cx, |workspace, cx| {
                            workspace.open_project_item::<Editor>(
                                None,
                                buffer.clone(),
                                true,
                                false,
                                true,
                                false,
                                window,
                                cx,
                            );
                        });
                    }
                }
                let mut status = format!(
                    "Replaced {replaced} occurrence{}",
                    if replaced == 1 { "" } else { "s" }
                );
                if skipped > 0 {
                    status.push_str(&format!(
                        ", skipped {skipped} read-only or unreadable file(s)"
                    ));
                }
                if !failed_saves.is_empty() {
                    status.push_str(&format!(
                        ", {} file(s) failed to save and were opened",
                        failed_saves.len()
                    ));
                }
                if unsaved > 0 {
                    status.push_str(&format!(
                        ", {unsaved} already-modified file(s) left unsaved"
                    ));
                }
                status.push('.');
                this.pending_replace = None;
                // Re-search instead of patching rows: replacements can create or move matches.
                this.run_search(cx);
                this.replace_status = Some(status.into());
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_in_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Empty filters are sent too: `None` would keep the project search's previous filters.
        let action = DeploySearch {
            query: Some(self.query_editor.read(cx).text(cx)),
            included_files: Some(self.include_editor.read(cx).text(cx)),
            excluded_files: Some(self.exclude_editor.read(cx).text(cx)),
            regex: Some(self.regex),
            case_sensitive: Some(self.case_sensitive),
            whole_word: Some(self.whole_word),
            ..Default::default()
        };
        window.dispatch_action(Box::new(action), cx);
    }

    fn select_row(&mut self, row_ix: usize, cx: &mut Context<Self>) {
        self.selected_row = Some(row_ix);
        self.scroll_handle
            .scroll_to_item(row_ix, ScrollStrategy::Nearest);
        cx.notify();
    }

    /// VS Code keys in the result tree: ↑↓ select, Enter previews, Cmd+Enter pins,
    /// ←→ collapse and expand files, Escape returns to the query.
    fn handle_results_key(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row_count = self.rows.len();
        let selected = self.selected_row.filter(|ix| *ix < row_count);
        let handled = match (event.keystroke.key.as_str(), selected) {
            ("down", None) if row_count > 0 => {
                self.select_row(0, cx);
                true
            }
            ("down", Some(ix)) => {
                self.select_row((ix + 1).min(row_count.saturating_sub(1)), cx);
                true
            }
            ("up", Some(0)) | ("escape", _) => {
                window.focus(&self.query_editor.focus_handle(cx), cx);
                true
            }
            ("up", Some(ix)) => {
                self.select_row(ix - 1, cx);
                true
            }
            ("enter", Some(ix)) => {
                match self.rows[ix] {
                    Row::File(file_ix) => self.toggle_file(file_ix, cx),
                    Row::Match(file_ix, match_ix) => {
                        let pin = event.keystroke.modifiers.platform;
                        // Leave focus in the tree so the next match is one keystroke away.
                        self.open_match(file_ix, match_ix, !pin, false, window, cx);
                    }
                }
                true
            }
            ("left", Some(ix)) => {
                match self.rows[ix] {
                    Row::File(file_ix) if !self.collapsed.contains(&file_ix) => {
                        self.toggle_file(file_ix, cx)
                    }
                    Row::Match(file_ix, _) => {
                        if let Some(parent) = self
                            .rows
                            .iter()
                            .position(|row| matches!(row, Row::File(ix) if *ix == file_ix))
                        {
                            self.select_row(parent, cx);
                        }
                    }
                    Row::File(_) => {}
                }
                true
            }
            ("right", Some(ix)) => {
                if let Row::File(file_ix) = self.rows[ix]
                    && self.collapsed.contains(&file_ix)
                {
                    self.toggle_file(file_ix, cx);
                }
                true
            }
            _ => false,
        };
        if handled {
            cx.stop_propagation();
        }
    }

    fn collapse_all(&mut self, cx: &mut Context<Self>) {
        self.collapsed = (0..self.results.len()).collect();
        self.rebuild_rows();
        cx.notify();
    }

    fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_editor
            .update(cx, |editor, cx| editor.set_text("", window, cx));
        self.pending_debounce = None;
        self.run_search(cx);
    }

    fn header_button(
        &self,
        id: &'static str,
        icon: IconName,
        tooltip: &'static str,
        on_click: impl Fn(&mut Self, &mut Window, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        IconButton::new(id, icon)
            .icon_size(IconSize::Small)
            .icon_color(Color::Muted)
            .tooltip(Tooltip::text(tooltip))
            .on_click(cx.listener(move |this, _, window, cx| on_click(this, window, cx)))
    }
}

impl Drop for SearchPanel {
    fn drop(&mut self) {
        if let Some(progress) = &self.walk_progress {
            progress.cancelled.store(true, Ordering::Relaxed);
        }
    }
}

impl Focusable for SearchPanel {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for SearchPanel {}

impl Render for SearchPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let row_count = self.rows.len();
        let toggles = vec![
            self.render_option(
                "search-panel-case",
                IconName::CaseSensitive,
                "Match Case",
                self.case_sensitive,
                |this| this.case_sensitive = !this.case_sensitive,
                cx,
            )
            .into_any_element(),
            self.render_option(
                "search-panel-word",
                IconName::WholeWord,
                "Match Whole Word",
                self.whole_word,
                |this| this.whole_word = !this.whole_word,
                cx,
            )
            .into_any_element(),
            self.render_option(
                "search-panel-regex",
                IconName::Regex,
                "Use Regular Expression",
                self.regex,
                |this| this.regex = !this.regex,
                cx,
            )
            .into_any_element(),
        ];
        let filter_label =
            |text: &'static str| Label::new(text).size(LabelSize::Small).color(Color::Muted);
        let has_results = !self.results.is_empty();

        v_flex()
            .id("search-panel")
            .key_context("SearchPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .child(
                h_flex()
                    .flex_none()
                    .h(HEADER_HEIGHT)
                    .pl_3()
                    .pr_1p5()
                    .justify_between()
                    .child(
                        Label::new("SEARCH")
                            .size(LabelSize::Custom(rems_from_px(11_f32)))
                            .weight(gpui::FontWeight::SEMIBOLD),
                    )
                    .child(
                        h_flex()
                            .gap_0p5()
                            .child(self.header_button(
                                "search-panel-refresh",
                                IconName::RotateCw,
                                "Refresh",
                                |this, _, cx| this.run_search(cx),
                                cx,
                            ))
                            .child(self.header_button(
                                "search-panel-clear",
                                IconName::Close,
                                "Clear Search Results",
                                |this, window, cx| this.clear(window, cx),
                                cx,
                            ))
                            .when(has_results, |this| {
                                this.child(self.header_button(
                                    "search-panel-collapse",
                                    IconName::ListCollapse,
                                    "Collapse All",
                                    |this, _, cx| this.collapse_all(cx),
                                    cx,
                                ))
                            }),
                    ),
            )
            .child(
                v_flex()
                    .flex_none()
                    .px_3()
                    .gap_1()
                    .child(
                        h_flex()
                            .gap_0p5()
                            .items_start()
                            .child(
                                div().h(INPUT_HEIGHT).flex().items_center().child(
                                    IconButton::new(
                                        "search-panel-toggle-replace",
                                        if self.show_replace {
                                            IconName::ChevronDown
                                        } else {
                                            IconName::ChevronRight
                                        },
                                    )
                                    .icon_size(IconSize::Small)
                                    .icon_color(Color::Muted)
                                    .tooltip(Tooltip::text("Toggle Replace"))
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.show_replace = !this.show_replace;
                                            cx.notify();
                                        },
                                    )),
                                ),
                            )
                            .child(
                                v_flex()
                                    .flex_1()
                                    .min_w_0()
                                    .gap_1()
                                    .child(
                                        div()
                                            // ↓ from the query moves into the result tree, as in VS Code.
                                            .capture_key_down(cx.listener(
                                                |this, event: &KeyDownEvent, window, cx| {
                                                    if event.keystroke.key == "down"
                                                        && !this.rows.is_empty()
                                                    {
                                                        window.focus(&this.results_focus, cx);
                                                        this.select_row(0, cx);
                                                        cx.stop_propagation();
                                                    }
                                                },
                                            ))
                                            .child(self.render_input(
                                                &self.query_editor,
                                                toggles,
                                                window,
                                                cx,
                                            )),
                                    )
                                    .when(self.show_replace, |this| {
                                        let replace_all = IconButton::new(
                                            "search-panel-replace-all",
                                            IconName::ReplaceAll,
                                        )
                                        .shape(IconButtonShape::Square)
                                        .icon_size(IconSize::XSmall)
                                        .disabled(
                                            !has_results
                                                || self.limit_reached
                                                || !matches!(self.status, Status::Done)
                                                || self.pending_replace.is_some(),
                                        )
                                        .tooltip(Tooltip::text("Replace All"))
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.replace(ReplaceScope::All, window, cx)
                                        }))
                                        .into_any_element();
                                        this.child(self.render_input(
                                            &self.replace_editor,
                                            vec![replace_all],
                                            window,
                                            cx,
                                        ))
                                    }),
                            ),
                    )
                    .when(self.show_filters, |this| {
                        this.child(filter_label("files to include"))
                            .child(self.render_input(&self.include_editor, Vec::new(), window, cx))
                            .child(filter_label("files to exclude"))
                            .child(self.render_input(&self.exclude_editor, Vec::new(), window, cx))
                    })
                    .child(
                        h_flex()
                            .h(ROW_HEIGHT)
                            .justify_between()
                            .child(
                                h_flex()
                                    .min_w_0()
                                    .gap_1()
                                    .child(
                                        Label::new(self.summary().unwrap_or_default())
                                            .size(LabelSize::Small)
                                            .color(Color::Muted)
                                            .single_line()
                                            .truncate(),
                                    )
                                    .when(has_results, |this| {
                                        this.child(
                                            Button::new(
                                                "search-panel-open-in-editor",
                                                "Open in editor",
                                            )
                                            .label_size(LabelSize::Small)
                                            .color(Color::Accent)
                                            .tooltip(Tooltip::text(
                                                "Open these results in an editor tab",
                                            ))
                                            .on_click(
                                                cx.listener(|this, _, window, cx| {
                                                    this.open_in_editor(window, cx)
                                                }),
                                            ),
                                        )
                                    }),
                            )
                            .child(
                                IconButton::new("search-panel-filters", IconName::Ellipsis)
                                    .icon_size(IconSize::Small)
                                    .icon_color(Color::Muted)
                                    .toggle_state(self.show_filters)
                                    .tooltip(Tooltip::text("Toggle Search Details"))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_filters = !this.show_filters;
                                        cx.notify();
                                    })),
                            ),
                    ),
            )
            .when_some(self.replace_status.clone(), |this, status| {
                this.child(
                    div().px_3().child(
                        Label::new(status)
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
                )
            })
            .child(
                div()
                    .id("search-panel-results-container")
                    .flex_1()
                    .min_h_0()
                    .track_focus(&self.results_focus)
                    .on_key_down(cx.listener(Self::handle_results_key))
                    .child(
                        uniform_list(
                            "search-panel-results",
                            row_count,
                            cx.processor(|this, range: Range<usize>, window, cx| {
                                range
                                    .map(|row_ix| {
                                        this.render_row(row_ix, this.rows[row_ix], window, cx)
                                    })
                                    .collect()
                            }),
                        )
                        .with_decoration(
                            ui::indent_guides(px(INDENT_SIZE), IndentGuideColors::panel(cx))
                                .with_compute_indents_fn(cx.entity(), |this, range, _, _| {
                                    this.rows[range]
                                        .iter()
                                        .map(|row| match row {
                                            Row::File(_) => 0,
                                            Row::Match(..) => 1,
                                        })
                                        .collect()
                                }),
                        )
                        .track_scroll(&self.scroll_handle)
                        .size_full(),
                    ),
            )
    }
}

impl Panel for SearchPanel {
    fn persistent_name() -> &'static str {
        "Search Panel"
    }

    fn panel_key() -> &'static str {
        SEARCH_PANEL_KEY
    }

    fn activation_focus_handle(&self, cx: &App) -> FocusHandle {
        self.query_editor.focus_handle(cx)
    }

    fn position(&self, _window: &Window, _cx: &App) -> DockPosition {
        self.position
    }

    fn position_is_valid(&self, position: DockPosition) -> bool {
        // shortcut: left side only; moving it needs a persisted setting (BACKLOG).
        position == DockPosition::Left
    }

    fn set_position(&mut self, position: DockPosition, _: &mut Window, cx: &mut Context<Self>) {
        self.position = position;
        cx.notify();
    }

    fn default_size(&self, _window: &Window, _cx: &App) -> Pixels {
        px(320.)
    }

    fn icon(&self, _window: &Window, _cx: &App) -> Option<IconName> {
        Some(IconName::MagnifyingGlass)
    }

    fn icon_tooltip(&self, _window: &Window, _cx: &App) -> Option<&'static str> {
        Some("Search")
    }

    fn toggle_action(&self) -> Box<dyn Action> {
        Box::new(ToggleFocus)
    }

    fn activation_priority(&self) -> u32 {
        // Unique within the dock; the activity bar applies VS Code's display order.
        4
    }
}

fn file_labels(path: &RelPath, path_style: PathStyle) -> (SharedString, SharedString) {
    (
        path.file_name().unwrap_or_default().to_string().into(),
        path.parent()
            .map(|parent| parent.display(path_style).to_string())
            .unwrap_or_default()
            .into(),
    )
}

/// One preview line per match, from match start/end points and an optional anchor.
fn build_match_lines(
    snapshot: &language::BufferSnapshot,
    matches: impl Iterator<Item = (Point, Point, Option<Anchor>)>,
) -> Vec<MatchLine> {
    matches
        .map(|(start, end, anchor)| {
            let line_start = Point::new(start.row, 0);
            let line_end = Point::new(start.row, snapshot.line_len(start.row));
            let line = snapshot
                .text_for_range(line_start..line_end)
                .collect::<String>();
            let match_end_column = (end.row == start.row).then_some(end.column as usize);
            let (preview, highlight) =
                match_preview(&line, start.column as usize, match_end_column);
            MatchLine {
                anchor,
                point: start,
                line_number: start.row + 1,
                preview: preview.into(),
                highlight,
            }
        })
        .collect()
}

/// Builds the one-line preview shown under a file: leading indentation is
/// dropped, context before the match is cut to [`MAX_CONTEXT_BEFORE_MATCH`]
/// characters with a "…" prefix so the match is always visible, long lines are
/// cut at a char boundary, and the highlight is remapped onto the result.
/// Columns are byte offsets into `line`; a match continuing past this line has no end column.
fn match_preview(
    line: &str,
    start_column: usize,
    end_column: Option<usize>,
) -> (String, Range<usize>) {
    let indent = line.len() - line.trim_start().len();
    let start = start_column.clamp(indent, line.len());
    let end = end_column
        .map_or(line.len(), |end| end.min(line.len()))
        .max(start);
    let start = line.floor_char_boundary(start);
    let end = line.ceil_char_boundary(end);

    let context_start = line[indent..start]
        .char_indices()
        .rev()
        .nth(MAX_CONTEXT_BEFORE_MATCH - 1)
        .map_or(indent, |(offset, _)| indent + offset);
    let prefix = if context_start > indent { "…" } else { "" };
    let mut preview = format!("{prefix}{}", &line[context_start..]);
    let shift = prefix.len() as isize - context_start as isize;
    let remap = |offset: usize| (offset as isize + shift) as usize;
    let mut highlight = remap(start)..remap(end);

    if preview.len() > MAX_LINE_PREVIEW_CHARS {
        let cut = preview
            .floor_char_boundary(MAX_LINE_PREVIEW_CHARS.max(highlight.end.min(preview.len())));
        preview.truncate(cut);
    }
    highlight.end = highlight.end.min(preview.len());
    highlight.start = highlight.start.min(highlight.end);
    (preview, highlight)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{TestAppContext, VisualTestContext};
    use project::FakeFs;
    use serde_json::json;
    use settings::SettingsStore;
    use workspace::MultiWorkspace;

    #[gpui::test]
    async fn test_open_match_preview_and_pin(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings = SettingsStore::test(cx);
            cx.set_global(settings);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            editor::init(cx);
            crate::init(cx);
        });
        let fs = FakeFs::new(cx.background_executor.clone());
        fs.insert_tree(
            "/dir",
            json!({ "one.rs": "let needle = 1;", "two.rs": "fn f() { needle(); }" }),
        )
        .await;
        let project = Project::test(fs, ["/dir".as_ref()], cx).await;
        let window = cx.add_window(|window, cx| MultiWorkspace::test_new(project, window, cx));
        let workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();
        let cx = &mut VisualTestContext::from_window(window.into(), cx);
        let panel = workspace.update_in(cx, |workspace, window, cx| {
            cx.new(|cx| SearchPanel::new(workspace, window, cx))
        });

        panel.update_in(cx, |panel, window, cx| {
            panel
                .query_editor
                .update(cx, |editor, cx| editor.set_text("needle", window, cx));
            panel.run_search(cx);
        });
        cx.run_until_parked();
        assert_eq!(panel.read_with(cx, |panel, _| panel.results.len()), 2);

        let item_count = |cx: &mut VisualTestContext| {
            workspace.read_with(cx, |workspace, cx| {
                workspace.active_pane().read(cx).items_len()
            })
        };

        // Single clicks reuse one preview tab.
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(0, 0, true, true, window, cx)
        });
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, true, true, window, cx)
        });
        cx.run_until_parked();
        assert_eq!(item_count(cx), 1);

        // Double clicking the same result again pins it without duplicating the tab.
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, false, true, window, cx)
        });
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, false, true, window, cx)
        });
        cx.run_until_parked();
        assert_eq!(item_count(cx), 1);
        workspace.read_with(cx, |workspace, cx| {
            let pane = workspace.active_pane().read(cx);
            let active = pane.active_item().expect("an editor is open");
            assert!(!pane.is_active_preview_item(active.item_id()));
        });
    }

    async fn init_panel_test(
        cx: &mut TestAppContext,
        files: serde_json::Value,
    ) -> (Entity<SearchPanel>, Entity<Project>, VisualTestContext) {
        cx.update(|cx| {
            let settings = SettingsStore::test(cx);
            cx.set_global(settings);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            editor::init(cx);
            crate::init(cx);
        });
        let fs = FakeFs::new(cx.background_executor.clone());
        fs.insert_tree("/dir", files).await;
        let project = Project::test(fs, ["/dir".as_ref()], cx).await;
        let window =
            cx.add_window(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        let workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();
        let mut cx = VisualTestContext::from_window(window.into(), cx);
        let panel = workspace.update_in(&mut cx, |workspace, window, cx| {
            cx.new(|cx| SearchPanel::new(workspace, window, cx))
        });
        (panel, project, cx)
    }

    fn search(panel: &Entity<SearchPanel>, query: &str, regex: bool, cx: &mut VisualTestContext) {
        panel.update_in(cx, |panel, window, cx| {
            panel.regex = regex;
            panel
                .query_editor
                .update(cx, |editor, cx| editor.set_text(query, window, cx));
            panel.run_search(cx);
        });
        cx.run_until_parked();
    }

    fn replace(
        panel: &Entity<SearchPanel>,
        with: &str,
        scope: ReplaceScope,
        cx: &mut VisualTestContext,
    ) {
        panel.update_in(cx, |panel, window, cx| {
            panel
                .replace_editor
                .update(cx, |editor, cx| editor.set_text(with, window, cx));
            panel.replace(scope, window, cx);
        });
        cx.run_until_parked();
    }

    async fn buffer_text(
        project: &Entity<Project>,
        path: &str,
        cx: &mut VisualTestContext,
    ) -> (String, bool) {
        let buffer = project
            .update(cx, |project, cx| project.open_local_buffer(path, cx))
            .await
            .unwrap();
        buffer.read_with(cx, |buffer, _| (buffer.text(), buffer.is_dirty()))
    }

    #[gpui::test]
    async fn test_replace_all_saves_clean_files(cx: &mut TestAppContext) {
        let (panel, project, mut cx) = init_panel_test(
            cx,
            json!({ "one.rs": "let needle = needle;", "two.rs": "needle();" }),
        )
        .await;
        search(&panel, "needle", false, &mut cx);
        replace(&panel, "pin", ReplaceScope::All, &mut cx);

        assert_eq!(
            buffer_text(&project, "/dir/one.rs", &mut cx).await,
            ("let pin = pin;".into(), false)
        );
        assert_eq!(
            buffer_text(&project, "/dir/two.rs", &mut cx).await,
            ("pin();".into(), false)
        );
        panel.read_with(&cx, |panel, _| assert!(panel.results.is_empty()));
    }

    #[gpui::test]
    async fn test_replace_single_match_and_capture_groups(cx: &mut TestAppContext) {
        let (panel, project, mut cx) = init_panel_test(cx, json!({ "one.rs": "a1 b2\nc3" })).await;

        search(&panel, "b2", false, &mut cx);
        replace(&panel, "B2", ReplaceScope::Match(0, 0), &mut cx);
        assert_eq!(
            buffer_text(&project, "/dir/one.rs", &mut cx).await.0,
            "a1 B2\nc3"
        );

        search(&panel, r"([a-z])(\d)", true, &mut cx);
        // Replace only the second match (row 0, "c3" is on row 1): "$2$1" swaps letter and digit.
        let target = panel.read_with(&cx, |panel, _| {
            panel.results[0]
                .matches
                .iter()
                .position(|m| m.line_number == 2)
                .unwrap()
        });
        replace(&panel, "$2$1", ReplaceScope::Match(0, target), &mut cx);
        assert_eq!(
            buffer_text(&project, "/dir/one.rs", &mut cx).await.0,
            "a1 B2\n3c"
        );
    }

    #[gpui::test]
    async fn test_results_refresh_after_replace_creates_matches(cx: &mut TestAppContext) {
        let (panel, project, mut cx) = init_panel_test(cx, json!({ "one.rs": "a a" })).await;
        search(&panel, "a", false, &mut cx);
        replace(&panel, "a a", ReplaceScope::Match(0, 0), &mut cx);
        panel.read_with(&cx, |panel, _| {
            assert_eq!(panel.results[0].matches.len(), 3)
        });
        // The second row now points at inserted text; only that occurrence must change.
        replace(&panel, "b", ReplaceScope::Match(0, 1), &mut cx);
        assert_eq!(
            buffer_text(&project, "/dir/one.rs", &mut cx).await,
            ("a b a".into(), false)
        );
    }

    #[test]
    fn test_match_preview() {
        let (preview, highlight) = match_preview("    foo(SendSmsCode);", 8, Some(19));
        assert_eq!(preview, "foo(SendSmsCode);");
        assert_eq!(&preview[highlight], "SendSmsCode");

        // Multi-byte text keeps byte offsets on char boundaries.
        let line = "  // 只查原开户结果 SendSmsCode";
        let start = line.find("SendSmsCode").unwrap();
        let (preview, highlight) = match_preview(line, start, Some(start + 11));
        assert_eq!(&preview[highlight], "SendSmsCode");

        // A match spanning lines highlights to the end of this line.
        let (preview, highlight) = match_preview("let x = `multi", 8, None);
        assert_eq!(&preview[highlight], "`multi");

        // Long leading context is cut with "…" so the match stays visible.
        let long = format!("{}needle tail", "中".repeat(100));
        let start = long.find("needle").unwrap();
        let (preview, highlight) = match_preview(&long, start, Some(start + 6));
        assert!(preview.starts_with('…'));
        assert_eq!(&preview[highlight], "needle");
        assert_eq!(
            preview.chars().count(),
            1 + MAX_CONTEXT_BEFORE_MATCH + "needle tail".len()
        );
    }
}
