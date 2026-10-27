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
    Action, App, Context, Entity, EventEmitter, FocusHandle, Focusable, Pixels, Subscription, Task,
    WeakEntity, Window, actions, px, uniform_list,
};
use language::{Buffer, Point, ToPoint as _};
use project::{
    Project, ProjectPath,
    search::{SearchQuery, SearchResult},
};
use util::paths::PathStyle;
use util::rel_path::RelPath;

use crate::disk_search::{self, WalkProgress};
use text::Anchor;
use ui::{
    IconButton, IconButtonShape, IndentGuideColors, ListItem, ListItemSpacing, Tooltip, prelude::*,
    rems_from_px,
};
use util::{ResultExt as _, paths::PathMatcher};
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
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
                disk_search::DiskRoot {
                    worktree_id: worktree.id(),
                    abs_path: worktree.abs_path(),
                }
            })
            .collect::<Vec<_>>();
        // Unsaved edits live only in memory: search those buffers directly and skip their files on disk.
        let dirty_buffers = project
            .opened_buffers(cx)
            .into_iter()
            .filter(|buffer| buffer.read(cx).is_dirty())
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

            let mut chunks = pin!(candidates.ready_chunks(64));
            while let Some(chunk) = chunks.next().await {
                for candidate in chunk {
                    let Ok(snapshot) = this.update(cx, |_, cx| {
                        Buffer::build_snapshot(
                            text::Rope::from(candidate.text.as_str()),
                            None,
                            None,
                            None,
                            cx,
                        )
                    }) else {
                        return;
                    };
                    let query = query.clone();
                    let lines = cx
                        .background_spawn(async move {
                            let snapshot = snapshot.await;
                            let ranges = query.search(&snapshot, None).await;
                            build_match_lines(
                                &snapshot,
                                ranges.into_iter().map(|range| {
                                    (
                                        snapshot.offset_to_point(range.start),
                                        snapshot.offset_to_point(range.end),
                                        None,
                                    )
                                }),
                            )
                        })
                        .await;
                    let target = ProjectPath {
                        worktree_id: candidate.worktree_id,
                        path: candidate.rel_path,
                    };
                    let reached_limit = this
                        .update(cx, |this, cx| {
                            this.push_path_result(target, lines, cx);
                            this.match_count() >= MAX_MATCHES
                        })
                        .unwrap_or(true);
                    if reached_limit {
                        progress.cancelled.store(true, Ordering::Relaxed);
                        this.update(cx, |this, _| this.limit_reached = true).ok();
                        break;
                    }
                }
                if this
                    .update(cx, |this, cx| {
                        this.rebuild_rows();
                        cx.notify();
                    })
                    .is_err()
                {
                    return;
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
                            workspace.activate_item(&editor, true, true, window, cx);
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
                            true,
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
                        true,
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
        let focused = selected && self.focus_handle.contains_focused(window, cx);
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
                        this.open_match(file_ix, match_ix, allow_preview, window, cx);
                        cx.notify();
                    }),
                )
                .into_any_element()
            }
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
                    .child(self.render_input(&self.query_editor, toggles, window, cx))
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
                                Label::new(self.summary().unwrap_or_default())
                                    .size(LabelSize::Small)
                                    .color(Color::Muted)
                                    .single_line()
                                    .truncate(),
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
            .child(
                div().flex_1().min_h_0().child(
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
            panel.open_match(0, 0, true, window, cx)
        });
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, true, window, cx)
        });
        cx.run_until_parked();
        assert_eq!(item_count(cx), 1);

        // Double clicking the same result again pins it without duplicating the tab.
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, false, window, cx)
        });
        panel.update_in(cx, |panel, window, cx| {
            panel.open_match(1, 0, false, window, cx)
        });
        cx.run_until_parked();
        assert_eq!(item_count(cx), 1);
        workspace.read_with(cx, |workspace, cx| {
            let pane = workspace.active_pane().read(cx);
            let active = pane.active_item().expect("an editor is open");
            assert!(!pane.is_active_preview_item(active.item_id()));
        });
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
