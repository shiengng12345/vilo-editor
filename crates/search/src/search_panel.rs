//! Vilo: VS Code style search side bar.
//!
//! Runs the same `project.search` as the center Project Search view, but shows
//! results as a per-file tree in the left dock instead of a multibuffer.

use std::ops::Range;
use std::pin::pin;

use collections::HashSet;
use editor::{Editor, EditorEvent};
use futures::StreamExt as _;
use gpui::{
    Action, App, Context, Entity, EventEmitter, FocusHandle, Focusable, Pixels, Subscription, Task,
    WeakEntity, Window, actions, px, uniform_list,
};
use language::{Buffer, Point, ToPoint as _};
use project::{
    Project,
    search::{SearchQuery, SearchResult},
};
use text::Anchor;
use ui::{HighlightedLabel, IconButton, IconButtonShape, Tooltip, prelude::*};
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
    range: Range<Anchor>,
    line_number: u32,
    preview: SharedString,
    highlight: Range<usize>,
}

struct FileResult {
    buffer: Entity<Buffer>,
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
    status: Status,
    position: DockPosition,
    pending_debounce: Option<Task<()>>,
    pending_search: Option<Task<()>>,
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
            status: Status::Idle,
            position: DockPosition::Left,
            pending_debounce: None,
            pending_search: None,
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
        self.limit_reached = false;
        self.rebuild_rows();
        let query = match self.build_query(cx) {
            None => {
                self.status = Status::Idle;
                self.pending_search = None;
                cx.notify();
                return;
            }
            Some(Err(_)) => {
                self.status = Status::InvalidQuery;
                self.pending_search = None;
                cx.notify();
                return;
            }
            Some(Ok(query)) => query,
        };
        self.status = Status::Searching;
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
                                this.push_result(buffer, ranges, cx)
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
            this.update(cx, |this, cx| {
                this.status = Status::Done;
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn push_result(&mut self, buffer: Entity<Buffer>, ranges: Vec<Range<Anchor>>, cx: &App) {
        if ranges.is_empty() {
            return;
        }
        let buffer_ref = buffer.read(cx);
        let snapshot = buffer_ref.snapshot();
        let icon = buffer_ref
            .file()
            .and_then(|file| file_icons::FileIcons::get_icon(file.path().as_std_path(), cx));
        let (file_name, directory) = match buffer_ref.file() {
            Some(file) => {
                let path = file.path();
                (
                    path.file_name().unwrap_or_default().to_string().into(),
                    path.parent()
                        .map(|parent| parent.display(file.path_style(cx)).to_string())
                        .unwrap_or_default()
                        .into(),
                )
            }
            None => ("untitled".into(), SharedString::default()),
        };
        let matches = ranges
            .into_iter()
            .map(|range| {
                let start = range.start.to_point(&snapshot);
                let end = range.end.to_point(&snapshot);
                let line_start = Point::new(start.row, 0);
                let line_end = Point::new(start.row, snapshot.line_len(start.row));
                let line = snapshot
                    .text_for_range(line_start..line_end)
                    .collect::<String>();
                let match_end_column = (end.row == start.row).then_some(end.column as usize);
                let (preview, highlight) =
                    match_preview(&line, start.column as usize, match_end_column);
                MatchLine {
                    range,
                    line_number: start.row + 1,
                    preview: preview.into(),
                    highlight,
                }
            })
            .collect();
        self.results.push(FileResult {
            buffer,
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
        let buffer = file.buffer.clone();
        // Anchors follow edits made after the search, so this lands on the current match position.
        let point = search_match
            .range
            .start
            .to_point(&buffer.read(cx).snapshot());
        let Some(workspace) = self.workspace.upgrade() else {
            return;
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

    fn summary(&self) -> Option<SharedString> {
        let files = self.results.len();
        let matches = self.match_count();
        let text = match self.status {
            Status::Idle => return None,
            Status::InvalidQuery => "Invalid search query".to_string(),
            Status::WaitingForScan if files == 0 => "Waiting for project scan…".to_string(),
            Status::Searching if files == 0 => "Searching…".to_string(),
            Status::Done if files == 0 => "No results found".to_string(),
            _ if self.limit_reached => {
                format!("{matches} results in {files} files (limit reached, refine the search)")
            }
            _ => format!(
                "{matches} result{} in {files} file{}",
                if matches == 1 { "" } else { "s" },
                if files == 1 { "" } else { "s" }
            ),
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
        cx: &App,
    ) -> impl IntoElement {
        h_flex()
            .w_full()
            .flex_none()
            .h(px(26.))
            .pl_1p5()
            .pr_0p5()
            .gap_0p5()
            .rounded_sm()
            .border_1()
            .border_color(cx.theme().colors().border)
            .bg(cx.theme().colors().editor_background)
            .child(div().flex_1().min_w_0().child(editor.clone()))
            .children(trailing)
    }

    fn render_row(&self, row: Row, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors();
        match row {
            Row::File(file_ix) => {
                let file = &self.results[file_ix];
                let collapsed = self.collapsed.contains(&file_ix);
                h_flex()
                    .id(("search-file", file_ix))
                    .w_full()
                    .px_1()
                    .gap_1()
                    .cursor_pointer()
                    .hover(|style| style.bg(colors.ghost_element_hover))
                    .child(
                        Icon::new(if collapsed {
                            IconName::ChevronRight
                        } else {
                            IconName::ChevronDown
                        })
                        .size(IconSize::Small)
                        .color(Color::Muted),
                    )
                    .when_some(file.icon.clone(), |this, icon| {
                        this.child(
                            Icon::from_path(icon)
                                .size(IconSize::Small)
                                .color(Color::Muted),
                        )
                    })
                    .child(
                        Label::new(file.file_name.clone())
                            .size(LabelSize::Small)
                            .single_line(),
                    )
                    .child(
                        div().flex_1().min_w_0().child(
                            Label::new(file.directory.clone())
                                .color(Color::Muted)
                                .size(LabelSize::XSmall)
                                .single_line()
                                .truncate(),
                        ),
                    )
                    .child(
                        div()
                            .flex_none()
                            .px_1p5()
                            .rounded_full()
                            .bg(colors.element_background)
                            .child(
                                Label::new(file.matches.len().to_string()).size(LabelSize::XSmall),
                            ),
                    )
                    .on_click(cx.listener(move |this, _, _, cx| this.toggle_file(file_ix, cx)))
                    .into_any_element()
            }
            Row::Match(file_ix, match_ix) => {
                let search_match = &self.results[file_ix].matches[match_ix];
                let preview = search_match.preview.clone();
                let highlight = search_match.highlight.clone();
                h_flex()
                    .id(ElementId::NamedInteger(
                        "search-match".into(),
                        ((file_ix as u64) << 32) | match_ix as u64,
                    ))
                    .w_full()
                    .pl_6()
                    .pr_1()
                    .cursor_pointer()
                    .hover(|style| style.bg(colors.ghost_element_hover))
                    .tooltip(Tooltip::text(format!("Line {}", search_match.line_number)))
                    .child(
                        HighlightedLabel::from_ranges(preview, vec![highlight])
                            .size(LabelSize::Small)
                            .single_line()
                            .truncate(),
                    )
                    .on_click(
                        cx.listener(move |this, event: &gpui::ClickEvent, window, cx| {
                            // Single click previews, double click pins the tab, as in VS Code.
                            let allow_preview = event.click_count() < 2;
                            this.open_match(file_ix, match_ix, allow_preview, window, cx);
                        }),
                    )
                    .into_any_element()
            }
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
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
            |text: &'static str| Label::new(text).size(LabelSize::XSmall).color(Color::Muted);

        v_flex()
            .id("search-panel")
            .key_context("SearchPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .child(
                h_flex().flex_none().h(px(32.)).px_3().child(
                    Label::new("SEARCH")
                        .size(LabelSize::XSmall)
                        .weight(gpui::FontWeight::SEMIBOLD),
                ),
            )
            .child(
                v_flex()
                    .flex_none()
                    .px_3()
                    .gap_1()
                    .child(self.render_input(&self.query_editor, toggles, cx))
                    .child(
                        h_flex().justify_end().child(
                            IconButton::new("search-panel-filters", IconName::Ellipsis)
                                .icon_size(IconSize::XSmall)
                                .toggle_state(self.show_filters)
                                .tooltip(Tooltip::text("Toggle Search Details"))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.show_filters = !this.show_filters;
                                    cx.notify();
                                })),
                        ),
                    )
                    .when(self.show_filters, |this| {
                        this.child(filter_label("files to include"))
                            .child(self.render_input(&self.include_editor, Vec::new(), cx))
                            .child(filter_label("files to exclude"))
                            .child(self.render_input(&self.exclude_editor, Vec::new(), cx))
                    })
                    .when_some(self.summary(), |this, summary| {
                        this.child(
                            Label::new(summary)
                                .size(LabelSize::XSmall)
                                .color(Color::Muted),
                        )
                    }),
            )
            .child(
                div().flex_1().min_h_0().pt_1().child(
                    uniform_list(
                        "search-panel-results",
                        row_count,
                        cx.processor(|this, range: Range<usize>, _window, cx| {
                            this.rows[range]
                                .to_vec()
                                .into_iter()
                                .map(|row| this.render_row(row, cx))
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

/// Builds the one-line preview shown under a file: leading indentation is
/// dropped, long lines are cut at a char boundary, and the highlight is
/// clamped to what remains. Columns are byte offsets into `line`; a match that
/// continues past this line has no end column.
fn match_preview(
    line: &str,
    start_column: usize,
    end_column: Option<usize>,
) -> (String, Range<usize>) {
    let indent = line.len() - line.trim_start().len();
    let mut preview = line[indent..].to_string();
    if preview.len() > MAX_LINE_PREVIEW_CHARS {
        let cut = preview.floor_char_boundary(MAX_LINE_PREVIEW_CHARS);
        preview.truncate(cut);
    }
    let start = start_column.saturating_sub(indent).min(preview.len());
    let end = end_column
        .map_or(preview.len(), |end| end.saturating_sub(indent))
        .min(preview.len())
        .max(start);
    (preview, start..end)
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

        // Long lines are truncated and the highlight is clamped.
        let long = format!("{}needle", "中".repeat(100));
        let (preview, highlight) = match_preview(&long, 300, Some(306));
        assert!(preview.len() <= MAX_LINE_PREVIEW_CHARS);
        assert!(preview.is_char_boundary(preview.len()));
        assert!(highlight.is_empty());
    }
}
