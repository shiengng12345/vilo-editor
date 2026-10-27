use crate::dock::{Dock, PanelHandle};
use crate::pane::DeploySearch;
use gpui::{Action, Anchor, Entity, Focusable, Subscription};
use settings::SettingsStore;
use std::sync::Arc;
use ui::{ContextMenu, CountBadge, IconButton, PopoverMenu, Tooltip, prelude::*};

pub const ACTIVITY_BAR_WIDTH: Pixels = px(48.);
const ACTIVE_INDICATOR_WIDTH: Pixels = px(2.);
const PROJECT_PANEL_NAME: &str = "Project Panel";

/// Vilo: VS Code style activity bar along the left edge of the window.
///
/// It shows the left dock's panels plus views that Zed implements as center
/// items (Search, Extensions), so it reads the dock instead of extending the
/// dock's own panel buttons.
pub struct ActivityBar {
    left_dock: Entity<Dock>,
    _subscriptions: Vec<Subscription>,
}

impl ActivityBar {
    pub fn new(left_dock: Entity<Dock>, cx: &mut Context<Self>) -> Self {
        let subscriptions = vec![
            cx.observe(&left_dock, |_, _, cx| cx.notify()),
            cx.observe_global::<SettingsStore>(|_, cx| cx.notify()),
        ];
        Self {
            left_dock,
            _subscriptions: subscriptions,
        }
    }

    fn panel_item(
        &self,
        index: usize,
        panel: &Arc<dyn PanelHandle>,
        window: &Window,
        cx: &App,
    ) -> Option<AnyElement> {
        let dock = self.left_dock.read(cx);
        let icon = panel.icon(window, cx)?;
        let tooltip = panel.icon_tooltip(window, cx)?;
        let is_active = dock.is_open() && dock.active_panel_index() == Some(index);
        // Clicking the active view collapses the side bar, as in VS Code.
        let action = if is_active {
            dock.toggle_action()
        } else {
            panel.toggle_action(window, cx)
        };
        let badge = panel
            .icon_label(window, cx)
            .and_then(|label| label.parse::<usize>().ok());
        Some(render_item(
            panel.persistent_name().into(),
            icon,
            tooltip.into(),
            action,
            is_active,
            badge,
            dock.focus_handle(cx),
            cx,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn render_item(
    id: SharedString,
    icon: IconName,
    tooltip: SharedString,
    action: Box<dyn Action>,
    is_active: bool,
    badge: Option<usize>,
    focus_handle: gpui::FocusHandle,
    cx: &App,
) -> AnyElement {
    let indicator_color = cx.theme().colors().text_accent;
    h_flex()
        .relative()
        .w_full()
        .justify_center()
        .child(
            div()
                .absolute()
                .left_0()
                .top_0()
                .bottom_0()
                .w(ACTIVE_INDICATOR_WIDTH)
                .when(is_active, |this| this.bg(indicator_color)),
        )
        .child(
            div()
                .relative()
                .child(
                    IconButton::new(id, icon)
                        .icon_size(IconSize::Medium)
                        .size(ButtonSize::Large)
                        .toggle_state(is_active)
                        .aria_label(tooltip.clone())
                        .tooltip({
                            let action = action.boxed_clone();
                            move |_window, cx| Tooltip::for_action(tooltip.clone(), &*action, cx)
                        })
                        .on_click(move |_, window, cx| {
                            // Actions route through the focused element, so focus the dock first.
                            window.focus(&focus_handle, cx);
                            window.dispatch_action(action.boxed_clone(), cx);
                        }),
                )
                .when_some(badge, |this, count| this.child(CountBadge::new(count))),
        )
        .into_any_element()
}

impl Render for ActivityBar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let dock = self.left_dock.read(cx);
        let focus_handle = dock.focus_handle(cx);
        let panels = dock.panels().cloned().collect::<Vec<_>>();

        let mut items = Vec::new();
        let mut search_placed = false;
        for (index, panel) in panels.iter().enumerate() {
            items.extend(self.panel_item(index, panel, window, cx));
            if panel.persistent_name() == PROJECT_PANEL_NAME {
                items.push(search_item(focus_handle.clone(), cx));
                search_placed = true;
            }
        }
        if !search_placed {
            items.insert(0, search_item(focus_handle.clone(), cx));
        }
        items.push(render_item(
            "activity-bar-extensions".into(),
            IconName::Blocks,
            "Extensions".into(),
            zed_actions::Extensions::default().boxed_clone(),
            false,
            None,
            focus_handle,
            cx,
        ));

        let colors = cx.theme().colors();
        v_flex()
            .id("activity-bar")
            .flex_none()
            .h_full()
            .w(ACTIVITY_BAR_WIDTH)
            .py_1()
            .justify_between()
            .bg(colors.title_bar_background)
            .border_r_1()
            .border_color(colors.border)
            .child(v_flex().w_full().gap_1().children(items))
            .child(
                h_flex().w_full().justify_center().child(
                    PopoverMenu::new("activity-bar-manage")
                        .trigger_with_tooltip(
                            IconButton::new("activity-bar-manage-button", IconName::Settings)
                                .icon_size(IconSize::Medium)
                                .size(ButtonSize::Large),
                            Tooltip::text("Manage"),
                        )
                        .anchor(Anchor::BottomLeft)
                        .attach(Anchor::BottomRight)
                        .menu(|window, cx| {
                            Some(ContextMenu::build(window, cx, |menu, _, _| {
                                menu.action("Settings", zed_actions::OpenSettings.boxed_clone())
                                    .action(
                                        "Keyboard Shortcuts",
                                        zed_actions::OpenKeymap.boxed_clone(),
                                    )
                                    .separator()
                                    .action(
                                        "Themes",
                                        zed_actions::theme_selector::Toggle::default()
                                            .boxed_clone(),
                                    )
                                    .action(
                                        "Extensions",
                                        zed_actions::Extensions::default().boxed_clone(),
                                    )
                            }))
                        }),
                ),
            )
    }
}

// shortcut: Search opens Zed's center project search until the W0.5 search side bar exists,
// so it never shows the active indicator; switch it to the side bar panel then.
fn search_item(focus_handle: gpui::FocusHandle, cx: &App) -> AnyElement {
    render_item(
        "activity-bar-search".into(),
        IconName::MagnifyingGlass,
        "Search".into(),
        DeploySearch::default().boxed_clone(),
        false,
        None,
        focus_handle,
        cx,
    )
}
