use crate::dock::{Dock, PanelHandle};
use gpui::{Action, Anchor, Entity, Focusable, Subscription};
use settings::SettingsStore;
use std::sync::Arc;
use ui::{ContextMenu, CountBadge, IconButton, PopoverMenu, Tooltip, prelude::*, rems_from_px};

pub const ACTIVITY_BAR_WIDTH: Pixels = px(48.);
const ACTIVE_INDICATOR_WIDTH: Pixels = px(2.);
/// VS Code's activity bar order; other panels follow in dock order.
const VS_CODE_ORDER: &[&str] = &["Project Panel", "Search Panel", "GitPanel", "DebugPanel"];

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
            format!("activity-bar-{}", panel.persistent_name()).into(),
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
    let colors = cx.theme().colors();
    // VS Code metrics: 48px square items, 24px icons, inactive icons at 65% until hovered.
    div()
        .id(id)
        .relative()
        .flex()
        .items_center()
        .justify_center()
        .size(ACTIVITY_BAR_WIDTH)
        .flex_none()
        .cursor_pointer()
        .tab_index(0isize)
        .aria_label(tooltip.clone())
        .opacity(if is_active { 1. } else { 0.65 })
        .child(
            div()
                .absolute()
                .left_0()
                .top(px(12.))
                .h(px(24.))
                .w(ACTIVE_INDICATOR_WIDTH)
                .when(is_active, |this| this.bg(colors.text_accent)),
        )
        .child(
            Icon::new(icon)
                .size(IconSize::Custom(rems_from_px(24_f32)))
                .color(if is_active {
                    Color::Default
                } else {
                    Color::Muted
                }),
        )
        .when_some(badge, |this, count| {
            this.child(
                div()
                    .absolute()
                    .top(px(6.))
                    .right(px(6.))
                    .child(CountBadge::new(count)),
            )
        })
        .tooltip({
            let action = action.boxed_clone();
            move |_window, cx| Tooltip::for_action(tooltip.clone(), &*action, cx)
        })
        .on_click(move |_, window, cx| {
            // Actions route through the focused element, so focus the dock first.
            window.focus(&focus_handle, cx);
            window.dispatch_action(action.boxed_clone(), cx);
        })
        .hover(move |style| style.bg(colors.ghost_element_hover).opacity(1.))
        .into_any_element()
}

impl Render for ActivityBar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let dock = self.left_dock.read(cx);
        let focus_handle = dock.focus_handle(cx);
        let panels = dock.panels().cloned().collect::<Vec<_>>();

        let mut ordered = panels.iter().enumerate().collect::<Vec<_>>();
        ordered.sort_by_key(|(index, panel)| {
            let rank = VS_CODE_ORDER
                .iter()
                .position(|name| *name == panel.persistent_name())
                .unwrap_or(VS_CODE_ORDER.len());
            (rank, *index)
        });
        let mut items = ordered
            .into_iter()
            .filter_map(|(index, panel)| self.panel_item(index, panel, window, cx))
            .collect::<Vec<_>>();
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
            .child(v_flex().w_full().children(items))
            .child(
                h_flex().w_full().justify_center().child(
                    PopoverMenu::new("activity-bar-manage")
                        .trigger_with_tooltip(
                            IconButton::new("activity-bar-manage-button", IconName::Settings)
                                .icon_size(IconSize::Custom(rems_from_px(24_f32)))
                                .icon_color(Color::Muted)
                                .size(ButtonSize::Large)
                                .tab_index(0isize),
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
