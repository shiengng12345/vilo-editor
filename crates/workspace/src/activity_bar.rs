use crate::Workspace;
use crate::dock::{Dock, PanelHandle};
use gpui::{Action, Anchor, Entity, Focusable, Subscription, WeakEntity};
use settings::SettingsStore;
use std::sync::Arc;
use ui::{ContextMenu, CountBadge, IconButton, PopoverMenu, Tooltip, prelude::*, rems_from_px};

pub const ACTIVITY_BAR_WIDTH: Pixels = px(48.);
const ACTIVE_INDICATOR_WIDTH: Pixels = px(2.);
/// VS Code's activity bar order; other panels follow in dock order.
const VS_CODE_ORDER: &[&str] = &["Project Panel", "Search Panel", "GitPanel", "DebugPanel"];

/// VS Code's Codicon and view name for a panel, keyed by its persistent name.
fn vs_code_view(persistent_name: &str) -> Option<(&'static str, &'static str)> {
    Some(match persistent_name {
        "Project Panel" => ("icons/vilo/codicons/files.svg", "Explorer"),
        "Search Panel" => ("icons/vilo/codicons/search.svg", "Search"),
        "GitPanel" => ("icons/vilo/codicons/source-control.svg", "Source Control"),
        "DebugPanel" => ("icons/vilo/codicons/debug-alt.svg", "Run and Debug"),
        "Outline Panel" => ("icons/vilo/codicons/list-tree.svg", "Outline"),
        _ => return None,
    })
}

/// Vilo: VS Code style activity bar along the left edge of the window.
///
/// It shows the left dock's panels plus views that Zed implements as center
/// items (Search, Extensions), so it reads the dock instead of extending the
/// dock's own panel buttons.
pub struct ActivityBar {
    left_dock: Entity<Dock>,
    workspace: WeakEntity<Workspace>,
    modules: Vec<ModuleItem>,
    active_module: Option<&'static str>,
    _subscriptions: Vec<Subscription>,
}

#[derive(Clone)]
pub(crate) struct ModuleItem {
    pub id: &'static str,
    pub icon_path: &'static str,
    pub tooltip: SharedString,
}

impl ActivityBar {
    pub fn new(
        left_dock: Entity<Dock>,
        workspace: WeakEntity<Workspace>,
        cx: &mut Context<Self>,
    ) -> Self {
        let subscriptions = vec![
            cx.observe(&left_dock, |_, _, cx| cx.notify()),
            cx.observe_global::<SettingsStore>(|_, cx| cx.notify()),
        ];
        Self {
            left_dock,
            workspace,
            modules: Vec::new(),
            active_module: None,
            _subscriptions: subscriptions,
        }
    }

    pub(crate) fn set_modules(
        &mut self,
        modules: Vec<ModuleItem>,
        active_module: Option<&'static str>,
        cx: &mut Context<Self>,
    ) {
        self.modules = modules;
        self.active_module = active_module;
        cx.notify();
    }

    fn panel_item(
        &self,
        index: usize,
        panel: &Arc<dyn PanelHandle>,
        window: &Window,
        cx: &App,
    ) -> Option<AnyElement> {
        let dock = self.left_dock.read(cx);
        let zed_icon = panel.icon(window, cx)?;
        let (icon, tooltip) = match vs_code_view(panel.persistent_name()) {
            Some((path, name)) => (Icon::from_path(path), SharedString::from(name)),
            None => (Icon::new(zed_icon), panel.icon_tooltip(window, cx)?.into()),
        };
        // While a module covers the code layout, no panel reads as active, and a click
        // returns to the code layout with that panel shown.
        let in_module = self.active_module.is_some();
        let is_active = !in_module && dock.is_open() && dock.active_panel_index() == Some(index);
        // Clicking the active view collapses the side bar, as in VS Code.
        let action = if is_active {
            dock.toggle_action()
        } else {
            panel.toggle_action(window, cx)
        };
        let focus_handle = dock.focus_handle(cx);
        let workspace = self.workspace.clone();
        let dispatch = action.boxed_clone();
        let badge = panel
            .icon_label(window, cx)
            .and_then(|label| label.parse::<usize>().ok());
        Some(render_item(
            format!("activity-bar-{}", panel.persistent_name()).into(),
            icon,
            tooltip,
            Some(action),
            is_active,
            badge,
            Box::new(move |window, cx| {
                if in_module {
                    workspace
                        .update(cx, |workspace, cx| workspace.show_code(false, window, cx))
                        .ok();
                }
                // Actions route through the focused element, so focus the dock first.
                window.focus(&focus_handle, cx);
                window.dispatch_action(dispatch.boxed_clone(), cx);
            }),
            cx,
        ))
    }

    fn module_item(&self, module: &ModuleItem, cx: &App) -> AnyElement {
        let id = module.id;
        let workspace = self.workspace.clone();
        render_item(
            format!("activity-bar-module-{id}").into(),
            Icon::from_path(module.icon_path),
            module.tooltip.clone(),
            None,
            self.active_module == Some(id),
            None,
            Box::new(move |window, cx| {
                workspace
                    .update(cx, |workspace, cx| {
                        workspace.activate_module(id, window, cx)
                    })
                    .ok();
            }),
            cx,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn render_item(
    id: SharedString,
    icon: Icon,
    tooltip: SharedString,
    tooltip_action: Option<Box<dyn Action>>,
    is_active: bool,
    badge: Option<usize>,
    on_click: Box<dyn Fn(&mut Window, &mut App)>,
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
            icon.size(IconSize::Custom(rems_from_px(24_f32)))
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
        .tooltip(move |_window, cx| match &tooltip_action {
            Some(action) => Tooltip::for_action(tooltip.clone(), &**action, cx),
            None => Tooltip::simple(tooltip.clone(), cx),
        })
        .on_click(move |_, window, cx| on_click(window, cx))
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
        items.extend(
            self.modules
                .iter()
                .map(|module| self.module_item(module, cx)),
        );
        let extensions = zed_actions::Extensions::default().boxed_clone();
        items.push(render_item(
            "activity-bar-extensions".into(),
            Icon::from_path("icons/vilo/codicons/extensions.svg"),
            "Extensions".into(),
            Some(extensions.boxed_clone()),
            false,
            None,
            Box::new(move |window, cx| {
                window.focus(&focus_handle, cx);
                window.dispatch_action(extensions.boxed_clone(), cx);
            }),
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
