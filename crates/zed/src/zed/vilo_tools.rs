use gpui::{App, FocusHandle, Focusable, prelude::*};
use ui::prelude::*;
use workspace::Workspace;

const MODULE_ID: &str = "tools";

/// The Tools module: a full-window page that will host Vilo's tools. Empty for now.
pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, cx| {
        workspace.register_module(
            MODULE_ID,
            "icons/vilo/codicons/tools.svg",
            "Tools",
            |_, cx| {
                let page = cx.new(|cx| ToolsPage {
                    focus_handle: cx.focus_handle(),
                });
                let focus_handle = page.focus_handle(cx);
                (page.into(), focus_handle)
            },
            cx,
        );
    })
    .detach();
}

struct ToolsPage {
    focus_handle: FocusHandle,
}

impl Focusable for ToolsPage {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ToolsPage {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .p_8()
            .gap_1()
            .bg(cx.theme().colors().editor_background)
            .child(Headline::new("Tools").size(HeadlineSize::Large))
            .child(Label::new("No tools yet.").color(Color::Muted))
    }
}
