mod vault;
mod vault_view;

use gpui::{App, Entity, FocusHandle, Focusable, prelude::*};
use ui::prelude::*;
use workspace::Workspace;

use vault_view::VaultView;

const MODULE_ID: &str = "tools";

/// The Tools module: a full-window page hosting Vilo's tools. Vault is the first.
pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, cx| {
        workspace.register_module(
            MODULE_ID,
            "icons/vilo/codicons/tools.svg",
            "Tools",
            |window, cx| {
                let vault = cx.new(|cx| VaultView::new(window, cx));
                let page = cx.new(|_| ToolsPage { vault });
                let focus_handle = page.focus_handle(cx);
                (page.into(), focus_handle)
            },
            cx,
        );
    })
    .detach();
}

struct ToolsPage {
    vault: Entity<VaultView>,
}

impl Focusable for ToolsPage {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.vault.focus_handle(cx)
    }
}

impl Render for ToolsPage {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors();
        v_flex()
            .size_full()
            .bg(colors.editor_background)
            .child(
                h_flex()
                    .px_4()
                    .py_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(colors.border)
                    .child(Label::new("Tools").color(Color::Muted))
                    .child(Label::new("/").color(Color::Muted))
                    .child(Label::new("Vault")),
            )
            .child(div().flex_1().min_h_0().child(self.vault.clone()))
    }
}
