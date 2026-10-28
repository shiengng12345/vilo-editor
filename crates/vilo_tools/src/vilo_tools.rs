mod vault;
mod vault_compare;
mod vault_import;
mod vault_view;

use gpui::{App, Entity, FocusHandle, Focusable, prelude::*};
use ui::{Button, ButtonStyle};
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
            |_, cx| {
                let page = cx.new(|cx| ToolsPage {
                    focus_handle: cx.focus_handle(),
                    vault: None,
                    open: None,
                });
                let focus_handle = page.focus_handle(cx);
                (page.into(), focus_handle)
            },
            cx,
        );
    })
    .detach();
}

#[derive(Clone, Copy, PartialEq)]
enum Tool {
    Vault,
}

struct ToolsPage {
    focus_handle: FocusHandle,
    /// Built on first open and kept, so returning to it keeps its state.
    vault: Option<Entity<VaultView>>,
    open: Option<Tool>,
}

impl ToolsPage {
    fn open_tool(&mut self, tool: Tool, window: &mut Window, cx: &mut Context<Self>) {
        let vault = self
            .vault
            .get_or_insert_with(|| cx.new(|cx| VaultView::new(window, cx)))
            .clone();
        self.open = Some(tool);
        window.focus(&vault.focus_handle(cx), cx);
        cx.notify();
    }

    fn render_home(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors();
        v_flex()
            .p_8()
            .gap_4()
            .child(Headline::new("Tools").size(HeadlineSize::Large))
            .child(
                h_flex().gap_4().flex_wrap().child(
                    v_flex()
                        .id("tool-vault")
                        .w(px(260.))
                        .p_4()
                        .gap_2()
                        .rounded_lg()
                        .border_1()
                        .border_color(colors.border)
                        .bg(colors.element_background)
                        .cursor_pointer()
                        .hover(|style| style.bg(colors.element_hover))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_tool(Tool::Vault, window, cx)
                        }))
                        .child(
                            h_flex()
                                .gap_2()
                                .child(Icon::new(IconName::Lock).color(Color::Muted))
                                .child(Label::new("Vault")),
                        )
                        .child(
                            Label::new("Browse HashiCorp Vault secrets, read-only.")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        ),
                ),
            )
    }
}

impl Focusable for ToolsPage {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ToolsPage {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let vault = self.vault.clone().filter(|_| self.open == Some(Tool::Vault));
        v_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(colors.editor_background)
            .when_some(vault, |this, vault| {
                this.child(
                    h_flex()
                        .px_4()
                        .py_2()
                        .gap_2()
                        .border_b_1()
                        .border_color(colors.border)
                        .child(
                            Button::new("tools-home", "Tools")
                                .style(ButtonStyle::Subtle)
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.open = None;
                                    window.focus(&this.focus_handle, cx);
                                    cx.notify();
                                })),
                        )
                        .child(Label::new("/").size(LabelSize::Small).color(Color::Muted))
                        .child(Label::new("Vault").size(LabelSize::Small)),
                )
                .child(div().flex_1().min_h_0().child(vault))
            })
            .when(self.open.is_none(), |this| this.child(self.render_home(cx)))
    }
}
