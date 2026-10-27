use editor::{
    Editor,
    actions::{ConvertIndentationToSpaces, ConvertIndentationToTabs},
};
use gpui::{
    Anchor, App, Context, Entity, IntoElement, ParentElement, Render, Subscription, WeakEntity,
    Window, div,
};
use ui::{Button, ButtonCommon, ContextMenu, FluentBuilder, LabelSize, PopoverMenu, Tooltip};
use workspace::{HideStatusItem, StatusItemView, item::ItemHandle};

/// VS Code's "Spaces: 2" / "Tab Size: 4" status bar item.
pub struct ActiveBufferIndentation {
    label: Option<String>,
    active_editor: Option<WeakEntity<Editor>>,
    _observe_active_editor: Option<Subscription>,
}

impl ActiveBufferIndentation {
    pub fn new() -> Self {
        Self {
            label: None,
            active_editor: None,
            _observe_active_editor: None,
        }
    }

    fn update(&mut self, editor: Entity<Editor>, _: &mut Window, cx: &mut Context<Self>) {
        self.label = editor.read(cx).active_buffer(cx).map(|_| {
            let settings = editor.read(cx).buffer().read(cx).language_settings(cx);
            let kind = if settings.hard_tabs {
                "Tab Size"
            } else {
                "Spaces"
            };
            format!("{kind}: {}", settings.tab_size)
        });
        self.active_editor = Some(editor.downgrade());
        cx.notify();
    }
}

impl Render for ActiveBufferIndentation {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().when_some(self.label.clone(), |el, label| {
            let editor = self.active_editor.clone();
            el.child(
                PopoverMenu::new("change-indentation")
                    .trigger(
                        Button::new("change-indentation-button", label)
                            .label_size(LabelSize::Small)
                            .tab_index(0isize)
                            .tooltip(Tooltip::text("Select Indentation")),
                    )
                    .anchor(Anchor::BottomLeft)
                    .menu(move |window, cx| {
                        let editor = editor.clone()?.upgrade()?;
                        // Call the editor directly: a menu action would dispatch to the status bar's focus.
                        Some(ContextMenu::build(window, cx, |menu, _, _| {
                            let to_spaces = editor.clone();
                            let to_tabs = editor;
                            menu.entry(
                                "Convert Indentation to Spaces",
                                Some(Box::new(ConvertIndentationToSpaces)),
                                move |window, cx| {
                                    to_spaces.update(cx, |editor, cx| {
                                        editor.convert_indentation_to_spaces(
                                            &ConvertIndentationToSpaces,
                                            window,
                                            cx,
                                        )
                                    })
                                },
                            )
                            .entry(
                                "Convert Indentation to Tabs",
                                Some(Box::new(ConvertIndentationToTabs)),
                                move |window, cx| {
                                    to_tabs.update(cx, |editor, cx| {
                                        editor.convert_indentation_to_tabs(
                                            &ConvertIndentationToTabs,
                                            window,
                                            cx,
                                        )
                                    })
                                },
                            )
                        }))
                    }),
            )
        })
    }
}

impl StatusItemView for ActiveBufferIndentation {
    fn set_active_pane_item(
        &mut self,
        active_pane_item: Option<&dyn ItemHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(editor) = active_pane_item.and_then(|item| item.downcast::<Editor>()) {
            self._observe_active_editor = Some(cx.observe_in(&editor, window, Self::update));
            self.update(editor, window, cx);
        } else {
            self.label = None;
            self.active_editor = None;
            self._observe_active_editor = None;
        }
        cx.notify();
    }

    fn hide_setting(&self, _: &App) -> Option<HideStatusItem> {
        None
    }
}
