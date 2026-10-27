use editor::{
    Editor,
    actions::{ConvertIndentationToSpaces, ConvertIndentationToTabs},
};
use gpui::{
    Anchor, App, Context, Entity, IntoElement, ParentElement, Render, Subscription, WeakEntity,
    Window, div,
};
use language::language_settings::LanguageSettings;
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
        // The active buffer, not the multibuffer: its settings come from the first excerpt only.
        self.label = editor.read(cx).active_buffer(cx).map(|buffer| {
            let settings = LanguageSettings::for_buffer(buffer.read(cx), cx);
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

#[cfg(test)]
mod tests {
    use super::*;
    use editor::{EditorMode, MultiBuffer, SelectionEffects};
    use gpui::{AppContext as _, BorrowAppContext as _, TestAppContext, VisualTestContext};
    use language::{Language, LanguageConfig, Point};
    use settings::{LanguageSettingsContent, SettingsContent, SettingsStore};
    use std::{num::NonZeroU32, sync::Arc};
    use workspace::AppState;

    #[gpui::test]
    async fn test_label_follows_the_active_buffer(cx: &mut TestAppContext) {
        cx.update(|cx| {
            AppState::test(cx);
            settings::init(cx);
            editor::init(cx);
            cx.update_global::<SettingsStore, _>(|store, cx| {
                store.update_user_settings(cx, &|settings: &mut SettingsContent| {
                    settings.project.all_languages.languages.0.extend([
                        (
                            "Rust".into(),
                            LanguageSettingsContent {
                                tab_size: NonZeroU32::new(4),
                                ..Default::default()
                            },
                        ),
                        (
                            "Go".into(),
                            LanguageSettingsContent {
                                tab_size: NonZeroU32::new(8),
                                hard_tabs: Some(true),
                                ..Default::default()
                            },
                        ),
                    ]);
                });
            });
        });
        let multi_buffer = cx.update(|cx| {
            let multi_buffer = MultiBuffer::build_multi(
                [
                    ("fn a() {}", vec![Point::row_range(0..1)]),
                    ("func b() {}", vec![Point::row_range(0..1)]),
                ],
                cx,
            );
            for buffer in multi_buffer.read(cx).all_buffers() {
                let name = if buffer.read(cx).text().starts_with("fn ") {
                    "Rust"
                } else {
                    "Go"
                };
                let language = Arc::new(Language::new(
                    LanguageConfig {
                        name: name.into(),
                        ..Default::default()
                    },
                    None,
                ));
                buffer.update(cx, |buffer, cx| buffer.set_language(Some(language), cx));
            }
            multi_buffer
        });
        let (editor, cx) = cx.add_window_view(|window, cx| {
            Editor::new(EditorMode::full(), multi_buffer, None, window, cx)
        });
        let item = cx.new(|_| ActiveBufferIndentation::new());
        let label = |item: &Entity<ActiveBufferIndentation>, cx: &mut VisualTestContext| {
            item.update_in(cx, |item, window, cx| {
                item.update(editor.clone(), window, cx);
                item.label.clone()
            })
        };
        assert_eq!(label(&item, cx).as_deref(), Some("Spaces: 4"));

        editor.update_in(cx, |editor, window, cx| {
            editor.change_selections(SelectionEffects::no_scroll(), window, cx, |s| {
                s.select_ranges([Point::new(1, 0)..Point::new(1, 0)])
            });
        });
        assert_eq!(label(&item, cx).as_deref(), Some("Tab Size: 8"));
    }
}
