use std::path::PathBuf;

use anyhow::{Context as _, Result};
use editor::Editor;
use gpui::{AsyncApp, Entity, PathPromptOptions, Task, Window, prelude::*};
use ui::{Button, ButtonStyle, Checkbox, ContextMenu, PopoverMenu, ToggleState, prelude::*};

use super::{Connection, RootTokenNeedsConsent, VaultView, store_token};
use crate::{
    vault::normalize_origin,
    vault_import::{ConnectionEntry, parse_entries},
};

#[derive(Clone, PartialEq)]
enum RowAction {
    Add,
    Skip,
    /// Replace this existing connection; chosen by the user, never guessed.
    Update(String),
}

struct ImportRow {
    entry: ConnectionEntry,
    origin: Result<String, String>,
    token: Entity<Editor>,
    action: RowAction,
    status: Option<(SharedString, Color)>,
}

pub(super) struct ImportState {
    input: Entity<Editor>,
    rows: Vec<ImportRow>,
    message: Option<SharedString>,
    acknowledge_plaintext: bool,
    accept_root: bool,
    running: bool,
    _task: Option<Task<()>>,
}

pub(super) fn editor(placeholder: &str, text: &str, masked: bool, window: &mut Window, cx: &mut App) -> Entity<Editor> {
    let placeholder = placeholder.to_string();
    let text = text.to_string();
    cx.new(|cx| {
        let mut editor = Editor::single_line(window, cx);
        editor.set_placeholder_text(&placeholder, window, cx);
        if !text.is_empty() {
            editor.set_text(text, window, cx);
        }
        if masked {
            editor.set_masked(true, cx);
        }
        editor
    })
}

impl VaultView {
    pub(super) fn open_import(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| {
            let mut editor = Editor::auto_height(4, 12, window, cx);
            editor.set_placeholder_text(
                "Paste a JSON list, or one connection per line:\nCP UAT, CP, UAT, https://vault-cp-uat…, hvs.token (optional)",
                window,
                cx,
            );
            editor
        });
        self.form = None;
        self.compare = None;
        self.import = Some(ImportState {
            input,
            rows: Vec::new(),
            message: None,
            acknowledge_plaintext: false,
            accept_root: false,
            running: false,
            _task: None,
        });
        cx.notify();
    }

    fn preview_import(&mut self, text: String, window: &mut Window, cx: &mut Context<Self>) {
        let entries = match parse_entries(&text) {
            Ok(entries) => entries,
            Err(error) => {
                if let Some(import) = &mut self.import {
                    import.message = Some(error.to_string().into());
                }
                cx.notify();
                return;
            }
        };
        let rows = entries
            .into_iter()
            .map(|mut entry| {
                let origin = normalize_origin(&entry.address)
                    .map(|origin| origin.to_string())
                    .map_err(|error| error.to_string());
                let already_saved = origin
                    .as_ref()
                    .is_ok_and(|origin| self.connections.iter().any(|c| &c.address == origin));
                let token = editor(
                    "Token",
                    entry.token.take().as_deref().unwrap_or_default(),
                    true,
                    window,
                    cx,
                );
                ImportRow {
                    entry,
                    action: if already_saved || origin.is_err() {
                        RowAction::Skip
                    } else {
                        RowAction::Add
                    },
                    origin,
                    token,
                    status: None,
                }
            })
            .collect::<Vec<_>>();
        if let Some(import) = &mut self.import {
            // Tokens move into masked fields; the pasted text is cleared.
            import
                .input
                .update(cx, |editor, cx| editor.set_text("", window, cx));
            import.message = Some(format!("{} connections found. Check each row, then import.", rows.len()).into());
            import.rows = rows;
        }
        cx.notify();
    }

    fn choose_import_file(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let paths = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Import".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = paths.await else {
                return;
            };
            let Some(path) = paths.into_iter().next() else {
                return;
            };
            let text = cx
                .background_spawn(async move { std::fs::read_to_string(&path) })
                .await;
            this.update_in(cx, |this, window, cx| match text {
                Ok(text) => this.preview_import(text, window, cx),
                Err(error) => {
                    if let Some(import) = &mut this.import {
                        import.message = Some(format!("Could not read the file: {error}").into());
                    }
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
    }

    fn run_import(&mut self, cx: &mut Context<Self>) {
        let Some(import) = &mut self.import else {
            return;
        };
        let needs_consent = import.rows.iter().any(|row| {
            row.action != RowAction::Skip
                && row.origin.as_ref().is_ok_and(|origin| origin.starts_with("http://"))
        });
        // Consent comes before any token is sent to an http address.
        if needs_consent && !import.acknowledge_plaintext {
            import.message = Some(
                "Some addresses use http: tokens and values would cross the network unencrypted. \
                 Tick the box to continue."
                    .into(),
            );
            cx.notify();
            return;
        }
        let jobs = import
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                (
                    index,
                    row.entry.clone(),
                    row.origin.clone(),
                    row.action.clone(),
                    row.token.read(cx).text(cx).trim().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let accept_root = import.accept_root;
        import.running = true;
        import.message = Some("Importing…".into());
        let http = cx.http_client();
        let task = cx.spawn(async move |this, cx| {
            for (index, entry, origin, action, token) in jobs {
                let set_status = |this: &gpui::WeakEntity<VaultView>, cx: &mut AsyncApp, text: String, color: Color| {
                    this.update(cx, |this, cx| {
                        if let Some(row) = this.import.as_mut().and_then(|import| import.rows.get_mut(index)) {
                            row.status = Some((text.into(), color));
                        }
                        cx.notify();
                    })
                    .ok();
                };
                if action == RowAction::Skip {
                    set_status(&this, cx, "Skipped".into(), Color::Muted);
                    continue;
                }
                let origin = match origin {
                    Ok(origin) => origin,
                    Err(error) => {
                        set_status(&this, cx, error, Color::Error);
                        continue;
                    }
                };
                let existing = match &action {
                    RowAction::Update(id) => this
                        .read_with(cx, |this, _| this.connections.iter().find(|c| &c.id == id).cloned())
                        .ok()
                        .flatten(),
                    _ => None,
                };
                // A token is reused only for the same connection at the same address.
                let keeps_token = existing.as_ref().is_some_and(|existing| existing.address == origin);
                if token.is_empty() && !keeps_token {
                    set_status(&this, cx, "Needs a token".into(), Color::Warning);
                    continue;
                }
                let connection = Connection {
                    id: existing
                        .as_ref()
                        .map(|existing| existing.id.clone())
                        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
                    name: entry.name.clone(),
                    site: entry.site.clone(),
                    environment: entry.environment.clone(),
                    plaintext_acknowledged: origin.starts_with("http://"),
                    address: origin,
                };
                if !token.is_empty()
                    && let Err(error) = store_token(&connection, &token, accept_root, http.clone(), cx).await
                {
                    let message = if error.is::<RootTokenNeedsConsent>() {
                        "Root token: tick “Allow root tokens” and import again".to_string()
                    } else {
                        format!("{error:#}")
                    };
                    set_status(&this, cx, message, Color::Error);
                    continue;
                }
                let persisted = this.update(cx, |this, cx| {
                    match this.connections.iter_mut().find(|c| c.id == connection.id) {
                        Some(existing) => *existing = connection.clone(),
                        None => this.connections.push(connection.clone()),
                    }
                    this.persist(cx)
                });
                let result = match persisted {
                    Ok(task) => task.await,
                    Err(error) => Err(error),
                };
                match result {
                    Ok(()) => set_status(&this, cx, "Imported".into(), Color::Success),
                    Err(error) => set_status(&this, cx, format!("Not saved: {error:#}"), Color::Error),
                }
            }
            this.update(cx, |this, cx| {
                if let Some(import) = &mut this.import {
                    import.running = false;
                    import.message = Some(
                        "Done. If you imported from a file that holds tokens, delete that file now."
                            .into(),
                    );
                }
                cx.notify();
            })
            .ok();
        });
        if let Some(import) = &mut self.import {
            import._task = Some(task);
        }
        cx.notify();
    }

    pub(super) fn export_connections(&mut self, cx: &mut Context<Self>) {
        let entries = self
            .connections
            .iter()
            .map(|c| ConnectionEntry {
                name: c.name.clone(),
                site: c.site.clone(),
                environment: c.environment.clone(),
                address: c.address.clone(),
                token: None,
            })
            .collect::<Vec<_>>();
        let directory = dirs_home();
        let path = cx.prompt_for_new_path(&directory, Some("vault-connections.json"));
        cx.spawn(async move |_, cx| -> Result<()> {
            let Ok(Ok(Some(path))) = path.await else {
                return Ok(());
            };
            let json = serde_json::to_vec_pretty(&entries)?;
            cx.background_spawn(async move { std::fs::write(&path, json) })
                .await
                .context("Could not write the export file")
        })
        .detach_and_log_err(cx);
    }

    pub(super) fn render_import(&self, import: &ImportState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let mut table = v_flex().border_1().border_color(colors.border).rounded_md();
        for (index, row) in import.rows.iter().enumerate() {
            let same_origin = row
                .origin
                .as_ref()
                .map(|origin| {
                    self.connections
                        .iter()
                        .filter(|c| &c.address == origin)
                        .map(|c| (c.id.clone(), c.label()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let action_label = match &row.action {
                RowAction::Add => "Add as new".to_string(),
                RowAction::Skip => "Skip".to_string(),
                RowAction::Update(id) => format!(
                    "Update {}",
                    self.connections.iter().find(|c| &c.id == id).map_or(String::new(), Connection::label)
                ),
            };
            let view = cx.entity().downgrade();
            table = table.child(
                h_flex()
                    .px_3()
                    .py_1p5()
                    .gap_3()
                    .when(index > 0, |this| this.border_t_1().border_color(colors.border_variant))
                    .child(
                        v_flex()
                            .w(px(240.))
                            .flex_none()
                            .child(Label::new(
                                [row.entry.site.as_str(), row.entry.environment.as_str(), row.entry.name.as_str()]
                                    .into_iter()
                                    .filter(|part| !part.is_empty())
                                    .collect::<Vec<_>>()
                                    .join(" · "),
                            ))
                            .child(
                                Label::new(match &row.origin {
                                    Ok(origin) => origin.clone(),
                                    Err(error) => error.clone(),
                                })
                                .size(LabelSize::XSmall)
                                .color(if row.origin.is_ok() { Color::Muted } else { Color::Error }),
                            ),
                    )
                    .child(
                        div()
                            .flex_1()
                            .px_2()
                            .py_1()
                            .rounded_sm()
                            .border_1()
                            .border_color(colors.border)
                            .child(row.token.clone()),
                    )
                    .child(
                        PopoverMenu::new(SharedString::from(format!("import-action-{index}")))
                            .trigger(
                                Button::new(SharedString::from(format!("import-action-button-{index}")), action_label)
                                    .label_size(LabelSize::Small)
                                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                            )
                            .menu(move |window, cx| {
                                let view = view.clone();
                                let same_origin = same_origin.clone();
                                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                    let mut options = vec![
                                        ("Add as new".to_string(), RowAction::Add),
                                        ("Skip".to_string(), RowAction::Skip),
                                    ];
                                    options.extend(
                                        same_origin
                                            .iter()
                                            .map(|(id, label)| (format!("Update {label}"), RowAction::Update(id.clone()))),
                                    );
                                    for (label, action) in options {
                                        let view = view.clone();
                                        menu = menu.entry(label, None, move |_, cx| {
                                            view.update(cx, |this, cx| {
                                                if let Some(row) =
                                                    this.import.as_mut().and_then(|import| import.rows.get_mut(index))
                                                {
                                                    row.action = action.clone();
                                                }
                                                cx.notify();
                                            })
                                            .ok();
                                        });
                                    }
                                    menu
                                }))
                            }),
                    )
                    .child(
                        div().w(px(220.)).flex_none().when_some(row.status.clone(), |this, (text, color)| {
                            this.child(Label::new(text).size(LabelSize::Small).color(color))
                        }),
                    ),
            );
        }
        let has_http = import
            .rows
            .iter()
            .any(|row| row.origin.as_ref().is_ok_and(|origin| origin.starts_with("http://")));
        v_flex()
            .id("vault-import")
            .flex_1()
            .overflow_y_scroll()
            .gap_3()
            .child(Headline::new("Import Vault connections").size(HeadlineSize::Small))
            .child(
                Label::new(
                    "Tokens are saved to the macOS keychain only. Existing connections are never \
                     overwritten unless you pick “Update” on a row.",
                )
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .child(
                div()
                    .px_2()
                    .py_1()
                    .rounded_sm()
                    .border_1()
                    .border_color(colors.border)
                    .child(import.input.clone()),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("import-preview", "Preview pasted text").on_click(cx.listener(
                            |this, _, window, cx| {
                                let text = this
                                    .import
                                    .as_ref()
                                    .map(|import| import.input.read(cx).text(cx))
                                    .unwrap_or_default();
                                this.preview_import(text, window, cx)
                            },
                        )),
                    )
                    .child(
                        Button::new("import-file", "Choose JSON file…")
                            .on_click(cx.listener(|this, _, window, cx| this.choose_import_file(window, cx))),
                    ),
            )
            .when(!import.rows.is_empty(), |this| this.child(table))
            .when(has_http, |this| {
                this.child(
                    Checkbox::new("import-acknowledge-plaintext", ToggleState::from(import.acknowledge_plaintext))
                        .label("I understand the http addresses send tokens and values unencrypted.")
                        .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                            if let Some(import) = &mut this.import {
                                import.acknowledge_plaintext = state.selected();
                            }
                            cx.notify();
                        })),
                )
            })
            .when(!import.rows.is_empty(), |this| {
                this.child(
                    Checkbox::new("import-accept-root", ToggleState::from(import.accept_root))
                        .label("Allow root tokens")
                        .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                            if let Some(import) = &mut this.import {
                                import.accept_root = state.selected();
                            }
                            cx.notify();
                        })),
                )
            })
            .when_some(import.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(Color::Muted))
            })
            .child(
                h_flex()
                    .gap_2()
                    .when(!import.rows.is_empty(), |this| {
                        this.child(
                            Button::new("import-run", "Import")
                                .style(ButtonStyle::Filled)
                                .disabled(import.running)
                                .on_click(cx.listener(|this, _, _, cx| this.run_import(cx))),
                        )
                    })
                    .child(Button::new("import-close", "Close").on_click(cx.listener(|this, _, _, cx| {
                        this.import = None;
                        cx.notify();
                    }))),
            )
            .into_any_element()
    }

}

fn dirs_home() -> PathBuf {
    paths::home_dir().clone()
}

