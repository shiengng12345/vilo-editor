//! Editing, inserting and deleting one MongoDB document at a time. Every write is reviewed in a
//! confirm step, logged before it is sent, sent once, and a write whose outcome is unknown locks
//! its document until the page is reloaded.

use std::io::Write as _;

use anyhow::{Context as _, Result, anyhow};
use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, Tooltip, prelude::*};
use vilo_database_core::{
    bson::{Bson, Document},
    edit::{build_delete, build_update, to_edit_text},
    mongo::WriteError,
    shell::parse_query,
};

use crate::{Connection, DatabaseView, boxed, editor as single_line, run};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum DocOp {
    Edit,
    Insert,
    Delete,
}

/// The reviewed write: exactly what will be sent.
#[derive(Clone)]
enum Planned {
    Update { filter: Document, update: Document },
    Insert { document: Document },
    Delete { filter: Document },
}

impl Planned {
    fn rendered(&self, values: bool) -> String {
        let show = |document: &Document| {
            if values {
                vilo_database_core::mongo::to_display_json(document)
            } else {
                // Without values only the shape shows: field names and operators.
                let names = document.keys().cloned().collect::<Vec<_>>().join(", ");
                format!("{{ {names} }}")
            }
        };
        match self {
            Planned::Update { filter, update } => {
                format!("updateOne(\n  {},\n  {}\n)", show(filter), show(update))
            }
            Planned::Insert { document } => format!("insertOne(\n  {}\n)", show(document)),
            Planned::Delete { filter } => format!("deleteOne(\n  {}\n)", show(filter)),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Planned::Update { .. } => "updateOne",
            Planned::Insert { .. } => "insertOne",
            Planned::Delete { .. } => "deleteOne",
        }
    }
}

pub(crate) struct DocWrite {
    op: DocOp,
    database: String,
    collection: String,
    /// The document as loaded from the server; `None` for an insert.
    original: Option<Document>,
    text: Option<Entity<Editor>>,
    planned: Option<Planned>,
    /// The connection name typed to confirm a write to production.
    typed: Entity<Editor>,
    show_values: bool,
    sending: bool,
    message: Option<SharedString>,
    _task: Option<gpui::Task<()>>,
}

impl DocWrite {
    pub(crate) fn sending_now(&self) -> bool {
        self.sending
    }

    pub(crate) fn editing(&self) -> bool {
        self.text.is_some() && self.planned.is_none()
    }
}

/// One key per document, for in-flight and unknown-outcome locks.
fn target_key(connection: &str, database: &str, collection: &str, id: &Bson) -> String {
    format!(
        "{connection}/{database}/{collection}/{}",
        id.clone().into_canonical_extjson()
    )
}

/// Appends one line to the write log; a write is not sent if its intent cannot be logged.
pub(crate) fn audit(line: serde_json::Value) -> Result<()> {
    let path = paths::config_dir().join("database_audit.log");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(&path)
        .context("Vilo could not open the write log")?;
    writeln!(file, "{line}").context("Vilo could not write to the write log")?;
    file.sync_data()
        .context("Vilo could not save the write log")?;
    Ok(())
}

/// Production, or an environment nobody named, is treated as production.
pub(crate) fn guarded(connection: &Connection) -> bool {
    connection.production() || connection.environment.trim().is_empty()
}

impl DatabaseView {
    pub(crate) fn writes_allowed(&self, connection: &Connection) -> bool {
        if guarded(connection) {
            self.session_writes.contains(&connection.id)
        } else {
            connection.allow_writes
        }
    }

    /// Production switches are kept for this run of Vilo only; others are saved with the connection.
    pub(crate) fn toggle_writes(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        if guarded(&connection) {
            if !self.session_writes.remove(&connection.id) {
                self.session_writes.insert(connection.id.clone());
            }
        } else if let Some(stored) = self
            .connections
            .iter_mut()
            .find(|stored| stored.id == connection.id)
        {
            stored.allow_writes = !stored.allow_writes;
            self.persist(cx).detach_and_log_err(cx);
        }
        cx.notify();
    }

    fn write_target(&self) -> Option<(Connection, String, String)> {
        let connection = self.active_connection()?.clone();
        let tab = self.tab()?;
        Some((connection, tab.database.clone(), tab.collection.clone()))
    }

    fn open_write(
        &mut self,
        op: DocOp,
        original: Option<Document>,
        text: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some((_, database, collection)) = self.write_target() else {
            return;
        };
        let text = text.map(|text| {
            let language = self.query_language.clone();
            cx.new(|cx| {
                let mut editor = Editor::auto_height(8, 28, window, cx);
                editor.set_text(text, window, cx);
                if let (Some(language), Some(buffer)) =
                    (language, editor.buffer().read(cx).as_singleton())
                {
                    buffer.update(cx, |buffer, cx| buffer.set_language(Some(language), cx));
                }
                editor
            })
        });
        let mut write = DocWrite {
            op,
            database,
            collection,
            original,
            text,
            planned: None,
            typed: single_line("Type the connection name", "", false, window, cx),
            show_values: false,
            sending: false,
            message: None,
            _task: None,
        };
        if op == DocOp::Delete {
            match write.original.as_ref().map(build_delete) {
                Some(Ok(filter)) => write.planned = Some(Planned::Delete { filter }),
                Some(Err(error)) => write.message = Some(format!("{error:#}").into()),
                None => {}
            }
        }
        self.doc_write = Some(write);
        cx.notify();
    }

    /// Edit and delete start from the document as the server has it now, not the page's copy,
    /// which may be projected or out of date.
    pub(crate) fn start_doc_write(
        &mut self,
        op: DocOp,
        id: Option<Bson>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.mongo() else {
            return;
        };
        let Some((connection, database, collection)) = self.write_target() else {
            return;
        };
        let Some(id) = id else {
            self.open_write(DocOp::Insert, None, Some("{\n  \n}".into()), window, cx);
            return;
        };
        if self
            .unknown_writes
            .contains(&target_key(&connection.id, &database, &collection, &id))
        {
            self.status_toast(
                "A write to this document has an unknown outcome. Refresh the page to check it first.",
                cx,
            );
            return;
        }
        let task = run(cx, async move {
            session.find_by_id(&database, &collection, &id).await
        });
        cx.spawn_in(window, async move |this, cx| {
            let result = task.await;
            this.update_in(cx, |this, window, cx| match result {
                Ok(Some(document)) => {
                    let text = to_edit_text(&document);
                    match (op, text) {
                        (DocOp::Delete, _) => this.open_write(op, Some(document), None, window, cx),
                        (DocOp::Insert, Some(_)) => {
                            let mut copy = document;
                            copy.remove("_id");
                            let text = to_edit_text(&copy);
                            this.open_write(op, None, text, window, cx)
                        }
                        (DocOp::Edit, Some(text)) => {
                            this.open_write(op, Some(document), Some(text), window, cx)
                        }
                        (_, None) => this.status_toast(
                            "This document holds a value Vilo cannot edit without changing it.",
                            cx,
                        ),
                    }
                }
                Ok(None) => this.status_toast("The document is gone. Refresh the page.", cx),
                Err(error) => this.status_toast(&format!("{error:#}"), cx),
            })
            .ok();
        })
        .detach();
    }

    fn status_toast(&mut self, message: &str, cx: &mut Context<Self>) {
        self.write_notice = Some(message.to_string().into());
        cx.notify();
    }

    fn review_write(&mut self, cx: &mut Context<Self>) {
        let Some(write) = &mut self.doc_write else {
            return;
        };
        let planned = (|| {
            let text = write
                .text
                .as_ref()
                .map(|text| text.read(cx).text(cx))
                .unwrap_or_default();
            match write.op {
                DocOp::Delete => Err(anyhow!("Nothing to review")),
                DocOp::Insert => Ok(Planned::Insert {
                    document: parse_query(&text)?,
                }),
                DocOp::Edit => {
                    let original = write
                        .original
                        .as_ref()
                        .ok_or_else(|| anyhow!("The document did not load"))?;
                    let (filter, update) = build_update(original, &parse_query(&text)?)?;
                    Ok(Planned::Update { filter, update })
                }
            }
        })();
        match planned {
            Ok(planned) => {
                write.planned = Some(planned);
                write.message = None;
            }
            Err(error) => write.message = Some(format!("{error:#}").into()),
        }
        cx.notify();
    }

    fn send_write(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.mongo() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        if !self.writes_allowed(&connection) {
            return;
        }
        let generation = self.generation;
        let Some(write) = &mut self.doc_write else {
            return;
        };
        let Some(planned) = write.planned.clone() else {
            return;
        };
        if write.sending {
            return;
        }
        if guarded(&connection) && write.typed.read(cx).text(cx).trim() != connection.name.trim() {
            write.message = Some("Type the connection name exactly to confirm.".into());
            cx.notify();
            return;
        }
        let id = write
            .original
            .as_ref()
            .and_then(|original| original.get("_id"))
            .cloned();
        let key = id
            .as_ref()
            .map(|id| target_key(&connection.id, &write.database, &write.collection, id));
        write.sending = true;
        write.message = Some("Sending…".into());
        let operation = uuid::Uuid::new_v4().simple().to_string();
        let intent = serde_json::json!({
            "time": chrono::Utc::now().to_rfc3339(),
            "operation": operation,
            "connection": connection.id,
            "name": connection.name,
            "site": connection.site,
            "environment": connection.environment,
            "target": format!("{}.{}", write.database, write.collection),
            "_id": id.map(|id| id.into_canonical_extjson()),
            "kind": planned.kind(),
            "phase": "intent",
        });
        let (database, collection) = (write.database.clone(), write.collection.clone());
        let task = run(cx, async move {
            audit(intent)?;
            let outcome = match planned {
                Planned::Update { filter, update } => session
                    .update_guarded(&database, &collection, filter, update)
                    .await
                    .map(|outcome| match (outcome.matched, outcome.modified) {
                        (0, _) => "changed".to_string(),
                        (_, 0) => "unchanged".to_string(),
                        _ => "ok".to_string(),
                    }),
                Planned::Delete { filter } => session
                    .delete_guarded(&database, &collection, filter)
                    .await
                    .map(|deleted| if deleted == 0 { "changed" } else { "ok" }.to_string()),
                Planned::Insert { document } => session
                    .insert_document(&database, &collection, document)
                    .await
                    .map(|_| "ok".to_string()),
            };
            let result = match &outcome {
                Ok(result) => result.clone(),
                Err(WriteError::Refused(_)) => "refused".into(),
                Err(WriteError::Unknown(_)) => "unknown".into(),
            };
            audit(
                serde_json::json!({ "operation": operation, "phase": "result", "result": result }),
            )
            .ok();
            Ok(outcome)
        });
        write._task = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(write) = &mut this.doc_write else {
                    return;
                };
                write.sending = false;
                match result {
                    Ok(Ok(result)) if result == "ok" => {
                        this.doc_write = None;
                        this.find(cx);
                    }
                    Ok(Ok(result)) if result == "unchanged" => {
                        write.message = Some("The document already has these values; nothing changed.".into());
                    }
                    Ok(Ok(_)) => {
                        write.message = Some(
                            "The document changed since it was loaded, so nothing was written. Close, refresh and try again."
                                .into(),
                        );
                    }
                    Ok(Err(WriteError::Unknown(message))) => {
                        if let Some(key) = key {
                            this.unknown_writes.insert(key);
                        }
                        this.write_notice = Some(
                            format!("The outcome of the last write is unknown ({message}). Refresh the page to check it before writing again.")
                                .into(),
                        );
                        this.doc_write = None;
                    }
                    Ok(Err(WriteError::Refused(message))) => {
                        write.message = Some(format!("Not written: {message}").into());
                    }
                    Err(error) => write.message = Some(format!("Not sent: {error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(crate) fn render_doc_write(&self, write: &DocWrite, cx: &mut Context<Self>) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = guarded(connection);
        let title = match write.op {
            DocOp::Edit => "Edit document",
            DocOp::Insert => "Insert document",
            DocOp::Delete => "Delete document",
        };
        let target = format!(
            "{} · {} · {} · {}.{}",
            connection.site,
            connection.environment,
            connection.name,
            write.database,
            write.collection
        );
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if !this.doc_write.as_ref().is_some_and(|write| write.sending) {
                this.doc_write = None;
                cx.notify();
            }
        };
        v_flex()
            .id("database-doc-write")
            .gap_3()
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(Headline::new(title).size(HeadlineSize::Small))
            .child(
                Label::new(target)
                    .size(LabelSize::Small)
                    .weight(gpui::FontWeight::SEMIBOLD)
                    .color(if production {
                        Color::Error
                    } else {
                        Color::Muted
                    }),
            )
            .map(|this| match (&write.planned, &write.text) {
                (None, Some(text)) => this.child(boxed(text.clone(), cx)),
                (Some(planned), _) => this
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Label::new("This will be sent once:").size(LabelSize::Small))
                            .child(
                                Button::new(
                                    "database-doc-write-values",
                                    if write.show_values {
                                        "Hide values"
                                    } else {
                                        "Show values"
                                    },
                                )
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        if let Some(write) = &mut this.doc_write {
                                            write.show_values = !write.show_values;
                                        }
                                        cx.notify();
                                    },
                                )),
                            ),
                    )
                    .child(boxed(
                        Label::new(planned.rendered(write.show_values))
                            .size(LabelSize::Small)
                            .buffer_font(cx),
                        cx,
                    ))
                    .when(production, |this| {
                        this.child(
                            Label::new(format!(
                                "This is a production connection. Type “{}” to confirm.",
                                connection.name
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Error),
                        )
                        .child(boxed(write.typed.clone(), cx))
                    }),
                (None, None) => this,
            })
            .when_some(write.message.clone(), |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(if write.sending {
                            Color::Muted
                        } else {
                            Color::Warning
                        }),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| match &write.planned {
                        None => this.child(
                            Button::new("database-doc-write-review", "Review")
                                .style(ButtonStyle::Filled)
                                .on_click(cx.listener(|this, _, _, cx| this.review_write(cx))),
                        ),
                        Some(planned) => this
                            .child(
                                Button::new(
                                    "database-doc-write-send",
                                    match planned {
                                        Planned::Delete { .. } => "Delete",
                                        Planned::Insert { .. } => "Insert",
                                        Planned::Update { .. } => "Update",
                                    },
                                )
                                .style(ButtonStyle::Filled)
                                .color(if matches!(planned, Planned::Delete { .. }) {
                                    Color::Error
                                } else {
                                    Color::Default
                                })
                                .disabled(write.sending)
                                .on_click(cx.listener(|this, _, _, cx| this.send_write(cx))),
                            )
                            .when(write.text.is_some(), |this| {
                                this.child(
                                    Button::new("database-doc-write-back", "Back to editing")
                                        .disabled(write.sending)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(write) = &mut this.doc_write {
                                                write.planned = None;
                                                write.message = None;
                                            }
                                            cx.notify();
                                        })),
                                )
                            }),
                    })
                    .child(
                        Button::new("database-doc-write-cancel", "Cancel")
                            .disabled(write.sending)
                            .tooltip(Tooltip::text("Nothing is sent"))
                            .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }
}
