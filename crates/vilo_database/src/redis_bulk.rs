//! Bulk delete, as RedisInsight's Bulk Actions: preview the keys a pattern matches, then delete
//! exactly those in batches. Each batch is logged before it is sent and sent once; an unknown
//! outcome stops the run and locks bulk deletes on this connection until the user checked it.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use editor::Editor;
use gpui::{Entity, Task, Window, prelude::*, uniform_list};
use ui::{Button, ButtonStyle, ContextMenu, PopoverMenu, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::quote_bytes,
    redis_bulk::{BulkPreview, MAX_KEYS, batches},
};

use crate::{
    DatabaseView, boxed, editor as single_line, mongo_write,
    redis_pubsub::{
        acknowledge_publishes, pass_operation_lock, settle_operation_lock, take_operation_lock,
    },
    redis_view::{KEY_TYPES, escape_bytes},
    run,
};

pub(crate) struct BulkForm {
    /// The session the dialog was opened for; a preview never applies to another one.
    generation: u64,
    pattern: Entity<Editor>,
    /// A SCAN TYPE, or every type.
    kind: Option<&'static str>,
    preview: Option<Result<BulkPreview, SharedString>>,
    /// The pattern and type the preview was made for; deleting needs them unchanged.
    previewed: Option<(String, Option<&'static str>)>,
    /// Set to stop a running preview.
    scanning: Option<Arc<AtomicBool>>,
    _preview: Option<Task<()>>,
    typed_name: Entity<Editor>,
    typed_pattern: Entity<Editor>,
    run: Option<BulkRun>,
    message: Option<SharedString>,
}

struct BulkRun {
    keys: usize,
    batches: usize,
    done: usize,
    deleted: u64,
    /// Set to stop before the next batch; the batch being sent finishes.
    stop: Arc<AtomicBool>,
    running: bool,
    outcome: Option<(SharedString, Color)>,
    /// The batch whose outcome is unknown and which holds the bulk lock, until the user checked it.
    unknown: Option<String>,
}

impl BulkForm {
    pub(crate) fn running(&self) -> bool {
        self.scanning.is_some() || self.run.as_ref().is_some_and(|run| run.running)
    }

    /// Kept open while running, and while an unknown outcome waits for its I checked it here.
    pub(crate) fn closable(&self) -> bool {
        !self.running() && !self.run.as_ref().is_some_and(|run| run.unknown.is_some())
    }
}

/// Bulk deletes on one connection and database run one at a time, across windows.
fn lock_key(connection: &str, database: u8) -> String {
    format!("{connection}/{database}/redis/bulk")
}

impl DatabaseView {
    pub(crate) fn open_bulk_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self
            .redis_state
            .as_ref()
            .is_none_or(|state| state.switching_database())
        {
            return;
        }
        let pattern = single_line("Pattern, e.g. session:*", "", false, window, cx);
        let typed_name = single_line("Type the connection name", "", false, window, cx);
        let typed_pattern = single_line("Type the pattern", "", false, window, cx);
        self.bulk_delete = Some(BulkForm {
            generation: self.generation,
            pattern,
            kind: None,
            preview: None,
            previewed: None,
            scanning: None,
            _preview: None,
            typed_name,
            typed_pattern,
            run: None,
            message: None,
        });
        cx.notify();
    }

    fn preview_bulk_delete(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(form) = &mut self.bulk_delete else {
            return;
        };
        if form.running() || form.run.is_some() || form.generation != generation {
            return;
        }
        let pattern = form.pattern.read(cx).text(cx).trim().to_string();
        let kind = form.kind;
        let cancel = Arc::new(AtomicBool::new(false));
        form.scanning = Some(cancel.clone());
        form.preview = None;
        form.previewed = None;
        form.message = Some("Scanning…".into());
        let task = run(cx, {
            let pattern = pattern.clone();
            async move {
                session
                    .bulk_preview(&pattern, kind, MAX_KEYS, &cancel)
                    .await
            }
        });
        form._preview = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(form) = &mut this.bulk_delete else {
                    return;
                };
                form.scanning = None;
                form.message = None;
                form.previewed = Some((pattern, kind));
                form.preview = Some(result.map_err(|error| format!("{error:#}").into()));
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn send_bulk_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        if !self.writes_allowed(&connection) {
            return;
        }
        let database = session.database();
        let clustered = session.is_clustered();
        let log_checked = self.write_log_checked;
        let generation = self.generation;
        let Some(form) = &mut self.bulk_delete else {
            return;
        };
        // The keys belong to the session they were found in; never send them to another one.
        if form.generation != generation {
            form.message = Some(
                "The connection changed since this was opened. Close it and start again.".into(),
            );
            cx.notify();
            return;
        }
        if form.running() || form.run.is_some() {
            return;
        }
        let (Some(Ok(preview)), Some((pattern, kind))) = (&form.preview, &form.previewed) else {
            return;
        };
        let (pattern, kind) = (pattern.clone(), *kind);
        if form.pattern.read(cx).text(cx).trim() != pattern || form.kind != kind {
            form.message =
                Some("The pattern or type changed since the preview. Preview again.".into());
            cx.notify();
            return;
        }
        if preview.keys.is_empty() {
            return;
        }
        if mongo_write::guarded(&connection)
            && (form.typed_name.read(cx).text(cx).trim() != connection.name.trim()
                || form.typed_pattern.read(cx).text(cx).trim() != pattern)
        {
            form.message =
                Some("Type the connection name and the pattern exactly to confirm.".into());
            cx.notify();
            return;
        }
        // Until the write log was read, an unsettled bulk delete from before a restart is not known.
        if !log_checked {
            form.message = Some(
                "Vilo has not finished checking the write log for unsettled writes, so it does not bulk delete yet."
                    .into(),
            );
            cx.notify();
            return;
        }
        let lock = lock_key(&connection.id, database);
        let run_id = uuid::Uuid::new_v4().simple().to_string();
        // The lock is always owned by the batch in flight, the operation id its log lines carry,
        // so a log read meanwhile sees that batch as pending and a restart restores the lock.
        let mut owner = format!("{run_id}:0");
        if let Err(unknown) = take_operation_lock(&lock, &owner) {
            form.message = Some(
                if unknown {
                    "A bulk delete on this connection has an unknown outcome. Check its keys, then press I checked it in its window or in the write log banner."
                } else {
                    "A bulk delete on this connection is still running."
                }
                .into(),
            );
            cx.notify();
            return;
        }
        for confirm in [&form.typed_name, &form.typed_pattern] {
            confirm.update(cx, |confirm, cx| confirm.set_text("", window, cx));
        }
        let batches = batches(&preview.keys, clustered);
        let stop = Arc::new(AtomicBool::new(false));
        form.message = None;
        form.run = Some(BulkRun {
            keys: preview.keys.len(),
            batches: batches.len(),
            done: 0,
            deleted: 0,
            stop: stop.clone(),
            running: true,
            outcome: None,
            unknown: None,
        });
        cx.spawn(async move |this, cx| {
            let total = batches.len();
            let mut ended = None;
            let mut unknown = None;
            for (index, batch) in batches.into_iter().enumerate() {
                if stop.load(Ordering::Relaxed) {
                    ended = Some((
                        format!("Stopped: {index} of {total} batches were sent; the rest were not."),
                        Color::Warning,
                    ));
                    break;
                }
                let operation = format!("{run_id}:{index}");
                if operation != owner {
                    if !pass_operation_lock(&lock, &owner, &operation) {
                        ended = Some((
                            format!("Stopped: the bulk lock was lost after {index} of {total} batches; the rest were not sent."),
                            Color::Error,
                        ));
                        break;
                    }
                    owner = operation.clone();
                }
                let intent = serde_json::json!({
                    "time": chrono::Utc::now().to_rfc3339(),
                    "operation": operation,
                    "connection": connection.id,
                    "name": connection.name,
                    "site": connection.site,
                    "environment": connection.environment,
                    "db": database,
                    "key": batch.keys.first().map(|key| quote_bytes(key)),
                    "target": format!(
                        "batch {} of {total}: {} keys matching {pattern}{}",
                        index + 1,
                        batch.keys.len(),
                        batch.slot.map(|slot| format!(" in slot {slot}")).unwrap_or_default()
                    ),
                    "kind": "bulk-delete",
                    // Restores the bulk lock after a restart until the outcome is settled.
                    "lock": lock,
                    "phase": "intent",
                });
                let summary = mongo_write::summarize_intent(&intent);
                let task = cx.update(|cx| run(cx, {
                    let session = session.clone();
                    let operation = operation.clone();
                    async move {
                        if let Err(error) = mongo_write::audit(intent) {
                            return anyhow::Ok(Err(format!("{error:#}")));
                        }
                        let outcome = session.delete_batch(&batch).await;
                        let result = match &outcome {
                            Ok(_) => "ok",
                            Err(WriteError::Refused(_)) => "refused",
                            Err(WriteError::Unknown(_)) => "unknown",
                        };
                        let logged = mongo_write::audit(serde_json::json!({ "operation": operation, "phase": "result", "result": result }))
                            .err()
                            .map(|error| format!("{error:#}"));
                        Ok(Ok((outcome, logged)))
                    }
                }));
                let result = match task.await {
                    Ok(Ok(done)) => Ok(done),
                    // The intent could not be logged, so nothing was sent.
                    Ok(Err(not_logged)) => Err(not_logged),
                    // The task itself failed, possibly after sending.
                    Err(error) => Ok((Err(WriteError::Unknown(format!("{error:#}"))), None)),
                };
                if let Ok((Err(WriteError::Unknown(message)), _)) = &result {
                    unknown = Some((operation.clone(), summary.clone(), message.clone()));
                }
                let alive = this
                    .update(cx, |this, cx| {
                        if let Ok((_, Some(error))) = &result {
                            this.unlogged_writes.push((
                                operation.clone(),
                                format!("{summary} · its result is not in the log: {error}"),
                            ));
                        }
                        if let Some(run) = this.bulk_delete.as_mut().and_then(|form| form.run.as_mut()) {
                            run.done = index + 1;
                            if let Ok((Ok(deleted), _)) = &result {
                                run.deleted += deleted;
                            }
                        }
                        cx.notify();
                    })
                    .is_ok();
                match result {
                    Ok((Ok(_), None)) => {}
                    Ok((Ok(_), Some(error))) => {
                        ended = Some((
                            format!("Batch {} was deleted but its result could not be logged ({error}); the rest were not sent.", index + 1),
                            Color::Error,
                        ));
                        break;
                    }
                    Ok((Err(WriteError::Refused(message)), _)) => {
                        ended = Some((
                            format!("Batch {} was refused ({message}); the rest were not sent.", index + 1),
                            Color::Error,
                        ));
                        break;
                    }
                    Ok((Err(WriteError::Unknown(message)), _)) => {
                        ended = Some((
                            format!("The outcome of batch {} is unknown ({message}); the rest were not sent. Check its keys, then press I checked it.", index + 1),
                            Color::Error,
                        ));
                        break;
                    }
                    Err(not_logged) => {
                        ended = Some((
                            format!("Batch {} was not sent: the write log could not record it ({not_logged}). The rest were not sent either.", index + 1),
                            Color::Error,
                        ));
                        break;
                    }
                }
                // The window closed: stop before the next batch rather than delete unseen.
                if !alive {
                    ended = Some((
                        format!("Stopped: the window closed after {} of {total} batches; the rest were not sent.", index + 1),
                        Color::Warning,
                    ));
                    break;
                }
            }
            // Settled before touching the window, which may be closed by now.
            settle_operation_lock(&lock, &owner, unknown.is_some());
            this.update(cx, |this, cx| {
                if let Some((_, summary, message)) = &unknown {
                    this.write_notice = Some(
                        format!("The outcome of {summary} is unknown ({message}). Check its keys, then press I checked it in the bulk delete dialog.")
                            .into(),
                    );
                }
                if let Some(run) = this.bulk_delete.as_mut().and_then(|form| form.run.as_mut()) {
                    run.running = false;
                    run.outcome = Some(ended.map(|(text, color)| (text.into(), color)).unwrap_or_else(|| {
                        let gone = (run.keys as u64).saturating_sub(run.deleted);
                        (
                            if gone == 0 {
                                format!("Done: {} keys deleted.", run.deleted)
                            } else {
                                format!("Done: {} keys deleted; {gone} were already gone.", run.deleted)
                            }
                            .into(),
                            Color::Success,
                        )
                    }));
                    run.unknown = unknown.map(|(operation, _, _)| operation);
                }
                this.close_redis_key();
                this.scan(true, cx);
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    fn release_bulk_lock(&mut self, cx: &mut Context<Self>) {
        let Some(operation) = self
            .bulk_delete
            .as_ref()
            .and_then(|form| form.run.as_ref())
            .and_then(|run| run.unknown.clone())
        else {
            return;
        };
        let task = cx.background_spawn({
            let operation = operation.clone();
            async move { mongo_write::acknowledge_writes(&[operation]) }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            // Released before touching the window, which may be closed by now.
            if result.is_ok() {
                acknowledge_publishes(std::slice::from_ref(&operation));
            }
            this.update(cx, |this, cx| {
                let run = this.bulk_delete.as_mut().and_then(|form| form.run.as_mut());
                match result {
                    Ok(()) => {
                        if let Some(run) = run {
                            run.unknown = None;
                            run.outcome = Some((
                                "Checked. Preview again before deleting more.".into(),
                                Color::Muted,
                            ));
                        }
                        this.dangling_writes
                            .retain(|(known, _)| *known != operation);
                        this.unlogged_writes
                            .retain(|(known, _)| *known != operation);
                        this.write_notice = None;
                    }
                    Err(error) => {
                        if let Some(run) = run {
                            run.outcome = Some((
                                format!("Vilo could not record that it was checked: {error:#}")
                                    .into(),
                                Color::Error,
                            ));
                        }
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    pub(crate) fn render_bulk_delete(&self, form: &BulkForm, cx: &mut Context<Self>) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = mongo_write::guarded(connection);
        let view = cx.entity().downgrade();
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if this.bulk_delete.as_ref().is_none_or(BulkForm::closable) {
                this.bulk_delete = None;
                cx.notify();
            }
        };
        let kind_label = form
            .kind
            .and_then(|kind| KEY_TYPES.iter().find(|(_, value)| *value == kind))
            .map_or("All types", |(label, _)| label);
        let kind_menu = PopoverMenu::new("database-bulk-kind")
            .trigger(
                Button::new("database-bulk-kind-button", kind_label)
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                    .disabled(form.running() || form.run.is_some()),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    let choices = std::iter::once(("All types", None)).chain(
                        // The list's own "all types" entry has no SCAN TYPE; it is the one above.
                        KEY_TYPES
                            .iter()
                            .filter(|(_, value)| !value.is_empty())
                            .map(|(label, value)| (*label, Some(*value))),
                    );
                    for (label, kind) in choices {
                        let view = view.clone();
                        menu = menu.entry(label, None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(form) = &mut this.bulk_delete {
                                    form.kind = kind;
                                }
                                cx.notify();
                            })
                            .ok();
                        });
                    }
                    menu
                }))
            });
        let keys = match &form.preview {
            Some(Ok(preview)) => preview.keys.len(),
            _ => 0,
        };
        let key_list = uniform_list(
            "database-bulk-keys",
            keys,
            cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                let Some(Some(Ok(preview))) = this.bulk_delete.as_ref().map(|form| &form.preview)
                else {
                    return Vec::new();
                };
                range
                    .filter_map(|index| preview.keys.get(index))
                    .map(|key| {
                        Label::new(escape_bytes(key))
                            .size(LabelSize::Small)
                            .buffer_font(cx)
                            .single_line()
                            .truncate()
                            .into_any_element()
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .h(px(160.));
        let summary = match (&form.preview, &form.previewed) {
            (Some(Ok(preview)), Some((pattern, _))) => Some((
                format!(
                    "{} keys match {pattern}{}. Exactly these are deleted; Vilo does not scan again.",
                    preview.keys.len(),
                    form.kind
                        .map(|kind| format!(" (type {kind})"))
                        .unwrap_or_default()
                ),
                preview.incomplete.clone(),
            )),
            _ => None,
        };
        let run = form.run.as_ref();
        let can_delete = keys > 0 && run.is_none() && form.scanning.is_none();
        v_flex()
            .id("database-bulk-delete")
            .gap_3()
            .w(px(560.))
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(Headline::new("Bulk delete").size(HeadlineSize::Small))
            .child(
                Label::new(format!(
                    "{} · {} · {} · db{}",
                    connection.site,
                    connection.environment,
                    connection.name,
                    self.redis().map_or(0, |session| session.database())
                ))
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(if production { Color::Error } else { Color::Muted }),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(div().flex_1().child(boxed(form.pattern.clone(), cx)))
                    .child(kind_menu)
                    .child(if form.scanning.is_some() {
                        Button::new("database-bulk-stop-scan", "Stop")
                            .style(ButtonStyle::Outlined)
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(cancel) = this.bulk_delete.as_ref().and_then(|form| form.scanning.as_ref()) {
                                    cancel.store(true, Ordering::Relaxed);
                                }
                                cx.notify();
                            }))
                    } else {
                        Button::new("database-bulk-preview", "Preview")
                            .style(ButtonStyle::Filled)
                            .disabled(run.is_some())
                            .on_click(cx.listener(|this, _, _, cx| this.preview_bulk_delete(cx)))
                    }),
            )
            .child(
                Label::new(format!(
                    "The preview SCANs every node, keeping at most {MAX_KEYS} keys and stopping after 60 s."
                ))
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .when_some(summary, |this, (summary, incomplete)| {
                this.child(Label::new(summary).size(LabelSize::Small))
                    .when_some(incomplete, |this, incomplete| {
                        this.child(
                            Label::new(format!(
                                "Not every matching key is listed: {incomplete} Deleting removes only the keys listed."
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Warning),
                        )
                    })
                    .child(boxed(key_list, cx))
            })
            .when_some(
                form.preview.as_ref().and_then(|preview| preview.as_ref().err()).cloned(),
                |this, error| this.child(Label::new(error).size(LabelSize::Small).color(Color::Error)),
            )
            .when(production && can_delete, |this| {
                this.child(
                    Label::new(format!(
                        "This is a production connection. Type “{}” and the pattern to confirm.",
                        connection.name
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Error),
                )
                .child(
                    h_flex()
                        .gap_2()
                        .child(div().flex_1().child(boxed(form.typed_name.clone(), cx)))
                        .child(div().flex_1().child(boxed(form.typed_pattern.clone(), cx))),
                )
            })
            .when_some(run, |this, run| {
                this.child(
                    Label::new(format!(
                        "{} of {} batches sent · {} keys deleted",
                        run.done, run.batches, run.deleted
                    ))
                    .size(LabelSize::Small),
                )
                .when_some(run.outcome.clone(), |this, (outcome, color)| {
                    this.child(Label::new(outcome).size(LabelSize::Small).color(color))
                })
            })
            .when_some(form.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(Color::Muted))
            })
            .child(
                h_flex()
                    .gap_2()
                    .when(can_delete, |this| {
                        this.child(
                            Button::new("database-bulk-send", format!("Delete {keys} keys"))
                                .style(ButtonStyle::Tinted(ui::TintColor::Error))
                                .on_click(cx.listener(|this, _, window, cx| this.send_bulk_delete(window, cx))),
                        )
                    })
                    .when(run.is_some_and(|run| run.running), |this| {
                        this.child(
                            Button::new("database-bulk-stop", "Stop after this batch")
                                .style(ButtonStyle::Outlined)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    if let Some(run) = this.bulk_delete.as_ref().and_then(|form| form.run.as_ref()) {
                                        run.stop.store(true, Ordering::Relaxed);
                                    }
                                    cx.notify();
                                })),
                        )
                    })
                    .when(run.is_some_and(|run| run.unknown.is_some()), |this| {
                        this.child(
                            Button::new("database-bulk-checked", "I checked it")
                                .on_click(cx.listener(|this, _, _, cx| this.release_bulk_lock(cx))),
                        )
                    })
                    .child(
                        Button::new(
                            "database-bulk-close",
                            if run.is_some() { "Close" } else { "Cancel" },
                        )
                        .disabled(!form.closable())
                        .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }
}
