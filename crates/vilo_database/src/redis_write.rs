//! Changing one Redis key at a time from its detail panel. Each write is reviewed against the value
//! read at review time, confirmed, logged before it is sent and sent once, like MongoDB writes.

use anyhow::{Result, anyhow};
use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, Tooltip, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::{Bytes, RedisWrite, quote_bytes},
};

use crate::{DatabaseView, boxed, editor as single_line, mongo_write, run};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum RedisOp {
    EditString,
    HashSet,
    HashDelete,
    SetAdd,
    SetRemove,
    ZSetScore,
    ZSetRemove,
    ListPush { head: bool },
    ListRemove,
    Delete,
    Rename,
    Expire,
    Persist,
}

impl RedisOp {
    pub(crate) fn label(self) -> &'static str {
        match self {
            RedisOp::EditString => "Edit value",
            RedisOp::HashSet => "Set a field",
            RedisOp::HashDelete => "Delete a field",
            RedisOp::SetAdd => "Add a member",
            RedisOp::SetRemove => "Remove a member",
            RedisOp::ZSetScore => "Set a member's score",
            RedisOp::ZSetRemove => "Remove a member",
            RedisOp::ListPush { head: true } => "Push to the head",
            RedisOp::ListPush { head: false } => "Push to the tail",
            RedisOp::ListRemove => "Remove the first equal value",
            RedisOp::Delete => "Delete key",
            RedisOp::Rename => "Rename key",
            RedisOp::Expire => "Set time to live",
            RedisOp::Persist => "Remove time to live",
        }
    }

    /// The operations offered for a key of this type, value edits first.
    pub(crate) fn for_kind(kind: &str) -> Vec<RedisOp> {
        let mut ops = match kind {
            "string" => vec![RedisOp::EditString],
            "hash" => vec![RedisOp::HashSet, RedisOp::HashDelete],
            "set" => vec![RedisOp::SetAdd, RedisOp::SetRemove],
            "zset" => vec![RedisOp::ZSetScore, RedisOp::ZSetRemove],
            "list" => vec![
                RedisOp::ListPush { head: true },
                RedisOp::ListPush { head: false },
                RedisOp::ListRemove,
            ],
            _ => Vec::new(),
        };
        ops.extend([
            RedisOp::Rename,
            RedisOp::Expire,
            RedisOp::Persist,
            RedisOp::Delete,
        ]);
        ops
    }

    fn inputs(self) -> &'static [&'static str] {
        match self {
            RedisOp::EditString => &["Value"],
            RedisOp::HashSet => &["Field", "Value"],
            RedisOp::HashDelete => &["Field"],
            RedisOp::SetAdd | RedisOp::SetRemove | RedisOp::ZSetRemove => &["Member"],
            RedisOp::ZSetScore => &["Member", "Score"],
            RedisOp::ListPush { .. } | RedisOp::ListRemove => &["Value"],
            RedisOp::Rename => &["New name"],
            RedisOp::Expire => &["Seconds"],
            RedisOp::Delete | RedisOp::Persist => &[],
        }
    }
}

pub(crate) struct RedisWriteForm {
    op: RedisOp,
    key: Bytes,
    inputs: Vec<(&'static str, Entity<Editor>)>,
    planned: Option<RedisWrite>,
    typed: Entity<Editor>,
    show_values: bool,
    sending: bool,
    message: Option<SharedString>,
    _task: Option<gpui::Task<()>>,
}

impl RedisWriteForm {
    pub(crate) fn sending_now(&self) -> bool {
        self.sending
    }
}

fn lock_key(connection: &str, key: &[u8]) -> String {
    format!("{connection}/redis/{}", quote_bytes(key))
}

impl DatabaseView {
    pub(crate) fn redis_writes_locked(&self, key: &[u8]) -> bool {
        self.active
            .as_ref()
            .is_some_and(|connection| self.unknown_writes.contains(&lock_key(connection, key)))
    }

    pub(crate) fn clear_redis_lock(&mut self, key: &[u8]) {
        if let Some(connection) = &self.active {
            self.unknown_writes.remove(&lock_key(connection, key));
        }
    }

    /// `current` prefills the value of a string edit; it must be text to be edited here.
    pub(crate) fn start_redis_write(
        &mut self,
        op: RedisOp,
        key: Bytes,
        current: Option<Bytes>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.redis_writes_locked(&key) {
            self.write_notice = Some(
                "A write to this key has an unknown outcome. Refresh the key to check it first."
                    .into(),
            );
            cx.notify();
            return;
        }
        let mut inputs = Vec::new();
        for label in op.inputs() {
            let editor = if op == RedisOp::EditString {
                let Some(text) = current
                    .clone()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
                else {
                    self.write_notice =
                        Some("This value is not text, so Vilo cannot edit it here.".into());
                    cx.notify();
                    return;
                };
                cx.new(|cx| {
                    let mut editor = Editor::auto_height(4, 24, window, cx);
                    editor.set_text(text, window, cx);
                    editor
                })
            } else {
                single_line(label, "", false, window, cx)
            };
            inputs.push((*label, editor));
        }
        let mut form = RedisWriteForm {
            op,
            key,
            inputs,
            planned: None,
            typed: single_line("Type the connection name", "", false, window, cx),
            show_values: false,
            sending: false,
            message: None,
            _task: None,
        };
        // Operations without inputs go straight to review.
        if form.inputs.is_empty() {
            form.planned = Some(match op {
                RedisOp::Persist => RedisWrite::Persist {
                    key: form.key.clone(),
                },
                _ => RedisWrite::Delete {
                    key: form.key.clone(),
                },
            });
        }
        self.redis_write = Some(form);
        cx.notify();
    }

    fn review_redis_write(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(form) = &mut self.redis_write else {
            return;
        };
        let values = form
            .inputs
            .iter()
            .map(|(_, editor)| editor.read(cx).text(cx))
            .collect::<Vec<_>>();
        let (op, key) = (form.op, form.key.clone());
        form.message = Some("Reading the current value…".into());
        let task = run(cx, async move {
            let input = |index: usize| -> Result<Bytes> {
                let text = values.get(index).cloned().unwrap_or_default();
                Ok(text.into_bytes())
            };
            let trimmed = |index: usize| -> Result<String> {
                let text = values
                    .get(index)
                    .map(|text| text.trim().to_string())
                    .unwrap_or_default();
                if text.is_empty() {
                    return Err(anyhow!("Fill in every field."));
                }
                Ok(text)
            };
            Ok(match op {
                RedisOp::EditString => RedisWrite::SetString {
                    expected: session
                        .get_string(&key)
                        .await?
                        .ok_or_else(|| anyhow!("The key is gone or is no longer a string."))?,
                    value: input(0)?,
                    key,
                },
                RedisOp::HashSet => {
                    let field = trimmed(0)?.into_bytes();
                    RedisWrite::HashSet {
                        expected: session.hash_get(&key, &field).await?,
                        value: input(1)?,
                        field,
                        key,
                    }
                }
                RedisOp::HashDelete => {
                    let field = trimmed(0)?.into_bytes();
                    RedisWrite::HashDelete {
                        expected: session
                            .hash_get(&key, &field)
                            .await?
                            .ok_or_else(|| anyhow!("The hash has no such field."))?,
                        field,
                        key,
                    }
                }
                RedisOp::SetAdd => RedisWrite::SetAdd {
                    member: input(0)?,
                    key,
                },
                RedisOp::SetRemove => RedisWrite::SetRemove {
                    member: input(0)?,
                    key,
                },
                RedisOp::ZSetScore => {
                    let member = trimmed(0)?.into_bytes();
                    let score = trimmed(1)?
                        .parse::<f64>()
                        .map_err(|_| anyhow!("The score must be a number."))?;
                    RedisWrite::ZSetScore {
                        expected: session.zset_score(&key, &member).await?,
                        member,
                        score,
                        key,
                    }
                }
                RedisOp::ZSetRemove => RedisWrite::ZSetRemove {
                    member: input(0)?,
                    key,
                },
                RedisOp::ListPush { head } => RedisWrite::ListPush {
                    head,
                    value: input(0)?,
                    key,
                },
                RedisOp::ListRemove => RedisWrite::ListRemoveFirst {
                    value: input(0)?,
                    key,
                },
                RedisOp::Rename => RedisWrite::Rename {
                    to: trimmed(0)?.into_bytes(),
                    key,
                },
                RedisOp::Expire => {
                    let seconds = trimmed(0)?
                        .parse::<u64>()
                        .ok()
                        .filter(|seconds| *seconds > 0)
                        .ok_or_else(|| {
                            anyhow!("Seconds must be a whole number above 0; use Delete key to remove it now.")
                        })?;
                    RedisWrite::Expire { seconds, key }
                }
                RedisOp::Delete => RedisWrite::Delete { key },
                RedisOp::Persist => RedisWrite::Persist { key },
            })
        });
        form._task = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if let Some(form) = &mut this.redis_write {
                    match result {
                        Ok(planned) => {
                            form.planned = Some(planned);
                            form.message = None;
                        }
                        Err(error) => form.message = Some(format!("{error:#}").into()),
                    }
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn send_redis_write(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        if !self.writes_allowed(&connection) {
            return;
        }
        let generation = self.generation;
        let Some(form) = &mut self.redis_write else {
            return;
        };
        let Some(planned) = form.planned.clone() else {
            return;
        };
        if form.sending {
            return;
        }
        if mongo_write::guarded(&connection)
            && form.typed.read(cx).text(cx).trim() != connection.name.trim()
        {
            form.message = Some("Type the connection name exactly to confirm.".into());
            cx.notify();
            return;
        }
        form.sending = true;
        form.message = Some("Sending…".into());
        let operation = uuid::Uuid::new_v4().simple().to_string();
        let intent = serde_json::json!({
            "time": chrono::Utc::now().to_rfc3339(),
            "operation": operation,
            "connection": connection.id,
            "name": connection.name,
            "site": connection.site,
            "environment": connection.environment,
            "key": quote_bytes(planned.key()),
            "kind": planned.name(),
            "phase": "intent",
        });
        let task = run(cx, {
            let planned = planned.clone();
            async move {
                mongo_write::audit(intent)?;
                let outcome = session.write(&planned).await;
                let result = match &outcome {
                    Ok(true) => "ok",
                    Ok(false) => "unchanged",
                    Err(WriteError::Refused(_)) => "refused",
                    Err(WriteError::Unknown(_)) => "unknown",
                };
                let logged = mongo_write::audit(serde_json::json!({ "operation": operation, "phase": "result", "result": result }))
                    .err()
                    .map(|error| format!("{error:#}"));
                Ok((outcome, logged))
            }
        });
        form._task = Some(cx.spawn(async move |this, cx| {
            let (result, logged) = match task.await {
                Ok((outcome, logged)) => (Ok(outcome), logged),
                Err(error) => (Err(error), None),
            };
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(form) = &mut this.redis_write else {
                    return;
                };
                form.sending = false;
                let key = planned.key().to_vec();
                match result {
                    Ok(Ok(true)) => {
                        this.redis_write = None;
                        // A key that moved or went is dropped from the list; others reload in place.
                        if matches!(planned, RedisWrite::Delete { .. } | RedisWrite::Rename { .. }) {
                            this.close_redis_key();
                            this.scan(true, cx);
                        } else {
                            this.reopen_redis_key(key, cx);
                        }
                    }
                    Ok(Ok(false)) => {
                        form.message = Some(
                            match planned {
                                RedisWrite::Rename { .. } => "Nothing changed: the new name is already taken, or the key is gone.",
                                RedisWrite::ListRemoveFirst { .. } | RedisWrite::SetRemove { .. } | RedisWrite::ZSetRemove { .. } => "Nothing changed: the value is not there.",
                                _ => "Nothing changed: the value changed since it was read, or the key is gone. Refresh and try again.",
                            }
                            .into(),
                        );
                    }
                    Ok(Err(WriteError::Unknown(message))) => {
                        if let Some(connection) = &this.active {
                            this.unknown_writes.insert(lock_key(connection, &key));
                        }
                        this.write_notice = Some(
                            format!("The outcome of the last write is unknown ({message}). Refresh the key to check it before writing again.")
                                .into(),
                        );
                        this.redis_write = None;
                    }
                    Ok(Err(WriteError::Refused(message))) => {
                        form.message = Some(format!("Not written: {message}").into());
                    }
                    Err(error) => form.message = Some(format!("Not sent: {error:#}").into()),
                }
                if let Some(error) = logged {
                    let note = format!("The write log has its intent but not its result ({error}).");
                    this.write_notice = Some(match this.write_notice.take() {
                        Some(existing) => format!("{existing} {note}").into(),
                        None => note.into(),
                    });
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(crate) fn render_redis_write(
        &self,
        form: &RedisWriteForm,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = mongo_write::guarded(connection);
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if !this.redis_write.as_ref().is_some_and(|form| form.sending) {
                this.redis_write = None;
                cx.notify();
            }
        };
        v_flex()
            .id("database-redis-write")
            .gap_3()
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(Headline::new(form.op.label()).size(HeadlineSize::Small))
            .child(
                Label::new(format!(
                    "{} · {} · {} · {}",
                    connection.site,
                    connection.environment,
                    connection.name,
                    quote_bytes(&form.key)
                ))
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(if production {
                    Color::Error
                } else {
                    Color::Muted
                }),
            )
            .map(|this| match &form.planned {
                None => this.children(form.inputs.iter().map(|(label, editor)| {
                    v_flex()
                        .gap_1()
                        .child(
                            Label::new(*label)
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                        .child(boxed(editor.clone(), cx))
                })),
                Some(planned) => this
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Label::new("This will be sent once:").size(LabelSize::Small))
                            .child(
                                Button::new(
                                    "database-redis-write-values",
                                    if form.show_values {
                                        "Hide values"
                                    } else {
                                        "Show values"
                                    },
                                )
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        if let Some(form) = &mut this.redis_write {
                                            form.show_values = !form.show_values;
                                        }
                                        cx.notify();
                                    },
                                )),
                            ),
                    )
                    .child(boxed(
                        Label::new(planned.describe(form.show_values))
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
                        .child(boxed(form.typed.clone(), cx))
                    }),
            })
            .when_some(form.message.clone(), |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(if form.sending {
                            Color::Muted
                        } else {
                            Color::Warning
                        }),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| match &form.planned {
                        None => this.child(
                            Button::new("database-redis-write-review", "Review")
                                .style(ButtonStyle::Filled)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.review_redis_write(cx)),
                                ),
                        ),
                        Some(planned) => this
                            .child(
                                Button::new("database-redis-write-send", planned.name())
                                    .style(ButtonStyle::Filled)
                                    .color(if matches!(planned, RedisWrite::Delete { .. }) {
                                        Color::Error
                                    } else {
                                        Color::Default
                                    })
                                    .disabled(form.sending)
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.send_redis_write(cx)),
                                    ),
                            )
                            .when(!form.inputs.is_empty(), |this| {
                                this.child(
                                    Button::new("database-redis-write-back", "Back to editing")
                                        .disabled(form.sending)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(form) = &mut this.redis_write {
                                                form.planned = None;
                                                form.message = None;
                                            }
                                            cx.notify();
                                        })),
                                )
                            }),
                    })
                    .child(
                        Button::new("database-redis-write-cancel", "Cancel")
                            .disabled(form.sending)
                            .tooltip(Tooltip::text("Nothing is sent"))
                            .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }
}
