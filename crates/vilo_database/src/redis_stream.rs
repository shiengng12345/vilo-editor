//! A stream's consumer groups, as RedisInsight shows them: each group's consumers and the
//! entries still pending for it, with acknowledging, claiming and removing groups. XACK and
//! XCLAIM change what the group's consumers will process and reloading cannot show whether
//! they happened, so an unknown outcome locks the group until the user checked it, across
//! windows and restarts.

use gpui::{Window, prelude::*};
use ui::{Button, IconButton, Tooltip, prelude::*};
use vilo_database_core::redis::{
    Bytes, GROUP_NAME_NOT_TEXT, PendingEntry, RedisWrite, StreamConsumer, quote_bytes,
};

use crate::{DatabaseView, Fetch, mongo_write, redis_pubsub, redis_write::RedisOp, run};

/// Pending entries shown per group; the group's own count says how many there are in all.
const PENDING_SHOWN: u32 = 100;

pub(crate) struct StreamGroupView {
    key: Bytes,
    group: String,
    data: Fetch<(Vec<StreamConsumer>, Vec<PendingEntry>)>,
    problem: Option<SharedString>,
}

/// The lock XACK and XCLAIM take on a group.
pub(crate) fn group_lock(connection: &str, database: u8, key: &[u8], group: &str) -> String {
    format!(
        "{connection}/redis/db{database}/stream/{}/{}",
        quote_bytes(key),
        quote_bytes(group.as_bytes())
    )
}

fn field<'a>(group: &'a [(String, String)], name: &str) -> &'a str {
    group
        .iter()
        .find(|(field, _)| field == name)
        .map_or("", |(_, value)| value.as_str())
}

impl DatabaseView {
    pub(crate) fn open_stream_group(&mut self, key: Bytes, group: String, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(state) = self.redis_state.as_mut() else {
            return;
        };
        state.stream_group = Some(StreamGroupView {
            key: key.clone(),
            group: group.clone(),
            data: Fetch::Loading,
            problem: None,
        });
        let task = run(cx, {
            let (key, group) = (key.clone(), group.clone());
            async move {
                let consumers = session.stream_consumers(&key, &group).await?;
                let pending = session.stream_pending(&key, &group, PENDING_SHOWN).await?;
                Ok((consumers, pending))
            }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some(view) = this
                    .redis_state
                    .as_mut()
                    .and_then(|state| state.stream_group.as_mut())
                    .filter(|view| view.key == key && view.group == group)
                {
                    view.data = match result {
                        Ok(data) => Fetch::Loaded(data),
                        Err(error) => Fetch::Failed(format!("{error:#}").into()),
                    };
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn reload_stream_group(&mut self, cx: &mut Context<Self>) {
        let open = self
            .redis_state
            .as_ref()
            .and_then(|state| state.stream_group.as_ref())
            .map(|view| (view.key.clone(), view.group.clone()));
        if let Some((key, group)) = open {
            self.open_stream_group(key, group, cx);
        }
    }

    fn current_group_lock(&self, key: &[u8], group: &str) -> Option<String> {
        let connection = self.active.as_ref()?;
        let database = self.redis().map_or(0, |session| session.database());
        Some(group_lock(connection, database, key, group))
    }

    /// Settles an unknown XACK or XCLAIM once the user checked the group: the acknowledgement
    /// goes into the write log first, so it stays settled after a restart.
    fn check_group_operation(&mut self, key: Bytes, group: String, cx: &mut Context<Self>) {
        let Some(lock) = self.current_group_lock(&key, &group) else {
            return;
        };
        let Some((operation, true)) = redis_pubsub::operation_lock(&lock) else {
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
                redis_pubsub::acknowledge_publishes(std::slice::from_ref(&operation));
                crate::shared_unlogged_writes().retain(|(known, _)| *known != operation);
            }
            this.update(cx, |this, cx| {
                match result {
                    Ok(()) => {
                        this.dangling_writes
                            .retain(|(known, _)| *known != operation);
                    }
                    Err(error) => {
                        if let Some(view) = this
                            .redis_state
                            .as_mut()
                            .and_then(|state| state.stream_group.as_mut())
                            .filter(|view| view.key == key && view.group == group)
                        {
                            view.problem = Some(
                                format!("Vilo could not record that it was checked: {error:#}")
                                    .into(),
                            );
                        }
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// The groups of a stream key, and the open group's consumers and pending entries.
    pub(crate) fn render_stream_groups(
        &self,
        key: &Bytes,
        groups: &Result<Vec<Vec<(String, String)>>, String>,
        open: Option<&StreamGroupView>,
        writable: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let groups = match groups {
            Ok(groups) if groups.is_empty() => {
                return Label::new("No consumer groups.")
                    .size(LabelSize::Small)
                    .color(Color::Muted)
                    .into_any_element();
            }
            Ok(groups) => groups,
            Err(error) => {
                return Label::new(format!("The consumer groups could not be read: {error}"))
                    .size(LabelSize::Small)
                    .color(Color::Error)
                    .into_any_element();
            }
        };
        let open = open.filter(|view| view.key == *key);
        let rows = groups.iter().enumerate().map(|(index, group)| {
            let name = field(group, "name").to_string();
            // Shown, but never opened or destroyed: its shown name is not its real name.
            let usable = field(group, GROUP_NAME_NOT_TEXT).is_empty();
            let selected = open.is_some_and(|view| view.group == name);
            let summary = format!(
                "{name}{}  ·  consumers {}  ·  pending {}  ·  last delivered {}{}",
                if usable {
                    ""
                } else {
                    " (name is not text; use the CLI)"
                },
                field(group, "consumers"),
                field(group, "pending"),
                field(group, "last-delivered-id"),
                match field(group, "lag") {
                    "" => String::new(),
                    lag => format!("  ·  lag {lag}"),
                }
            );
            h_flex()
                .id(("database-stream-group", index))
                .px_2()
                .gap_2()
                .rounded_sm()
                .when(selected, |this| this.bg(colors.element_selected))
                .when(usable, |this| {
                    this.cursor_pointer()
                        .hover(|style| style.bg(colors.element_hover))
                        .on_click(cx.listener({
                            let (key, name) = (key.clone(), name.clone());
                            move |this, _, _, cx| {
                                this.open_stream_group(key.clone(), name.clone(), cx)
                            }
                        }))
                })
                .child(
                    div()
                        .flex_1()
                        .child(Label::new(summary).size(LabelSize::Small).buffer_font(cx)),
                )
                .when(writable && usable, |this| {
                    this.child(
                        IconButton::new(("database-stream-group-destroy", index), IconName::Trash)
                            .icon_size(IconSize::XSmall)
                            .tooltip(Tooltip::text("Destroy this group"))
                            .on_click(cx.listener({
                                let (key, name) = (key.clone(), name.clone());
                                move |this, _, window, cx| {
                                    this.start_planned_redis_write(
                                        RedisOp::GroupDestroy,
                                        RedisWrite::GroupDestroy {
                                            key: key.clone(),
                                            group: name.clone(),
                                        },
                                        window,
                                        cx,
                                    )
                                }
                            })),
                    )
                })
        });
        v_flex()
            .gap_1()
            .child(
                Label::new("Consumer Groups")
                    .size(LabelSize::Small)
                    .weight(gpui::FontWeight::SEMIBOLD),
            )
            .children(rows)
            .when_some(open, |this, view| {
                this.child(self.render_open_group(view, writable, cx))
            })
            .into_any_element()
    }

    fn render_open_group(
        &self,
        view: &StreamGroupView,
        writable: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let lock = self
            .current_group_lock(&view.key, &view.group)
            .and_then(|lock| redis_pubsub::operation_lock(&lock));
        let body = match &view.data {
            Fetch::Loading => Label::new("Loading…")
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element(),
            Fetch::Failed(error) => Label::new(error.clone())
                .size(LabelSize::Small)
                .color(Color::Error)
                .into_any_element(),
            Fetch::Loaded((consumers, pending)) => v_flex()
                .gap_1()
                .child(
                    Label::new(format!("Consumers of {}", view.group))
                        .size(LabelSize::Small)
                        .weight(gpui::FontWeight::SEMIBOLD),
                )
                .children(consumers.iter().map(|consumer| {
                    Label::new(format!(
                        "{}  ·  pending {}  ·  idle {} ms{}",
                        consumer.name,
                        consumer.pending,
                        consumer.idle_ms,
                        consumer
                            .inactive_ms
                            .map(|inactive| format!("  ·  inactive {inactive} ms"))
                            .unwrap_or_default()
                    ))
                    .size(LabelSize::Small)
                    .buffer_font(cx)
                }))
                .when(consumers.is_empty(), |this| {
                    this.child(
                        Label::new("No consumers yet.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                })
                .child(
                    Label::new(format!(
                        "Pending entries (the first {PENDING_SHOWN} at most, oldest first)"
                    ))
                    .size(LabelSize::Small)
                    .weight(gpui::FontWeight::SEMIBOLD),
                )
                .children(pending.iter().enumerate().map(|(index, entry)| {
                    let blocked = lock.is_some();
                    h_flex()
                        .gap_2()
                        .child(
                            div().flex_1().child(
                                Label::new(format!(
                                    "{}  ·  {}  ·  idle {} ms  ·  delivered {} times",
                                    entry.id, entry.consumer, entry.idle_ms, entry.deliveries
                                ))
                                .size(LabelSize::Small)
                                .buffer_font(cx),
                            ),
                        )
                        .when(writable, |this| {
                            this.child(
                                Button::new(("database-stream-ack", index), "Acknowledge")
                                    .label_size(LabelSize::Small)
                                    .disabled(blocked)
                                    .tooltip(Tooltip::text(
                                        "XACK: the group stops treating this entry as pending",
                                    ))
                                    .on_click(cx.listener({
                                        let planned = RedisWrite::StreamAck {
                                            key: view.key.clone(),
                                            group: view.group.clone(),
                                            id: entry.id.clone(),
                                        };
                                        move |this, _, window, cx| {
                                            this.start_planned_redis_write(
                                                RedisOp::StreamAck,
                                                planned.clone(),
                                                window,
                                                cx,
                                            )
                                        }
                                    })),
                            )
                            .when_some(u64::try_from(entry.idle_ms).ok(), |this, idle| {
                                // A negative idle cannot serve as XCLAIM's minimum, so no claim is offered.
                                this.child(
                                    Button::new(("database-stream-claim", index), "Claim")
                                        .label_size(LabelSize::Small)
                                        .disabled(blocked)
                                        .tooltip(Tooltip::text(
                                            "XCLAIM: give this entry to another consumer, only if it is still idle as long as shown",
                                        ))
                                        .on_click(cx.listener({
                                            let claim = (
                                                view.key.clone(),
                                                view.group.clone(),
                                                entry.id.clone(),
                                                idle,
                                            );
                                            move |this, _, window: &mut Window, cx| {
                                                let (key, group, id, idle) = claim.clone();
                                                this.start_stream_claim(key, group, id, idle, window, cx)
                                            }
                                        })),
                                )
                            })
                        })
                }))
                .when(pending.is_empty(), |this| {
                    this.child(
                        Label::new("Nothing is pending.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                })
                .into_any_element(),
        };
        v_flex()
            .gap_1()
            .p_2()
            .rounded_md()
            .border_1()
            .border_color(cx.theme().colors().border)
            .map(|this| match &lock {
                Some((_, false)) => this.child(
                    Label::new(
                        "An acknowledgement or claim for this group is still waiting for its answer.",
                    )
                    .size(LabelSize::Small)
                    .color(Color::Warning),
                ),
                Some((_, true)) => this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            Label::new(
                                "The outcome of an acknowledgement or claim for this group is unknown. Check the pending entries, then confirm.",
                            )
                            .size(LabelSize::Small)
                            .color(Color::Warning),
                        )
                        .child(
                            Button::new("database-stream-checked", "I checked it")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener({
                                    let (key, group) = (view.key.clone(), view.group.clone());
                                    move |this, _, _, cx| {
                                        this.check_group_operation(key.clone(), group.clone(), cx)
                                    }
                                })),
                        ),
                ),
                None => this,
            })
            .when_some(view.problem.clone(), |this, problem| {
                this.child(Label::new(problem).size(LabelSize::Small).color(Color::Error))
            })
            .child(body)
            .into_any_element()
    }
}
