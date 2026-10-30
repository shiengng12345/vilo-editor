//! The Redis Pub/Sub tab, as RedisInsight's: subscribe to patterns, channels or shard channels
//! on a connection of its own and watch messages arrive; publish a message. Publishing is a
//! write: it needs the write switch, production asks for the connection name, it is logged
//! before sending and sent once.

use std::collections::VecDeque;

use editor::Editor;
use gpui::{
    Entity, ListHorizontalSizingBehavior, UniformListScrollHandle, Window, prelude::*, uniform_list,
};
use ui::{Button, ButtonStyle, ContextMenu, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::quote_bytes,
    redis_pubsub::{MAX_MESSAGE_BYTES, PubSubEvent, PubSubMessage, Stop, Subscribe},
};

use crate::{
    DatabaseView, boxed, editor as single_line, mongo_write, redis_view::escape_bytes, run,
};

/// Messages kept for the view, by payload bytes; older ones are dropped and counted.
const BUFFER_BYTES: usize = 4 * 1024 * 1024;
/// Events taken from the subscription per view update, so a busy channel redraws in batches.
const BATCH: usize = 500;

enum Entry {
    Message(PubSubMessage),
    /// Messages lost at this point; consecutive losses share one row.
    Lost(u64),
    /// Messages lost, or the end of the subscription, at this point in the stream.
    Note(SharedString),
}

pub(crate) struct PubSubState {
    kind: Subscribe,
    names: Option<Entity<Editor>>,
    /// The subscription and the task moving its events into `entries`; dropping both
    /// unsubscribes and closes the connection.
    live: Option<(Stop, gpui::Task<()>)>,
    /// Bumped by each subscribe and by leaving, so a subscription that finishes opening after
    /// the user moved on is closed at once instead of running off-screen.
    attempt: u64,
    subscribed_to: Option<SharedString>,
    entries: VecDeque<Entry>,
    bytes: usize,
    received: u64,
    lost: u64,
    /// Messages dropped from the view to stay within `BUFFER_BYTES`.
    trimmed: u64,
    problem: Option<SharedString>,
    publish_channel: Option<Entity<Editor>>,
    publish_message: Option<Entity<Editor>>,
    publish_sharded: bool,
    confirm: Option<Entity<Editor>>,
    publishing: bool,
    publish_result: Option<(bool, SharedString)>,
    /// The lock of a publish whose outcome is unknown, until the user checked it.
    publish_locked: Option<String>,
    scroll: UniformListScrollHandle,
}

impl Default for PubSubState {
    fn default() -> Self {
        Self {
            kind: Subscribe::Patterns,
            names: None,
            live: None,
            attempt: 0,
            subscribed_to: None,
            entries: VecDeque::new(),
            bytes: 0,
            received: 0,
            lost: 0,
            trimmed: 0,
            problem: None,
            publish_channel: None,
            publish_message: None,
            publish_sharded: false,
            confirm: None,
            publishing: false,
            publish_result: None,
            publish_locked: None,
            scroll: UniformListScrollHandle::new(),
        }
    }
}

impl PubSubState {
    /// Leaving the tab ends the subscription and releases its connection.
    pub(crate) fn leave(&mut self) {
        self.unsubscribe("Unsubscribed: you left Pub/Sub.");
    }

    fn unsubscribe(&mut self, note: &'static str) {
        self.attempt += 1;
        self.problem = None;
        if self.live.take().is_some() {
            self.push(Entry::Note(note.into()));
            self.subscribed_to = None;
        }
    }

    fn push(&mut self, entry: Entry) {
        if let (Entry::Lost(more), Some(Entry::Lost(count))) = (&entry, self.entries.back_mut()) {
            *count += more;
            return;
        }
        self.bytes += cost(&entry);
        self.entries.push_back(entry);
        while self.bytes > BUFFER_BYTES {
            let Some(oldest) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= cost(&oldest);
            if matches!(oldest, Entry::Message(_)) {
                self.trimmed += 1;
            }
        }
    }

    fn take(&mut self, event: PubSubEvent) {
        match event {
            PubSubEvent::Message(message) => {
                self.received += 1;
                self.push(Entry::Message(message));
            }
            PubSubEvent::Lost(count) => {
                self.lost += count;
                self.push(Entry::Lost(count));
            }
            PubSubEvent::Ended(reason) => {
                self.live = None;
                self.subscribed_to = None;
                self.push(Entry::Note(
                    format!("Subscription ended: {reason} Nothing after this was received; subscribe again.").into(),
                ));
            }
        }
    }
}

/// Notes count against the view's budget too, so they cannot pile up without limit.
fn cost(entry: &Entry) -> usize {
    match entry {
        Entry::Message(message) => message.cost(),
        Entry::Lost(_) => 64,
        Entry::Note(note) => note.len() + 64,
    }
}

fn time_of(message: &PubSubMessage) -> String {
    chrono::DateTime::<chrono::Local>::from(message.time)
        .format("%H:%M:%S%.3f")
        .to_string()
}

/// Publishes still awaiting their answer or ended unknown, by lock key: the operation holding
/// the lock and whether its outcome is unknown (else pending). Shared by every window of this
/// process, so no window can publish past another's unsettled publish.
static PUBLISH_LOCKS: std::sync::Mutex<std::collections::BTreeMap<String, (String, bool)>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

fn publish_locks()
-> std::sync::MutexGuard<'static, std::collections::BTreeMap<String, (String, bool)>> {
    // The map stays consistent even if a holder panicked: every change is a single insert or remove.
    PUBLISH_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// From the write log at startup: locks an unsettled publish's channel as unknown, unless a
/// publish of this process already holds it (a pending one keeps its own lock).
pub(crate) fn recover_publish_lock(lock: &str, operation: &str) {
    publish_locks()
        .entry(lock.to_string())
        .or_insert_with(|| (operation.to_string(), true));
}

pub(crate) fn publish_pending(operation: &str) -> bool {
    publish_locks()
        .values()
        .any(|(owner, unknown)| owner == operation && !unknown)
}

/// The user checked these operations; their unknown publish locks go, pending ones never.
pub(crate) fn acknowledge_publishes(operations: &[String]) {
    publish_locks().retain(|_, (owner, unknown)| !(*unknown && operations.contains(owner)));
}

/// Channels are shared by every logical database of a server, so the lock is per connection.
fn lock_key(connection: &str, channel: &str) -> String {
    format!("{connection}/redis/pubsub/{channel}")
}

impl DatabaseView {
    fn pubsub(&mut self) -> Option<&mut PubSubState> {
        self.redis_state.as_mut().map(|state| &mut state.pubsub)
    }

    pub(crate) fn open_pubsub(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(pubsub) = self.pubsub() else {
            return;
        };
        let missing = pubsub.names.is_none();
        if missing {
            let names = single_line(
                "Patterns or channels, separated by spaces, e.g. * or news.*",
                "*",
                false,
                window,
                cx,
            );
            let channel = single_line("Channel", "", false, window, cx);
            let message = single_line("Message", "", false, window, cx);
            let confirm = single_line("Type the connection name", "", false, window, cx);
            if let Some(pubsub) = self.pubsub() {
                pubsub.names = Some(names);
                pubsub.publish_channel = Some(channel);
                pubsub.publish_message = Some(message);
                pubsub.confirm = Some(confirm);
            }
        }
        cx.notify();
    }

    fn subscribe(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(pubsub) = self.pubsub() else {
            return;
        };
        let text = pubsub
            .names
            .as_ref()
            .map(|names| names.read(cx).text(cx))
            .unwrap_or_default();
        let names = text
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let kind = pubsub.kind;
        pubsub.live = None;
        pubsub.subscribed_to = None;
        pubsub.attempt += 1;
        let attempt = pubsub.attempt;
        pubsub.problem = Some("Subscribing…".into());
        let task = run(cx, async move { session.subscribe(kind, names).await });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(pubsub) = this.pubsub() else {
                    return;
                };
                // Dropping a subscription that arrives too late closes it.
                if pubsub.attempt != attempt {
                    return;
                }
                match result {
                    Ok(subscription) => {
                        let (mut events, stop) = subscription.into_parts();
                        pubsub.problem = None;
                        pubsub.subscribed_to = Some(text.trim().to_string().into());
                        let pump = cx.spawn(async move |this, cx| {
                            while let Some(first) = events.next().await {
                                let mut batch = vec![first];
                                while batch.len() < BATCH {
                                    match events.try_next() {
                                        Some(event) => batch.push(event),
                                        None => break,
                                    }
                                }
                                let alive = this
                                    .update(cx, |this, cx| {
                                        // A replaced subscription's pump must not touch the new one.
                                        let Some(pubsub) = this
                                            .pubsub()
                                            .filter(|pubsub| pubsub.attempt == attempt)
                                        else {
                                            return false;
                                        };
                                        for event in batch {
                                            pubsub.take(event);
                                        }
                                        cx.notify();
                                        true
                                    })
                                    .unwrap_or(false);
                                if !alive {
                                    return;
                                }
                                // Lets the window draw and take input between batches of a busy channel.
                                cx.background_executor()
                                    .timer(std::time::Duration::from_millis(10))
                                    .await;
                            }
                            // The stream ended without saying why; never show it as still live.
                            this.update(cx, |this, cx| {
                                if let Some(pubsub) = this.pubsub()
                                    && pubsub.attempt == attempt
                                    && pubsub.live.is_some()
                                {
                                    pubsub.take(PubSubEvent::Ended(
                                        "The subscription stopped.".into(),
                                    ));
                                    cx.notify();
                                }
                            })
                            .ok();
                        });
                        pubsub.live = Some((stop, pump));
                    }
                    Err(error) => pubsub.problem = Some(format!("{error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    fn publish(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let database = session.database();
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        if !self.writes_allowed(&connection) {
            return;
        }
        let read = |editor: &Option<Entity<Editor>>, cx: &App| {
            editor
                .as_ref()
                .map(|editor| editor.read(cx).text(cx))
                .unwrap_or_default()
        };
        let clustered = session.is_clustered();
        let Some(pubsub) = self.redis_state.as_ref().map(|state| &state.pubsub) else {
            return;
        };
        if pubsub.publishing {
            return;
        }
        let (channel, payload, typed, sharded) = (
            read(&pubsub.publish_channel, cx),
            read(&pubsub.publish_message, cx),
            read(&pubsub.confirm, cx),
            pubsub.publish_sharded,
        );
        let refuse = |this: &mut Self, message: &str, cx: &mut Context<Self>| {
            if let Some(pubsub) = this.pubsub() {
                pubsub.publish_result = Some((false, message.to_string().into()));
            }
            cx.notify();
        };
        if channel.is_empty() {
            return refuse(self, "Name the channel.", cx);
        }
        // Until the write log was read, an unsettled publish from before a restart is not known.
        if !self.write_log_checked {
            return refuse(
                self,
                "Vilo has not finished checking the write log for unsettled writes, so it does not publish yet. If the log could not be read, its banner says why.",
                cx,
            );
        }
        if mongo_write::guarded(&connection) && typed.trim() != connection.name.trim() {
            return refuse(self, "Type the connection name exactly to confirm.", cx);
        }
        let lock = lock_key(&connection.id, &channel);
        let held = publish_locks().get(&lock).cloned();
        match held {
            Some((_, false)) => {
                return refuse(
                    self,
                    "An earlier publish to this channel is still waiting for its answer; it times out as unknown after 30 seconds.",
                    cx,
                );
            }
            Some((_, true)) => {
                // The tab may have been rebuilt since; offer the confirmation again.
                if let Some(pubsub) = self.pubsub() {
                    pubsub.publish_locked = Some(lock);
                }
                return refuse(
                    self,
                    "An earlier publish to this channel has an unknown outcome. Check whether its subscribers got it, then press I checked it.",
                    cx,
                );
            }
            None => {}
        }
        let operation = uuid::Uuid::new_v4().simple().to_string();
        let kind = if sharded { "SPUBLISH" } else { "PUBLISH" };
        // The message itself is not logged, only its size, like values of key writes.
        let intent = serde_json::json!({
            "time": chrono::Utc::now().to_rfc3339(),
            "operation": operation,
            "connection": connection.id,
            "name": connection.name,
            "site": connection.site,
            "environment": connection.environment,
            "db": database,
            "key": quote_bytes(channel.as_bytes()),
            "target": format!("channel {} ({} bytes)", quote_bytes(channel.as_bytes()), payload.len()),
            "kind": kind,
            // Restores the channel's lock after a restart until the outcome is settled.
            "lock": lock,
            "phase": "intent",
        });
        let summary = mongo_write::summarize_intent(&intent);
        let task = run(cx, {
            let operation = operation.clone();
            async move {
                mongo_write::audit(intent)?;
                let outcome = session.publish(&channel, payload.as_bytes(), sharded).await;
                let result = match &outcome {
                    Ok(_) => "ok",
                    Err(WriteError::Refused(_)) => "refused",
                    Err(WriteError::Unknown(_)) => "unknown",
                };
                let logged = mongo_write::audit(serde_json::json!({ "operation": operation, "phase": "result", "result": result }))
                    .err()
                    .map(|error| format!("{error:#}"));
                Ok((outcome, logged))
            }
        });
        let generation = self.generation;
        // Locked while in flight too, so a rebuilt tab cannot send the same channel again before
        // the answer is known; only this operation's own answer changes its lock.
        publish_locks().insert(lock.clone(), (operation.clone(), false));
        if let Some(pubsub) = self.pubsub() {
            pubsub.publishing = true;
            pubsub.publish_result = Some((true, "Publishing…".into()));
        }
        // Detached, not kept in the tab's state: the outcome must reach the write log and the
        // lock even if the user switches connection or database while it is in flight.
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                let mut locked = None;
                let unknown = matches!(result, Ok((Err(WriteError::Unknown(_)), _)));
                {
                    let mut locks = publish_locks();
                    if locks.get(&lock).is_some_and(|(owner, _)| *owner == operation) {
                        if unknown {
                            locks.insert(lock.clone(), (operation.clone(), true));
                        } else {
                            locks.remove(&lock);
                        }
                    }
                }
                let message = match result {
                    Ok((outcome, logged)) => {
                        if let Some(error) = logged {
                            this.unlogged_writes.push((
                                operation,
                                format!("{summary} · its result is not in the log: {error}"),
                            ));
                        }
                        match outcome {
                            // PUBLISH counts only the node that took it; a Cluster forwards to the others.
                            Ok(count) if clustered && !sharded => (true, format!("Published; {count} subscribers on the node that took it received it. Subscribers on other nodes are not counted.")),
                            Ok(0) => (true, "Published; no subscriber was listening, so nobody received it.".to_string()),
                            Ok(count) => (true, format!("Published; {count} subscribers received it.")),
                            Err(WriteError::Refused(message)) => (false, format!("Not published: {message}")),
                            Err(WriteError::Unknown(message)) => {
                                // A publish cannot be checked by reloading anything, and is never sent again.
                                locked = Some(lock.clone());
                                (false, format!("The outcome is unknown ({message}): it may have been delivered. Vilo will not send it again; check the subscribers, then press I checked it."))
                            }
                        }
                    }
                    Err(error) => (false, format!("Not sent: {error:#}")),
                };
                if this.generation == generation
                    && let Some(pubsub) = this.pubsub()
                {
                    pubsub.publishing = false;
                    pubsub.publish_result = Some((message.0, message.1.into()));
                    if locked.is_some() {
                        pubsub.publish_locked = locked;
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    /// Settles an unknown publish once the user checked it: the acknowledgement goes into the
    /// write log first, so the channel stays settled after a restart.
    fn release_publish_lock(&mut self, cx: &mut Context<Self>) {
        let Some(lock) = self
            .pubsub()
            .and_then(|pubsub| pubsub.publish_locked.clone())
        else {
            return;
        };
        // Only an outcome already known to be unknown can be acknowledged, never a pending one.
        let held = publish_locks()
            .get(&lock)
            .filter(|(_, unknown)| *unknown)
            .map(|(operation, _)| operation.clone());
        let Some(operation) = held else {
            return;
        };
        let task = cx.background_spawn({
            let operation = operation.clone();
            async move { mongo_write::acknowledge_writes(&[operation]) }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                match result {
                    Ok(()) => {
                        acknowledge_publishes(std::slice::from_ref(&operation));
                        this.dangling_writes
                            .retain(|(known, _)| *known != operation);
                        this.unlogged_writes
                            .retain(|(known, _)| *known != operation);
                        if let Some(pubsub) = this.pubsub() {
                            pubsub.publish_locked = None;
                            pubsub.publish_result = None;
                        }
                    }
                    Err(error) => {
                        if let Some(pubsub) = this.pubsub() {
                            pubsub.publish_result = Some((
                                false,
                                format!("Vilo could not record that it was checked: {error:#}")
                                    .into(),
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

    pub(crate) fn render_pubsub(&self, pubsub: &PubSubState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let writable = self
            .active_connection()
            .is_some_and(|connection| self.writes_allowed(connection));
        let production = self.active_connection().is_some_and(mongo_write::guarded);
        let kind_label = |kind: Subscribe| match kind {
            Subscribe::Patterns => "Pattern",
            Subscribe::Channels => "Channel",
            Subscribe::Shard => "Shard channel",
        };
        let live = pubsub.live.is_some();
        let kind_menu = PopoverMenu::new("database-pubsub-kind")
            .trigger(
                Button::new("database-pubsub-kind-button", kind_label(pubsub.kind))
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                    .disabled(live),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for kind in [Subscribe::Patterns, Subscribe::Channels, Subscribe::Shard] {
                        let view = view.clone();
                        menu = menu.entry(kind_label(kind), None, move |_, cx| {
                            view.update(cx, |this, cx| {
                                if let Some(pubsub) = this.pubsub() {
                                    pubsub.kind = kind;
                                }
                                cx.notify();
                            })
                            .ok();
                        });
                    }
                    menu
                }))
            });
        let status = match (&pubsub.subscribed_to, &pubsub.problem) {
            (_, Some(problem)) => Label::new(problem.clone())
                .size(LabelSize::Small)
                .color(Color::Warning),
            (Some(names), None) => Label::new(format!(
                "Subscribed to {names} · {} messages{}{}",
                pubsub.received,
                if pubsub.lost > 0 {
                    format!(" · {} lost", pubsub.lost)
                } else {
                    String::new()
                },
                if pubsub.trimmed > 0 {
                    format!(" · {} older ones dropped from view", pubsub.trimmed)
                } else {
                    String::new()
                },
            ))
            .size(LabelSize::Small)
            .color(Color::Success),
            (None, None) => Label::new("Not subscribed.")
                .size(LabelSize::Small)
                .color(Color::Muted),
        };
        let rows = pubsub
            .entries
            .iter()
            .map(|entry| match entry {
                Entry::Message(message) => (
                    false,
                    time_of(message),
                    format!(
                        "{}{}",
                        message.channel,
                        if message.sharded { " (shard)" } else { "" }
                    ),
                    format!(
                        "{}{}",
                        escape_bytes(&message.payload),
                        if message.size > message.payload.len() {
                            format!("  … cut, {} bytes in all", message.size)
                        } else {
                            String::new()
                        }
                    ),
                ),
                Entry::Lost(count) => (
                    true,
                    String::new(),
                    String::new(),
                    format!(
                        "{count} messages lost here (they came faster than Vilo could show them)"
                    ),
                ),
                Entry::Note(note) => (true, String::new(), String::new(), note.to_string()),
            })
            .collect::<Vec<_>>();
        let widest = rows
            .iter()
            .enumerate()
            .max_by_key(|(_, row)| row.3.len())
            .map(|(index, _)| index);
        let list = uniform_list(
            "database-pubsub-messages",
            rows.len(),
            move |range, _, cx| {
                range
                    .filter_map(|index| rows.get(index).cloned())
                    .map(|(note, time, channel, message)| {
                        h_flex()
                            .px_2()
                            .gap_3()
                            .whitespace_nowrap()
                            .child(
                                div().w(px(100.)).flex_none().child(
                                    Label::new(time)
                                        .size(LabelSize::Small)
                                        .color(Color::Muted)
                                        .buffer_font(cx),
                                ),
                            )
                            .child(
                                div().w(px(180.)).flex_none().child(
                                    Label::new(channel)
                                        .size(LabelSize::Small)
                                        .single_line()
                                        .truncate(),
                                ),
                            )
                            .child(
                                Label::new(message)
                                    .size(LabelSize::Small)
                                    .buffer_font(cx)
                                    .color(if note { Color::Warning } else { Color::Default }),
                            )
                            .into_any_element()
                    })
                    .collect::<Vec<_>>()
            },
        )
        .with_horizontal_sizing_behavior(ListHorizontalSizingBehavior::Unconstrained)
        .with_width_from_item(widest)
        .track_scroll(&pubsub.scroll)
        .flex_1();
        let editor = |editor: &Option<Entity<Editor>>, cx: &mut Context<Self>| {
            editor
                .clone()
                .map(|editor| boxed(editor, cx).into_any_element())
                .unwrap_or_else(|| div().into_any_element())
        };
        let locked = pubsub.publish_locked.is_some();
        v_flex()
            .size_full()
            .p_3()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(kind_menu)
                    .child(div().flex_1().child(editor(&pubsub.names, cx)))
                    .child(if live {
                        Button::new("database-pubsub-unsubscribe", "Unsubscribe")
                            .style(ButtonStyle::Outlined)
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(pubsub) = this.pubsub() {
                                    pubsub.unsubscribe("Unsubscribed.");
                                }
                                cx.notify();
                            }))
                    } else {
                        Button::new("database-pubsub-subscribe", "Subscribe")
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, _, cx| this.subscribe(cx)))
                    })
                    .child(
                        Button::new("database-pubsub-clear", "Clear")
                            .style(ButtonStyle::Subtle)
                            .tooltip(Tooltip::text("Clear the messages shown; the subscription goes on"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(pubsub) = this.pubsub() {
                                    pubsub.entries.clear();
                                    pubsub.bytes = 0;
                                }
                                cx.notify();
                            })),
                    ),
            )
            .child(status)
            .child(
                Label::new(format!(
                    "Messages arrive on a connection of their own. Up to {} MB are kept here and each is cut at {} KB. Nothing is replayed: messages missed while Vilo lagged or the connection dropped are counted as lost. Leaving this tab unsubscribes.",
                    BUFFER_BYTES / (1024 * 1024),
                    MAX_MESSAGE_BYTES / 1024
                ))
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .py_1()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_background)
                    .child(list),
            )
            .child(
                v_flex()
                    .gap_2()
                    .pt_2()
                    .border_t_1()
                    .border_color(colors.border)
                    .child(Label::new("Publish").size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(div().w(px(220.)).child(editor(&pubsub.publish_channel, cx)))
                            .child(div().flex_1().child(editor(&pubsub.publish_message, cx)))
                            .child(
                                Button::new("database-pubsub-sharded", "Sharded")
                                    .label_size(LabelSize::Small)
                                    .toggle_state(pubsub.publish_sharded)
                                    .style(if pubsub.publish_sharded { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                                    .tooltip(Tooltip::text("SPUBLISH to a shard channel instead of PUBLISH"))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        if let Some(pubsub) = this.pubsub() {
                                            pubsub.publish_sharded = !pubsub.publish_sharded;
                                        }
                                        cx.notify();
                                    })),
                            )
                            .when(production, |this| this.child(div().w(px(200.)).child(editor(&pubsub.confirm, cx))))
                            .child(
                                Button::new("database-pubsub-publish", "Publish")
                                    .style(ButtonStyle::Filled)
                                    .disabled(!writable || pubsub.publishing)
                                    .tooltip(Tooltip::text(if writable {
                                        "Sent once; a lost answer is reported as unknown, never resent"
                                    } else {
                                        "Switch writes on (top bar) to publish"
                                    }))
                                    .on_click(cx.listener(|this, _, _, cx| this.publish(cx))),
                            ),
                    )
                    .when_some(pubsub.publish_result.clone(), |this, (ok, message)| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .child(Label::new(message).size(LabelSize::Small).color(if ok { Color::Success } else { Color::Warning }))
                                .when(locked, |this| {
                                    this.child(
                                        Button::new("database-pubsub-checked", "I checked it")
                                            .label_size(LabelSize::Small)
                                            .on_click(cx.listener(|this, _, _, cx| this.release_publish_lock(cx))),
                                    )
                                }),
                        )
                    }),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(size: usize) -> PubSubMessage {
        PubSubMessage {
            time: std::time::SystemTime::UNIX_EPOCH,
            channel: "c".into(),
            sharded: false,
            payload: vec![b'x'; size],
            size,
        }
    }

    #[test]
    fn test_publish_locks() {
        // Unique names: the lock table is shared by the whole process.
        publish_locks().insert("test/a".into(), ("pending-a".into(), false));
        recover_publish_lock("test/a", "pending-a");
        assert_eq!(
            publish_locks().get("test/a"),
            Some(&("pending-a".to_string(), false)),
            "recovery keeps a pending lock"
        );
        assert!(publish_pending("pending-a"));
        acknowledge_publishes(&["pending-a".to_string()]);
        assert!(
            publish_locks().contains_key("test/a"),
            "a pending lock cannot be acknowledged"
        );
        recover_publish_lock("test/b", "old-b");
        assert!(!publish_pending("old-b"));
        acknowledge_publishes(&["old-b".to_string()]);
        assert!(
            !publish_locks().contains_key("test/b"),
            "an unknown lock goes once checked"
        );
        publish_locks().remove("test/a");
    }

    #[test]
    fn test_buffer_keeps_newest_within_bytes() {
        let mut state = PubSubState::default();
        // Four fit with their per-entry share; the fifth and sixth push out the oldest two.
        let size = BUFFER_BYTES / 4 - 100;
        for _ in 0..6 {
            state.take(PubSubEvent::Message(message(size)));
        }
        assert_eq!(state.received, 6);
        assert_eq!(state.trimmed, 2);
        assert!(state.bytes <= BUFFER_BYTES);
        let mut empty = PubSubState::default();
        for _ in 0..200_000 {
            empty.take(PubSubEvent::Message(message(0)));
        }
        assert!(
            empty.bytes <= BUFFER_BYTES,
            "empty messages still cost room"
        );
        assert!(empty.trimmed > 0);
        state.take(PubSubEvent::Lost(3));
        assert_eq!(state.lost, 3);
        state.take(PubSubEvent::Lost(2));
        assert!(
            matches!(state.entries.back(), Some(Entry::Lost(5))),
            "losses in a row share a row"
        );
        let mut notes = PubSubState::default();
        for _ in 0..200_000 {
            notes.push(Entry::Note("a note".into()));
        }
        assert!(notes.bytes <= BUFFER_BYTES, "notes cost room too");
        state.take(PubSubEvent::Ended("The connection closed.".into()));
        assert!(state.live.is_none() && state.subscribed_to.is_none());
    }
}
