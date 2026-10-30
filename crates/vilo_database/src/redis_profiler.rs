//! The Profiler panel, as RedisInsight's: MONITOR one node for a few seconds and list the
//! commands it runs. MONITOR slows a busy server, so on production (or a connection with no
//! environment) every run needs the connection name typed, and a run always ends by itself.

use std::collections::VecDeque;

use editor::Editor;
use gpui::{
    Entity, ListHorizontalSizingBehavior, Task, UniformListScrollHandle, Window, prelude::*,
    uniform_list,
};
use ui::{Button, ButtonStyle, ContextMenu, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::redis_monitor::{
    DEFAULT_SECONDS, MAX_LINE_BYTES, Monitor, MonitorBatch, MonitorLine,
};

use crate::{
    DatabaseView, boxed, editor as single_line, mongo_write, redis_analyze::slow_command,
    redis_view::escape_bytes, run,
};

/// Lines kept for the view, by bytes and by count; older ones are dropped and counted.
const BUFFER_BYTES: usize = 4 * 1024 * 1024;
const BUFFER_LINES: usize = 10_000;
const POLL: std::time::Duration = std::time::Duration::from_millis(200);

pub(crate) struct ProfilerState {
    /// Index into the session's nodes; only a cluster has more than one.
    node: usize,
    seconds: u64,
    confirm: Option<Entity<Editor>>,
    show_args: bool,
    /// The running MONITOR; dropping it closes its connection.
    monitor: Option<Monitor>,
    _poll: Option<Task<()>>,
    starting: bool,
    /// Bumped by every start and stop, so a run that finishes opening after the user moved on
    /// is closed at once.
    attempt: u64,
    entries: VecDeque<MonitorLine>,
    bytes: usize,
    received: u64,
    lost: u64,
    trimmed: u64,
    status: Option<(SharedString, Color)>,
    /// Where and for how long the current run goes, for its status once the server accepts it.
    target: SharedString,
    scroll: UniformListScrollHandle,
}

impl Default for ProfilerState {
    fn default() -> Self {
        Self {
            node: 0,
            seconds: DEFAULT_SECONDS,
            confirm: None,
            show_args: false,
            monitor: None,
            _poll: None,
            starting: false,
            attempt: 0,
            entries: VecDeque::new(),
            bytes: 0,
            received: 0,
            lost: 0,
            trimmed: 0,
            status: None,
            target: SharedString::default(),
            scroll: UniformListScrollHandle::new(),
        }
    }
}

impl ProfilerState {
    pub(crate) fn stop(&mut self, note: &'static str) {
        if self.monitor.is_some() || self.starting {
            self.status = Some((note.into(), Color::Muted));
        }
        self.monitor = None;
        self._poll = None;
        self.starting = false;
        self.attempt += 1;
    }

    fn take(&mut self, batch: MonitorBatch) {
        if batch.started {
            self.status = Some((format!("Running {}…", self.target).into(), Color::Success));
        }
        self.lost += batch.lost;
        for line in batch.lines {
            self.received += 1;
            self.bytes += line.cost();
            self.entries.push_back(line);
        }
        while self.bytes > BUFFER_BYTES || self.entries.len() > BUFFER_LINES {
            let Some(oldest) = self.entries.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(oldest.cost());
            self.trimmed += 1;
        }
        if let Some(ended) = batch.ended {
            self.status = Some(match ended {
                Ok(reason) => (reason.into(), Color::Muted),
                Err(reason) => (reason.into(), Color::Warning),
            });
            self.monitor = None;
        }
    }
}

fn time_of(line: &MonitorLine) -> String {
    let Some((seconds, fraction)) = line.time.split_once('.') else {
        return line.time.clone();
    };
    let (Ok(seconds), Ok(micros)) = (seconds.parse::<i64>(), fraction.parse::<u32>()) else {
        return line.time.clone();
    };
    chrono::DateTime::from_timestamp(seconds, micros.saturating_mul(1000))
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|| line.time.clone())
}

/// What the row shows after the time, db and client.
fn command_text(line: &MonitorLine, show_args: bool) -> String {
    if line.raw {
        let text = line
            .args
            .first()
            .map(|raw| escape_bytes(raw))
            .unwrap_or_default();
        return format!("Not MONITOR output: {text}");
    }
    let mut text = slow_command(&line.args, show_args);
    if line.cut {
        text.push_str(&format!("  … line cut at {} KB", MAX_LINE_BYTES / 1024));
    }
    text
}

impl DatabaseView {
    fn profiler(&mut self) -> Option<&mut ProfilerState> {
        self.redis_state.as_mut().map(|state| &mut state.profiler)
    }

    pub(crate) fn open_profiler(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let missing = self
            .profiler()
            .is_some_and(|profiler| profiler.confirm.is_none());
        if missing {
            let confirm = single_line("Type the connection name", "", false, window, cx);
            if let Some(profiler) = self.profiler() {
                profiler.confirm = Some(confirm);
            }
        }
    }

    fn start_profiler(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        let generation = self.generation;
        let nodes = session.scan_nodes();
        let Some(profiler) = self.profiler() else {
            return;
        };
        if profiler.monitor.is_some() || profiler.starting {
            return;
        }
        if mongo_write::guarded(&connection)
            && profiler
                .confirm
                .as_ref()
                .map(|confirm| confirm.read(cx).text(cx))
                .unwrap_or_default()
                .trim()
                != connection.name.trim()
        {
            profiler.status = Some((
                "MONITOR slows a busy server. Type the connection name exactly to run it here."
                    .into(),
                Color::Warning,
            ));
            cx.notify();
            return;
        }
        let Some(node) = nodes.get(profiler.node).cloned() else {
            return;
        };
        // The name is typed again for every run.
        if let Some(confirm) = &profiler.confirm {
            confirm.update(cx, |confirm, cx| confirm.set_text("", window, cx));
        }
        let seconds = profiler.seconds;
        profiler.attempt += 1;
        let attempt = profiler.attempt;
        profiler.starting = true;
        profiler.status = Some(("Starting…".into(), Color::Muted));
        profiler.target = format!("on {} for {seconds} s", node.label).into();
        let task = run(cx, async move { session.monitor(&node, seconds).await });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                // Dropping a run that arrives too late closes it.
                let Some(profiler) = this
                    .profiler()
                    .filter(|profiler| profiler.attempt == attempt)
                else {
                    return;
                };
                profiler.starting = false;
                match result {
                    Ok(monitor) => {
                        profiler.monitor = Some(monitor);
                        profiler._poll = Some(cx.spawn(async move |this, cx| {
                            loop {
                                cx.background_executor().timer(POLL).await;
                                let running = this
                                    .update(cx, |this, cx| {
                                        let Some(profiler) = this
                                            .profiler()
                                            .filter(|profiler| profiler.attempt == attempt)
                                        else {
                                            return false;
                                        };
                                        let Some(monitor) = &profiler.monitor else {
                                            return false;
                                        };
                                        let batch = monitor.take();
                                        profiler.take(batch);
                                        cx.notify();
                                        profiler.monitor.is_some()
                                    })
                                    .unwrap_or(false);
                                if !running {
                                    return;
                                }
                            }
                        }));
                    }
                    Err(error) => {
                        profiler.status = Some((format!("{error:#}").into(), Color::Warning))
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn render_profiler(
        &self,
        profiler: &ProfilerState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let production = self.active_connection().is_some_and(mongo_write::guarded);
        let nodes = self
            .redis()
            .map(|session| session.scan_nodes())
            .unwrap_or_default();
        let busy = profiler.monitor.is_some() || profiler.starting;
        let node_menu = (nodes.len() > 1).then(|| {
            let labels = nodes
                .iter()
                .map(|node| node.label.clone())
                .collect::<Vec<_>>();
            let current = labels
                .get(profiler.node)
                .cloned()
                .unwrap_or_else(|| "Choose a node".into());
            let view = view.clone();
            PopoverMenu::new("database-profiler-node")
                .trigger(
                    Button::new("database-profiler-node-button", current)
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                        .disabled(busy),
                )
                .menu(move |window, cx| {
                    let labels = labels.clone();
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        for (index, label) in labels.into_iter().enumerate() {
                            let view = view.clone();
                            menu = menu.entry(label, None, move |_, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(profiler) = this.profiler() {
                                        profiler.node = index;
                                    }
                                    cx.notify();
                                })
                                .ok();
                            });
                        }
                        menu
                    }))
                })
        });
        let seconds_menu = {
            PopoverMenu::new("database-profiler-seconds")
                .trigger(
                    Button::new(
                        "database-profiler-seconds-button",
                        format!("{} s", profiler.seconds),
                    )
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Subtle)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                    .disabled(busy),
                )
                .menu(move |window, cx| {
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        for seconds in [10, 30, 60] {
                            let view = view.clone();
                            menu = menu.entry(format!("{seconds} s"), None, move |_, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(profiler) = this.profiler() {
                                        profiler.seconds = seconds;
                                    }
                                    cx.notify();
                                })
                                .ok();
                            });
                        }
                        menu
                    }))
                })
        };
        let counts = format!(
            "{} commands{}{}",
            profiler.received,
            if profiler.lost > 0 {
                format!(
                    " · {} lost (they came faster than Vilo could take them)",
                    profiler.lost
                )
            } else {
                String::new()
            },
            if profiler.trimmed > 0 {
                format!(" · {} older ones dropped from view", profiler.trimmed)
            } else {
                String::new()
            },
        );
        let widest = profiler
            .entries
            .iter()
            .enumerate()
            .max_by_key(|(_, line)| line.cost())
            .map(|(index, _)| index);
        let list = uniform_list(
            "database-profiler-lines",
            profiler.entries.len(),
            cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                let Some(profiler) = this.redis_state.as_ref().map(|state| &state.profiler) else {
                    return Vec::new();
                };
                range
                    .filter_map(|index| profiler.entries.get(index))
                    .map(|line| {
                        h_flex()
                            .px_2()
                            .gap_3()
                            .whitespace_nowrap()
                            .child(
                                div().w(px(90.)).flex_none().child(
                                    Label::new(time_of(line))
                                        .size(LabelSize::Small)
                                        .color(Color::Muted)
                                        .buffer_font(cx),
                                ),
                            )
                            .child(
                                div().w(px(30.)).flex_none().child(
                                    Label::new(line.db.clone())
                                        .size(LabelSize::Small)
                                        .color(Color::Muted),
                                ),
                            )
                            .child(
                                div().w(px(150.)).flex_none().child(
                                    Label::new(line.client.clone())
                                        .size(LabelSize::Small)
                                        .color(Color::Muted)
                                        .single_line()
                                        .truncate(),
                                ),
                            )
                            .child(
                                Label::new(command_text(line, profiler.show_args))
                                    .size(LabelSize::Small)
                                    .buffer_font(cx)
                                    .color(if line.raw || line.cut {
                                        Color::Warning
                                    } else {
                                        Color::Default
                                    }),
                            )
                            .into_any_element()
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .with_horizontal_sizing_behavior(ListHorizontalSizingBehavior::Unconstrained)
        .with_width_from_item(widest)
        .track_scroll(&profiler.scroll)
        .flex_1();
        v_flex()
            .size_full()
            .p_2()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .children(node_menu)
                    .child(seconds_menu)
                    .when(production && !busy, |this| {
                        this.children(profiler.confirm.clone().map(|confirm| {
                            div().w(px(200.)).child(boxed(confirm, cx))
                        }))
                    })
                    .child(if busy {
                        Button::new("database-profiler-stop", "Stop")
                            .style(ButtonStyle::Outlined)
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(profiler) = this.profiler() {
                                    profiler.stop("Stopped.");
                                }
                                cx.notify();
                            }))
                    } else {
                        Button::new("database-profiler-start", "Start")
                            .style(ButtonStyle::Filled)
                            .tooltip(Tooltip::text(
                                "MONITOR copies every command the server runs to Vilo, which slows a busy server",
                            ))
                            .on_click(cx.listener(|this, _, window, cx| this.start_profiler(window, cx)))
                    })
                    .child(
                        Button::new(
                            "database-profiler-args",
                            if profiler.show_args { "Hide arguments" } else { "Show arguments" },
                        )
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .tooltip(Tooltip::text("Arguments can hold personal data and secrets"))
                        .on_click(cx.listener(|this, _, _, cx| {
                            if let Some(profiler) = this.profiler() {
                                profiler.show_args = !profiler.show_args;
                            }
                            cx.notify();
                        })),
                    )
                    .child(
                        Button::new("database-profiler-clear", "Clear")
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(profiler) = this.profiler() {
                                    profiler.entries.clear();
                                    profiler.bytes = 0;
                                    profiler.trimmed = 0;
                                }
                                cx.notify();
                            })),
                    )
                    .child(Label::new(counts).size(LabelSize::Small).color(Color::Muted)),
            )
            .when_some(profiler.status.clone(), |this, (status, color)| {
                this.child(Label::new(status).size(LabelSize::Small).color(color))
            })
            .when(profiler.entries.is_empty(), |this| {
                this.child(
                    Label::new(format!(
                        "MONITOR on one node, on a connection of its own, for at most {} s; it stops by itself and closing this panel stops it. Lines over {} KB are cut, and lines Vilo could not keep up with are counted as lost.",
                        profiler.seconds,
                        MAX_LINE_BYTES / 1024
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Muted),
                )
            })
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_background)
                    .child(list),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(args: &[&[u8]], cut: bool, raw: bool) -> MonitorLine {
        MonitorLine {
            time: "1700000000.123456".into(),
            db: "0".into(),
            client: "127.0.0.1:5000".into(),
            args: args.iter().map(|arg| arg.to_vec()).collect(),
            cut,
            raw,
        }
    }

    #[test]
    fn test_rows_hide_arguments_and_say_what_was_cut() {
        let set = line(&[b"set", b"user:1", b"secret"], false, false);
        assert_eq!(command_text(&set, false), "SET ‹2 arguments›");
        assert_eq!(command_text(&set, true), "SET \"user:1\" \"secret\"");
        let cut = line(&[b"set", b"k", b"abc"], true, false);
        assert!(command_text(&cut, false).ends_with("line cut at 16 KB"));
        let raw = line(&[b"-ERR \x01"], false, true);
        assert_eq!(command_text(&raw, false), "Not MONITOR output: -ERR \\x01");
    }

    #[test]
    fn test_buffer_keeps_newest_within_limits() {
        let mut profiler = ProfilerState::default();
        profiler.target = "on server for 10 s".into();
        profiler.take(MonitorBatch {
            started: true,
            ..Default::default()
        });
        assert_eq!(
            profiler.status,
            Some(("Running on server for 10 s…".into(), Color::Success))
        );
        let big = vec![b'x'; 1024 * 1024];
        profiler.take(MonitorBatch {
            lines: (0..6)
                .map(|_| line(&[b"set", &big], false, false))
                .collect(),
            lost: 2,
            ..Default::default()
        });
        assert_eq!(profiler.received, 6);
        assert_eq!(profiler.lost, 2);
        assert!(profiler.bytes <= BUFFER_BYTES);
        assert_eq!(profiler.entries.len() as u64 + profiler.trimmed, 6);

        profiler.take(MonitorBatch {
            lines: (0..BUFFER_LINES + 5)
                .map(|_| line(&[b"ping"], false, false))
                .collect(),
            ended: Some(Ok("Stopped after 10 s.".into())),
            ..Default::default()
        });
        assert_eq!(profiler.entries.len(), BUFFER_LINES);
        assert_eq!(
            profiler.status,
            Some(("Stopped after 10 s.".into(), Color::Muted))
        );
        profiler.take(MonitorBatch {
            ended: Some(Err("MONITOR was refused: NOPERM".into())),
            ..Default::default()
        });
        assert_eq!(
            profiler.status,
            Some(("MONITOR was refused: NOPERM".into(), Color::Warning))
        );
    }
}
