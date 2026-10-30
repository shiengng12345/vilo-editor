//! The Redis Analyze tab: the slow log of every primary and the connections each one holds.
//! Resetting the slow log and changing its threshold are administration writes and go through
//! the same switch, confirmation and write log as key writes.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, ContextMenu, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::{AdminWrite, ClientInfo, NodeReport, SlowEntry, quote_bytes},
    redis_analysis::{Analysis, Bucket, KEYS_PER_NODE_CHOICES, KeyStat, TIME_LIMIT},
};

use crate::{
    DatabaseView, Fetch, boxed, editor as single_line, mongo_write,
    redis_view::{badge, describe_size, describe_ttl, escape_bytes},
    run,
};

/// Slow log entries asked of each node unless the user picks another number.
const SLOWLOG_COUNT: u32 = 128;

#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum AnalyzeTab {
    #[default]
    SlowLog,
    Clients,
    Database,
}

/// Tells a running analysis to stop scanning once its report is replaced or dropped; the scan
/// runs on the Tokio runtime, which dropping the GPUI task alone does not stop.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum ClientSort {
    #[default]
    Idle,
    Age,
}

impl AnalyzeState {
    pub(crate) fn tab(&self) -> AnalyzeTab {
        self.tab
    }

    /// Leaving Database Analysis stops a report still running, so it does not keep scanning
    /// the server unseen.
    pub(crate) fn leave_database_analysis(&mut self) {
        if self._analysis.take().is_some() {
            self.analysis = None;
            self.analysis_message =
                Some("The last report was stopped when you left Database Analysis.".into());
        }
    }
}

#[derive(Default)]
pub(crate) struct AnalyzeState {
    tab: AnalyzeTab,
    slowlog: Option<Fetch<NodeReport<SlowEntry>>>,
    /// Per node: name, slowlog-log-slower-than (µs), slowlog-max-len.
    config: Option<NodeReport<(String, Option<i64>, Option<i64>)>>,
    count: u32,
    show_args: bool,
    clients: Option<Fetch<NodeReport<ClientInfo>>>,
    client_filter: Option<Entity<Editor>>,
    client_sort: ClientSort,
    analysis: Option<Fetch<Analysis>>,
    /// Keys sampled per node; 0 until picked, meaning the middle choice.
    analysis_sample: u64,
    analysis_confirm: Option<Entity<Editor>>,
    analysis_message: Option<SharedString>,
    _analysis: Option<(StopOnDrop, gpui::Task<()>)>,
}

struct NodeOutcome {
    node: String,
    result: Result<(), String>,
    /// The operation and its summary when its result line could not be logged.
    unlogged: Option<(String, String)>,
}

pub(crate) struct AdminWriteForm {
    write: Option<AdminWrite>,
    /// Set once the write has been attempted; the form never sends twice.
    sent: bool,
    threshold: Entity<Editor>,
    typed: Entity<Editor>,
    sending: bool,
    /// One line per node once sent.
    results: Vec<(String, Result<(), String>)>,
    message: Option<SharedString>,
    _task: Option<gpui::Task<()>>,
}

impl AdminWriteForm {
    pub(crate) fn sending_now(&self) -> bool {
        self.sending
    }
}

fn local_time(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| seconds.to_string())
}

/// The command as the slow log saw it; arguments can hold personal data, so by default only the
/// command name and how many arguments followed are shown.
pub(crate) fn slow_command(args: &[Vec<u8>], show_args: bool) -> String {
    let Some((name, rest)) = args.split_first() else {
        return String::new();
    };
    let name = String::from_utf8_lossy(name).to_uppercase();
    if !show_args {
        return match rest.len() {
            0 => name,
            1 => format!("{name} ‹1 argument›"),
            count => format!("{name} ‹{count} arguments›"),
        };
    }
    let text = std::iter::once(name)
        .chain(rest.iter().map(|arg| quote_bytes(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    match text.char_indices().nth(4000) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text,
    }
}

/// Client address, name and node; Redis before 4 sends no client fields.
pub(crate) fn slow_client(entry: &SlowEntry) -> String {
    let client = match (entry.client.as_str(), entry.client_name.as_str()) {
        ("", "") => String::new(),
        (address, "") => address.to_string(),
        ("", name) => format!("({name})"),
        (address, name) => format!("{address} ({name})"),
    };
    if client.is_empty() {
        entry.node.clone()
    } else {
        format!("{client} · {}", entry.node)
    }
}

/// The whole command, every argument quoted, for copying.
pub(crate) fn full_command(args: &[Vec<u8>]) -> String {
    args.iter()
        .enumerate()
        .map(|(index, arg)| {
            if index == 0 {
                String::from_utf8_lossy(arg).to_uppercase()
            } else {
                quote_bytes(arg)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Clients matching every word of the filter, sorted with the longest idle (or oldest) first.
pub(crate) fn filter_clients(
    clients: &[ClientInfo],
    filter: &str,
    sort: ClientSort,
) -> Vec<ClientInfo> {
    let words = filter
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    let mut matching = clients
        .iter()
        .filter(|client| {
            let line = client
                .fields
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase();
            words.iter().all(|word| line.contains(word.as_str()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let field = match sort {
        ClientSort::Idle => "idle",
        ClientSort::Age => "age",
    };
    matching.sort_by_key(|client| std::cmp::Reverse(client.get(field).parse::<u64>().unwrap_or(0)));
    matching
}

/// One report section as rows of bars, scaled to the section's largest memory (or key count when
/// memory is unknown).
fn bars(title: &str, buckets: &[Bucket], colors: &theme::ThemeColors) -> AnyElement {
    let by_memory = buckets.iter().any(|bucket| bucket.memory > 0);
    let measure = |bucket: &Bucket| {
        if by_memory {
            bucket.memory
        } else {
            bucket.keys
        }
    };
    let widest = buckets.iter().map(measure).max().unwrap_or(0).max(1);
    v_flex()
        .gap_1()
        .child(
            Label::new(title.to_string())
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD),
        )
        .children(buckets.iter().map(|bucket| {
            h_flex()
                .gap_2()
                .child(
                    div().w(px(160.)).flex_none().child(
                        Label::new(bucket.name.clone())
                            .size(LabelSize::Small)
                            .single_line()
                            .truncate(),
                    ),
                )
                .child(
                    div().w(px(240.)).flex_none().child(
                        div()
                            .h(px(10.))
                            .rounded_sm()
                            .bg(colors.text_accent)
                            .w(px(240. * measure(bucket) as f32 / widest as f32)),
                    ),
                )
                .child(
                    Label::new(format!(
                        "{} keys · {}",
                        bucket.keys,
                        describe_size(bucket.memory)
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Muted),
                )
        }))
        .into_any_element()
}

fn top_keys(title: &str, keys: &[KeyStat], cx: &App) -> AnyElement {
    v_flex()
        .gap_1()
        .child(
            Label::new(title.to_string())
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD),
        )
        .children(keys.iter().map(|stat| {
            h_flex()
                .gap_3()
                .child(h_flex().w(px(64.)).flex_none().child(badge(&stat.kind)))
                .child(
                    div().flex_1().min_w_0().child(
                        Label::new(escape_bytes(&stat.key))
                            .size(LabelSize::Small)
                            .buffer_font(cx)
                            .single_line()
                            .truncate(),
                    ),
                )
                .child(
                    div().w(px(90.)).flex_none().child(
                        Label::new(stat.memory.map_or("?".to_string(), describe_size))
                            .size(LabelSize::Small),
                    ),
                )
                .child(
                    div().w(px(90.)).flex_none().child(
                        Label::new(
                            stat.length
                                .map_or("?".to_string(), |length| format!("{length} items")),
                        )
                        .size(LabelSize::Small),
                    ),
                )
                .child(
                    div().w(px(90.)).flex_none().child(
                        Label::new(stat.ttl_ms.map_or("?".to_string(), describe_ttl))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
                )
        }))
        .into_any_element()
}

fn render_report(analysis: &Analysis, colors: &theme::ThemeColors, cx: &App) -> AnyElement {
    let coverage = analysis.nodes.iter().map(|node| {
        let total = node
            .total
            .map_or("?".to_string(), |total| total.to_string());
        let share = node
            .total
            .filter(|total| *total > 0)
            .map(|total| format!(" ({}%)", node.scanned * 100 / total))
            .unwrap_or_default();
        let unread = node
            .first_unread_error
            .as_ref()
            .map(|error| format!("; {} keys could not be read ({error})", node.unread))
            .unwrap_or_default();
        let (text, color) = match &node.error {
            Some(error) => (
                format!("{}: stopped, {error}{unread}", node.label),
                Color::Error,
            ),
            None if node.unread > 0 => (
                format!(
                    "{}: sampled {} of {total}{share}{unread}",
                    node.label, node.scanned
                ),
                Color::Warning,
            ),
            None if node.finished => (
                format!(
                    "{}: every key visited, {} of {total}",
                    node.label, node.scanned
                ),
                Color::Muted,
            ),
            None => (
                format!("{}: sampled {} of {total}{share}", node.label, node.scanned),
                Color::Muted,
            ),
        };
        Label::new(text).size(LabelSize::Small).color(color)
    });
    v_flex()
        .gap_4()
        .child(
            v_flex()
                .gap_1()
                .child(Label::new(format!(
                    "{} keys sampled · {} · {:.1} s",
                    analysis.sampled,
                    describe_size(analysis.memory),
                    analysis.elapsed.as_secs_f32()
                )))
                .children(coverage)
                .when_some(analysis.stopped.clone(), |this, stopped| {
                    this.child(Label::new(stopped).size(LabelSize::Small).color(Color::Warning))
                })
                .when(analysis.vanished > 0, |this| {
                    this.child(
                        Label::new(format!(
                            "{} keys were deleted or expired while the scan ran.",
                            analysis.vanished
                        ))
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                    )
                })
                .when(analysis.length_unknown > 0, |this| {
                    this.child(
                        Label::new(format!(
                            "The length of {} keys is unknown; they are left out of the length ranking.",
                            analysis.length_unknown
                        ))
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                    )
                })
                .when(analysis.memory_unknown > 0, |this| {
                    this.child(
                        Label::new(format!(
                            "Memory is unknown for {} keys (MEMORY USAGE not answered); memory figures leave them out.",
                            analysis.memory_unknown
                        ))
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                    )
                }),
        )
        .child(bars("Summary per data type", &analysis.by_type, colors))
        .child(bars("Memory likely to be freed over time", &analysis.by_ttl, colors))
        .when(!analysis.namespaces.is_empty(), |this| {
            this.child(bars("Top namespaces in the sample", &analysis.namespaces, colors))
        })
        .child(top_keys("Largest keys in the sample, by memory", &analysis.top_memory, cx))
        .child(top_keys("Largest keys in the sample, by length", &analysis.top_length, cx))
        .into_any_element()
}

impl DatabaseView {
    fn analyze(&mut self) -> Option<&mut AnalyzeState> {
        self.redis_state.as_mut().map(|state| &mut state.analyze)
    }

    pub(crate) fn load_slowlog(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(analyze) = self.analyze() else {
            return;
        };
        if analyze.count == 0 {
            analyze.count = SLOWLOG_COUNT;
        }
        let count = analyze.count;
        analyze.slowlog = Some(Fetch::Loading);
        let task = run(cx, async move {
            let entries = session.slowlog(count).await;
            let config = session.slowlog_config().await;
            Ok((entries, config))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(analyze) = this.analyze() {
                    match result {
                        Ok((entries, config)) => {
                            analyze.slowlog = Some(Fetch::Loaded(entries));
                            analyze.config = Some(config);
                        }
                        Err(error) => {
                            analyze.slowlog = Some(Fetch::Failed(format!("{error:#}").into()));
                            analyze.config = None;
                        }
                    }
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    fn analysis_sample(analyze: &AnalyzeState) -> u64 {
        if analyze.analysis_sample == 0 {
            KEYS_PER_NODE_CHOICES[1]
        } else {
            analyze.analysis_sample
        }
    }

    /// A full report scans and reads many keys, so production asks for the connection name
    /// even though nothing is written.
    fn start_analysis(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        let generation = self.generation;
        let delimiter = self
            .redis_state
            .as_ref()
            .map_or(":".to_string(), |state| state.delimiter().to_string());
        let Some(analyze) = self.analyze() else {
            return;
        };
        let typed = analyze
            .analysis_confirm
            .as_ref()
            .map(|editor| editor.read(cx).text(cx))
            .unwrap_or_default();
        if mongo_write::guarded(&connection) && typed.trim() != connection.name.trim() {
            analyze.analysis_message =
                Some("Type the connection name exactly to start the analysis.".into());
            cx.notify();
            return;
        }
        let sample = Self::analysis_sample(analyze);
        let stop = Arc::new(AtomicBool::new(false));
        let task = run(cx, {
            let stop = stop.clone();
            async move { Ok(session.analyze(sample, &delimiter, &stop).await) }
        });
        analyze.analysis = Some(Fetch::Loading);
        analyze.analysis_message = None;
        analyze._analysis = Some((
            StopOnDrop(stop),
            cx.spawn(async move |this, cx| {
                let result = task.await;
                this.update(cx, |this, cx| {
                    if this.generation != generation {
                        return;
                    }
                    if let Some(analyze) = this.analyze() {
                        analyze.analysis = Some(match result {
                            Ok(analysis) => Fetch::Loaded(analysis),
                            Err(error) => Fetch::Failed(format!("{error:#}").into()),
                        });
                        analyze._analysis = None;
                        cx.notify();
                    }
                })
                .ok();
            }),
        ));
        cx.notify();
    }

    fn cancel_analysis(&mut self, cx: &mut Context<Self>) {
        if let Some(analyze) = self.analyze() {
            analyze._analysis = None;
            analyze.analysis = None;
            analyze.analysis_message =
                Some("Cancelled; the scan stops after its current page.".into());
        }
        cx.notify();
    }

    pub(crate) fn load_clients(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let connection = self.generation;
        let Some(analyze) = self.analyze() else {
            return;
        };
        analyze.clients = Some(Fetch::Loading);
        let task = run(cx, async move { Ok(session.clients().await) });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != connection {
                    return;
                }
                if let Some(analyze) = this.analyze() {
                    analyze.clients = Some(match result {
                        Ok(report) => Fetch::Loaded(report),
                        Err(error) => Fetch::Failed(format!("{error:#}").into()),
                    });
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn open_analyze_tab(
        &mut self,
        tab: AnalyzeTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(analyze) = self.analyze() else {
            return;
        };
        if tab != AnalyzeTab::Database {
            analyze.leave_database_analysis();
        }
        analyze.tab = tab;
        let (load_slowlog, load_clients) = (analyze.slowlog.is_none(), analyze.clients.is_none());
        if analyze.analysis_confirm.is_none() {
            let confirm = single_line("Type the connection name", "", false, window, cx);
            if let Some(analyze) = self.analyze() {
                analyze.analysis_confirm = Some(confirm);
            }
        }
        let Some(analyze) = self.analyze() else {
            return;
        };
        if analyze.client_filter.is_none() {
            let filter = single_line(
                "Filter connections, e.g. name=worker or cmd=subscribe",
                "",
                false,
                window,
                cx,
            );
            cx.subscribe(&filter, |_, _, _: &editor::EditorEvent, cx| cx.notify())
                .detach();
            if let Some(analyze) = self.analyze() {
                analyze.client_filter = Some(filter);
            }
        }
        match tab {
            AnalyzeTab::SlowLog if load_slowlog => self.load_slowlog(cx),
            AnalyzeTab::Clients if load_clients => self.load_clients(cx),
            _ => cx.notify(),
        }
    }

    pub(crate) fn start_admin_write(
        &mut self,
        reset: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self
            .writes_to_check()
            .iter()
            .any(|(operation, _)| self.admin_to_check.contains(operation))
        {
            self.write_notice = Some(
                "An earlier slow log change has an outcome to check. Check it, then mark it checked above."
                    .into(),
            );
            cx.notify();
            return;
        }
        if self
            .redis_state
            .as_ref()
            .is_some_and(|state| state.switching_database())
        {
            return;
        }
        let threshold = single_line(
            "Microseconds, e.g. 10000 (0 logs every command, -1 none)",
            "",
            false,
            window,
            cx,
        );
        self.admin_write = Some(AdminWriteForm {
            write: reset.then_some(AdminWrite::SlowlogReset),
            sent: false,
            threshold,
            typed: single_line("Type the connection name", "", false, window, cx),
            sending: false,
            results: Vec::new(),
            message: None,
            _task: None,
        });
        cx.notify();
    }

    fn review_admin_write(&mut self, cx: &mut Context<Self>) {
        let Some(form) = &mut self.admin_write else {
            return;
        };
        match form.threshold.read(cx).text(cx).trim().parse::<i64>() {
            Ok(micros) if micros >= -1 => {
                form.write = Some(AdminWrite::SlowlogThreshold { micros });
                form.message = None;
            }
            _ => form.message = Some("Type a whole number of microseconds, -1 or more.".into()),
        }
        cx.notify();
    }

    fn send_admin_write(&mut self, cx: &mut Context<Self>) {
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
        let Some(form) = &mut self.admin_write else {
            return;
        };
        let Some(write) = form.write.clone() else {
            return;
        };
        // One attempt per form: a second press could repeat a node that already applied it.
        if form.sending || form.sent {
            return;
        }
        if mongo_write::guarded(&connection)
            && form.typed.read(cx).text(cx).trim() != connection.name.trim()
        {
            form.message = Some("Type the connection name exactly to confirm.".into());
            cx.notify();
            return;
        }
        let nodes = session.scan_nodes();
        if nodes.is_empty() {
            form.message = Some("No Redis node is known yet; refresh and try again.".into());
            cx.notify();
            return;
        }
        let node_count = nodes.len();
        form.sending = true;
        form.sent = true;
        form.message = Some("Sending…".into());
        form.results.clear();
        let task = run(cx, async move {
            let mut outcomes = Vec::new();
            // One node at a time, each logged before it is sent. A node whose intent cannot be
            // logged, or whose outcome is unknown, stops the rest so nothing more is sent blind.
            let mut nodes = nodes.into_iter();
            while let Some(node) = nodes.next() {
                let operation = uuid::Uuid::new_v4().simple().to_string();
                let intent = serde_json::json!({
                    "time": chrono::Utc::now().to_rfc3339(),
                    "operation": operation,
                    "connection": connection.id,
                    "name": connection.name,
                    "site": connection.site,
                    "environment": connection.environment,
                    "target": format!("node {}", node.label),
                    "kind": write.name(),
                    // The requested value, so an unknown outcome can be checked against it later.
                    "value": match &write {
                        AdminWrite::SlowlogThreshold { micros } => Some(micros.to_string()),
                        AdminWrite::SlowlogReset => None,
                    },
                    "phase": "intent",
                });
                let summary = mongo_write::summarize_intent(&intent);
                if let Err(error) = mongo_write::audit(intent) {
                    outcomes.push(NodeOutcome {
                        node: node.label.clone(),
                        result: Err(format!(
                            "not sent: the write log could not record it ({error:#})"
                        )),
                        unlogged: None,
                    });
                    break;
                }
                let outcome = session.admin_write(&node, &write).await;
                let result = match &outcome {
                    Ok(()) => "ok",
                    Err(WriteError::Refused(_)) => "refused",
                    Err(WriteError::Unknown(_)) => "unknown",
                };
                let logged = mongo_write::audit(
                    serde_json::json!({ "operation": operation, "phase": "result", "result": result }),
                );
                let unknown = matches!(outcome, Err(WriteError::Unknown(_)));
                // An unknown outcome is something to check even though its result line is logged.
                let unknown_note = match &outcome {
                    Err(WriteError::Unknown(message)) => {
                        Some(format!("outcome unknown: {message}"))
                    }
                    _ => None,
                };
                let to_check = match (unknown_note, logged) {
                    (Some(note), Err(error)) => Some(format!(
                        "{summary} · {note} · its result is not in the log: {error:#}"
                    )),
                    (None, Err(error)) => Some(format!(
                        "{summary} · its result is not in the log: {error:#}"
                    )),
                    (Some(note), Ok(())) => Some(format!("{summary} · {note}")),
                    (None, Ok(())) => None,
                };
                outcomes.push(NodeOutcome {
                    node: node.label.clone(),
                    result: outcome.map_err(|error| error.to_string()),
                    unlogged: to_check.map(|reason| (operation, reason)),
                });
                if unknown {
                    break;
                }
            }
            // Every node gets a line, including those never tried after a stop.
            for node in nodes {
                outcomes.push(NodeOutcome {
                    node: node.label,
                    result: Err("not sent: stopped after an earlier node".into()),
                    unlogged: None,
                });
            }
            anyhow::Ok(outcomes)
        });
        form._task = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                // Recorded even if the page moved on, like key writes.
                if let Ok(outcomes) = &result {
                    for outcome in outcomes {
                        if let Some(entry) = outcome.unlogged.clone() {
                            this.admin_to_check.insert(entry.0.clone());
                            this.unlogged_writes.push(entry);
                        }
                    }
                }
                let same_session = this.generation == generation;
                let Some(form) = &mut this.admin_write else {
                    cx.notify();
                    return;
                };
                form.sending = false;
                match result {
                    Ok(outcomes) => {
                        let all_ok = outcomes.len() == node_count
                            && outcomes.iter().all(|outcome| outcome.result.is_ok());
                        form.message = Some(
                            if all_ok {
                                "Done on every node."
                            } else {
                                "Not every node was done; see each node below."
                            }
                            .into(),
                        );
                        form.results = outcomes
                            .into_iter()
                            .map(|outcome| (outcome.node, outcome.result))
                            .collect();
                        // Any node may have changed, so the shown settings are read again.
                        if same_session {
                            this.load_slowlog(cx);
                        }
                    }
                    Err(error) => form.message = Some(format!("Not sent: {error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(crate) fn render_admin_write(
        &self,
        form: &AdminWriteForm,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = mongo_write::guarded(connection);
        let nodes = self
            .redis()
            .map(|session| {
                session
                    .scan_nodes()
                    .into_iter()
                    .map(|node| node.label)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if !this.admin_write.as_ref().is_some_and(|form| form.sending) {
                this.admin_write = None;
                cx.notify();
            }
        };
        let done = form.sent && !form.sending;
        v_flex()
            .id("database-admin-write")
            .gap_3()
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(
                Headline::new(match form.write {
                    Some(AdminWrite::SlowlogReset) => "Reset the slow log",
                    _ => "Set the slow log threshold",
                })
                .size(HeadlineSize::Small),
            )
            .child(
                Label::new(format!(
                    "{} · {} · {} · {} node{}",
                    connection.site,
                    connection.environment,
                    connection.name,
                    nodes.len(),
                    if nodes.len() == 1 { "" } else { "s" }
                ))
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(if production {
                    Color::Error
                } else {
                    Color::Muted
                }),
            )
            .map(|this| match &form.write {
                None => this.child(boxed(form.threshold.clone(), cx)),
                Some(write) => this
                    .child(
                        Label::new("This will be sent once to each node:").size(LabelSize::Small),
                    )
                    .child(boxed(
                        Label::new(format!("{}\non {}", write.describe(), nodes.join(", ")))
                            .size(LabelSize::Small)
                            .buffer_font(cx),
                        cx,
                    ))
                    .when(production && !done, |this| {
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
            .children(form.results.iter().map(|(node, result)| {
                Label::new(match result {
                    Ok(()) => format!("{node}: done"),
                    Err(error) => format!("{node}: {error}"),
                })
                .size(LabelSize::Small)
                .color(if result.is_ok() {
                    Color::Success
                } else {
                    Color::Error
                })
            }))
            .when_some(form.message.clone(), |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| match (&form.write, done) {
                        (None, _) => this.child(
                            Button::new("database-admin-review", "Review")
                                .style(ButtonStyle::Filled)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.review_admin_write(cx)),
                                ),
                        ),
                        (Some(write), false) => this.child(
                            Button::new("database-admin-send", write.name())
                                .style(ButtonStyle::Filled)
                                .disabled(form.sending)
                                .on_click(cx.listener(|this, _, _, cx| this.send_admin_write(cx))),
                        ),
                        (Some(_), true) => this,
                    })
                    .child(
                        Button::new(
                            "database-admin-cancel",
                            if done { "Close" } else { "Cancel" },
                        )
                        .disabled(form.sending)
                        .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }

    fn render_database_analysis(
        &self,
        analyze: &AnalyzeState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let production = self.active_connection().is_some_and(mongo_write::guarded);
        let running = analyze._analysis.is_some();
        let sample = Self::analysis_sample(analyze);
        let view = cx.entity().downgrade();
        let sample_menu = PopoverMenu::new("database-analysis-sample")
            .trigger(
                Button::new(
                    "database-analysis-sample-button",
                    format!("Up to {sample} keys per node"),
                )
                .label_size(LabelSize::Small)
                .style(ButtonStyle::Subtle)
                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall))
                .disabled(running),
            )
            .menu(move |window, cx| {
                let view = view.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    for choice in KEYS_PER_NODE_CHOICES {
                        let view = view.clone();
                        menu = menu.entry(
                            format!("Up to {choice} keys per node"),
                            None,
                            move |_, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(analyze) = this.analyze() {
                                        analyze.analysis_sample = choice;
                                    }
                                    cx.notify();
                                })
                                .ok();
                            },
                        );
                    }
                    menu
                }))
            });
        let controls = h_flex()
            .gap_2()
            .child(sample_menu)
            .when(production && !running, |this| {
                this.when_some(analyze.analysis_confirm.clone(), |this, confirm| {
                    this.child(div().w(px(220.)).child(boxed(confirm, cx)))
                })
            })
            .child(if running {
                Button::new("database-analysis-cancel", "Cancel")
                    .style(ButtonStyle::Outlined)
                    .on_click(cx.listener(|this, _, _, cx| this.cancel_analysis(cx)))
            } else {
                Button::new("database-analysis-start", "New report")
                    .style(ButtonStyle::Filled)
                    .on_click(cx.listener(|this, _, _, cx| this.start_analysis(cx)))
            });
        let note = Label::new(format!(
            "Samples up to {sample} keys on each primary with SCAN, in small batches with a pause between pages, and stops after {} seconds. Figures describe the keys sampled; the largest keys are the largest of those, not of the whole database.{}",
            TIME_LIMIT.as_secs(),
            if production {
                " This is a production connection: type its name to start."
            } else {
                ""
            }
        ))
        .size(LabelSize::Small)
        .color(Color::Muted);
        let body = match &analyze.analysis {
            None => Label::new("No report yet. Pick a sample size and press New report.")
                .color(Color::Muted)
                .into_any_element(),
            Some(Fetch::Loading) => Label::new(format!(
                "Analyzing… up to {} seconds.",
                TIME_LIMIT.as_secs()
            ))
            .color(Color::Muted)
            .into_any_element(),
            Some(Fetch::Failed(message)) => Label::new(message.clone())
                .color(Color::Error)
                .into_any_element(),
            Some(Fetch::Loaded(analysis)) => render_report(analysis, &colors, cx),
        };
        v_flex()
            .gap_3()
            .child(controls)
            .child(note)
            .when_some(analyze.analysis_message.clone(), |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                )
            })
            .child(body)
            .into_any_element()
    }

    pub(crate) fn render_analyze(
        &self,
        analyze: &AnalyzeState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let writable = self
            .active_connection()
            .is_some_and(|connection| self.writes_allowed(connection));
        let sub_tab =
            |id: &'static str, label: &'static str, tab: AnalyzeTab, cx: &mut Context<Self>| {
                Button::new(id, label)
                    .style(if analyze.tab == tab {
                        ButtonStyle::Filled
                    } else {
                        ButtonStyle::Subtle
                    })
                    .toggle_state(analyze.tab == tab)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_analyze_tab(tab, window, cx)
                    }))
            };
        let header = h_flex()
            .gap_2()
            .child(sub_tab(
                "database-analyze-slowlog",
                "Slow Log",
                AnalyzeTab::SlowLog,
                cx,
            ))
            .child(sub_tab(
                "database-analyze-clients",
                "Clients",
                AnalyzeTab::Clients,
                cx,
            ))
            .child(sub_tab(
                "database-analyze-database",
                "Database Analysis",
                AnalyzeTab::Database,
                cx,
            ))
            .child(div().flex_1())
            .child(
                IconButton::new("database-analyze-refresh", IconName::RotateCw)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Refresh"))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        let tab = this.redis_state.as_ref().map(|state| state.analyze.tab);
                        match tab {
                            Some(AnalyzeTab::Clients) => this.load_clients(cx),
                            // A report is costly, so it only runs from its own button.
                            Some(AnalyzeTab::Database) => {}
                            _ => this.load_slowlog(cx),
                        }
                    })),
            );
        let failures = |failures: &[(String, String)]| {
            v_flex().children(failures.iter().map(|(node, error)| {
                Label::new(format!("{node}: {error}"))
                    .size(LabelSize::Small)
                    .color(Color::Error)
            }))
        };
        let body = match analyze.tab {
            AnalyzeTab::SlowLog => match &analyze.slowlog {
                None | Some(Fetch::Loading) => Label::new("Loading…")
                    .color(Color::Muted)
                    .into_any_element(),
                Some(Fetch::Failed(message)) => Label::new(message.clone())
                    .color(Color::Error)
                    .into_any_element(),
                Some(Fetch::Loaded(report)) => {
                    let config = analyze.config.as_ref();
                    let settings = config
                        .map(|config| {
                            config
                                .items
                                .iter()
                                .map(|(node, threshold, length)| {
                                    format!(
                                        "{node}: logs commands slower than {} µs, keeps {}",
                                        threshold
                                            .map_or("?".to_string(), |value| value.to_string()),
                                        length.map_or("?".to_string(), |value| value.to_string())
                                    )
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let mut table = v_flex()
                        .rounded_md()
                        .border_1()
                        .border_color(colors.border)
                        .child(
                            h_flex()
                                .px_3()
                                .py_1()
                                .gap_3()
                                .bg(colors.element_background)
                                .child(
                                    div()
                                        .w(px(60.))
                                        .child(Label::new("ID").size(LabelSize::Small)),
                                )
                                .child(
                                    div()
                                        .w(px(150.))
                                        .child(Label::new("Time").size(LabelSize::Small)),
                                )
                                .child(
                                    div()
                                        .w(px(90.))
                                        .child(Label::new("Duration").size(LabelSize::Small)),
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Label::new("Command").size(LabelSize::Small)),
                                )
                                .child(
                                    div()
                                        .w(px(220.))
                                        .child(Label::new("Client · node").size(LabelSize::Small)),
                                ),
                        );
                    for entry in &report.items {
                        table = table.child(
                            h_flex()
                                .px_3()
                                .py_0p5()
                                .gap_3()
                                .border_t_1()
                                .border_color(colors.border_variant)
                                .child(
                                    div().w(px(60.)).child(
                                        Label::new(entry.id.to_string())
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .color(Color::Muted),
                                    ),
                                )
                                .child(
                                    div().w(px(150.)).child(
                                        Label::new(local_time(entry.time))
                                            .size(LabelSize::Small)
                                            .buffer_font(cx),
                                    ),
                                )
                                .child(
                                    div().w(px(90.)).child(
                                        Label::new(format!(
                                            "{:.2} ms",
                                            entry.micros as f64 / 1000.
                                        ))
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .color(
                                            if entry.micros >= 100_000 {
                                                Color::Error
                                            } else {
                                                Color::Default
                                            },
                                        ),
                                    ),
                                )
                                .child(
                                    div().flex_1().min_w_0().child(
                                        Label::new(slow_command(&entry.args, analyze.show_args))
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .when(!analyze.show_args, |label| label.truncate()),
                                    ),
                                )
                                .child({
                                    let full = full_command(&entry.args);
                                    IconButton::new(
                                        SharedString::from(format!(
                                            "database-slowlog-copy-{}-{}",
                                            entry.node, entry.id
                                        )),
                                        IconName::Copy,
                                    )
                                    .icon_size(IconSize::XSmall)
                                    .tooltip(Tooltip::text(
                                        "Copy the whole command with its arguments",
                                    ))
                                    .on_click(
                                        move |_, _, cx| {
                                            cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                                                full.clone(),
                                            ))
                                        },
                                    )
                                })
                                .child(
                                    div().w(px(220.)).child(
                                        Label::new(slow_client(entry))
                                            .size(LabelSize::Small)
                                            .color(Color::Muted)
                                            .truncate(),
                                    ),
                                ),
                        );
                    }
                    v_flex()
                        .gap_2()
                        .child(
                            h_flex()
                                .gap_2()
                                .child(
                                    Label::new(format!(
                                        "{} entries · newest {} per node",
                                        report.items.len(),
                                        if analyze.count == 0 { SLOWLOG_COUNT } else { analyze.count }
                                    ))
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                                )
                                .child(
                                    Button::new("database-slowlog-args", if analyze.show_args { "Hide arguments" } else { "Show arguments" })
                                        .label_size(LabelSize::Small)
                                        .tooltip(Tooltip::text("Arguments can hold keys and values with personal data"))
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(analyze) = this.analyze() {
                                                analyze.show_args = !analyze.show_args;
                                            }
                                            cx.notify();
                                        })),
                                )
                                .child(
                                    Button::new("database-slowlog-more", "Newest 1024")
                                        .label_size(LabelSize::Small)
                                        .disabled(analyze.count >= 1024)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(analyze) = this.analyze() {
                                                analyze.count = 1024;
                                            }
                                            this.load_slowlog(cx);
                                        })),
                                )
                                .child(div().flex_1())
                                .child(
                                    Button::new("database-slowlog-threshold", "Set threshold…")
                                        .label_size(LabelSize::Small)
                                        .disabled(!writable)
                                        .tooltip(Tooltip::text(if writable { "CONFIG SET slowlog-log-slower-than on every node" } else { "Switch writes on to change the threshold" }))
                                        .on_click(cx.listener(|this, _, window, cx| this.start_admin_write(false, window, cx))),
                                )
                                .child(
                                    Button::new("database-slowlog-reset", "Reset…")
                                        .label_size(LabelSize::Small)
                                        .color(Color::Error)
                                        .disabled(!writable)
                                        .tooltip(Tooltip::text(if writable { "SLOWLOG RESET on every node" } else { "Switch writes on to reset the slow log" }))
                                        .on_click(cx.listener(|this, _, window, cx| this.start_admin_write(true, window, cx))),
                                ),
                        )
                        .children(settings.into_iter().map(|line| Label::new(line).size(LabelSize::Small).color(Color::Muted)))
                        .when_some(config.filter(|config| !config.failures.is_empty()), |this, config| {
                            this.child(Label::new("The threshold could not be read (CONFIG may be off-limits):").size(LabelSize::Small).color(Color::Warning))
                                .child(failures(&config.failures))
                        })
                        .child(failures(&report.failures))
                        .child(if report.items.is_empty() {
                            Label::new("The slow log is empty.").color(Color::Muted).into_any_element()
                        } else {
                            table.into_any_element()
                        })
                        .into_any_element()
                }
            },
            AnalyzeTab::Database => self.render_database_analysis(analyze, cx),
            AnalyzeTab::Clients => match &analyze.clients {
                None | Some(Fetch::Loading) => Label::new("Loading…")
                    .color(Color::Muted)
                    .into_any_element(),
                Some(Fetch::Failed(message)) => Label::new(message.clone())
                    .color(Color::Error)
                    .into_any_element(),
                Some(Fetch::Loaded(report)) => {
                    let filter = analyze
                        .client_filter
                        .as_ref()
                        .map(|filter| filter.read(cx).text(cx))
                        .unwrap_or_default();
                    let shown = filter_clients(&report.items, &filter, analyze.client_sort);
                    let columns: [(&str, f32); 9] = [
                        ("id", 60.),
                        ("addr", 170.),
                        ("name", 120.),
                        ("user", 90.),
                        ("db", 40.),
                        ("age", 70.),
                        ("idle", 70.),
                        ("cmd", 130.),
                        ("flags", 60.),
                    ];
                    let mut table = v_flex()
                        .rounded_md()
                        .border_1()
                        .border_color(colors.border)
                        .child(
                            h_flex()
                                .px_3()
                                .py_1()
                                .gap_2()
                                .bg(colors.element_background)
                                .children(columns.iter().map(|(name, width)| {
                                    div()
                                        .w(px(*width))
                                        .flex_none()
                                        .child(Label::new(*name).size(LabelSize::Small))
                                }))
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Label::new("node").size(LabelSize::Small)),
                                ),
                        );
                    for client in &shown {
                        table = table.child(
                            h_flex()
                                .px_3()
                                .py_0p5()
                                .gap_2()
                                .border_t_1()
                                .border_color(colors.border_variant)
                                .children(columns.iter().map(|(name, width)| {
                                    div().w(px(*width)).flex_none().child(
                                        Label::new(client.get(name).to_string())
                                            .size(LabelSize::Small)
                                            .buffer_font(cx)
                                            .truncate(),
                                    )
                                }))
                                .child(
                                    div().flex_1().min_w_0().child(
                                        Label::new(client.node.clone())
                                            .size(LabelSize::Small)
                                            .color(Color::Muted)
                                            .truncate(),
                                    ),
                                ),
                        );
                    }
                    let sort = analyze.client_sort;
                    v_flex()
                        .gap_2()
                        .child(
                            h_flex()
                                .gap_2()
                                .when_some(analyze.client_filter.clone(), |this, filter| {
                                    this.child(div().w(px(360.)).child(boxed(filter, cx)))
                                })
                                .child(
                                    Label::new(format!(
                                        "{} of {} connections",
                                        shown.len(),
                                        report.items.len()
                                    ))
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                                )
                                .child(div().flex_1())
                                .child(
                                    Button::new(
                                        "database-clients-sort",
                                        match sort {
                                            ClientSort::Idle => "Sorted by idle",
                                            ClientSort::Age => "Sorted by age",
                                        },
                                    )
                                    .label_size(LabelSize::Small)
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            if let Some(analyze) = this.analyze() {
                                                analyze.client_sort = match analyze.client_sort {
                                                    ClientSort::Idle => ClientSort::Age,
                                                    ClientSort::Age => ClientSort::Idle,
                                                };
                                            }
                                            cx.notify();
                                        },
                                    )),
                                ),
                        )
                        .child(failures(&report.failures))
                        .child(table)
                        .into_any_element()
                }
            },
        };
        v_flex()
            .size_full()
            .p_3()
            .gap_3()
            .child(header)
            .child(
                v_flex()
                    .id("database-analyze-body")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .child(body),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_command_hides_arguments() {
        let args = vec![b"set".to_vec(), b"user:1".to_vec(), b"secret".to_vec()];
        assert_eq!(slow_command(&args, false), "SET ‹2 arguments›");
        assert_eq!(slow_command(&args, true), "SET \"user:1\" \"secret\"");
        assert_eq!(slow_command(&[b"ping".to_vec()], false), "PING");
        assert_eq!(slow_command(&[], false), "");
        let long = vec![b"set".to_vec(), vec![b'x'; 1000]];
        assert!(slow_command(&long, true).chars().count() <= 1008);
        let script = vec![b"eval".to_vec(), vec![b'y'; 5000]];
        assert!(slow_command(&script, true).ends_with('…'));
        assert_eq!(full_command(&script).len(), "EVAL ".len() + 5002);
    }

    #[test]
    fn test_slow_client() {
        let mut entry = SlowEntry {
            node: "server".into(),
            id: 1,
            time: 0,
            micros: 0,
            args: Vec::new(),
            client: String::new(),
            client_name: String::new(),
        };
        assert_eq!(slow_client(&entry), "server");
        entry.client = "127.0.0.1:1".into();
        assert_eq!(slow_client(&entry), "127.0.0.1:1 · server");
        entry.client_name = "worker".into();
        assert_eq!(slow_client(&entry), "127.0.0.1:1 (worker) · server");
    }

    #[test]
    fn test_filter_and_sort_clients() {
        let clients = vilo_database_core::redis::parse_client_list(
            "n",
            "id=1 name=worker idle=5 age=100 cmd=get\nid=2 name=api idle=50 age=10 cmd=subscribe\nid=3 name=worker-2 idle=1 age=500 cmd=get\n",
        );
        let ids = |list: Vec<ClientInfo>| {
            list.iter()
                .map(|client| client.get("id").to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(filter_clients(&clients, "", ClientSort::Idle)),
            ["2", "1", "3"]
        );
        assert_eq!(
            ids(filter_clients(&clients, "", ClientSort::Age)),
            ["3", "1", "2"]
        );
        assert_eq!(
            ids(filter_clients(&clients, "WORKER cmd=get", ClientSort::Age)),
            ["3", "1"]
        );
        assert!(filter_clients(&clients, "nothing", ClientSort::Idle).is_empty());
    }
}
