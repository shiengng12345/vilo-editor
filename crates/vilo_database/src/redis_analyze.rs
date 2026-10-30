//! The Redis Analyze tab: the slow log of every primary and the connections each one holds.
//! Resetting the slow log and changing its threshold are administration writes and go through
//! the same switch, confirmation and write log as key writes.

use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, Tooltip, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::{AdminWrite, ClientInfo, NodeReport, SlowEntry, quote_bytes},
};

use crate::{DatabaseView, Fetch, boxed, editor as single_line, mongo_write, run};

/// Slow log entries asked of each node unless the user picks another number.
const SLOWLOG_COUNT: u32 = 128;

#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum AnalyzeTab {
    #[default]
    SlowLog,
    Clients,
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
}

pub(crate) struct AdminWriteForm {
    write: Option<AdminWrite>,
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
    match text.char_indices().nth(240) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text,
    }
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
                            analyze.slowlog = Some(Fetch::Failed(format!("{error:#}").into()))
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
        analyze.tab = tab;
        let (load_slowlog, load_clients) = (analyze.slowlog.is_none(), analyze.clients.is_none());
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
        form.results.clear();
        let task = run(cx, async move {
            let mut results = Vec::new();
            // One node at a time, each logged before it is sent; a node whose outcome is unknown
            // stops the rest so nothing more is sent blind.
            for node in session.scan_nodes() {
                let operation = uuid::Uuid::new_v4().simple().to_string();
                mongo_write::audit(serde_json::json!({
                    "time": chrono::Utc::now().to_rfc3339(),
                    "operation": operation,
                    "connection": connection.id,
                    "name": connection.name,
                    "site": connection.site,
                    "environment": connection.environment,
                    "target": format!("node {}", node.label),
                    "kind": write.name(),
                    "phase": "intent",
                }))?;
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
                results.push((
                    node.label.clone(),
                    match (outcome, logged) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Ok(()), Err(error)) => {
                            Err(format!("done, but the write log has no result: {error:#}"))
                        }
                        (Err(error), _) => Err(error.to_string()),
                    },
                ));
                if unknown {
                    break;
                }
            }
            Ok(results)
        });
        form._task = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                // The per-node outcome is shown even if the page moved on; the slow log only
                // reloads for the same session.
                let same_session = this.generation == generation;
                let Some(form) = &mut this.admin_write else {
                    return;
                };
                form.sending = false;
                match result {
                    Ok(results) => {
                        let all_ok = results.iter().all(|(_, result)| result.is_ok());
                        form.results = results;
                        form.message = Some(
                            if all_ok {
                                "Done on every node."
                            } else {
                                "Not every node succeeded; see below."
                            }
                            .into(),
                        );
                        if all_ok && same_session {
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
        let done = !form.results.is_empty();
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
            .child(div().flex_1())
            .child(
                IconButton::new("database-analyze-refresh", IconName::RotateCw)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Refresh"))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        let tab = this.redis_state.as_ref().map(|state| state.analyze.tab);
                        match tab {
                            Some(AnalyzeTab::Clients) => this.load_clients(cx),
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
                                            .truncate(),
                                    ),
                                )
                                .child(
                                    div().w(px(220.)).child(
                                        Label::new(format!(
                                            "{}{} · {}",
                                            entry.client,
                                            if entry.client_name.is_empty() {
                                                String::new()
                                            } else {
                                                format!(" ({})", entry.client_name)
                                            },
                                            entry.node
                                        ))
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
        assert!(slow_command(&long, true).chars().count() <= 241);
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
