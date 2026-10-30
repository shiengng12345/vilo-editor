//! Running Workbench writes and command files. A write typed in the Workbench, or a file of
//! commands, opens one confirm step; then each command runs in order on the session it was
//! checked for. Writes are logged before sending and sent once, and the first refusal, error or
//! unknown outcome stops the rest.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use editor::Editor;
use gpui::{Entity, PathPromptOptions, Window, prelude::*, uniform_list};
use ui::{Button, ButtonStyle, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::{format_reply, quote_bytes},
    redis_workbench::{MAX_FILE_BYTES, Planned, parse_command_file},
};

use crate::{DatabaseView, boxed, editor as single_line, mongo_write, run};

pub(crate) struct WorkbenchRun {
    /// The session the commands were checked for; they never run on another one.
    generation: u64,
    source: SharedString,
    commands: Vec<(usize, String, Planned)>,
    typed: Entity<Editor>,
    stop: Arc<AtomicBool>,
    running: bool,
    started: bool,
    done: usize,
    outcome: Option<(SharedString, Color)>,
}

impl WorkbenchRun {
    pub(crate) fn closable(&self) -> bool {
        !self.running
    }

    fn writes(&self) -> usize {
        self.commands
            .iter()
            .filter(|(_, _, planned)| matches!(planned, Planned::Write(_)))
            .count()
    }
}

impl DatabaseView {
    pub(crate) fn open_workbench_run(
        &mut self,
        source: SharedString,
        commands: Vec<(usize, String, Planned)>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Checked for the session in place; a switch in progress would replace it.
        if let Some(state) = self.redis_state.as_mut()
            && state.switching_database()
        {
            state.workbench.push((
                format!("Run commands from {source}"),
                Err("A database switch is in progress; try again when it finishes.".into()),
            ));
            cx.notify();
            return;
        }
        let typed = single_line("Type the connection name", "", false, window, cx);
        self.workbench_run = Some(WorkbenchRun {
            generation: self.generation,
            source,
            commands,
            typed,
            stop: Arc::new(AtomicBool::new(false)),
            running: false,
            started: false,
            done: 0,
            outcome: None,
        });
        cx.notify();
    }

    /// Asks for a command file, checks every line, and opens the confirm step for it.
    pub(crate) fn pick_command_file(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let generation = self.generation;
        let paths = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Run".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = paths.await else {
                return;
            };
            let Some(path) = paths.into_iter().next() else {
                return;
            };
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string());
            let parsed = cx
                .background_spawn(async move {
                    use std::io::Read as _;
                    // Read with a cap, so a file that grows after it was picked is still bounded.
                    let mut bytes = Vec::new();
                    std::fs::File::open(&path)?
                        .take(MAX_FILE_BYTES + 1)
                        .read_to_end(&mut bytes)?;
                    anyhow::ensure!(
                        bytes.len() as u64 <= MAX_FILE_BYTES,
                        "The file is larger than {MAX_FILE_BYTES} bytes; Vilo runs files of at most that size."
                    );
                    let text = String::from_utf8(bytes)
                        .map_err(|error| anyhow::anyhow!("The file is not UTF-8 text: {error}"))?;
                    parse_command_file(&text)
                })
                .await;
            this.update_in(cx, |this, window, cx| {
                if this.generation != generation {
                    return;
                }
                match parsed {
                    Ok(commands) => {
                        this.open_workbench_run(format!("the file {name}").into(), commands, window, cx)
                    }
                    Err(error) => {
                        if let Some(state) = this.redis_state.as_mut() {
                            state.workbench.push((
                                format!("Run the file {name}"),
                                Err(format!("{error:#}")),
                            ));
                        }
                        cx.notify();
                    }
                }
            })
            .ok();
        })
        .detach();
    }

    fn start_workbench_run(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(connection) = self.active_connection().cloned() else {
            return;
        };
        let writable = self.writes_allowed(&connection);
        let database = session.database();
        let generation = self.generation;
        let Some(run_state) = &mut self.workbench_run else {
            return;
        };
        if run_state.running || run_state.started {
            return;
        }
        if run_state.generation != generation {
            run_state.outcome = Some((
                "The connection changed since these were checked. Close this and start again."
                    .into(),
                Color::Error,
            ));
            cx.notify();
            return;
        }
        let writes = run_state.writes();
        if writes > 0 && !writable {
            run_state.outcome = Some((
                "Switch writes on (top bar) to run writes.".into(),
                Color::Error,
            ));
            cx.notify();
            return;
        }
        if writes > 0
            && mongo_write::guarded(&connection)
            && run_state.typed.read(cx).text(cx).trim() != connection.name.trim()
        {
            run_state.outcome = Some((
                "Type the connection name exactly to confirm.".into(),
                Color::Error,
            ));
            cx.notify();
            return;
        }
        // Keys already waiting for a check refuse the whole batch, before its first command.
        let locked = run_state
            .commands
            .iter()
            .filter(|(_, _, planned)| match planned {
                Planned::Write(write) => write.keys.iter().any(|key| {
                    self.unknown_writes.contains(&crate::redis_write::lock_key(
                        &connection.id,
                        database,
                        key,
                    ))
                }),
                Planned::Read(_) => false,
            })
            .map(|(number, ..)| number.to_string())
            .collect::<Vec<_>>();
        let Some(run_state) = &mut self.workbench_run else {
            return;
        };
        if !locked.is_empty() {
            run_state.outcome = Some((
                format!(
                    "Nothing ran: line {} writes keys whose last write has an unknown outcome. Refresh those keys to check them first.",
                    locked.join(", ")
                )
                .into(),
                Color::Error,
            ));
            cx.notify();
            return;
        }
        run_state
            .typed
            .update(cx, |typed, cx| typed.set_text("", window, cx));
        run_state.running = true;
        run_state.started = true;
        run_state.outcome = None;
        let commands = run_state.commands.clone();
        let stop = run_state.stop.clone();
        let total = commands.len();
        cx.spawn(async move |this, cx| {
            let mut ended = None;
            for (index, (number, line, planned)) in commands.into_iter().enumerate() {
                // Checked before every command: the batch was confirmed for this session only.
                let same_session = this
                    .update(cx, |this, _| this.generation == generation)
                    .unwrap_or(false);
                if !same_session {
                    ended = Some((
                        format!("Stopped: the connection changed after {index} of {total} commands; the rest did not run."),
                        Color::Error,
                    ));
                    break;
                }
                if stop.load(Ordering::Relaxed) {
                    ended = Some((
                        format!("Stopped: {index} of {total} commands ran; the rest did not."),
                        Color::Warning,
                    ));
                    break;
                }
                let failed = match planned {
                    Planned::Read(words) => {
                        let task = cx.update(|cx| {
                            run(cx, {
                                let session = session.clone();
                                async move { Ok(format_reply(&session.run_read_command(words).await?)) }
                            })
                        });
                        let result = task.await.map_err(|error| format!("{error:#}"));
                        let failed = result.as_ref().err().cloned();
                        let alive = this
                            .update(cx, |this, cx| {
                                if let Some(state) = this.redis_state.as_mut() {
                                    state.workbench.push((line.clone(), result));
                                }
                                if let Some(run) = this.workbench_run.as_mut() {
                                    run.done = index + 1;
                                }
                                cx.notify();
                            })
                            .is_ok();
                        if !alive {
                            break;
                        }
                        failed.map(|error| format!("line {number} failed ({error})"))
                    }
                    Planned::Write(write) => {
                        // A key whose last write has an unknown outcome is checked before it is written again.
                        let locked = this
                            .update(cx, |this, _| {
                                write.keys.iter().any(|key| {
                                    this.unknown_writes.contains(&crate::redis_write::lock_key(
                                        &connection.id,
                                        database,
                                        key,
                                    ))
                                })
                            })
                            .unwrap_or(true);
                        if locked {
                            ended = Some((
                                format!("Line {number} writes a key whose last write has an unknown outcome. Refresh that key to check it first; the rest did not run."),
                                Color::Error,
                            ));
                            break;
                        }
                        let operation = uuid::Uuid::new_v4().simple().to_string();
                        let intent = serde_json::json!({
                            "time": chrono::Utc::now().to_rfc3339(),
                            "operation": operation,
                            "connection": connection.id,
                            "name": connection.name,
                            "site": connection.site,
                            "environment": connection.environment,
                            "db": database,
                            "key": write.keys.first().map(|key| quote_bytes(key)),
                            "keys": write.keys.iter().map(|key| quote_bytes(key)).collect::<Vec<_>>(),
                            "kind": format!("workbench {}", write.name),
                            "phase": "intent",
                        });
                        let summary = mongo_write::summarize_intent(&intent);
                        let task = cx.update(|cx| {
                            run(cx, {
                                let session = session.clone();
                                let operation = operation.clone();
                                let write = write.clone();
                                async move {
                                    if let Err(error) = mongo_write::audit(intent) {
                                        return anyhow::Ok(Err(format!("{error:#}")));
                                    }
                                    let outcome = session.run_write(&write).await;
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
                            })
                        });
                        let result = match task.await {
                            Ok(Ok(done)) => Ok(done),
                            // The intent could not be logged, so nothing was sent.
                            Ok(Err(not_logged)) => Err(not_logged),
                            // The task itself failed, possibly after sending.
                            Err(error) => Ok((Err(WriteError::Unknown(format!("{error:#}"))), None)),
                        };
                        let alive = this
                            .update(cx, |this, cx| {
                                let shown = match &result {
                                    Ok((Ok(reply), _)) => Ok(format_reply(reply)),
                                    Ok((Err(WriteError::Refused(message)), _)) => {
                                        Err(format!("Not written: {message}"))
                                    }
                                    Ok((Err(WriteError::Unknown(message)), _)) => Err(format!(
                                        "The outcome is unknown ({message}); refresh its keys to check before writing them again."
                                    )),
                                    Err(not_logged) => {
                                        Err(format!("Not sent: the write log could not record it ({not_logged})"))
                                    }
                                };
                                if let Some(state) = this.redis_state.as_mut() {
                                    state.workbench.push((line.clone(), shown));
                                }
                                if let Ok((_, Some(error))) = &result {
                                    this.unlogged_writes.push((
                                        operation.clone(),
                                        format!("{summary} · its result is not in the log: {error}"),
                                    ));
                                }
                                if let Ok((Err(WriteError::Unknown(message)), _)) = &result {
                                    // The connection it was sent on, whatever is active now.
                                    for key in &write.keys {
                                        this.unknown_writes.insert(crate::redis_write::lock_key(
                                            &connection.id,
                                            database,
                                            key,
                                        ));
                                    }
                                    this.write_notice = Some(
                                        format!("The outcome of {summary} is unknown ({message}). Refresh its keys to check them before writing again.")
                                            .into(),
                                    );
                                }
                                if let Some(run) = this.workbench_run.as_mut() {
                                    run.done = index + 1;
                                }
                                cx.notify();
                            })
                            .is_ok();
                        let failed = match &result {
                            Ok((Ok(_), None)) => None,
                            Ok((Ok(_), Some(error))) => {
                                Some(format!("line {number} ran but its result could not be logged ({error})"))
                            }
                            Ok((Err(WriteError::Refused(message)), _)) => {
                                Some(format!("line {number} was refused ({message})"))
                            }
                            Ok((Err(WriteError::Unknown(message)), _)) => {
                                Some(format!("the outcome of line {number} is unknown ({message})"))
                            }
                            Err(not_logged) => Some(format!(
                                "line {number} was not sent: the write log could not record it ({not_logged})"
                            )),
                        };
                        if !alive {
                            break;
                        }
                        failed
                    }
                };
                if let Some(failed) = failed {
                    ended = Some((
                        format!("Stopped: {failed}; the rest did not run."),
                        Color::Error,
                    ));
                    break;
                }
            }
            this.update(cx, |this, cx| {
                if let Some(run) = this.workbench_run.as_mut() {
                    run.running = false;
                    run.outcome = Some(ended.map(|(text, color)| (text.into(), color)).unwrap_or_else(|| {
                        (format!("Done: all {total} commands ran.").into(), Color::Success)
                    }));
                    if run.writes() > 0 {
                        this.scan(true, cx);
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn render_workbench_run(
        &self,
        run_state: &WorkbenchRun,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = mongo_write::guarded(connection);
        let writes = run_state.writes();
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if this
                .workbench_run
                .as_ref()
                .is_none_or(WorkbenchRun::closable)
            {
                this.workbench_run = None;
                cx.notify();
            }
        };
        let list = uniform_list(
            "database-workbench-run-lines",
            run_state.commands.len(),
            cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                let Some(run) = this.workbench_run.as_ref() else {
                    return Vec::new();
                };
                range
                    .filter_map(|index| run.commands.get(index).map(|command| (index, command)))
                    .map(|(index, (number, line, planned))| {
                        let state = if index < run.done { "✓ " } else { "  " };
                        Label::new(format!(
                            "{state}{number:>5}  {}{line}",
                            if matches!(planned, Planned::Write(_)) {
                                "write  "
                            } else {
                                "read   "
                            }
                        ))
                        .size(LabelSize::Small)
                        .buffer_font(cx)
                        .single_line()
                        .truncate()
                        .into_any_element()
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .h(px(180.));
        v_flex()
            .id("database-workbench-run")
            .gap_3()
            .w(px(620.))
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(
                Headline::new(format!(
                    "Run {} command{} from {}",
                    run_state.commands.len(),
                    if run_state.commands.len() == 1 { "" } else { "s" },
                    run_state.source
                ))
                .size(HeadlineSize::Small),
            )
            .child(
                Label::new(format!(
                    "{} · {} · {} · db{} · {writes} write{}",
                    connection.site,
                    connection.environment,
                    connection.name,
                    self.redis().map_or(0, |session| session.database()),
                    if writes == 1 { "" } else { "s" }
                ))
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(if production && writes > 0 { Color::Error } else { Color::Muted }),
            )
            .child(
                Label::new(
                    "In order, each once. Writes are logged before they are sent; the first refusal, error or unknown outcome stops the rest.",
                )
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .child(boxed(list, cx))
            .when(production && writes > 0 && !run_state.started, |this| {
                this.child(
                    Label::new(format!(
                        "This is a production connection. Type “{}” to confirm.",
                        connection.name
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Error),
                )
                .child(boxed(run_state.typed.clone(), cx))
            })
            .when(run_state.started, |this| {
                this.child(
                    Label::new(format!(
                        "{} of {} ran",
                        run_state.done,
                        run_state.commands.len()
                    ))
                    .size(LabelSize::Small),
                )
            })
            .when_some(run_state.outcome.clone(), |this, (outcome, color)| {
                this.child(Label::new(outcome).size(LabelSize::Small).color(color))
            })
            .child(
                h_flex()
                    .gap_2()
                    .when(!run_state.started, |this| {
                        this.child(
                            Button::new("database-workbench-run-start", if writes > 0 { "Run" } else { "Run reads" })
                                .style(if writes > 0 {
                                    ButtonStyle::Tinted(ui::TintColor::Warning)
                                } else {
                                    ButtonStyle::Filled
                                })
                                .on_click(cx.listener(|this, _, window, cx| this.start_workbench_run(window, cx))),
                        )
                    })
                    .when(run_state.running, |this| {
                        this.child(
                            Button::new("database-workbench-run-stop", "Stop after this command")
                                .style(ButtonStyle::Outlined)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    if let Some(run) = this.workbench_run.as_ref() {
                                        run.stop.store(true, Ordering::Relaxed);
                                    }
                                    cx.notify();
                                })),
                        )
                    })
                    .child(
                        Button::new(
                            "database-workbench-run-close",
                            if run_state.started { "Close" } else { "Cancel" },
                        )
                        .disabled(!run_state.closable())
                        .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }
}
