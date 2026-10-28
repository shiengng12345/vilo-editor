use std::{io::Write as _, sync::Arc, time::Duration};

use editor::Editor;
use gpui::{Entity, Subscription, Task, Window, prelude::*};
use ui::{Button, ButtonStyle, Checkbox, ContextMenu, PopoverMenu, ToggleState, prelude::*};

use super::{Connection, Fetch, JenkinsView, editor};
use crate::jenkins::{
    CascadeSpec, ChoiceOption, FormSession, ParameterDefinition, ParameterKind, QueueState, WriteAction,
    WriteError, WriteOutcome,
};

/// How long Vilo follows a queued build before leaving it to the builds list.
const QUEUE_FOLLOW: Duration = Duration::from_secs(10 * 60);
const QUEUE_POLL: Duration = Duration::from_secs(1);

enum FieldValue {
    Text(Entity<Editor>),
    Choice(String),
    Flag(bool),
    /// An Active Choices cascade, recomputed by Jenkins from the fields it references.
    Cascade(Cascade),
}

#[derive(Clone, PartialEq)]
enum CascadeState {
    Loading,
    Ready,
    Failed(String),
}

struct Cascade {
    spec: CascadeSpec,
    options: Vec<ChoiceOption>,
    selected: Vec<String>,
    state: CascadeState,
    /// Values to restore when they are offered (from the build being rebuilt).
    wanted: Option<Vec<String>>,
    /// Restored values that are no longer offered.
    note: Option<String>,
}

struct Field {
    definition: ParameterDefinition,
    value: FieldValue,
}

enum Status {
    Editing(Option<SharedString>),
    Submitting,
    /// Queued on Jenkins; `why` is Jenkins' own reason for waiting.
    Queued { queue_id: u64, why: Option<String> },
    /// Done or definitively failed; the screen only reports it.
    Finished(SharedString, Color),
    /// The request may have reached Jenkins. Submitting again is blocked.
    Unknown(SharedString),
}

enum Kind {
    Build {
        definitions: Fetch<Vec<ParameterDefinition>>,
        fields: Vec<Field>,
        /// The build whose parameters were copied, and those parameters.
        from: Option<(u64, Vec<(String, String)>)>,
    },
    Stop { number: u64 },
}

pub(super) struct ActionState {
    job: Vec<String>,
    kind: Kind,
    /// The build page session that the cascades' choices are computed in.
    form: Option<Arc<FormSession>>,
    /// Bumped per cascade run; a run that is no longer the latest stops applying.
    cascade_revision: u64,
    cascade_rerun: bool,
    cascade_task: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,
    review: bool,
    confirm: Entity<Editor>,
    status: Status,
    _task: Option<Task<()>>,
}

/// Values that point at production even when the connection is not protected.
fn mentions_production(text: &str) -> bool {
    let text = text.to_lowercase();
    text.contains("prod") || text.contains("live")
}

fn append_audit(line: serde_json::Value, cx: &mut gpui::App) {
    let line = line.to_string();
    cx.background_spawn(async move {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths::config_dir().join("jenkins_audit.log"))?;
        writeln!(file, "{line}")
    })
    .detach_and_log_err(cx);
}

/// Cascades sorted so that each comes after the cascades it reads.
fn cascade_order(fields: &[Field]) -> Result<Vec<String>, String> {
    let cascades = fields
        .iter()
        .filter_map(|field| match &field.value {
            FieldValue::Cascade(cascade) => Some(&cascade.spec),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut order: Vec<String> = Vec::new();
    while order.len() < cascades.len() {
        let next = cascades.iter().find(|spec| {
            !order.contains(&spec.name)
                && spec
                    .referenced
                    .iter()
                    .all(|referenced| order.contains(referenced) || !cascades.iter().any(|other| &other.name == referenced))
        });
        match next {
            Some(spec) => order.push(spec.name.clone()),
            None => return Err("these parameters depend on each other in a loop".to_string()),
        }
    }
    Ok(order)
}

/// The text a referenced field contributes, as the plugin's page would send it.
fn field_text(fields: &[Field], name: &str, cx: &App) -> Option<String> {
    let field = fields.iter().find(|field| field.definition.name == name)?;
    match &field.value {
        FieldValue::Choice(choice) => Some(choice.clone()),
        FieldValue::Text(editor) if field.definition.kind != ParameterKind::Password => Some(editor.read(cx).text(cx)),
        FieldValue::Cascade(cascade) if cascade.state == CascadeState::Ready => Some(cascade.selected.join(",")),
        // Booleans, passwords and unfinished cascades are not something Vilo can stand in for.
        _ => None,
    }
}

/// Keeps what the user (or the rebuilt build) chose when Jenkins still offers it.
fn apply_choices(cascade: &mut Cascade, options: Vec<ChoiceOption>) {
    let offered = options.iter().filter(|option| !option.disabled).map(|option| option.key.clone()).collect::<Vec<_>>();
    let preselected = options.iter().filter(|option| option.selected && !option.disabled).map(|option| option.key.clone());
    let (selected, note) = match cascade.wanted.take() {
        Some(wanted) => {
            let missing = wanted.iter().filter(|value| !offered.contains(value)).cloned().collect::<Vec<_>>();
            let kept = wanted.into_iter().filter(|value| offered.contains(value)).collect::<Vec<_>>();
            (kept, (!missing.is_empty()).then(|| format!("No longer offered: {}", missing.join(", "))))
        }
        None if !cascade.selected.is_empty() => {
            let kept = cascade.selected.iter().filter(|value| offered.contains(value)).cloned().collect::<Vec<_>>();
            (kept, cascade.note.take())
        }
        None => (preselected.collect(), None),
    };
    cascade.selected = if cascade.spec.multiple { selected } else { selected.into_iter().take(1).collect() };
    cascade.note = note;
    cascade.options = options;
    cascade.state = CascadeState::Ready;
}

impl JenkinsView {
    fn connection(&self) -> Option<Connection> {
        let id = &self.session.as_ref()?.connection_id;
        self.connections.iter().find(|c| &c.id == id).cloned()
    }

    pub(super) fn start_build_form(
        &mut self,
        job: Vec<String>,
        from: Option<(u64, Vec<(String, String)>)>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let generation = self.generation;
        let definitions_job = job.clone();
        let task = cx.spawn_in(window, async move |this, cx| {
            let result = client.parameter_definitions(&definitions_job).await;
            let needs_form = result.as_ref().is_ok_and(|list| {
                list.iter()
                    .any(|definition| matches!(&definition.kind, ParameterKind::Unsupported(kind) if kind == "CascadeChoiceParameter"))
            });
            let form = if needs_form { Some(client.open_form(&definitions_job).await) } else { None };
            this.update_in(cx, |this, window, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(action) = &mut this.action else {
                    return;
                };
                let form = match form {
                    Some(Ok(form)) => Some(Arc::new(form)),
                    Some(Err(error)) => {
                        action.status = Status::Editing(Some(format!("Could not load the job's build page: {error}").into()));
                        None
                    }
                    None => None,
                };
                let Kind::Build { definitions, fields, from } = &mut action.kind else {
                    return;
                };
                let mut subscriptions = Vec::new();
                match result {
                    Ok(list) => {
                        let previous = from.as_ref().map(|(_, parameters)| parameters.clone()).unwrap_or_default();
                        let referenced = form
                            .as_ref()
                            .map(|form| form.cascades.iter().flat_map(|cascade| cascade.referenced.clone()).collect::<Vec<_>>())
                            .unwrap_or_default();
                        *fields = list
                            .iter()
                            .map(|definition| {
                                // Passwords are never copied from an earlier build.
                                let copied = (definition.kind != ParameterKind::Password)
                                    .then(|| previous.iter().find(|(name, _)| *name == definition.name).map(|(_, value)| value.clone()))
                                    .flatten();
                                let spec = form.as_ref().and_then(|form| {
                                    form.cascades.iter().find(|cascade| cascade.name == definition.name).cloned()
                                });
                                let initial = copied.clone().or_else(|| definition.default.clone()).unwrap_or_default();
                                let value = match (&definition.kind, spec) {
                                    (ParameterKind::Unsupported(kind), Some(spec)) if kind == "CascadeChoiceParameter" => {
                                        FieldValue::Cascade(Cascade {
                                            spec,
                                            options: Vec::new(),
                                            selected: Vec::new(),
                                            state: CascadeState::Loading,
                                            wanted: copied.map(|value| {
                                                value.split(',').map(str::to_string).filter(|value| !value.is_empty()).collect()
                                            }),
                                            note: None,
                                        })
                                    }
                                    (ParameterKind::Choice(_), _) => FieldValue::Choice(initial),
                                    (ParameterKind::Boolean, _) => FieldValue::Flag(initial == "true"),
                                    (ParameterKind::Password, _) => FieldValue::Text(editor("value", "", true, window, cx)),
                                    _ => {
                                        let input = editor("value", &initial, false, window, cx);
                                        // Typing in a field that cascades read recomputes them.
                                        if referenced.contains(&definition.name) {
                                            subscriptions.push(cx.subscribe(&input, |this, _, event: &editor::EditorEvent, cx| {
                                                if matches!(event, editor::EditorEvent::BufferEdited) {
                                                    this.run_cascades(cx);
                                                }
                                            }));
                                        }
                                        FieldValue::Text(input)
                                    }
                                };
                                Field { definition: definition.clone(), value }
                            })
                            .collect();
                        *definitions = Fetch::Loaded(list);
                    }
                    Err(error) => *definitions = Fetch::Failed(error.to_string().into()),
                }
                action.form = form;
                action._subscriptions = subscriptions;
                this.run_cascades(cx);
                cx.notify();
            })
            .ok();
        });
        self.action = Some(ActionState {
            job,
            kind: Kind::Build { definitions: Fetch::Loading, fields: Vec::new(), from },
            form: None,
            cascade_revision: 0,
            cascade_rerun: false,
            cascade_task: None,
            _subscriptions: Vec::new(),
            review: false,
            confirm: editor("Type the job name", "", false, window, cx),
            status: Status::Editing(None),
            _task: Some(task),
        });
        cx.notify();
    }

    pub(super) fn start_stop(&mut self, job: Vec<String>, number: u64, window: &mut Window, cx: &mut Context<Self>) {
        self.action = Some(ActionState {
            job,
            kind: Kind::Stop { number },
            form: None,
            cascade_revision: 0,
            cascade_rerun: false,
            cascade_task: None,
            _subscriptions: Vec::new(),
            review: true,
            confirm: editor("Type the job name", "", false, window, cx),
            status: Status::Editing(None),
            _task: None,
        });
        cx.notify();
    }

    /// The values to send, or why they cannot be sent yet.
    fn collected(&self, cx: &App) -> Result<Vec<(String, String)>, String> {
        let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &self.action else {
            return Ok(Vec::new());
        };
        let mut values = Vec::new();
        for field in fields {
            let value = match (&field.value, &field.definition.kind) {
                (FieldValue::Choice(choice), ParameterKind::Choice(choices)) => {
                    if !choices.contains(choice) {
                        return Err(format!("{}: “{choice}” is no longer one of its choices. Pick one.", field.definition.name));
                    }
                    choice.clone()
                }
                (FieldValue::Flag(flag), _) => flag.to_string(),
                (FieldValue::Text(editor), _) => editor.read(cx).text(cx),
                (FieldValue::Choice(choice), _) => choice.clone(),
                (FieldValue::Cascade(cascade), _) => match &cascade.state {
                    CascadeState::Ready => cascade.selected.join(","),
                    CascadeState::Loading => return Err(format!("{}: choices are still loading.", field.definition.name)),
                    CascadeState::Failed(reason) => return Err(format!("{}: {reason}", field.definition.name)),
                },
            };
            values.push((field.definition.name.clone(), value));
        }
        Ok(values)
    }

    /// Recomputes every cascade in dependency order, one Jenkins call pair at a
    /// time. A change while a run is going marks the run stale and queues one more.
    pub(super) fn run_cascades(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let generation = self.generation;
        let Some(action) = &mut self.action else {
            return;
        };
        let Some(form) = action.form.clone() else {
            return;
        };
        let Kind::Build { fields, .. } = &mut action.kind else {
            return;
        };
        let order = match cascade_order(fields) {
            Ok(order) if !order.is_empty() => order,
            Ok(_) => return,
            Err(reason) => {
                for field in fields.iter_mut() {
                    if let FieldValue::Cascade(cascade) = &mut field.value {
                        cascade.state = CascadeState::Failed(reason.clone());
                    }
                }
                cx.notify();
                return;
            }
        };
        // Anything shown before is stale until recomputed.
        for field in fields.iter_mut() {
            if let FieldValue::Cascade(cascade) = &mut field.value {
                cascade.state = CascadeState::Loading;
            }
        }
        action.review = false;
        if action.cascade_task.is_some() {
            action.cascade_rerun = true;
            cx.notify();
            return;
        }
        action.cascade_revision += 1;
        let revision = action.cascade_revision;
        action.cascade_task = Some(cx.spawn(async move |this, cx| {
            let is_current = |this: &JenkinsView| {
                this.generation == generation && this.action.as_ref().is_some_and(|action| action.cascade_revision == revision)
            };
            let mut failed = None::<String>;
            for name in order {
                let request = this.read_with(cx, |this, cx| {
                    let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &this.action else {
                        return None;
                    };
                    let spec = fields.iter().find_map(|field| match &field.value {
                        FieldValue::Cascade(cascade) if cascade.spec.name == name => Some(cascade.spec.clone()),
                        _ => None,
                    })?;
                    let values = spec
                        .referenced
                        .iter()
                        .map(|referenced| (referenced.clone(), field_text(fields, referenced, cx)))
                        .collect::<Vec<_>>();
                    Some((spec, values))
                });
                let Ok(Some((spec, values))) = request else {
                    return;
                };
                let result = match &failed {
                    Some(parent) => Err(format!("waits for {parent}")),
                    None if values.iter().any(|(_, value)| value.is_none()) => {
                        Err("references a parameter Vilo cannot read".to_string())
                    }
                    None => {
                        let values = values.into_iter().map(|(name, value)| (name, value.unwrap_or_default())).collect::<Vec<_>>();
                        client.cascade_choices(&form, &spec, &values).await.map_err(|error| error.to_string())
                    }
                };
                if result.is_err() && failed.is_none() {
                    failed = Some(name.clone());
                }
                let applied = this.update(cx, |this, cx| {
                    if !is_current(this) {
                        return false;
                    }
                    if let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &mut this.action
                        && let Some(cascade) = fields.iter_mut().find_map(|field| match &mut field.value {
                            FieldValue::Cascade(cascade) if cascade.spec.name == name => Some(cascade),
                            _ => None,
                        })
                    {
                        match result {
                            Ok(options) => apply_choices(cascade, options),
                            Err(reason) => cascade.state = CascadeState::Failed(reason),
                        }
                    }
                    cx.notify();
                    true
                });
                if !matches!(applied, Ok(true)) {
                    return;
                }
            }
            this.update(cx, |this, cx| {
                let Some(action) = this.action.as_mut().filter(|action| action.cascade_revision == revision) else {
                    return;
                };
                action.cascade_task = None;
                if std::mem::take(&mut action.cascade_rerun) {
                    this.run_cascades(cx);
                }
            })
            .ok();
        }));
        cx.notify();
    }

    /// A field changed: recompute cascades only when one of them reads it.
    fn field_changed(&mut self, name: &str, cx: &mut Context<Self>) {
        let read = matches!(&self.action, Some(ActionState { kind: Kind::Build { fields, .. }, .. })
            if fields.iter().any(|field| matches!(&field.value, FieldValue::Cascade(cascade) if cascade.spec.referenced.iter().any(|referenced| referenced == name))));
        if read {
            self.run_cascades(cx);
        }
        cx.notify();
    }

    fn needs_typed_confirmation(&self, connection: &Connection, values: &[(String, String)]) -> bool {
        let Some(action) = &self.action else {
            return true;
        };
        connection.protected
            || action.job.iter().any(|segment| mentions_production(segment))
            || values.iter().any(|(name, value)| {
                let name = name.to_lowercase();
                (name.contains("env") || name.contains("target") || name.contains("namespace")) && mentions_production(value)
            })
    }

    fn submit(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.connection() else {
            return;
        };
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let values = match self.collected(cx) {
            Ok(values) => values,
            Err(message) => {
                if let Some(action) = &mut self.action {
                    action.status = Status::Editing(Some(message.into()));
                }
                cx.notify();
                return;
            }
        };
        let typed_confirmation = self.needs_typed_confirmation(&connection, &values);
        let Some(action) = &mut self.action else {
            return;
        };
        if matches!(action.status, Status::Submitting | Status::Queued { .. } | Status::Unknown(_)) {
            return;
        }
        let job_name = action.job.last().cloned().unwrap_or_default();
        if typed_confirmation && action.confirm.read(cx).text(cx).trim() != job_name {
            action.status = Status::Editing(Some(format!("Type “{job_name}” to confirm.").into()));
            cx.notify();
            return;
        }
        let job = action.job.clone();
        let stop = match &action.kind {
            Kind::Stop { number } => Some(*number),
            Kind::Build { .. } => None,
        };
        let has_parameters = matches!(&action.kind, Kind::Build { fields, .. } if !fields.is_empty());
        action.status = Status::Submitting;
        let generation = self.generation;
        let audit_names = values.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
        let task = cx.spawn(async move |this, cx| {
            let outcome = match stop {
                Some(number) => client.write(WriteAction::Stop { job: &job, number }).await,
                None => {
                    client
                        .write(WriteAction::Build {
                            job: &job,
                            parameters: has_parameters.then_some(values.as_slice()),
                        })
                        .await
                }
            };
            this.update(cx, |_, cx| {
                append_audit(
                    serde_json::json!({
                        "time": time::OffsetDateTime::now_utc().unix_timestamp(),
                        "connection": connection.name,
                        "address": connection.address,
                        "user": connection.username,
                        "job": job.join("/"),
                        "action": match stop { Some(number) => format!("stop #{number}"), None => "build".to_string() },
                        // Names only: values can carry secrets.
                        "parameters": audit_names,
                        "outcome": match &outcome {
                            Ok(WriteOutcome::Queued { queue_id, reused }) => format!("queued {queue_id}{}", if *reused { " (existing)" } else { "" }),
                            Ok(WriteOutcome::StopRequested) => "stop requested".to_string(),
                            Err(WriteError::Refused(_)) => "refused".to_string(),
                            Err(WriteError::OutcomeUnknown(_)) => "unknown".to_string(),
                        },
                    }),
                    cx,
                );
            })
            .ok();
            let queue_id = match outcome {
                Ok(WriteOutcome::StopRequested) => {
                    this.update(cx, |this, cx| {
                        if let Some(action) = this.action.as_mut().filter(|_| this.generation == generation) {
                            action.status = Status::Finished(
                                "Stop requested. The build ends when Jenkins gets to it; its log shows when.".into(),
                                Color::Success,
                            );
                        }
                        cx.notify();
                    })
                    .ok();
                    return;
                }
                Ok(WriteOutcome::Queued { queue_id, .. }) => queue_id,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        if let Some(action) = this.action.as_mut().filter(|_| this.generation == generation) {
                            action.status = match error {
                                WriteError::Refused(_) => Status::Editing(Some(error.to_string().into())),
                                WriteError::OutcomeUnknown(_) => Status::Unknown(error.to_string().into()),
                            };
                        }
                        cx.notify();
                    })
                    .ok();
                    return;
                }
            };
            // Follow the queue item until it becomes a build. Stopping here never cancels it.
            let started = std::time::Instant::now();
            loop {
                let state = client.queue_item(queue_id).await;
                let resolved = match state {
                    Ok(QueueState::Started(number)) => Some(Ok(number)),
                    Ok(QueueState::Cancelled) => Some(Err("The build was cancelled while queued.".to_string())),
                    Ok(QueueState::Gone) => Some(match client.build_for_queue_item(&job, queue_id).await {
                        Ok(Some(number)) => Ok(number),
                        _ => Err("Jenkins no longer tracks this queue item; find the build in the list.".to_string()),
                    }),
                    Ok(QueueState::Waiting(why)) => {
                        this.update(cx, |this, cx| {
                            if let Some(action) = this.action.as_mut().filter(|_| this.generation == generation) {
                                action.status = Status::Queued { queue_id, why };
                            }
                            cx.notify();
                        })
                        .ok();
                        None
                    }
                    Err(_) => None,
                };
                if let Some(resolved) = resolved {
                    this.update(cx, |this, cx| {
                        if this.generation != generation {
                            return;
                        }
                        match resolved {
                            Ok(number) => {
                                this.action = None;
                                this.open_build(job.clone(), number, cx);
                            }
                            Err(message) => {
                                if let Some(action) = &mut this.action {
                                    action.status = Status::Finished(message.into(), Color::Warning);
                                }
                            }
                        }
                        cx.notify();
                    })
                    .ok();
                    return;
                }
                if started.elapsed() > QUEUE_FOLLOW {
                    this.update(cx, |this, cx| {
                        if let Some(action) = this.action.as_mut().filter(|_| this.generation == generation) {
                            action.status = Status::Finished(
                                "Still queued after 10 minutes. It stays queued on Jenkins; check the builds list later.".into(),
                                Color::Warning,
                            );
                        }
                        cx.notify();
                    })
                    .ok();
                    return;
                }
                cx.background_executor().timer(QUEUE_POLL).await;
            }
        });
        if let Some(action) = &mut self.action {
            action._task = Some(task);
        }
        cx.notify();
    }

    fn select_cascade(&mut self, index: usize, key: String, cx: &mut Context<Self>) {
        let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &mut self.action else {
            return;
        };
        let Some(field) = fields.get_mut(index) else {
            return;
        };
        let FieldValue::Cascade(cascade) = &mut field.value else {
            return;
        };
        if cascade.state != CascadeState::Ready {
            return;
        }
        if cascade.spec.multiple {
            if let Some(position) = cascade.selected.iter().position(|selected| *selected == key) {
                cascade.selected.remove(position);
            } else {
                cascade.selected.push(key);
            }
        } else {
            cascade.selected = vec![key];
        }
        cascade.note = None;
        let name = field.definition.name.clone();
        self.field_changed(&name, cx);
    }

    fn render_cascade(&self, index: usize, cascade: &Cascade, cx: &mut Context<Self>) -> AnyElement {
        match &cascade.state {
            CascadeState::Loading => {
                return Label::new("Loading choices from Jenkins…").size(LabelSize::Small).color(Color::Muted).into_any_element();
            }
            CascadeState::Failed(reason) => {
                return h_flex()
                    .gap_2()
                    .child(Label::new(format!("Could not load choices: {reason}")).size(LabelSize::Small).color(Color::Error))
                    .child(Button::new(SharedString::from(format!("jenkins-cascade-retry-{index}")), "Reload form").label_size(LabelSize::Small).on_click(
                        cx.listener(|this, _, window, cx| {
                            // A fresh page and session; earlier review state is dropped with it.
                            if let Some(action) = &this.action
                                && let Kind::Build { from, .. } = &action.kind
                            {
                                let (job, from) = (action.job.clone(), from.clone());
                                this.start_build_form(job, from, window, cx);
                            }
                        }),
                    ))
                    .into_any_element();
            }
            CascadeState::Ready => {}
        }
        let note = cascade.note.clone();
        let content = if cascade.spec.multiple {
            let mut list = v_flex().gap_0p5();
            for option in &cascade.options {
                let key = option.key.clone();
                list = list.child(
                    Checkbox::new(
                        SharedString::from(format!("jenkins-cascade-{index}-{}", option.key)),
                        ToggleState::from(cascade.selected.contains(&option.key)),
                    )
                    .label(option.label.clone())
                    .label_size(LabelSize::Small)
                    .disabled(option.disabled)
                    .on_click(cx.listener(move |this, _, _, cx| this.select_cascade(index, key.clone(), cx))),
                );
            }
            if cascade.options.is_empty() {
                list = list.child(Label::new("No choices offered.").size(LabelSize::Small).color(Color::Muted));
            }
            list.into_any_element()
        } else {
            let options = cascade
                .options
                .iter()
                .filter(|option| !option.disabled)
                .map(|option| (option.key.clone(), option.label.clone()))
                .collect::<Vec<_>>();
            let current = cascade
                .selected
                .first()
                .and_then(|key| cascade.options.iter().find(|option| &option.key == key))
                .map_or("Choose…".to_string(), |option| option.label.clone());
            let view = cx.entity().downgrade();
            PopoverMenu::new(SharedString::from(format!("jenkins-cascade-menu-{index}")))
                .trigger(
                    Button::new(SharedString::from(format!("jenkins-cascade-button-{index}")), current)
                        .label_size(LabelSize::Small)
                        .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                )
                .menu(move |window, cx| {
                    let options = options.clone();
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        for (key, label) in &options {
                            let view = view.clone();
                            let key = key.clone();
                            menu = menu.entry(label.clone(), None, move |_, cx| {
                                view.update(cx, |this, cx| this.select_cascade(index, key.clone(), cx)).ok();
                            });
                        }
                        menu
                    }))
                })
                .into_any_element()
        };
        v_flex()
            .gap_0p5()
            .child(content)
            .when_some(note, |this, note| this.child(Label::new(note).size(LabelSize::XSmall).color(Color::Warning)))
            .into_any_element()
    }

    pub(super) fn render_action(&self, action: &ActionState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let connection = self.connection();
        let job_name = action.job.last().cloned().unwrap_or_default();
        let boxed = |child: AnyElement| {
            div()
                .px_2()
                .py_1()
                .rounded_sm()
                .border_1()
                .border_color(colors.border)
                .bg(colors.editor_background)
                .child(child)
        };
        let title = match &action.kind {
            Kind::Stop { number } => format!("Stop #{number} of {job_name}"),
            Kind::Build { from: Some((number, _)), .. } => format!("Rebuild {job_name} from #{number}"),
            Kind::Build { from: None, .. } => format!("Build {job_name}"),
        };

        let mut body = v_flex().gap_2();
        let mut unsupported = Vec::new();
        if let Kind::Build { definitions, fields, from } = &action.kind {
            match definitions {
                Fetch::Loading => body = body.child(Label::new("Loading parameters…").color(Color::Muted)),
                Fetch::Failed(message) => body = body.child(Label::new(message.clone()).color(Color::Warning)),
                Fetch::Loaded(_) if fields.is_empty() => {
                    body = body.child(Label::new("This job has no parameters.").color(Color::Muted))
                }
                Fetch::Loaded(_) => {
                    unsupported = fields
                        .iter()
                        .filter_map(|field| match (&field.definition.kind, &field.value) {
                            (ParameterKind::Unsupported(_), FieldValue::Cascade(_)) => None,
                            (ParameterKind::Unsupported(kind), _) => Some(format!("{} ({kind})", field.definition.name)),
                            _ => None,
                        })
                        .collect();
                    let dropped = from
                        .as_ref()
                        .map(|(_, previous)| {
                            previous
                                .iter()
                                .filter(|(name, _)| !fields.iter().any(|field| &field.definition.name == name))
                                .map(|(name, _)| name.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    if !dropped.is_empty() {
                        body = body.child(
                            Label::new(format!("No longer parameters of this job, not sent: {}", dropped.join(", ")))
                                .size(LabelSize::Small)
                                .color(Color::Warning),
                        );
                    }
                    for (index, field) in fields.iter().enumerate() {
                        let input: AnyElement = match (&field.value, &field.definition.kind) {
                            (FieldValue::Choice(current), ParameterKind::Choice(choices)) => {
                                let choices = choices.clone();
                                let valid = choices.contains(current);
                                let view = cx.entity().downgrade();
                                PopoverMenu::new(SharedString::from(format!("jenkins-choice-{index}")))
                                    .trigger(
                                        Button::new(
                                            SharedString::from(format!("jenkins-choice-button-{index}")),
                                            if current.is_empty() { "Choose…".to_string() } else { current.clone() },
                                        )
                                        .label_size(LabelSize::Small)
                                        .color(if valid { Color::Default } else { Color::Error })
                                        .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                                    )
                                    .menu(move |window, cx| {
                                        let choices = choices.clone();
                                        let view = view.clone();
                                        Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                            for choice in &choices {
                                                let view = view.clone();
                                                let choice = choice.clone();
                                                menu = menu.entry(choice.clone(), None, move |_, cx| {
                                                    view.update(cx, |this, cx| {
                                                        if let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &mut this.action
                                                            && let Some(field) = fields.get_mut(index)
                                                        {
                                                            field.value = FieldValue::Choice(choice.clone());
                                                            let name = field.definition.name.clone();
                                                            this.field_changed(&name, cx);
                                                        }
                                                        cx.notify();
                                                    })
                                                    .ok();
                                                });
                                            }
                                            menu
                                        }))
                                    })
                                    .into_any_element()
                            }
                            (FieldValue::Flag(flag), _) => Checkbox::new(SharedString::from(format!("jenkins-flag-{index}")), ToggleState::from(*flag))
                                .on_click(cx.listener(move |this, state: &ToggleState, _, cx| {
                                    if let Some(ActionState { kind: Kind::Build { fields, .. }, .. }) = &mut this.action
                                        && let Some(field) = fields.get_mut(index)
                                    {
                                        field.value = FieldValue::Flag(state.selected());
                                    }
                                    cx.notify();
                                }))
                                .into_any_element(),
                            (FieldValue::Text(editor), _) => boxed(editor.clone().into_any_element()).into_any_element(),
                            (FieldValue::Choice(current), _) => Label::new(current.clone()).into_any_element(),
                            (FieldValue::Cascade(cascade), _) => self.render_cascade(index, cascade, cx),
                        };
                        body = body.child(
                            v_flex()
                                .gap_0p5()
                                .child(Label::new(field.definition.name.clone()).size(LabelSize::Small))
                                .when(!field.definition.description.is_empty(), |this| {
                                    this.child(Label::new(field.definition.description.clone()).size(LabelSize::XSmall).color(Color::Muted))
                                })
                                .child(input),
                        );
                    }
                }
            }
        }

        let blocked = !unsupported.is_empty();
        let collected = self.collected(cx);
        let typed = connection
            .as_ref()
            .is_some_and(|connection| self.needs_typed_confirmation(connection, collected.as_deref().unwrap_or_default()));
        let review = action.review && !blocked;
        let mut summary = v_flex().gap_1();
        if review {
            summary = summary
                .child(Label::new("Review").weight(gpui::FontWeight::SEMIBOLD))
                .child(
                    Label::new(format!(
                        "{} as {} · {}",
                        connection.as_ref().map_or(String::new(), |c| c.address.clone()),
                        connection.as_ref().map_or(String::new(), |c| c.username.clone()),
                        action.job.join(" / ")
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Muted)
                    .buffer_font(cx),
                );
            if let (Kind::Build { fields, .. }, Ok(values)) = (&action.kind, &collected) {
                for (field, (name, value)) in fields.iter().zip(values) {
                    let shown = if field.definition.kind == ParameterKind::Password {
                        if value.is_empty() { "(empty)".to_string() } else { "(provided)".to_string() }
                    } else {
                        value.clone()
                    };
                    summary = summary.child(Label::new(format!("{name} = {shown}")).size(LabelSize::Small).buffer_font(cx));
                }
            }
            if typed {
                summary = summary
                    .child(Label::new(format!("Type “{job_name}” to confirm.")).size(LabelSize::Small).color(Color::Warning))
                    .child(div().w(px(320.)).child(boxed(action.confirm.clone().into_any_element())));
            }
        }

        let status: Option<(SharedString, Color)> = match &action.status {
            Status::Editing(message) => message.clone().map(|message| (message, Color::Warning)),
            Status::Submitting => Some(("Sending to Jenkins…".into(), Color::Muted)),
            Status::Queued { queue_id, why } => Some((
                format!("Queued (item {queue_id}){}", why.as_ref().map_or(String::new(), |why| format!(": {why}"))).into(),
                Color::Accent,
            )),
            Status::Finished(message, color) => Some((message.clone(), *color)),
            Status::Unknown(message) => Some((message.clone(), Color::Error)),
        };
        let busy = matches!(action.status, Status::Submitting | Status::Queued { .. });
        let locked = busy || matches!(action.status, Status::Unknown(_) | Status::Finished(..));
        let job = action.job.clone();
        let base_job_url = connection
            .as_ref()
            .map(|c| format!("{}{}", c.address, crate::jenkins::job_path(&action.job)));

        v_flex()
            .id("jenkins-action")
            .flex_1()
            .overflow_y_scroll()
            .gap_3()
            .child(Headline::new(title).size(HeadlineSize::Small))
            .when(action.form.is_some(), |this| {
                this.child(
                    Label::new(
                        "Some choices are computed by scripts configured on this Jenkins; changing a value they depend on runs them again.",
                    )
                    .size(LabelSize::XSmall)
                    .color(Color::Muted),
                )
            })
            .when(blocked, |this| {
                this.child(
                    v_flex()
                        .gap_1()
                        .child(
                            Label::new(format!(
                                "Vilo cannot fill these parameters yet: {}. Use Jenkins for this job for now.",
                                unsupported.join(", ")
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Warning),
                        )
                        .when_some(base_job_url.clone(), |this, url| {
                            this.child(h_flex().child(
                                Button::new("jenkins-open-web", "Open Build with Parameters in Jenkins")
                                    .label_size(LabelSize::Small)
                                    .end_icon(Icon::new(IconName::ArrowUpRight).size(IconSize::XSmall))
                                    .on_click(move |_, _, cx| cx.open_url(&format!("{url}build?delay=0sec"))),
                            ))
                        }),
                )
            })
            .when(!review, |this| this.child(body))
            .child(summary)
            .when_some(status, |this, (message, color)| this.child(Label::new(message).size(LabelSize::Small).color(color)))
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| {
                        if locked {
                            this
                        } else if review {
                            let label = match &action.kind {
                                Kind::Stop { .. } => "Stop build",
                                Kind::Build { .. } => "Start build",
                            };
                            this.child(
                                Button::new("jenkins-submit", label)
                                    .style(match &action.kind {
                                        Kind::Stop { .. } => ButtonStyle::Tinted(ui::TintColor::Error),
                                        Kind::Build { .. } => ButtonStyle::Filled,
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| this.submit(cx))),
                            )
                            .when(matches!(action.kind, Kind::Build { .. }), |this| {
                                this.child(Button::new("jenkins-edit-again", "Back").on_click(cx.listener(|this, _, _, cx| {
                                    if let Some(action) = &mut this.action {
                                        action.review = false;
                                    }
                                    cx.notify();
                                })))
                            })
                        } else if !blocked {
                            this.child(
                                Button::new("jenkins-review", "Review")
                                    .style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        let check = this.collected(cx);
                                        if let Some(action) = &mut this.action {
                                            match check {
                                                Ok(_) => {
                                                    action.review = true;
                                                    action.status = Status::Editing(None);
                                                }
                                                Err(message) => action.status = Status::Editing(Some(message.into())),
                                            }
                                        }
                                        cx.notify();
                                    })),
                            )
                        } else {
                            this
                        }
                    })
                    .when(matches!(action.status, Status::Unknown(_)), |this| {
                        let job = job.clone();
                        this.child(
                            Button::new("jenkins-check-builds", "Show the job's builds")
                                .style(ButtonStyle::Filled)
                                .on_click(cx.listener(move |this, _, _, cx| this.open_job(job.clone(), cx))),
                        )
                    })
                    .child(div().flex_1())
                    .child(Button::new("jenkins-action-close", if locked { "Close" } else { "Cancel" }).disabled(busy && matches!(action.status, Status::Submitting)).on_click(
                        cx.listener(|this, _, _, cx| {
                            // Closing only stops watching; a queued build stays queued on Jenkins.
                            this.action = None;
                            cx.notify();
                        }),
                    )),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cascade_field(name: &str, referenced: &[&str], multiple: bool) -> Field {
        let spec = crate::jenkins::tests_support::spec(name, referenced, multiple);
        Field {
            definition: ParameterDefinition {
                name: name.into(),
                description: String::new(),
                kind: ParameterKind::Unsupported("CascadeChoiceParameter".into()),
                default: None,
            },
            value: FieldValue::Cascade(Cascade {
                spec,
                options: Vec::new(),
                selected: Vec::new(),
                state: CascadeState::Loading,
                wanted: None,
                note: None,
            }),
        }
    }

    fn option(key: &str, selected: bool, disabled: bool) -> ChoiceOption {
        ChoiceOption { label: key.to_uppercase(), key: key.into(), selected, disabled }
    }

    #[test]
    fn test_cascade_order_follows_dependencies_and_rejects_loops() {
        let fields = vec![
            cascade_field("SERVICES", &["NAMESPACE", "ENV_TYPE"], true),
            cascade_field("NAMESPACE", &["ENV_TYPE"], false),
        ];
        assert_eq!(cascade_order(&fields).unwrap(), vec!["NAMESPACE".to_string(), "SERVICES".to_string()]);
        let looped = vec![cascade_field("A", &["B"], false), cascade_field("B", &["A"], false)];
        assert!(cascade_order(&looped).is_err());
    }

    #[test]
    fn test_apply_choices_restores_only_offered_values() {
        let FieldValue::Cascade(mut cascade) = cascade_field("SERVICES", &["NAMESPACE"], true).value else {
            unreachable!()
        };
        cascade.wanted = Some(vec!["auth".into(), "gone".into(), "blocked".into()]);
        apply_choices(&mut cascade, vec![option("auth", false, false), option("ccms", true, false), option("blocked", false, true)]);
        assert_eq!(cascade.selected, vec!["auth".to_string()]);
        assert_eq!(cascade.note.as_deref(), Some("No longer offered: gone, blocked"));
        // With nothing wanted, Jenkins' own preselection applies.
        let FieldValue::Cascade(mut fresh) = cascade_field("NAMESPACE", &["ENV_TYPE"], false).value else {
            unreachable!()
        };
        apply_choices(&mut fresh, vec![option("a", false, false), option("b", true, false)]);
        assert_eq!(fresh.selected, vec!["b".to_string()]);
    }

    #[test]
    fn test_production_mentions() {
        assert!(mentions_production("LIVECHAT-PROD-UPDATE"));
        assert!(mentions_production("live"));
        assert!(!mentions_production("uat"));
    }
}
