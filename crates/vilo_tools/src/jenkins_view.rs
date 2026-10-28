mod actions;

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow};
use collections::{HashMap, HashSet};
use editor::Editor;
use gpui::{
    App, AsyncApp, Entity, FocusHandle, Focusable, ScrollStrategy, Task, TaskExt as _,
    UniformListScrollHandle, Window, prelude::*, uniform_list,
};
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, Checkbox, IconButton, Indicator, ToggleState, Tooltip, prelude::*};
use util::ResultExt as _;

use crate::jenkins::{
    BUILDS_SHOWN, Build, JenkinsClient, JenkinsError, JobEntry, Stage, normalize_base, strip_escapes,
};

/// Console text kept in memory per build; older lines are dropped first.
const MAX_LOG_BYTES: usize = 5 * 1024 * 1024;
const LOG_POLL: Duration = Duration::from_secs(2);
/// A build that never finishes stops being followed after this.
// shortcut: Vilo gets no signal when the Tools module is hidden, so a
// followed log keeps polling until the build ends, the view closes or this.
const MAX_FOLLOW: Duration = Duration::from_secs(2 * 60 * 60);

/// Top-level folders and the site they hold.
// shortcut: fixed to this team's Jenkins layout; make it editable per connection when another layout appears.
const SITE_FOLDERS: &[(&str, &str)] = &[("FPMS_NT", "CP"), ("NEWPORT", "NP"), ("BRAZIL", "BP"), ("A-SYSTEM", "CP1")];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    /// Normalized base URL, e.g. `https://jenkins.example.com/`.
    address: String,
    username: String,
    /// Every change needs the job name typed. On unless the user turns it off.
    #[serde(default = "protected_by_default")]
    protected: bool,
}

fn protected_by_default() -> bool {
    true
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-jenkins://{}", self.id)
    }

    /// The secret is used only for this exact address and user.
    fn credential_owner(&self) -> String {
        format!("{}|{}", self.address, self.username)
    }
}

enum Fetch<T> {
    Loading,
    Loaded(T),
    Failed(SharedString),
}

struct Session {
    connection_id: String,
    client: JenkinsClient,
    version: String,
}

struct BuildState {
    job: Vec<String>,
    number: u64,
    info: Option<Build>,
    stages: Option<Option<Vec<Stage>>>,
    lines: Vec<SharedString>,
    /// Text after the last newline, still being written.
    partial: String,
    retained_bytes: usize,
    truncated: bool,
    following: bool,
    message: Option<SharedString>,
    scroll: UniformListScrollHandle,
    _poll: Option<Task<()>>,
}

impl BuildState {
    fn append(&mut self, text: &str) {
        let text = format!("{}{}", std::mem::take(&mut self.partial), strip_escapes(text));
        let mut pieces = text.split('\n').collect::<Vec<_>>();
        self.partial = pieces.pop().unwrap_or_default().to_string();
        for line in pieces {
            self.retained_bytes += line.len() + 1;
            self.lines.push(line.to_string().into());
        }
        let mut drop = 0;
        while self.retained_bytes > MAX_LOG_BYTES && drop < self.lines.len() {
            self.retained_bytes -= self.lines[drop].len() + 1;
            drop += 1;
        }
        if drop > 0 {
            self.lines.drain(..drop);
            self.truncated = true;
        }
    }

    fn line_count(&self) -> usize {
        self.lines.len() + usize::from(!self.partial.is_empty())
    }

    fn line(&self, index: usize) -> SharedString {
        self.lines
            .get(index)
            .cloned()
            .unwrap_or_else(|| self.partial.clone().into())
    }
}

struct Form {
    editing: Option<String>,
    protected: bool,
    name: Entity<Editor>,
    address: Entity<Editor>,
    username: Entity<Editor>,
    secret: Entity<Editor>,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

pub struct JenkinsView {
    focus_handle: FocusHandle,
    path: PathBuf,
    connections: Vec<Connection>,
    session: Option<Session>,
    session_error: Option<SharedString>,
    /// Bumped on every connection change; late answers for an older one are dropped.
    generation: u64,
    folders: HashMap<Vec<String>, Fetch<Vec<JobEntry>>>,
    expanded: HashSet<Vec<String>>,
    show_disabled: bool,
    job: Option<(Vec<String>, Fetch<Vec<Build>>)>,
    build: Option<BuildState>,
    form: Option<Form>,
    action: Option<actions::ActionState>,
    tasks: Vec<Task<()>>,
}

fn editor(placeholder: &str, text: &str, masked: bool, window: &mut Window, cx: &mut App) -> Entity<Editor> {
    let placeholder = placeholder.to_string();
    let text = text.to_string();
    cx.new(|cx| {
        let mut editor = Editor::single_line(window, cx);
        editor.set_placeholder_text(&placeholder, window, cx);
        if !text.is_empty() {
            editor.set_text(text, window, cx);
        }
        if masked {
            editor.set_masked(true, cx);
        }
        editor
    })
}

async fn client_for(connection: &Connection, cx: &AsyncApp) -> Result<JenkinsClient, String> {
    let credential = cx
        .update(|cx| cx.read_credentials(&connection.credential_url()))
        .await
        .map_err(|_| "Could not read the password from the keychain.".to_string())?;
    let secret = match credential {
        Some((owner, secret)) if owner == connection.credential_owner() => {
            String::from_utf8(secret).map_err(|_| "The saved password is unreadable.".to_string())?
        }
        _ => return Err("No password or token saved for this address and user.".to_string()),
    };
    let base = normalize_base(&connection.address).map_err(|error| error.to_string())?;
    Ok(JenkinsClient::new(cx.update(|cx| cx.http_client()), base, &connection.username, &secret)
        .with_backoff(cx.background_executor().clone()))
}

fn format_time(millis: u64) -> String {
    let Ok(time) = time::OffsetDateTime::from_unix_timestamp((millis / 1000) as i64) else {
        return String::new();
    };
    let time = time::UtcOffset::current_local_offset().map_or(time, |offset| time.to_offset(offset));
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute()
    )
}

fn format_duration(millis: u64) -> String {
    let seconds = millis / 1000;
    match seconds {
        seconds if seconds >= 3600 => format!("{}h {}m", seconds / 3600, seconds / 60 % 60),
        seconds if seconds >= 60 => format!("{}m {}s", seconds / 60, seconds % 60),
        seconds => format!("{seconds}s"),
    }
}

fn result_color(result: Option<&str>, building: bool) -> Color {
    if building {
        return Color::Accent;
    }
    match result {
        Some("SUCCESS") => Color::Success,
        Some("FAILURE") => Color::Error,
        Some("UNSTABLE") => Color::Warning,
        _ => Color::Muted,
    }
}

fn ball_color(color: Option<&str>) -> Color {
    match color.map(|color| color.trim_end_matches("_anime")) {
        Some("blue") => Color::Success,
        Some("red") => Color::Error,
        Some("yellow") => Color::Warning,
        _ => Color::Muted,
    }
}

/// The parameters worth a column: lane, environment, branch, else the first two.
fn key_parameters(build: &Build) -> String {
    let wanted = ["ENV_TYPE", "NAMESPACE", "BRANCH", "Branch", "Environment", "VERSION"];
    let picked = build
        .parameters
        .iter()
        .filter(|(name, value)| wanted.contains(&name.as_str()) && !value.is_empty())
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    if picked.is_empty() {
        build
            .parameters
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .take(2)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("  ")
    } else {
        picked.join("  ")
    }
}

fn is_error_line(line: &str) -> bool {
    line.contains("ERROR") || line.contains("FAILED") || line.contains("Exception") || line.starts_with("error")
}

impl JenkinsView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let path = paths::config_dir().join("jenkins_connections.json");
        let connections = std::fs::read(&path)
            .ok()
            .and_then(|bytes| {
                serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
                    .map_err(|error| log::error!("ignoring unreadable {path:?}: {error}"))
                    .ok()
            })
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| serde_json::from_value::<Connection>(entry).log_err())
            .collect::<Vec<_>>();
        let mut view = Self {
            focus_handle: cx.focus_handle(),
            path,
            connections,
            session: None,
            session_error: None,
            generation: 0,
            folders: HashMap::default(),
            expanded: HashSet::default(),
            show_disabled: false,
            job: None,
            build: None,
            form: None,
            action: None,
            tasks: Vec::new(),
        };
        if view.connections.is_empty() {
            view.open_form(None, window, cx);
        }
        view
    }

    fn persist(&self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let path = self.path.clone();
        let connections = self.connections.clone();
        // shortcut: saves are rare and user-driven, so two in flight at once are not ordered.
        cx.background_spawn(async move {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let temporary = path.with_extension("json.tmp");
            std::fs::write(&temporary, serde_json::to_vec_pretty(&connections)?)?;
            std::fs::rename(&temporary, &path)?;
            Ok(())
        })
    }

    fn connect(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(connection) = self.connections.iter().find(|c| c.id == id).cloned() else {
            return;
        };
        self.generation += 1;
        let generation = self.generation;
        self.session = None;
        self.folders.clear();
        self.expanded.clear();
        self.job = None;
        self.build = None;
        self.form = None;
        self.action = None;
        self.tasks.clear();
        self.session_error = Some("Connecting…".into());
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = async {
                let client = client_for(&connection, cx).await.map_err(|error| anyhow!(error))?;
                let version = client.whoami().await?;
                anyhow::Ok((client, version))
            }
            .await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                match result {
                    Ok((client, version)) => {
                        this.session_error = None;
                        this.session = Some(Session {
                            connection_id: connection.id.clone(),
                            client,
                            version,
                        });
                        this.load_folder(Vec::new(), cx);
                    }
                    Err(error) => this.session_error = Some(error.to_string().into()),
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    fn load_folder(&mut self, folder: Vec<String>, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        self.folders.insert(folder.clone(), Fetch::Loading);
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = client.jobs(&folder).await;
            this.update(cx, |this, cx| {
                if this.generation == generation {
                    this.folders.insert(
                        folder,
                        match result {
                            Ok(mut jobs) => {
                                jobs.sort_by_key(|job| (!job.is_container, job.name.to_lowercase()));
                                Fetch::Loaded(jobs)
                            }
                            Err(error) => Fetch::Failed(error.to_string().into()),
                        },
                    );
                    cx.notify();
                }
            })
            .ok();
        }));
    }

    fn toggle_folder(&mut self, folder: Vec<String>, cx: &mut Context<Self>) {
        if !self.expanded.remove(&folder) {
            self.expanded.insert(folder.clone());
            if !matches!(self.folders.get(&folder), Some(Fetch::Loaded(_))) {
                self.load_folder(folder, cx);
            }
        }
        cx.notify();
    }

    fn open_job(&mut self, job: Vec<String>, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        self.build = None;
        self.form = None;
        self.action = None;
        self.job = Some((job.clone(), Fetch::Loading));
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = client.builds(&job).await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some((open, builds)) = &mut this.job
                    && *open == job
                {
                    *builds = match result {
                        Ok(list) => Fetch::Loaded(list),
                        Err(error) => Fetch::Failed(error.to_string().into()),
                    };
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn open_build(&mut self, job: Vec<String>, number: u64, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let generation = self.generation;
        let poll_job = job.clone();
        let poll = cx.spawn(async move |this, cx| {
            let started = Instant::now();
            let mut cursor = 0;
            let mut round = 0usize;
            loop {
                let is_current = |this: &JenkinsView| {
                    this.generation == generation
                        && this.build.as_ref().is_some_and(|build| build.job == poll_job && build.number == number)
                };
                if round.is_multiple_of(5) {
                    let stages = client.stages(&poll_job, number).await;
                    let info = client.build(&poll_job, number).await;
                    let keep_going = this.update(cx, |this, cx| {
                        if !is_current(this) {
                            return false;
                        }
                        if let Some(build) = &mut this.build {
                            if let Ok(stages) = stages {
                                build.stages = Some(stages);
                            }
                            if let Ok(info) = info {
                                build.info = Some(info);
                            }
                        }
                        cx.notify();
                        true
                    });
                    if !matches!(keep_going, Ok(true)) {
                        return;
                    }
                }
                let chunk = client.log(&poll_job, number, cursor).await;
                let finished = match chunk {
                    Ok(chunk) => {
                        // A smaller size than asked for means the log was reset.
                        let reset = chunk.next_start < cursor;
                        cursor = chunk.next_start;
                        let more = chunk.more;
                        let keep_going = this.update(cx, |this, cx| {
                            if !is_current(this) {
                                return false;
                            }
                            if let Some(build) = &mut this.build {
                                if reset {
                                    build.lines.clear();
                                    build.partial.clear();
                                    build.retained_bytes = 0;
                                }
                                build.append(&chunk.text);
                                if build.following && build.line_count() > 0 {
                                    build.scroll.scroll_to_item(build.line_count() - 1, ScrollStrategy::Bottom);
                                }
                            }
                            cx.notify();
                            true
                        });
                        if !matches!(keep_going, Ok(true)) {
                            return;
                        }
                        // "No more data" can be a pause in a running build; ask the build itself.
                        !more
                            && client
                                .build(&poll_job, number)
                                .await
                                .is_ok_and(|info| !info.building)
                    }
                    Err(error) => {
                        this.update(cx, |this, cx| {
                            if is_current(this)
                                && let Some(build) = &mut this.build
                            {
                                build.message = Some(error.to_string().into());
                            }
                            cx.notify();
                        })
                        .ok();
                        true
                    }
                };
                if finished || started.elapsed() > MAX_FOLLOW {
                    // One last read so the end of the log is not missed.
                    let tail = client.log(&poll_job, number, cursor).await;
                    let info = client.build(&poll_job, number).await;
                    let stages = client.stages(&poll_job, number).await;
                    this.update(cx, |this, cx| {
                        if !is_current(this) {
                            return;
                        }
                        if let Some(build) = &mut this.build {
                            if let Ok(tail) = tail {
                                build.append(&tail.text);
                            }
                            if let Ok(info) = info {
                                build.info = Some(info);
                            }
                            if let Ok(stages) = stages {
                                build.stages = Some(stages);
                            }
                            if started.elapsed() > MAX_FOLLOW {
                                build.message = Some("Stopped following after 2 hours. Reopen the build to continue.".into());
                            }
                        }
                        cx.notify();
                    })
                    .ok();
                    return;
                }
                round += 1;
                cx.background_executor().timer(LOG_POLL).await;
            }
        });
        // Replacing the build drops the previous poll and every line it held.
        self.build = Some(BuildState {
            job,
            number,
            info: None,
            stages: None,
            lines: Vec::new(),
            partial: String::new(),
            retained_bytes: 0,
            truncated: false,
            following: true,
            message: None,
            scroll: UniformListScrollHandle::new(),
            _poll: Some(poll),
        });
        cx.notify();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        self.form = Some(Form {
            name: editor("Name, e.g. Team Jenkins", existing.as_ref().map_or("", |c| c.name.as_str()), false, window, cx),
            address: editor(
                "https://jenkins.example.com/",
                existing.as_ref().map_or("", |c| c.address.as_str()),
                false,
                window,
                cx,
            ),
            username: editor("Username", existing.as_ref().map_or("", |c| c.username.as_str()), false, window, cx),
            secret: editor(
                if existing.is_some() {
                    "Password or API token (leave empty to keep the saved one)"
                } else {
                    "Password or API token"
                },
                "",
                true,
                window,
                cx,
            ),
            protected: existing.as_ref().is_none_or(|c| c.protected),
            editing,
            message: None,
            checking: false,
            _task: None,
        });
        cx.notify();
    }

    fn save_form(&mut self, cx: &mut Context<Self>) {
        let Some(form) = &mut self.form else {
            return;
        };
        let name = form.name.read(cx).text(cx).trim().to_string();
        let username = form.username.read(cx).text(cx).trim().to_string();
        let secret = form.secret.read(cx).text(cx).trim().to_string();
        let base = match normalize_base(&form.address.read(cx).text(cx)) {
            Ok(base) => base,
            Err(error) => {
                form.message = Some(error.to_string().into());
                cx.notify();
                return;
            }
        };
        if name.is_empty() || username.is_empty() {
            form.message = Some("Fill in a name and a username.".into());
            cx.notify();
            return;
        }
        let existing = form
            .editing
            .as_ref()
            .and_then(|id| self.connections.iter().find(|c| &c.id == id))
            .cloned();
        let connection = Connection {
            id: existing
                .as_ref()
                .map_or_else(|| uuid::Uuid::new_v4().simple().to_string(), |c| c.id.clone()),
            name,
            address: base.to_string(),
            username,
            protected: form.protected,
        };
        // A saved secret is reused only for the same address and user.
        let keeps_secret = existing
            .as_ref()
            .is_some_and(|existing| existing.credential_owner() == connection.credential_owner());
        if secret.is_empty() && !keeps_secret {
            form.message = Some("Enter the password or an API token.".into());
            cx.notify();
            return;
        }
        form.checking = true;
        form.message = Some("Checking…".into());
        form._task = Some(cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                if !secret.is_empty() {
                    let client = JenkinsClient::new(cx.update(|cx| cx.http_client()), base, &connection.username, &secret);
                    client.whoami().await?;
                    let url = connection.credential_url();
                    let owner = connection.credential_owner();
                    cx.update(|cx| cx.write_credentials(&url, &owner, secret.as_bytes()))
                        .await
                        .context("Vilo could not save the password to the keychain")?;
                    let read_back = cx.update(|cx| cx.read_credentials(&url)).await?;
                    if read_back
                        .as_ref()
                        .is_none_or(|(saved_owner, saved)| saved_owner != &owner || saved.as_slice() != secret.as_bytes())
                    {
                        return Err(anyhow!("The keychain did not keep the password."));
                    }
                }
                let persist = this.update(cx, |this, cx| {
                    match this.connections.iter_mut().find(|c| c.id == connection.id) {
                        Some(existing) => *existing = connection.clone(),
                        None => this.connections.push(connection.clone()),
                    }
                    this.persist(cx)
                })?;
                persist.await.context("Vilo could not save the connection")
            }
            .await;
            this.update(cx, |this, cx| match result {
                Ok(()) => this.connect(connection.id.clone(), cx),
                Err(error) => {
                    if let Some(form) = &mut this.form {
                        form.checking = false;
                        form.message = Some(match error.downcast_ref::<JenkinsError>() {
                            Some(jenkins) => jenkins.to_string().into(),
                            None => format!("{error:#}").into(),
                        });
                    }
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn delete_connection(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(index) = self.connections.iter().position(|c| c.id == id) else {
            return;
        };
        let connection = self.connections.remove(index);
        if self.session.as_ref().is_some_and(|session| session.connection_id == id) {
            self.generation += 1;
            self.session = None;
            self.job = None;
            self.build = None;
            self.folders.clear();
        }
        self.form = None;
        let persist = self.persist(cx);
        cx.spawn(async move |_, cx| {
            persist.await?;
            cx.update(|cx| cx.delete_credentials(&connection.credential_url()))
                .await
                .context("Vilo could not remove the Jenkins password from the keychain")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }
}

impl Focusable for JenkinsView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl JenkinsView {
    fn render_tree(&self, folder: Vec<String>, depth: usize, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let colors = cx.theme().colors().clone();
        let indent = px(8. + depth as f32 * 14.);
        let mut rows = Vec::new();
        match self.folders.get(&folder) {
            None | Some(Fetch::Loading) => rows.push(
                div()
                    .pl(indent)
                    .child(Label::new("Loading…").size(LabelSize::Small).color(Color::Muted))
                    .into_any_element(),
            ),
            Some(Fetch::Failed(message)) => rows.push(
                div()
                    .pl(indent)
                    .child(Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning))
                    .into_any_element(),
            ),
            Some(Fetch::Loaded(jobs)) => {
                for job in jobs {
                    let hidden = !self.show_disabled
                        && (job.color.as_deref() == Some("disabled") || (job.is_container && job.name == "BAK"));
                    if hidden {
                        continue;
                    }
                    let mut path = folder.clone();
                    path.push(job.name.clone());
                    let site = (depth == 0)
                        .then(|| SITE_FOLDERS.iter().find(|(name, _)| *name == job.name).map(|(_, site)| *site))
                        .flatten();
                    if job.is_container {
                        let expanded = self.expanded.contains(&path);
                        let toggle = path.clone();
                        rows.push(
                            h_flex()
                                .id(SharedString::from(format!("jenkins-folder-{}", path.join("/"))))
                                .pl(indent)
                                .py_0p5()
                                .gap_1()
                                .rounded_sm()
                                .cursor_pointer()
                                .hover(|style| style.bg(colors.element_hover))
                                .on_click(cx.listener(move |this, _, _, cx| this.toggle_folder(toggle.clone(), cx)))
                                .child(
                                    Icon::new(if expanded { IconName::ChevronDown } else { IconName::ChevronRight })
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                )
                                .child(
                                    Icon::new(if expanded { IconName::FolderOpen } else { IconName::Folder })
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                )
                                .child(Label::new(job.name.clone()).size(LabelSize::Small).truncate())
                                .when_some(site, |this, site| {
                                    this.child(Label::new(site).size(LabelSize::XSmall).color(Color::Accent))
                                })
                                .into_any_element(),
                        );
                        if expanded {
                            rows.extend(self.render_tree(path, depth + 1, cx));
                        }
                    } else {
                        let selected = self.job.as_ref().is_some_and(|(open, _)| *open == path);
                        let open = path.clone();
                        rows.push(
                            h_flex()
                                .id(SharedString::from(format!("jenkins-job-{}", path.join("/"))))
                                .pl(indent + px(14.))
                                .py_0p5()
                                .gap_1p5()
                                .rounded_sm()
                                .cursor_pointer()
                                .when(selected, |this| this.bg(colors.element_selected))
                                .hover(|style| style.bg(colors.element_hover))
                                .on_click(cx.listener(move |this, _, _, cx| this.open_job(open.clone(), cx)))
                                .child(Indicator::dot().color(ball_color(job.color.as_deref())))
                                .child(Label::new(job.name.clone()).size(LabelSize::Small).truncate())
                                .into_any_element(),
                        );
                    }
                }
            }
        }
        rows
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let active = self.session.as_ref().map(|session| session.connection_id.clone());
        let mut list = v_flex().gap_0p5().px_1();
        for connection in &self.connections {
            let id = connection.id.clone();
            let is_active = active.as_ref() == Some(&connection.id);
            list = list.child(
                h_flex()
                    .id(SharedString::from(format!("jenkins-connection-{}", connection.id)))
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .cursor_pointer()
                    .when(is_active, |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .tooltip(Tooltip::text(format!("{} as {}", connection.address, connection.username)))
                    .on_click(cx.listener(move |this, _, _, cx| this.connect(id.clone(), cx)))
                    .child(Icon::new(IconName::Server).size(IconSize::Small).color(Color::Muted))
                    .child(Label::new(connection.name.clone()).truncate()),
            );
        }
        v_flex()
            .w(px(280.))
            .flex_none()
            .h_full()
            .border_r_1()
            .border_color(colors.border)
            .bg(colors.panel_background)
            .child(
                h_flex()
                    .h(px(26.))
                    .pl_3()
                    .pr_1()
                    .justify_between()
                    .child(Label::new("CONNECTIONS").size(LabelSize::XSmall).weight(gpui::FontWeight::SEMIBOLD).color(Color::Muted))
                    .child(
                        IconButton::new("jenkins-add-connection", IconName::Plus)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Add a Jenkins connection"))
                            .on_click(cx.listener(|this, _, window, cx| this.open_form(None, window, cx))),
                    ),
            )
            .child(list)
            .child(
                h_flex()
                    .mt_2()
                    .h(px(26.))
                    .pl_3()
                    .pr_2()
                    .justify_between()
                    .border_t_1()
                    .border_color(colors.border_variant)
                    .child(Label::new("JOBS").size(LabelSize::XSmall).weight(gpui::FontWeight::SEMIBOLD).color(Color::Muted))
                    .child(
                        Checkbox::new("jenkins-show-disabled", ToggleState::from(self.show_disabled))
                            .label("Disabled")
                            .label_size(LabelSize::XSmall)
                            .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                                this.show_disabled = state.selected();
                                cx.notify();
                            })),
                    ),
            )
            .child(
                v_flex()
                    .id("jenkins-tree")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .px_1()
                    .pb_2()
                    .children(if self.session.is_some() {
                        self.render_tree(Vec::new(), 0, cx)
                    } else {
                        vec![
                            div()
                                .px_3()
                                .py_2()
                                .child(
                                    Label::new("Select a connection to browse its jobs.")
                                        .size(LabelSize::Small)
                                        .color(Color::Muted),
                                )
                                .into_any_element(),
                        ]
                    }),
            )
    }

    fn render_form(&self, form: &Form, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let input = |label: &'static str, editor: &Entity<Editor>| {
            v_flex()
                .gap_1()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                .child(
                    div()
                        .px_2()
                        .py_1()
                        .rounded_sm()
                        .border_1()
                        .border_color(colors.border)
                        .bg(colors.editor_background)
                        .child(editor.clone()),
                )
        };
        let editing = form.editing.clone();
        v_flex()
            .max_w(px(520.))
            .gap_3()
            .child(Headline::new(if form.editing.is_some() { "Edit Jenkins connection" } else { "Add a Jenkins connection" }).size(HeadlineSize::Small))
            .child(input("Name", &form.name))
            .child(input("Address", &form.address))
            .child(input("Username", &form.username))
            .child(input("Password or API token", &form.secret))
            .child(
                Checkbox::new("jenkins-protected", ToggleState::from(form.protected))
                    .label("Protected: confirm every build and stop by typing the job name")
                    .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                        if let Some(form) = &mut this.form {
                            form.protected = state.selected();
                        }
                        cx.notify();
                    })),
            )
            .child(
                Label::new(
                    "Either works. An API token can be revoked on its own without changing the password. \
                     It is kept in the macOS keychain and only sent to this address. Vilo only reads from Jenkins.",
                )
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .when_some(form.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(if form.checking { Color::Muted } else { Color::Warning }))
            })
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("jenkins-save", "Save and connect")
                            .style(ButtonStyle::Filled)
                            .disabled(form.checking)
                            .on_click(cx.listener(|this, _, _, cx| this.save_form(cx))),
                    )
                    .when(!self.connections.is_empty(), |this| {
                        this.child(Button::new("jenkins-cancel", "Cancel").on_click(cx.listener(|this, _, _, cx| {
                            this.form = None;
                            cx.notify();
                        })))
                    })
                    .when_some(editing, |this, id| {
                        this.child(
                            Button::new("jenkins-delete", "Delete connection")
                                .style(ButtonStyle::Subtle)
                                .color(Color::Error)
                                .on_click(cx.listener(move |this, _, _, cx| this.delete_connection(id.clone(), cx))),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_job(&self, job: &[String], builds: &Fetch<Vec<Build>>, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let content = match builds {
            Fetch::Loading => Label::new("Loading builds…").color(Color::Muted).into_any_element(),
            Fetch::Failed(message) => Label::new(message.clone()).color(Color::Warning).into_any_element(),
            Fetch::Loaded(list) if list.is_empty() => Label::new("No builds yet.").color(Color::Muted).into_any_element(),
            Fetch::Loaded(list) => {
                let mut table = v_flex().id("jenkins-builds").flex_1().overflow_y_scroll().border_1().border_color(colors.border).rounded_md();
                for (index, build) in list.iter().enumerate() {
                    let job = job.to_vec();
                    let number = build.number;
                    let result = if build.building { "RUNNING".to_string() } else { build.result.clone().unwrap_or_else(|| "—".into()) };
                    table = table.child(
                        h_flex()
                            .id(SharedString::from(format!("jenkins-build-{number}")))
                            .px_3()
                            .py_1p5()
                            .gap_3()
                            .cursor_pointer()
                            .hover(|style| style.bg(colors.element_hover))
                            .when(index > 0, |this| this.border_t_1().border_color(colors.border_variant))
                            .on_click(cx.listener(move |this, _, _, cx| this.open_build(job.clone(), number, cx)))
                            .child(div().w(px(70.)).flex_none().child(Label::new(format!("#{number}")).size(LabelSize::Small).buffer_font(cx)))
                            .child(div().w(px(90.)).flex_none().child(Label::new(result).size(LabelSize::Small).color(result_color(build.result.as_deref(), build.building))))
                            .child(div().w(px(130.)).flex_none().child(Label::new(format_time(build.timestamp)).size(LabelSize::Small).color(Color::Muted)))
                            .child(div().w(px(70.)).flex_none().child(Label::new(if build.building { "…".to_string() } else { format_duration(build.duration) }).size(LabelSize::Small).color(Color::Muted)))
                            .child(div().w(px(110.)).flex_none().child(Label::new(build.user.clone().unwrap_or_else(|| "—".into())).size(LabelSize::Small).truncate()))
                            .child(div().flex_1().min_w_0().child(Label::new(key_parameters(build)).size(LabelSize::Small).buffer_font(cx).color(Color::Muted).truncate())),
                    );
                }
                table.into_any_element()
            }
        };
        let refresh = job.to_vec();
        v_flex()
            .flex_1()
            .min_h_0()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(Headline::new(job.join(" / ")).size(HeadlineSize::Small))
                    .child(div().flex_1())
                    .child({
                        let job = job.to_vec();
                        Button::new("jenkins-build-now", "Build…")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::PlayFilled).size(IconSize::Small))
                            .on_click(cx.listener(move |this, _, window, cx| this.start_build_form(job.clone(), None, window, cx)))
                    })
                    .child(Label::new(format!("last {BUILDS_SHOWN} builds")).size(LabelSize::Small).color(Color::Muted))
                    .child(
                        IconButton::new("jenkins-refresh-builds", IconName::RotateCw)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Refresh"))
                            .on_click(cx.listener(move |this, _, _, cx| this.open_job(refresh.clone(), cx))),
                    ),
            )
            .child(content)
            .into_any_element()
    }

    fn render_build(&self, build: &BuildState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let (status, color) = match &build.info {
            Some(info) if info.building => ("RUNNING".to_string(), Color::Accent),
            Some(info) => (info.result.clone().unwrap_or_else(|| "—".into()), result_color(info.result.as_deref(), false)),
            None => ("…".to_string(), Color::Muted),
        };
        let back = build.job.clone();
        let mut stages = h_flex().gap_1().flex_wrap();
        match &build.stages {
            Some(Some(list)) => {
                for stage in list {
                    let color = match stage.status.as_str() {
                        "SUCCESS" => Color::Success,
                        "FAILED" => Color::Error,
                        "IN_PROGRESS" => Color::Accent,
                        "UNSTABLE" => Color::Warning,
                        _ => Color::Muted,
                    };
                    stages = stages.child(
                        h_flex()
                            .px_2()
                            .py_0p5()
                            .gap_1()
                            .rounded_sm()
                            .border_1()
                            .border_color(colors.border_variant)
                            .child(Indicator::dot().color(color))
                            .child(Label::new(stage.name.clone()).size(LabelSize::XSmall))
                            .child(Label::new(format_duration(stage.duration)).size(LabelSize::XSmall).color(Color::Muted)),
                    );
                }
            }
            Some(None) => stages = stages.child(Label::new("Stages are not available on this Jenkins.").size(LabelSize::XSmall).color(Color::Muted)),
            None => {}
        }
        let parameters = build
            .info
            .as_ref()
            .map(|info| {
                info.parameters
                    .iter()
                    .filter(|(_, value)| !value.is_empty())
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>()
                    .join("   ")
            })
            .unwrap_or_default();
        let view = cx.entity();
        let count = build.line_count();
        let log = uniform_list("jenkins-log", count, move |range, _, cx| {
            let build = view.read(cx).build.as_ref();
            range
                .map(|index| {
                    let line = build.map(|build| build.line(index)).unwrap_or_default();
                    let error = is_error_line(&line);
                    div()
                        .px_2()
                        .child(
                            Label::new(line)
                                .size(LabelSize::Small)
                                .buffer_font(cx)
                                .color(if error { Color::Error } else { Color::Default })
                                .truncate(),
                        )
                        .into_any_element()
                })
                .collect::<Vec<_>>()
        })
        .track_scroll(&build.scroll)
        .flex_1();

        v_flex()
            .flex_1()
            .min_h_0()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("jenkins-back", build.job.last().cloned().unwrap_or_default())
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::ArrowLeft).size(IconSize::Small))
                            .on_click(cx.listener(move |this, _, _, cx| this.open_job(back.clone(), cx))),
                    )
                    .child(Label::new(format!("#{}", build.number)).buffer_font(cx))
                    .child(Label::new(status).color(color))
                    .child(div().flex_1())
                    .when(build.info.as_ref().is_some_and(|info| info.building), |this| {
                        let job = build.job.clone();
                        let number = build.number;
                        this.child(
                            Button::new("jenkins-stop", "Stop")
                                .label_size(LabelSize::Small)
                                .start_icon(Icon::new(IconName::Stop).size(IconSize::Small))
                                .on_click(cx.listener(move |this, _, window, cx| this.start_stop(job.clone(), number, window, cx))),
                        )
                    })
                    .child({
                        let job = build.job.clone();
                        let parameters = build.info.as_ref().map(|info| info.parameters.clone()).unwrap_or_default();
                        let number = build.number;
                        Button::new("jenkins-rebuild", "Rebuild…")
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::RotateCw).size(IconSize::Small))
                            .tooltip(Tooltip::text("A new build with this build's parameters, which you can change"))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.start_build_form(job.clone(), Some((number, parameters.clone())), window, cx)
                            }))
                    })
                    .when_some(build.info.as_ref(), |this, info| {
                        this.child(
                            Label::new(format!(
                                "{} · {} · {}",
                                format_time(info.timestamp),
                                if info.building { "running".to_string() } else { format_duration(info.duration) },
                                info.user.clone().unwrap_or_else(|| "started by —".into())
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                        )
                    }),
            )
            .when(!parameters.is_empty(), |this| this.child(Label::new(parameters).size(LabelSize::Small).buffer_font(cx).color(Color::Muted)))
            .child(stages)
            .child(
                h_flex()
                    .gap_2()
                    .child(Label::new(format!("{count} lines")).size(LabelSize::XSmall).color(Color::Muted))
                    .when(build.truncated, |this| {
                        this.child(Label::new("only the last 5 MB is kept; earlier lines and searches skip them").size(LabelSize::XSmall).color(Color::Warning))
                    })
                    .child(div().flex_1())
                    .child(
                        Button::new("jenkins-first-error", "First error")
                            .label_size(LabelSize::Small)
                            .tooltip(Tooltip::text("Lines with ERROR, FAILED or Exception, in the kept text"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(build) = &mut this.build {
                                    let first = (0..build.line_count()).find(|index| is_error_line(&build.line(*index)));
                                    if let Some(index) = first {
                                        build.following = false;
                                        build.scroll.scroll_to_item(index, ScrollStrategy::Center);
                                    } else {
                                        build.message = Some("No error lines found.".into());
                                    }
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Checkbox::new("jenkins-follow", ToggleState::from(build.following))
                            .label("Follow")
                            .label_size(LabelSize::Small)
                            .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                                if let Some(build) = &mut this.build {
                                    build.following = state.selected();
                                }
                                cx.notify();
                            })),
                    ),
            )
            .when_some(build.message.clone(), |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Warning)))
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .py_1()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_subheader_background)
                    .child(log),
            )
            .into_any_element()
    }
}

impl Render for JenkinsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let connection = self
            .session
            .as_ref()
            .and_then(|session| self.connections.iter().find(|c| c.id == session.connection_id))
            .cloned();
        let header = h_flex()
            .px_4()
            .py_2p5()
            .gap_4()
            .border_b_1()
            .border_color(colors.border_variant)
            .child(match &connection {
                Some(connection) => v_flex()
                    .flex_1()
                    .min_w_0()
                    .child(Label::new(connection.name.clone()).size(LabelSize::Large).weight(gpui::FontWeight::SEMIBOLD))
                    .child(Label::new(connection.address.clone()).size(LabelSize::Small).color(Color::Muted).buffer_font(cx).truncate())
                    .child(
                        Label::new(format!(
                            "Signed in as {} · Jenkins {} · read-only",
                            connection.username,
                            self.session.as_ref().map_or("", |session| session.version.as_str())
                        ))
                        .size(LabelSize::XSmall)
                        .color(Color::Muted),
                    )
                    .into_any_element(),
                None => v_flex()
                    .flex_1()
                    .child(Label::new("Jenkins").size(LabelSize::Large).weight(gpui::FontWeight::SEMIBOLD))
                    .child(Label::new("No connection selected").size(LabelSize::Small).color(Color::Muted))
                    .into_any_element(),
            })
            .when_some(connection.as_ref().map(|c| c.id.clone()), |this, id| {
                this.child(
                    Button::new("jenkins-edit-connection", "Edit connection")
                        .label_size(LabelSize::Small)
                        .start_icon(Icon::new(IconName::Pencil).size(IconSize::Small))
                        .on_click(cx.listener(move |this, _, window, cx| this.open_form(Some(id.clone()), window, cx))),
                )
            });

        let main = if let Some(form) = &self.form {
            self.render_form(form, cx)
        } else if let Some(action) = &self.action {
            self.render_action(action, cx)
        } else if let Some(message) = &self.session_error {
            Label::new(message.clone()).color(Color::Muted).into_any_element()
        } else if let Some(build) = &self.build {
            self.render_build(build, cx)
        } else if let Some((job, builds)) = &self.job {
            self.render_job(job, builds, cx)
        } else if self.session.is_some() {
            Label::new("Pick a job on the left.").color(Color::Muted).into_any_element()
        } else {
            Label::new("Choose a connection on the left.").color(Color::Muted).into_any_element()
        };

        h_flex()
            .key_context("JenkinsView")
            .track_focus(&self.focus_handle)
            .size_full()
            .items_start()
            .child(self.render_sidebar(cx))
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .bg(colors.editor_background)
                    .child(header)
                    .child(v_flex().flex_1().min_h_0().p_4().gap_3().child(main)),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> BuildState {
        BuildState {
            job: Vec::new(),
            number: 1,
            info: None,
            stages: None,
            lines: Vec::new(),
            partial: String::new(),
            retained_bytes: 0,
            truncated: false,
            following: true,
            message: None,
            scroll: UniformListScrollHandle::new(),
            _poll: None,
        }
    }

    #[test]
    fn test_log_lines_join_across_chunks_and_strip_escapes() {
        let mut build = state();
        build.append("first\nsec");
        build.append("ond \u{1b}[31mred\u{1b}[0m\nthird");
        assert_eq!(build.line_count(), 3);
        assert_eq!(build.line(1).as_ref(), "second red");
        assert_eq!(build.line(2).as_ref(), "third");
    }

    #[test]
    fn test_log_keeps_only_the_tail() {
        let mut build = state();
        let line = "x".repeat(1024 * 1024 - 1);
        for _ in 0..7 {
            build.append(&format!("{line}\n"));
        }
        assert!(build.truncated);
        assert!(build.retained_bytes <= MAX_LOG_BYTES);
        assert_eq!(build.lines.len(), 5);
    }
}
