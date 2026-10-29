use std::path::PathBuf;

use anyhow::{Context as _, Result, anyhow};
use editor::Editor;
use gpui::{App, AsyncApp, Entity, FocusHandle, Focusable, Task, TaskExt as _, Window, prelude::*};
use serde::{Deserialize, Serialize};
use ui::{Button, ButtonStyle, IconButton, Tooltip, prelude::*};
use util::ResultExt as _;

use crate::sls::{LogsRequest, Progress, SlsClient, SlsError};

/// How far back the access check looks; short so it scans little.
const CHECK_WINDOW_SECONDS: i64 = 5 * 60;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    region: String,
    access_key_id: String,
    /// The only projects Vilo will query with this key.
    projects: Vec<String>,
}

impl Connection {
    fn credential_url(&self) -> String {
        format!("vilo-sls://{}", self.id)
    }

    /// The secret is used only for this region and AccessKey ID.
    fn credential_owner(&self) -> String {
        format!("{}|{}", self.region, self.access_key_id)
    }
}

fn is_production(project: &str) -> bool {
    let project = project.to_lowercase();
    project.contains("prod") || project.contains("live")
}

struct Form {
    editing: Option<String>,
    name: Entity<Editor>,
    region: Entity<Editor>,
    access_key_id: Entity<Editor>,
    secret: Entity<Editor>,
    projects: Entity<Editor>,
    message: Option<SharedString>,
    checking: bool,
    _task: Option<Task<()>>,
}

/// One line of the access check: what was asked and what SLS answered. Never log contents.
struct CheckRow {
    project: String,
    logstore: String,
    api: &'static str,
    outcome: Result<String, String>,
}

pub struct SlsView {
    focus_handle: FocusHandle,
    path: PathBuf,
    connections: Vec<Connection>,
    active: Option<String>,
    form: Option<Form>,
    checks: Vec<CheckRow>,
    checking: bool,
    message: Option<SharedString>,
    /// Bumped when the connection changes; late answers for an older one are dropped.
    generation: u64,
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

async fn client_for(connection: &Connection, cx: &AsyncApp) -> Result<SlsClient> {
    let credential = cx
        .update(|cx| cx.read_credentials(&connection.credential_url()))
        .await
        .context("Could not read the secret from the keychain.")?;
    let secret = match credential {
        Some((owner, secret)) if owner == connection.credential_owner() => {
            String::from_utf8(secret).map_err(|_| anyhow!("The saved secret is unreadable."))?
        }
        _ => return Err(anyhow!("No secret saved for this region and AccessKey. Edit the connection.")),
    };
    SlsClient::new(cx.update(|cx| cx.http_client()), &connection.region, &connection.access_key_id, &secret, connection.projects.clone())
}

fn outcome<T>(result: &Result<T, SlsError>, describe: impl FnOnce(&T) -> String) -> Result<String, String> {
    match result {
        Ok(value) => Ok(describe(value)),
        Err(error) => Err(error.to_string()),
    }
}

impl SlsView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let path = paths::config_dir().join("sls_connections.json");
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
            active: connections.first().map(|connection| connection.id.clone()),
            connections,
            form: None,
            checks: Vec::new(),
            checking: false,
            message: None,
            generation: 0,
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

    fn select(&mut self, id: String, cx: &mut Context<Self>) {
        self.generation += 1;
        self.active = Some(id);
        self.form = None;
        self.checks.clear();
        self.checking = false;
        self.message = None;
        self.tasks.clear();
        cx.notify();
    }

    fn open_form(&mut self, editing: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        let existing = editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        let value = |pick: fn(&Connection) -> String| existing.as_ref().map(pick).unwrap_or_default();
        let region = existing.as_ref().map_or_else(|| "ap-southeast-1".to_string(), |c| c.region.clone());
        self.form = Some(Form {
            name: editor("Name, e.g. CP logs", &value(|c| c.name.clone()), false, window, cx),
            region: editor("Region", &region, false, window, cx),
            access_key_id: editor("AccessKey ID", &value(|c| c.access_key_id.clone()), false, window, cx),
            secret: editor(
                if existing.is_some() { "Leave empty to keep the saved one" } else { "AccessKey Secret" },
                "",
                true,
                window,
                cx,
            ),
            projects: editor(
                "Projects Vilo may query, comma separated, e.g. platform-qat-aliyun-logs",
                &value(|c| c.projects.join(", ")),
                false,
                window,
                cx,
            ),
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
        let text = |editor: &Entity<Editor>, cx: &App| editor.read(cx).text(cx).trim().to_string();
        let (name, region, access_key_id, secret) =
            (text(&form.name, cx), text(&form.region, cx), text(&form.access_key_id, cx), text(&form.secret, cx));
        let projects = text(&form.projects, cx)
            .split(',')
            .map(|project| project.trim().to_string())
            .filter(|project| !project.is_empty())
            .collect::<Vec<_>>();
        if name.is_empty() || access_key_id.is_empty() || projects.is_empty() {
            form.message = Some("Fill in a name, the AccessKey ID and at least one project.".into());
            cx.notify();
            return;
        }
        let existing = form.editing.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        let connection = Connection {
            id: existing.as_ref().map_or_else(|| uuid::Uuid::new_v4().simple().to_string(), |c| c.id.clone()),
            name,
            region,
            access_key_id,
            projects,
        };
        let keeps_secret = existing.as_ref().is_some_and(|existing| existing.credential_owner() == connection.credential_owner());
        if secret.is_empty() && !keeps_secret {
            form.message = Some("Enter the AccessKey Secret.".into());
            cx.notify();
            return;
        }
        form.checking = true;
        form.message = Some("Checking the key…".into());
        form._task = Some(cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                if !secret.is_empty() {
                    let client = SlsClient::new(
                        cx.update(|cx| cx.http_client()),
                        &connection.region,
                        &connection.access_key_id,
                        &secret,
                        connection.projects.clone(),
                    )?;
                    // A signed request proves the key; a missing list permission is fine, a rejected key is not.
                    match client.list_logstores(&connection.projects[0]).await {
                        Ok(_) | Err(SlsError::Forbidden(_)) => {}
                        Err(error) => return Err(anyhow!(error)),
                    }
                    let url = connection.credential_url();
                    let owner = connection.credential_owner();
                    cx.update(|cx| cx.write_credentials(&url, &owner, secret.as_bytes()))
                        .await
                        .context("Vilo could not save the secret to the keychain")?;
                    let read_back = cx.update(|cx| cx.read_credentials(&url)).await?;
                    if read_back.as_ref().is_none_or(|(saved_owner, saved)| saved_owner != &owner || saved.as_slice() != secret.as_bytes()) {
                        return Err(anyhow!("The keychain did not keep the secret."));
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
                Ok(()) => this.select(connection.id.clone(), cx),
                Err(error) => {
                    if let Some(form) = &mut this.form {
                        form.checking = false;
                        form.message = Some(format!("{error:#}").into());
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
        if self.active.as_ref() == Some(&id) {
            self.generation += 1;
            self.active = self.connections.first().map(|connection| connection.id.clone());
            self.checks.clear();
        }
        self.form = None;
        let persist = self.persist(cx);
        cx.spawn(async move |_, cx| {
            persist.await?;
            cx.update(|cx| cx.delete_credentials(&connection.credential_url()))
                .await
                .context("Vilo could not remove the SLS secret from the keychain")
        })
        .detach_and_log_err(cx);
        cx.notify();
    }

    /// Calls each read API once per allowed project over a short window and records what SLS answered.
    /// Shows counts and statuses only, never log contents.
    fn run_check(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.active.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned() else {
            return;
        };
        self.checks.clear();
        self.checking = true;
        self.message = None;
        let generation = self.generation;
        self.tasks.push(cx.spawn(async move |this, cx| {
            let client = match client_for(&connection, cx).await {
                Ok(client) => client,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.checking = false;
                        this.message = Some(format!("{error:#}").into());
                        cx.notify();
                    })
                    .ok();
                    return;
                }
            };
            let push = |row: CheckRow, cx: &mut AsyncApp| {
                this.update(cx, |this, cx| {
                    if this.generation == generation {
                        this.checks.push(row);
                        cx.notify();
                    }
                })
                .is_ok()
            };
            let projects = client.list_projects().await;
            if !push(
                CheckRow {
                    project: "—".into(),
                    logstore: "—".into(),
                    api: "ListProject",
                    outcome: outcome(&projects, |projects| format!("{} projects visible", projects.len())),
                },
                cx,
            ) {
                return;
            }
            let to = time::OffsetDateTime::now_utc().unix_timestamp();
            let from = to - CHECK_WINDOW_SECONDS;
            for project in &connection.projects {
                let logstores = client.list_logstores(project).await;
                let logstore = logstores.as_ref().ok().and_then(|logstores| {
                    logstores.iter().find(|name| name.contains("fpms")).or_else(|| logstores.first()).cloned()
                });
                if !push(
                    CheckRow {
                        project: project.clone(),
                        logstore: "—".into(),
                        api: "ListLogStores",
                        outcome: outcome(&logstores, |logstores| format!("{} logstores", logstores.len())),
                    },
                    cx,
                ) {
                    return;
                }
                let Some(logstore) = logstore else {
                    continue;
                };
                let fields = client.index_fields(project, &logstore).await;
                let histogram = client.histograms(project, &logstore, "*", from, to).await;
                let logs = client
                    .get_logs(project, &logstore, &LogsRequest { query: "*".into(), from, to, line: 1, offset: 0, newest_first: true })
                    .await;
                let rows = [
                    ("GetIndex", outcome(&fields, |fields| format!("{} indexed fields", fields.len()))),
                    (
                        "GetHistograms",
                        outcome(&histogram, |buckets| {
                            let total: u64 = buckets.iter().map(|bucket| bucket.count).sum();
                            let complete = buckets.iter().all(|bucket| bucket.progress == Progress::Complete);
                            format!("{total} logs in 5 minutes{}", if complete { "" } else { " (partial)" })
                        }),
                    ),
                    (
                        "GetLogsV2",
                        outcome(&logs, |page| {
                            format!(
                                "{} row, {} keys{}",
                                page.rows.len(),
                                page.keys.len(),
                                if page.progress == Progress::Complete { "" } else { " (partial)" }
                            )
                        }),
                    ),
                ];
                for (api, outcome) in rows {
                    if !push(CheckRow { project: project.clone(), logstore: logstore.clone(), api, outcome }, cx) {
                        return;
                    }
                }
            }
            this.update(cx, |this, cx| {
                if this.generation == generation {
                    this.checking = false;
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }
}

impl Focusable for SlsView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl SlsView {
    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors().clone();
        let mut list = v_flex().gap_0p5().px_1();
        for connection in &self.connections {
            let id = connection.id.clone();
            list = list.child(
                h_flex()
                    .id(SharedString::from(format!("sls-connection-{}", connection.id)))
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .cursor_pointer()
                    .when(self.active.as_ref() == Some(&connection.id), |this| this.bg(colors.element_selected))
                    .hover(|style| style.bg(colors.element_hover))
                    .tooltip(Tooltip::text(format!("{} · {}", connection.region, connection.projects.join(", "))))
                    .on_click(cx.listener(move |this, _, _, cx| this.select(id.clone(), cx)))
                    .child(Icon::new(IconName::Server).size(IconSize::Small).color(Color::Muted))
                    .child(Label::new(connection.name.clone()).truncate()),
            );
        }
        v_flex()
            .w(px(300.))
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
                        IconButton::new("sls-add-connection", IconName::Plus)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Add an SLS connection"))
                            .on_click(cx.listener(|this, _, window, cx| this.open_form(None, window, cx))),
                    ),
            )
            .child(list)
    }

    fn render_form(&self, form: &Form, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let input = |label: &'static str, editor: &Entity<Editor>| {
            v_flex()
                .gap_1()
                .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                .child(div().px_2().py_1().rounded_sm().border_1().border_color(colors.border).bg(colors.editor_background).child(editor.clone()))
        };
        let editing = form.editing.clone();
        v_flex()
            .id("sls-form")
            .max_w(px(560.))
            .overflow_y_scroll()
            .gap_3()
            .child(Headline::new(if form.editing.is_some() { "Edit SLS connection" } else { "Add an SLS connection" }).size(HeadlineSize::Small))
            .child(input("Name", &form.name))
            .child(input("Region", &form.region))
            .child(input("AccessKey ID", &form.access_key_id))
            .child(input("AccessKey Secret", &form.secret))
            .child(input("Projects", &form.projects))
            .child(
                Label::new(
                    "The secret stays in the macOS keychain and is only used to sign requests to SLS in this region. \
                     Vilo only reads, and only from the projects listed here. A read-only RAM key \
                     (AliyunLogReadOnlyAccess) is safest; Vilo cannot narrow what the key itself allows.",
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
                        Button::new("sls-save", "Save")
                            .style(ButtonStyle::Filled)
                            .disabled(form.checking)
                            .on_click(cx.listener(|this, _, _, cx| this.save_form(cx))),
                    )
                    .when(!self.connections.is_empty(), |this| {
                        this.child(Button::new("sls-cancel", "Cancel").on_click(cx.listener(|this, _, _, cx| {
                            this.form = None;
                            cx.notify();
                        })))
                    })
                    .when_some(editing, |this, id| {
                        this.child(
                            Button::new("sls-delete", "Delete connection")
                                .style(ButtonStyle::Subtle)
                                .color(Color::Error)
                                .on_click(cx.listener(move |this, _, _, cx| this.delete_connection(id.clone(), cx))),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_check(&self, connection: &Connection, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let id = connection.id.clone();
        let cell = |text: String, width: Option<f32>, color: Color| {
            let label = Label::new(text).size(LabelSize::Small).color(color);
            match width {
                Some(width) => div().w(px(width)).flex_none().child(label.truncate()),
                None => div().flex_1().min_w_0().child(label),
            }
        };
        let mut table = v_flex().rounded_md().border_1().border_color(colors.border).child(
            h_flex()
                .px_3()
                .py_1()
                .gap_3()
                .bg(colors.element_background)
                .child(cell("PROJECT".into(), Some(240.), Color::Muted))
                .child(cell("LOGSTORE".into(), Some(200.), Color::Muted))
                .child(cell("API".into(), Some(120.), Color::Muted))
                .child(cell("RESULT".into(), None, Color::Muted)),
        );
        for row in &self.checks {
            let (text, color) = match &row.outcome {
                Ok(text) => (text.clone(), Color::Success),
                Err(text) => (text.clone(), Color::Error),
            };
            table = table.child(
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_3()
                    .items_start()
                    .border_t_1()
                    .border_color(colors.border_variant)
                    .child(cell(row.project.clone(), Some(240.), if is_production(&row.project) { Color::Warning } else { Color::Default }))
                    .child(cell(row.logstore.clone(), Some(200.), Color::Default))
                    .child(cell(row.api.to_string(), Some(120.), Color::Default))
                    .child(cell(text, None, color)),
            );
        }
        v_flex()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(Headline::new(connection.name.clone()).size(HeadlineSize::Small))
                    .child(Label::new(connection.region.clone()).size(LabelSize::Small).color(Color::Muted))
                    .child(div().flex_1())
                    .child(
                        Button::new("sls-run-check", if self.checking { "Checking…" } else { "Check access" })
                            .style(ButtonStyle::Filled)
                            .disabled(self.checking)
                            .tooltip(Tooltip::text("Calls each read API once per project over the last 5 minutes. Shows counts only."))
                            .on_click(cx.listener(|this, _, _, cx| this.run_check(cx))),
                    )
                    .child(Button::new("sls-edit", "Edit").on_click(cx.listener(move |this, _, window, cx| this.open_form(Some(id.clone()), window, cx)))),
            )
            .child(
                Label::new(format!("Allowed projects: {}", connection.projects.join(", ")))
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .when(connection.projects.iter().any(|project| is_production(project)), |this| {
                this.child(
                    Label::new("Includes production. The check reads counts from a 5-minute window and shows no log contents.")
                        .size(LabelSize::Small)
                        .color(Color::Warning),
                )
            })
            .when_some(self.message.clone(), |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Error)))
            .when(!self.checks.is_empty(), |this| this.child(table))
            .into_any_element()
    }
}

impl Render for SlsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let active = self.active.as_ref().and_then(|id| self.connections.iter().find(|c| &c.id == id)).cloned();
        let main = match (&self.form, active) {
            (Some(form), _) => self.render_form(form, cx),
            (None, Some(connection)) => self.render_check(&connection, cx),
            (None, None) => Label::new("Add an SLS connection to start.").color(Color::Muted).into_any_element(),
        };
        h_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .child(self.render_sidebar(cx))
            .child(v_flex().id("sls-main").flex_1().min_w_0().h_full().overflow_y_scroll().p_6().child(main))
    }
}
