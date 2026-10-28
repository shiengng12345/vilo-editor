use std::io::Write as _;

use collections::BTreeMap;
use editor::Editor;
use gpui::{Entity, Task, Window, prelude::*};
use ui::{Button, ButtonStyle, IconButton, Tooltip, prelude::*};

use super::{Fetch, VaultView, client_for, import::editor};
use crate::vault::{KvVersion, Mount, SecretRead, WriteError, parse_strict_json};

struct EditRow {
    key: Entity<Editor>,
    value: Entity<Editor>,
    /// The value is JSON (number, bool, object, array, null) rather than a string.
    json: bool,
    revealed: bool,
}

#[derive(Clone, Debug, PartialEq)]
enum Change {
    Added(String),
    Removed(String),
    Changed(String),
    /// Same key, different JSON type, e.g. a string that became an object.
    TypeChanged(String, &'static str, &'static str),
}

struct Review {
    mount: Mount,
    path: String,
    data: serde_json::Map<String, serde_json::Value>,
    changes: Vec<Change>,
}

enum Status {
    Editing(Option<SharedString>),
    Saving,
    /// Someone wrote a newer version; the edits are kept.
    Conflict,
    /// The write may have happened. Saving again is blocked until checked.
    Unknown(SharedString),
}

pub(super) struct EditState {
    connection_id: String,
    creating: bool,
    /// Where an edit writes; a new secret's target is resolved from `path` on review.
    target: Option<(Mount, String)>,
    path: Entity<Editor>,
    base_version: u64,
    original: BTreeMap<String, serde_json::Value>,
    rows: Vec<EditRow>,
    json_mode: bool,
    json: Entity<Editor>,
    review: Option<Review>,
    confirm: Entity<Editor>,
    status: Status,
    _task: Option<Task<()>>,
}

fn kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn changes(
    original: &BTreeMap<String, serde_json::Value>,
    data: &serde_json::Map<String, serde_json::Value>,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for (key, value) in data {
        match original.get(key) {
            None => changes.push(Change::Added(key.clone())),
            Some(before) if kind(before) != kind(value) => {
                changes.push(Change::TypeChanged(key.clone(), kind(before), kind(value)))
            }
            Some(before) if before != value => changes.push(Change::Changed(key.clone())),
            Some(_) => {}
        }
    }
    for key in original.keys() {
        if !data.contains_key(key) {
            changes.push(Change::Removed(key.clone()));
        }
    }
    changes
}

/// A value as the table shows it: strings as typed, everything else as JSON.
fn row_text(value: &serde_json::Value) -> (String, bool) {
    match value {
        serde_json::Value::String(text) => (text.clone(), false),
        other => (other.to_string(), true),
    }
}

fn audit_line(
    connection: &str,
    address: &str,
    path: &str,
    from: u64,
    to: u64,
    changes: &[Change],
) -> String {
    let keys = |pick: fn(&Change) -> Option<&String>| changes.iter().filter_map(pick).cloned().collect::<Vec<_>>();
    serde_json::json!({
        "time": time::OffsetDateTime::now_utc().unix_timestamp(),
        "connection": connection,
        "address": address,
        "path": path,
        "from_version": from,
        "to_version": to,
        "added": keys(|change| match change { Change::Added(key) => Some(key), _ => None }),
        "removed": keys(|change| match change { Change::Removed(key) => Some(key), _ => None }),
        "changed": keys(|change| match change {
            Change::Changed(key) | Change::TypeChanged(key, ..) => Some(key),
            _ => None,
        }),
    })
    .to_string()
}

/// Appends one value-free line to the local write log.
fn append_audit(line: String, cx: &mut App) {
    cx.background_spawn(async move {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths::config_dir().join("vault_audit.log"))?;
        writeln!(file, "{line}")
    })
    .detach_and_log_err(cx);
}

pub(super) struct DeleteState {
    version: u64,
    /// Restores a deleted version instead of deleting the current one.
    undelete: bool,
    confirm: Entity<Editor>,
    message: Option<SharedString>,
    running: bool,
    _task: Option<Task<()>>,
}

impl VaultView {
    pub(super) fn start_delete(&mut self, version: u64, undelete: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.notice = None;
        self.delete = Some(DeleteState {
            version,
            undelete,
            confirm: editor("Type the connection name", "", false, window, cx),
            message: None,
            running: false,
            _task: None,
        });
        cx.notify();
    }

    fn run_delete(&mut self, cx: &mut Context<Self>) {
        let (Some(state), Some(secret), Some(session)) = (&mut self.delete, &self.secret, &self.session) else {
            return;
        };
        let Some(connection) = self.connections.iter().find(|c| c.id == session.connection_id).cloned() else {
            return;
        };
        if connection.is_protected() && state.confirm.read(cx).text(cx).trim() != connection.name {
            state.message = Some(format!("Type “{}” to confirm.", connection.name).into());
            cx.notify();
            return;
        }
        let (mount, path, version, undelete) = (secret.mount.clone(), secret.path.clone(), state.version, state.undelete);
        state.running = true;
        state.message = None;
        state._task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let client = client_for(&connection, cx).await.map_err(WriteError::Failed)?;
                if undelete {
                    client.undelete_version(&mount, &path, version).await
                } else {
                    client.delete_version(&mount, &path, version).await
                }
            }
            .await;
            this.update(cx, |this, cx| {
                let full_path = format!("{}/{}", mount.path, path);
                match result {
                    Ok(()) => {
                        append_audit(
                            serde_json::json!({
                                "time": time::OffsetDateTime::now_utc().unix_timestamp(),
                                "connection": connection.label(),
                                "address": connection.address,
                                "path": full_path,
                                "action": if undelete { "undelete" } else { "delete" },
                                "version": version,
                            })
                            .to_string(),
                            cx,
                        );
                        this.delete = None;
                        this.secret = None;
                        this.open_secret(mount.clone(), path.clone(), None, cx);
                        this.notice = Some(if undelete {
                            format!("Restored version {version} of {full_path}. The newest version is unchanged.")
                        } else {
                            format!("Deleted version {version} of {full_path}. It can be restored from the version menu.")
                        }
                        .into());
                    }
                    Err(error) => {
                        if let Some(state) = &mut this.delete {
                            state.running = false;
                            state.message = Some(match error {
                                WriteError::OutcomeUnknown(_) => format!("{error} Reopen the secret to see its state."),
                                other => other.to_string(),
                            }
                            .into());
                        }
                    }
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(super) fn render_delete(&self, state: &DeleteState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let connection = self
            .session
            .as_ref()
            .and_then(|session| self.connections.iter().find(|c| c.id == session.connection_id));
        let protected = connection.is_some_and(|c| c.is_protected());
        v_flex()
            .p_3()
            .gap_2()
            .rounded_md()
            .border_1()
            .border_color(if state.undelete { colors.border } else { cx.theme().status().error })
            .child(
                Label::new(if state.undelete {
                    format!("Restore version {}? It will not become the newest version again.", state.version)
                } else {
                    format!(
                        "Delete version {}? Vault keeps it as deleted, so it can be restored until it is destroyed or pruned.",
                        state.version
                    )
                })
                .size(LabelSize::Small),
            )
            .when(protected, |this| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            Label::new(format!(
                                "Protected connection: type “{}”",
                                connection.map_or(String::new(), |c| c.name.clone())
                            ))
                            .size(LabelSize::Small)
                            .color(Color::Warning),
                        )
                        .child(
                            div()
                                .w(px(240.))
                                .px_2()
                                .py_0p5()
                                .rounded_sm()
                                .border_1()
                                .border_color(colors.border)
                                .child(state.confirm.clone()),
                        ),
                )
            })
            .when_some(state.message.clone(), |this, message| {
                this.child(Label::new(message).size(LabelSize::Small).color(Color::Error))
            })
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("vault-delete-confirm", if state.undelete { "Restore" } else { "Delete version" })
                            .style(if state.undelete { ButtonStyle::Filled } else { ButtonStyle::Tinted(ui::TintColor::Error) })
                            .disabled(state.running)
                            .on_click(cx.listener(|this, _, _, cx| this.run_delete(cx))),
                    )
                    .child(Button::new("vault-delete-cancel", "Cancel").disabled(state.running).on_click(cx.listener(
                        |this, _, _, cx| {
                            this.delete = None;
                            cx.notify();
                        },
                    ))),
            )
            .into_any_element()
    }

    fn new_row(&self, key: &str, value: &str, json: bool, window: &mut Window, cx: &mut Context<Self>) -> EditRow {
        EditRow {
            key: editor("KEY", key, false, window, cx),
            value: editor(if json { "JSON value" } else { "value" }, value, true, window, cx),
            json,
            revealed: false,
        }
    }

    pub(super) fn start_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(connection_id) = self.session.as_ref().map(|session| session.connection_id.clone()) else {
            return;
        };
        let Some(secret) = &self.secret else {
            return;
        };
        let Fetch::Loaded(SecretRead::Found(found)) = &secret.read else {
            return;
        };
        let (Some(version), KvVersion::V2, None, true) =
            (found.version, secret.mount.version, secret.version, found.exact_numbers)
        else {
            return;
        };
        let original = found.values.clone();
        let mount = secret.mount.clone();
        let full_path = format!("{}/{}", mount.path, secret.path);
        let rows = original
            .iter()
            .map(|(key, value)| {
                let (text, json) = row_text(value);
                self.new_row(key, &text, json, window, cx)
            })
            .collect();
        self.edit = Some(EditState {
            connection_id,
            creating: false,
            target: Some((mount, secret.path.clone())),
            path: {
                let path = editor("", &full_path, false, window, cx);
                path.update(cx, |editor, _| editor.set_read_only(true));
                path
            },
            base_version: version,
            original,
            rows,
            json_mode: false,
            json: cx.new(|cx| Editor::auto_height(8, 40, window, cx)),
            review: None,
            confirm: editor("Type the connection name", "", false, window, cx),
            status: Status::Editing(None),
            _task: None,
        });
        cx.notify();
    }

    pub(super) fn start_create(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(connection_id) = self.session.as_ref().map(|session| session.connection_id.clone()) else {
            return;
        };
        let folder = self
            .secret
            .as_ref()
            .map(|secret| {
                let full = format!("{}/{}", secret.mount.path, secret.path);
                full.rsplit_once('/').map(|(folder, _)| format!("{folder}/")).unwrap_or_default()
            })
            .unwrap_or_default();
        let rows = vec![self.new_row("", "", false, window, cx)];
        self.edit = Some(EditState {
            connection_id,
            creating: true,
            target: None,
            path: editor("mount/folder/name, e.g. FPMS-NT-V2/auth/uat-swim-x", &folder, false, window, cx),
            base_version: 0,
            original: BTreeMap::default(),
            rows,
            json_mode: false,
            json: cx.new(|cx| Editor::auto_height(8, 40, window, cx)),
            review: None,
            confirm: editor("Type the connection name", "", false, window, cx),
            status: Status::Editing(None),
            _task: None,
        });
        cx.notify();
    }

    /// The secret as currently edited, validated.
    fn edited_data(&self, cx: &App) -> Result<serde_json::Map<String, serde_json::Value>, String> {
        let edit = self.edit.as_ref().ok_or("Nothing is being edited.")?;
        if edit.json_mode {
            return match parse_strict_json(&edit.json.read(cx).text(cx))? {
                serde_json::Value::Object(object) => Ok(object),
                _ => Err("The secret must be a JSON object: { \"KEY\": value, … }".into()),
            };
        }
        let mut data = serde_json::Map::new();
        for row in &edit.rows {
            let key = row.key.read(cx).text(cx).trim().to_string();
            let text = row.value.read(cx).text(cx);
            if key.is_empty() {
                if text.is_empty() {
                    continue;
                }
                return Err("Every value needs a key.".into());
            }
            if data.contains_key(&key) {
                return Err(format!("The key “{key}” appears twice."));
            }
            let value = if row.json {
                parse_strict_json(&text).map_err(|error| format!("{key}: {error}"))?
            } else {
                serde_json::Value::String(text)
            };
            data.insert(key, value);
        }
        Ok(data)
    }

    fn toggle_json_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let data = match self.edited_data(cx) {
            Ok(data) => data,
            Err(error) => {
                if let Some(edit) = &mut self.edit {
                    edit.status = Status::Editing(Some(error.into()));
                }
                cx.notify();
                return;
            }
        };
        let Some(json_mode) = self.edit.as_ref().map(|edit| edit.json_mode) else {
            return;
        };
        if json_mode {
            let rows = data
                .iter()
                .map(|(key, value)| {
                    let (text, json) = row_text(value);
                    self.new_row(key, &text, json, window, cx)
                })
                .collect();
            if let Some(edit) = &mut self.edit {
                edit.rows = rows;
                edit.json_mode = false;
            }
        } else if let Some(edit) = &mut self.edit {
            let pretty = serde_json::to_string_pretty(&serde_json::Value::Object(data)).unwrap_or_default();
            edit.json.update(cx, |editor, cx| editor.set_text(pretty, window, cx));
            edit.json_mode = true;
        }
        if let Some(edit) = &mut self.edit {
            edit.status = Status::Editing(None);
        }
        cx.notify();
    }

    fn review_edit(&mut self, cx: &mut Context<Self>) {
        let data = self.edited_data(cx);
        let mounts = match self.session.as_ref().map(|session| &session.mounts) {
            Some(Fetch::Loaded(mounts)) => mounts.clone(),
            _ => Vec::new(),
        };
        let Some(edit) = &mut self.edit else {
            return;
        };
        let data = match data {
            Ok(data) => data,
            Err(error) => {
                edit.status = Status::Editing(Some(error.into()));
                cx.notify();
                return;
            }
        };
        let target = if edit.creating {
            let input = edit.path.read(cx).text(cx).trim().trim_matches('/').to_string();
            mounts
                .iter()
                .filter(|mount| input.starts_with(&format!("{}/", mount.path)))
                .max_by_key(|mount| mount.path.len())
                .map(|mount| (mount.clone(), input[mount.path.len() + 1..].to_string()))
                .filter(|(_, path)| !path.is_empty() && !path.ends_with('/'))
        } else {
            edit.target.clone()
        };
        let Some((mount, path)) = target else {
            edit.status = Status::Editing(Some("Start the path with a known KV mount, and end it with the secret name.".into()));
            cx.notify();
            return;
        };
        if mount.version != KvVersion::V2 {
            edit.status = Status::Editing(Some("KV v1 secrets are read-only in Vilo.".into()));
            cx.notify();
            return;
        }
        let changes = changes(&edit.original, &data);
        if changes.is_empty() {
            edit.status = Status::Editing(Some("Nothing changed.".into()));
            cx.notify();
            return;
        }
        edit.review = Some(Review { mount, path, data, changes });
        edit.status = Status::Editing(None);
        cx.notify();
    }

    fn write_edit(&mut self, cx: &mut Context<Self>) {
        let Some(edit) = &mut self.edit else {
            return;
        };
        let Some(connection) = self.connections.iter().find(|c| c.id == edit.connection_id).cloned() else {
            return;
        };
        let Some(review) = &edit.review else {
            return;
        };
        if connection.is_protected() && edit.confirm.read(cx).text(cx).trim() != connection.name {
            edit.status = Status::Editing(Some(format!("Type “{}” to confirm this write.", connection.name).into()));
            cx.notify();
            return;
        }
        let (mount, path, data, changes) = (review.mount.clone(), review.path.clone(), review.data.clone(), review.changes.clone());
        let creating = edit.creating;
        let base = edit.base_version;
        edit.status = Status::Saving;
        edit._task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let client = client_for(&connection, cx).await.map_err(WriteError::Failed)?;
                if creating
                    && client
                        .current_version(&mount, &path)
                        .await
                        .map_err(|error| WriteError::Failed(error.to_string()))?
                        .is_some()
                {
                    return Err(WriteError::Failed("A secret already exists at this path.".into()));
                }
                client.write_v2(&mount, &path, &data, base).await
            }
            .await;
            this.update(cx, |this, cx| {
                match result {
                    Ok(version) => {
                        let full_path = format!("{}/{}", mount.path, path);
                        let line = audit_line(&connection.label(), &connection.address, &full_path, base, version, &changes);
                        append_audit(line, cx);
                        this.edit = None;
                        // Listings and history may now be stale.
                        this.folders.retain(|(mount_path, _), _| mount_path != &mount.path);
                        this.expanded.retain(|(mount_path, _)| mount_path != &mount.path);
                        this.secret = None;
                        this.open_secret(mount.clone(), path.clone(), None, cx);
                        this.notice = Some(format!("Saved {full_path} as version {version}.").into());
                    }
                    Err(error) => {
                        if let Some(edit) = &mut this.edit {
                            edit.status = match error {
                                WriteError::Conflict => Status::Conflict,
                                WriteError::OutcomeUnknown(_) => Status::Unknown(error.to_string().into()),
                                other => Status::Editing(Some(other.to_string().into())),
                            };
                        }
                    }
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    /// After an unknown outcome: has Vault moved past the base version?
    fn check_write(&mut self, cx: &mut Context<Self>) {
        let Some(edit) = &mut self.edit else {
            return;
        };
        let (Some(review), Some(connection)) = (
            edit.review.as_ref(),
            self.connections.iter().find(|c| c.id == edit.connection_id).cloned(),
        ) else {
            return;
        };
        let (mount, path, base) = (review.mount.clone(), review.path.clone(), edit.base_version);
        edit.status = Status::Saving;
        edit._task = Some(cx.spawn(async move |this, cx| {
            let current = async {
                let client = client_for(&connection, cx).await?;
                client.current_version(&mount, &path).await.map_err(|error| error.to_string())
            }
            .await;
            this.update(cx, |this, cx| {
                if let Some(edit) = &mut this.edit {
                    edit.status = match current {
                        Ok(Some(version)) if version > base => Status::Unknown(
                            format!(
                                "Vault now has version {version} (you started from {base}). It may be your write or someone else's; open the secret to check before writing again."
                            )
                            .into(),
                        ),
                        Ok(_) => Status::Editing(Some("Nothing was written. You can save again.".into())),
                        Err(error) => Status::Unknown(format!("Still unknown: {error}").into()),
                    };
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(super) fn remask_edit(&mut self, cx: &mut Context<Self>) {
        if let Some(edit) = &mut self.edit {
            for row in &mut edit.rows {
                if row.revealed {
                    row.revealed = false;
                    row.value.update(cx, |editor, cx| editor.set_masked(true, cx));
                }
            }
        }
    }

    pub(super) fn render_edit(&self, edit: &EditState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let connection = self.connections.iter().find(|c| c.id == edit.connection_id);
        let field = |child: AnyElement| {
            div()
                .px_2()
                .py_1()
                .rounded_sm()
                .border_1()
                .border_color(colors.border)
                .bg(colors.editor_background)
                .child(child)
        };

        let mut body = v_flex().gap_2();
        if let Some(review) = &edit.review {
            let mut list = v_flex().gap_1();
            for change in &review.changes {
                let (sign, text, color) = match change {
                    Change::Added(key) => ("+", format!("{key} added"), Color::Success),
                    Change::Removed(key) => ("−", format!("{key} removed"), Color::Error),
                    Change::Changed(key) => ("~", format!("{key} changed"), Color::Warning),
                    Change::TypeChanged(key, from, to) => ("~", format!("{key} changed type: {from} → {to}"), Color::Warning),
                };
                list = list.child(
                    h_flex()
                        .gap_2()
                        .child(Label::new(sign).size(LabelSize::Small).color(color).buffer_font(cx))
                        .child(Label::new(text).size(LabelSize::Small).buffer_font(cx)),
                );
            }
            body = body
                .child(Label::new("Review before writing").weight(gpui::FontWeight::SEMIBOLD))
                .child(
                    Label::new(format!(
                        "{} · {} · {}/{} · {}",
                        connection.map_or(String::new(), |c| c.label()),
                        connection.map_or(String::new(), |c| c.address.clone()),
                        review.mount.path,
                        review.path,
                        if edit.creating { "new secret".to_string() } else { format!("based on version {}", edit.base_version) }
                    ))
                    .size(LabelSize::Small)
                    .color(Color::Muted)
                    .buffer_font(cx),
                )
                .child(Label::new("Values are not shown here. Vault keeps older versions; this write cannot be undone from Vilo.").size(LabelSize::XSmall).color(Color::Muted))
                .child(list)
                .when(connection.is_some_and(|c| c.is_protected()), |this| {
                    this.child(
                        v_flex()
                            .gap_1()
                            .child(
                                Label::new(format!(
                                    "This connection is protected. Type “{}” to confirm.",
                                    connection.map_or(String::new(), |c| c.name.clone())
                                ))
                                .size(LabelSize::Small)
                                .color(Color::Warning),
                            )
                            .child(div().w(px(320.)).child(field(edit.confirm.clone().into_any_element()))),
                    )
                });
        } else if edit.json_mode {
            body = body
                .child(Label::new("JSON mode shows every value.").size(LabelSize::XSmall).color(Color::Warning))
                .child(field(edit.json.clone().into_any_element()));
        } else {
            let mut table = v_flex().border_1().border_color(colors.border).rounded_md();
            for (index, row) in edit.rows.iter().enumerate() {
                let revealed = row.revealed;
                table = table.child(
                    h_flex()
                        .px_2()
                        .py_1()
                        .gap_2()
                        .when(index > 0, |this| this.border_t_1().border_color(colors.border_variant))
                        .child(div().w(px(260.)).flex_none().child(field(row.key.clone().into_any_element())))
                        .child(div().flex_1().min_w_0().child(field(row.value.clone().into_any_element())))
                        .child(
                            Button::new(SharedString::from(format!("edit-json-{index}")), if row.json { "JSON" } else { "text" })
                                .label_size(LabelSize::XSmall)
                                .style(if row.json { ButtonStyle::Tinted(ui::TintColor::Accent) } else { ButtonStyle::Subtle })
                                .tooltip(Tooltip::text("Switch between a text value and a JSON value"))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(row) = this.edit.as_mut().and_then(|edit| edit.rows.get_mut(index)) {
                                        row.json = !row.json;
                                    }
                                    cx.notify();
                                })),
                        )
                        .child(
                            IconButton::new(SharedString::from(format!("edit-reveal-{index}")), if revealed { IconName::EyeOff } else { IconName::Eye })
                                .icon_size(IconSize::Small)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(row) = this.edit.as_mut().and_then(|edit| edit.rows.get_mut(index)) {
                                        row.revealed = !row.revealed;
                                        let masked = !row.revealed;
                                        row.value.update(cx, |editor, cx| editor.set_masked(masked, cx));
                                    }
                                    cx.notify();
                                })),
                        )
                        .child(
                            IconButton::new(SharedString::from(format!("edit-remove-{index}")), IconName::Close)
                                .icon_size(IconSize::Small)
                                .tooltip(Tooltip::text("Remove this key"))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(edit) = &mut this.edit
                                        && index < edit.rows.len()
                                    {
                                        edit.rows.remove(index);
                                    }
                                    cx.notify();
                                })),
                        ),
                );
            }
            body = body.child(table).child(
                h_flex().child(Button::new("edit-add-row", "Add key").start_icon(Icon::new(IconName::Plus).size(IconSize::Small)).on_click(
                    cx.listener(|this, _, window, cx| {
                        let row = this.new_row("", "", false, window, cx);
                        if let Some(edit) = &mut this.edit {
                            edit.rows.push(row);
                        }
                        cx.notify();
                    }),
                )),
            );
        }

        let status: Option<(SharedString, Color)> = match &edit.status {
            Status::Editing(message) => message.clone().map(|message| (message, Color::Warning)),
            Status::Saving => Some(("Writing…".into(), Color::Muted)),
            Status::Conflict => Some((WriteError::Conflict.to_string().into(), Color::Error)),
            Status::Unknown(message) => Some((message.clone(), Color::Error)),
        };
        let saving = matches!(edit.status, Status::Saving);
        let unknown = matches!(edit.status, Status::Unknown(_));
        let conflict = matches!(edit.status, Status::Conflict);

        v_flex()
            .id("vault-edit")
            .flex_1()
            .overflow_y_scroll()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(Headline::new(if edit.creating { "New secret" } else { "Edit secret" }).size(HeadlineSize::Small))
                    .child(div().flex_1())
                    .when(edit.review.is_none(), |this| {
                        this.child(
                            Button::new("edit-mode", if edit.json_mode { "Table" } else { "JSON" })
                                .label_size(LabelSize::Small)
                                .start_icon(Icon::new(if edit.json_mode { IconName::ListTree } else { IconName::Code }).size(IconSize::Small))
                                .on_click(cx.listener(|this, _, window, cx| this.toggle_json_mode(window, cx))),
                        )
                    }),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Label::new("Path").size(LabelSize::Small).color(Color::Muted))
                    .child(div().flex_1().when(!edit.creating, |this| this.opacity(0.7)).child(field(edit.path.clone().into_any_element()))),
            )
            .child(body)
            .when_some(status, |this, (message, color)| this.child(Label::new(message).size(LabelSize::Small).color(color)))
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| {
                        if unknown {
                            this.child(
                                Button::new("edit-check", "Check what Vault has")
                                    .style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, _, cx| this.check_write(cx))),
                            )
                        } else if edit.review.is_some() {
                            this.child(
                                Button::new("edit-write", "Write to Vault")
                                    .style(ButtonStyle::Filled)
                                    .disabled(saving || conflict)
                                    .on_click(cx.listener(|this, _, _, cx| this.write_edit(cx))),
                            )
                            .child(Button::new("edit-back", "Back to editing").disabled(saving).on_click(cx.listener(|this, _, _, cx| {
                                if let Some(edit) = &mut this.edit {
                                    edit.review = None;
                                    edit.status = Status::Editing(None);
                                }
                                cx.notify();
                            })))
                        } else {
                            this.child(
                                Button::new("edit-review", "Review changes")
                                    .style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, _, cx| this.review_edit(cx))),
                            )
                        }
                    })
                    .child(div().flex_1())
                    .child(Button::new("edit-cancel", if conflict { "Discard edits and reload" } else { "Cancel" }).disabled(saving).on_click(cx.listener(
                        |this, _, _, cx| {
                            // Dropping the state drops every edited value.
                            this.edit = None;
                            if let Some(secret) = this.secret.take() {
                                this.open_secret(secret.mount, secret.path, None, cx);
                            }
                            cx.notify();
                        },
                    ))),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_changes_report_type_changes_and_removals() {
        let original = BTreeMap::from_iter([
            ("A".to_string(), json!("1")),
            ("B".to_string(), json!({"x": 1})),
            ("C".to_string(), json!("same")),
        ]);
        let data = serde_json::Map::from_iter([
            ("A".to_string(), json!(1)),
            ("C".to_string(), json!("same")),
            ("D".to_string(), json!("new")),
        ]);
        assert_eq!(
            changes(&original, &data),
            vec![
                Change::TypeChanged("A".into(), "string", "number"),
                Change::Added("D".into()),
                Change::Removed("B".into()),
            ]
        );
    }

    #[test]
    fn test_audit_line_has_keys_but_no_values() {
        let line = audit_line(
            "CP · UAT",
            "http://a/",
            "kv/auth/uat",
            3,
            4,
            &[Change::Changed("PASSWORD".into()), Change::Added("NEW".into())],
        );
        assert!(line.contains("PASSWORD") && line.contains("\"to_version\":4"));
        assert!(!line.contains("hunter2"));
    }

    #[test]
    fn test_row_text_keeps_json_looking_strings_as_text() {
        assert_eq!(row_text(&json!("{\"a\":1}")), ("{\"a\":1}".to_string(), false));
        assert_eq!(row_text(&json!({"a": 1})), ("{\"a\":1}".to_string(), true));
    }
}
