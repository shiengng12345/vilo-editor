//! Changing one Redis key at a time from its detail panel. Each write is reviewed against the value
//! read at review time, confirmed, logged before it is sent and sent once, like MongoDB writes.

use anyhow::{Result, anyhow};
use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, Tooltip, prelude::*};
use vilo_database_core::{
    mongo::WriteError,
    redis::{Bytes, MAX_TTL_SECONDS, NEW_KEY_ARGUMENTS, NewValue, RedisWrite, quote_bytes},
};

use crate::{DatabaseView, boxed, editor as single_line, mongo_write, run};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum RedisOp {
    EditString,
    HashSet,
    HashDelete,
    SetAdd,
    SetRemove,
    ZSetScore,
    ZSetRemove,
    ListPush {
        head: bool,
    },
    ListRemove,
    Delete,
    Rename,
    Expire,
    Persist,
    /// A new key of this Redis type ("string", "hash", "list", "set", "zset").
    AddKey(&'static str),
    ListSet {
        index: i64,
    },
    ListRemoveAt {
        index: i64,
    },
    StreamAdd,
    StreamRemove,
    GroupCreate,
    GroupDestroy,
    StreamAck,
    StreamClaim,
    EditJson,
}

impl RedisOp {
    pub(crate) fn label(self) -> &'static str {
        match self {
            RedisOp::EditString => "Edit value",
            RedisOp::HashSet => "Set a field",
            RedisOp::HashDelete => "Delete a field",
            RedisOp::SetAdd => "Add a member",
            RedisOp::SetRemove => "Remove a member",
            RedisOp::ZSetScore => "Set a member's score",
            RedisOp::ZSetRemove => "Remove a member",
            RedisOp::ListPush { head: true } => "Push to the head",
            RedisOp::ListPush { head: false } => "Push to the tail",
            RedisOp::ListRemove => "Remove the first equal value",
            RedisOp::Delete => "Delete key",
            RedisOp::Rename => "Rename key",
            RedisOp::Expire => "Set time to live",
            RedisOp::Persist => "Remove time to live",
            RedisOp::AddKey(_) => "Add key",
            RedisOp::ListSet { .. } => "Edit item",
            RedisOp::ListRemoveAt { .. } => "Remove item",
            RedisOp::StreamAdd => "Add an entry",
            RedisOp::StreamRemove => "Remove an entry",
            RedisOp::GroupCreate => "Create a consumer group",
            RedisOp::GroupDestroy => "Destroy a consumer group",
            RedisOp::StreamAck => "Acknowledge a pending entry",
            RedisOp::StreamClaim => "Claim a pending entry",
            RedisOp::EditJson => "Edit document",
        }
    }

    /// The operations offered for a key of this type, value edits first.
    pub(crate) fn for_kind(kind: &str) -> Vec<RedisOp> {
        let mut ops = match kind {
            "string" => vec![RedisOp::EditString],
            "hash" => vec![RedisOp::HashSet, RedisOp::HashDelete],
            "set" => vec![RedisOp::SetAdd, RedisOp::SetRemove],
            "zset" => vec![RedisOp::ZSetScore, RedisOp::ZSetRemove],
            "list" => vec![
                RedisOp::ListPush { head: true },
                RedisOp::ListPush { head: false },
                RedisOp::ListRemove,
            ],
            "stream" => vec![RedisOp::StreamAdd, RedisOp::GroupCreate],
            "ReJSON-RL" => vec![RedisOp::EditJson],
            _ => Vec::new(),
        };
        ops.extend([
            RedisOp::Rename,
            RedisOp::Expire,
            RedisOp::Persist,
            RedisOp::Delete,
        ]);
        ops
    }

    fn inputs(self) -> &'static [&'static str] {
        match self {
            RedisOp::EditString => &["Value"],
            RedisOp::HashSet => &["Field", "Value"],
            RedisOp::HashDelete => &["Field"],
            RedisOp::SetAdd | RedisOp::SetRemove | RedisOp::ZSetRemove => &["Member"],
            RedisOp::ZSetScore => &["Member", "Score"],
            RedisOp::ListPush { .. } | RedisOp::ListRemove => &["Value"],
            RedisOp::Rename => &["New name"],
            RedisOp::Expire => &["Seconds"],
            RedisOp::ListSet { .. } => &["Value"],
            RedisOp::AddKey(_) => &["Key name", "TTL in seconds (empty: no TTL)"],
            RedisOp::StreamAdd => &["Entry ID (* lets Redis choose)"],
            RedisOp::GroupCreate => &["Group name", "Start from ($ new entries only, 0 all)"],
            RedisOp::StreamClaim => &["Give to consumer"],
            RedisOp::EditJson => &["Document"],
            RedisOp::Delete
            | RedisOp::Persist
            | RedisOp::ListRemoveAt { .. }
            | RedisOp::StreamRemove
            | RedisOp::GroupDestroy
            | RedisOp::StreamAck => &[],
        }
    }

    /// The inputs of one value row of a new key; hashes, lists, sets and sorted sets can add more.
    fn row_inputs(self) -> &'static [&'static str] {
        match self {
            RedisOp::AddKey("string") => &["Value"],
            RedisOp::AddKey("hash") => &["Field", "Value"],
            RedisOp::AddKey("list") => &["Element"],
            RedisOp::AddKey("set") => &["Member"],
            RedisOp::AddKey("zset") => &["Member", "Score"],
            RedisOp::AddKey("json") => &["Document"],
            RedisOp::StreamAdd => &["Field", "Value"],
            _ => &[],
        }
    }
}

pub(crate) struct RedisWriteForm {
    op: RedisOp,
    key: Bytes,
    inputs: Vec<(&'static str, Entity<Editor>)>,
    /// The list item as it was shown, which a list row edit or removal must still find.
    shown_item: Option<Bytes>,
    /// For a claim: the group, the entry and how long it was idle when shown.
    claim: Option<(String, String, u64)>,
    planned: Option<RedisWrite>,
    typed: Entity<Editor>,
    show_values: bool,
    sending: bool,
    message: Option<SharedString>,
    _task: Option<gpui::Task<()>>,
    /// The operation this form sent, so a later answer only touches the form that sent it.
    sent: Option<String>,
}

impl RedisWriteForm {
    pub(crate) fn sending_now(&self) -> bool {
        self.sending
    }
}

pub(crate) fn lock_key(connection: &str, database: u8, key: &[u8]) -> String {
    format!("{connection}/redis/db{database}/{}", quote_bytes(key))
}

/// A new stream entry from the form: the ID ("*" when left empty), then field and value rows;
/// rows left empty are skipped.
fn stream_entry(values: &[String]) -> Result<(String, Vec<(Bytes, Bytes)>)> {
    let id = values.first().map(|text| text.trim()).unwrap_or_default();
    let fields = values
        .get(1..)
        .unwrap_or_default()
        .chunks(2)
        .filter(|row| row.iter().any(|text| !text.is_empty()))
        .map(|row| {
            let field = row.first().cloned().unwrap_or_default();
            if field.is_empty() {
                return Err(anyhow!("Every field needs a name."));
            }
            Ok((
                field.into_bytes(),
                row.get(1).cloned().unwrap_or_default().into_bytes(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if fields.is_empty() {
        return Err(anyhow!("Add at least one field."));
    }
    let id = if id.is_empty() { "*" } else { id };
    Ok((id.to_string(), fields))
}

/// The new key from the form: name, optional TTL, then value rows. Rows left empty are skipped.
fn new_key(kind: &'static str, values: &[String]) -> Result<RedisWrite> {
    let name = values.first().cloned().unwrap_or_default();
    if name.trim().is_empty() {
        return Err(anyhow!("Name the key."));
    }
    let ttl = values.get(1).map(|text| text.trim()).unwrap_or_default();
    let ttl_seconds = if ttl.is_empty() {
        None
    } else {
        Some(
            ttl.parse::<u64>()
                .ok()
                .filter(|seconds| (1..=MAX_TTL_SECONDS).contains(seconds))
                .ok_or_else(|| {
                    anyhow!(
                        "The TTL must be a whole number of seconds from 1 to {MAX_TTL_SECONDS}, or empty."
                    )
                })?,
        )
    };
    let width = RedisOp::AddKey(kind).row_inputs().len().max(1);
    let rows = values
        .get(2..)
        .unwrap_or_default()
        .chunks(width)
        .filter(|row| kind == "string" || kind == "json" || row.iter().any(|text| !text.is_empty()))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Err(anyhow!("Add at least one value."));
    }
    let column = |row: &[String], index: usize| row.get(index).cloned().unwrap_or_default();
    let value = match kind {
        "string" => NewValue::String(column(rows[0], 0).into_bytes()),
        "hash" => NewValue::Hash(
            rows.iter()
                .map(|row| {
                    let field = column(row, 0);
                    if field.is_empty() {
                        return Err(anyhow!("Every field needs a name."));
                    }
                    Ok((field.into_bytes(), column(row, 1).into_bytes()))
                })
                .collect::<Result<_>>()?,
        ),
        "list" => NewValue::List(rows.iter().map(|row| column(row, 0).into_bytes()).collect()),
        "set" => NewValue::Set(rows.iter().map(|row| column(row, 0).into_bytes()).collect()),
        "zset" => NewValue::SortedSet(
            rows.iter()
                .map(|row| {
                    let score = column(row, 1)
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .filter(|score| !score.is_nan())
                        .ok_or_else(|| anyhow!("Every score must be a number."))?;
                    Ok((score, column(row, 0).into_bytes()))
                })
                .collect::<Result<_>>()?,
        ),
        "json" => {
            let document = column(rows[0], 0);
            serde_json::from_str::<serde_json::Value>(&document)
                .map_err(|error| anyhow!("The document is not valid JSON: {error}"))?;
            NewValue::Json(document)
        }
        _ => return Err(anyhow!("Vilo cannot create {kind} keys.")),
    };
    Ok(RedisWrite::AddKey {
        key: name.into_bytes(),
        value,
        ttl_seconds,
    })
}

impl DatabaseView {
    pub(crate) fn redis_writes_locked(&self, key: &[u8]) -> bool {
        self.active.as_ref().is_some_and(|connection| {
            self.unknown_writes
                .contains(&lock_key(connection, self.redis_database(), key))
        })
    }

    fn redis_database(&self) -> u8 {
        self.redis().map_or(0, |session| session.database())
    }

    /// The key's lock as a read starting now sees it, to clear once that read succeeds.
    pub(crate) fn redis_lock_seen(&self, key: &[u8]) -> Option<(String, u64)> {
        let connection = self.active.as_ref()?;
        let lock = lock_key(connection, self.redis_database(), key);
        let version = self.unknown_writes.version(&lock)?;
        Some((lock, version))
    }

    /// `prefill` fills the inputs in order (a row's field and value, a string's value); it must be
    /// text to be edited here. `shown_item` is the list item a row edit was started from.
    pub(crate) fn start_redis_write(
        &mut self,
        op: RedisOp,
        key: Bytes,
        prefill: Vec<Bytes>,
        shown_item: Option<Bytes>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_start_redis_write(&key, cx) {
            return;
        }
        let Ok(prefill) = prefill
            .into_iter()
            .map(String::from_utf8)
            .collect::<Result<Vec<_>, _>>()
        else {
            self.write_notice = Some("This value is not text, so Vilo cannot edit it here.".into());
            cx.notify();
            return;
        };
        if op == RedisOp::EditString && prefill.is_empty() {
            self.write_notice = Some("This value is not text, so Vilo cannot edit it here.".into());
            cx.notify();
            return;
        }
        let multiline = |op: RedisOp, label: &str| {
            matches!(op, RedisOp::EditString | RedisOp::AddKey("string")) && label == "Value"
                || label == "Document"
        };
        let mut inputs = Vec::new();
        for (index, label) in op.inputs().iter().chain(op.row_inputs()).enumerate() {
            let text = prefill.get(index).cloned().unwrap_or_default();
            let editor = if multiline(op, label) {
                cx.new(|cx| {
                    let mut editor = Editor::auto_height(4, 24, window, cx);
                    editor.set_text(text, window, cx);
                    editor
                })
            } else {
                single_line(label, &text, false, window, cx)
            };
            inputs.push((*label, editor));
        }
        let mut form = RedisWriteForm {
            op,
            key,
            inputs,
            shown_item,
            claim: None,
            planned: None,
            typed: single_line("Type the connection name", "", false, window, cx),
            show_values: false,
            sending: false,
            message: None,
            _task: None,
            sent: None,
        };
        // Operations without inputs go straight to review.
        if form.inputs.is_empty() {
            // Spelled out, so an item removal can never fall through to deleting the key.
            form.planned = match (op, form.shown_item.clone()) {
                (RedisOp::Persist, _) => Some(RedisWrite::Persist {
                    key: form.key.clone(),
                }),
                (RedisOp::Delete, _) => Some(RedisWrite::Delete {
                    key: form.key.clone(),
                }),
                _ => None,
            };
            if form.planned.is_none() {
                self.write_notice = Some("Remove items from their row in the list.".into());
                cx.notify();
                return;
            }
        }
        self.redis_write = Some(form);
        cx.notify();
    }

    /// Refuses while the database is switching or while a write to the key has an unknown outcome.
    fn can_start_redis_write(&mut self, key: &[u8], cx: &mut Context<Self>) -> bool {
        if self
            .redis_state
            .as_ref()
            .is_some_and(|state| state.switching_database())
        {
            // Shown by the database menu, so it goes when the switch ends.
            if let Some(state) = self.redis_state.as_mut() {
                state.switch_error =
                    Some("Wait for the database switch to finish, then edit.".into());
            }
            cx.notify();
            return false;
        }
        if self.redis_writes_locked(key) {
            self.write_notice = Some(
                "A write to this key has an unknown outcome. Refresh the key to check it first."
                    .into(),
            );
            cx.notify();
            return false;
        }
        true
    }

    /// Opens the confirm step for a write already built from a shown row, so a removal keeps the
    /// row's exact bytes (text or not) instead of going through text inputs.
    pub(crate) fn start_planned_redis_write(
        &mut self,
        op: RedisOp,
        planned: RedisWrite,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.can_start_redis_write(planned.key(), cx) {
            return;
        }
        self.redis_write = Some(RedisWriteForm {
            op,
            key: planned.key().to_vec(),
            inputs: Vec::new(),
            shown_item: None,
            claim: None,
            planned: Some(planned),
            typed: single_line("Type the connection name", "", false, window, cx),
            show_values: false,
            sending: false,
            message: None,
            _task: None,
            sent: None,
        });
        cx.notify();
    }

    /// A claim form for one pending entry, which keeps the idle time shown as XCLAIM's minimum.
    pub(crate) fn start_stream_claim(
        &mut self,
        key: Bytes,
        group: String,
        id: String,
        idle_ms: u64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.start_redis_write(RedisOp::StreamClaim, key, Vec::new(), None, window, cx);
        if let Some(form) = self
            .redis_write
            .as_mut()
            .filter(|form| form.op == RedisOp::StreamClaim)
        {
            form.claim = Some((group, id, idle_ms));
        }
    }

    fn add_redis_write_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(form) = &mut self.redis_write else {
            return;
        };
        for label in form.op.row_inputs() {
            let editor = single_line(label, "", false, window, cx);
            form.inputs.push((*label, editor));
        }
        cx.notify();
    }

    fn review_redis_write(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let Some(form) = &mut self.redis_write else {
            return;
        };
        let values = form
            .inputs
            .iter()
            .map(|(_, editor)| editor.read(cx).text(cx))
            .collect::<Vec<_>>();
        let (op, key, shown_item, claim) = (
            form.op,
            form.key.clone(),
            form.shown_item.clone(),
            form.claim.clone(),
        );
        form.message = Some("Reading the current value…".into());
        let task = run(cx, async move {
            let input = |index: usize| -> Result<Bytes> {
                let text = values.get(index).cloned().unwrap_or_default();
                Ok(text.into_bytes())
            };
            // Names (fields, members) are taken exactly as typed, so " a " never reaches "a".
            let named = |index: usize| -> Result<Bytes> {
                let text = values.get(index).cloned().unwrap_or_default();
                if text.is_empty() {
                    return Err(anyhow!("Fill in every field."));
                }
                Ok(text.into_bytes())
            };
            let trimmed = |index: usize| -> Result<String> {
                let text = values
                    .get(index)
                    .map(|text| text.trim().to_string())
                    .unwrap_or_default();
                if text.is_empty() {
                    return Err(anyhow!("Fill in every field."));
                }
                Ok(text)
            };
            Ok(match op {
                RedisOp::EditString => RedisWrite::SetString {
                    expected: session
                        .get_string(&key)
                        .await?
                        .ok_or_else(|| anyhow!("The key is gone or is no longer a string."))?,
                    value: input(0)?,
                    key,
                },
                RedisOp::HashSet => {
                    let field = named(0)?;
                    RedisWrite::HashSet {
                        expected: session.hash_get(&key, &field).await?,
                        value: input(1)?,
                        field,
                        key,
                    }
                }
                RedisOp::HashDelete => {
                    let field = named(0)?;
                    RedisWrite::HashDelete {
                        expected: session
                            .hash_get(&key, &field)
                            .await?
                            .ok_or_else(|| anyhow!("The hash has no such field."))?,
                        field,
                        key,
                    }
                }
                RedisOp::SetAdd => RedisWrite::SetAdd {
                    member: input(0)?,
                    key,
                },
                RedisOp::SetRemove => RedisWrite::SetRemove {
                    member: input(0)?,
                    key,
                },
                RedisOp::ZSetScore => {
                    let member = named(0)?;
                    let score = trimmed(1)?
                        .parse::<f64>()
                        .map_err(|_| anyhow!("The score must be a number."))?;
                    RedisWrite::ZSetScore {
                        expected: session.zset_score(&key, &member).await?,
                        member,
                        score,
                        key,
                    }
                }
                RedisOp::ZSetRemove => RedisWrite::ZSetRemove {
                    member: input(0)?,
                    key,
                },
                RedisOp::ListPush { head } => RedisWrite::ListPush {
                    head,
                    value: input(0)?,
                    key,
                },
                RedisOp::ListRemove => RedisWrite::ListRemoveFirst {
                    value: input(0)?,
                    key,
                },
                RedisOp::Rename => RedisWrite::Rename {
                    to: trimmed(0)?.into_bytes(),
                    key,
                },
                RedisOp::Expire => {
                    let seconds = trimmed(0)?
                        .parse::<u64>()
                        .ok()
                        .filter(|seconds| *seconds > 0)
                        .ok_or_else(|| {
                            anyhow!("Seconds must be a whole number above 0; use Delete key to remove it now.")
                        })?;
                    RedisWrite::Expire { seconds, key }
                }
                RedisOp::Delete => RedisWrite::Delete { key },
                RedisOp::Persist => RedisWrite::Persist { key },
                RedisOp::ListSet { index } => RedisWrite::ListSet {
                    expected: shown_item.ok_or_else(|| anyhow!("Open the item from the list."))?,
                    value: input(0)?,
                    index,
                    key,
                },
                RedisOp::ListRemoveAt { .. } => {
                    return Err(anyhow!("Remove items from their row in the list."));
                }
                RedisOp::AddKey(kind) => new_key(kind, &values)?,
                RedisOp::StreamAdd => {
                    let (id, fields) = stream_entry(&values)?;
                    RedisWrite::StreamAdd { id, fields, key }
                }
                RedisOp::GroupCreate => {
                    let start = values.get(1).map(|text| text.trim()).unwrap_or_default();
                    RedisWrite::GroupCreate {
                        group: trimmed(0)?,
                        start: if start.is_empty() {
                            "$".to_string()
                        } else {
                            start.to_string()
                        },
                        key,
                    }
                }
                RedisOp::StreamClaim => {
                    let (group, id, min_idle_ms) = claim
                        .ok_or_else(|| anyhow!("Claim entries from the group's pending list."))?;
                    RedisWrite::StreamClaim {
                        consumer: trimmed(0)?,
                        group,
                        min_idle_ms,
                        id,
                        key,
                    }
                }
                RedisOp::EditJson => {
                    let value = values.first().cloned().unwrap_or_default();
                    serde_json::from_str::<serde_json::Value>(&value)
                        .map_err(|error| anyhow!("The document is not valid JSON: {error}"))?;
                    RedisWrite::JsonSet {
                        expected: session.json_get(&key).await?.ok_or_else(|| {
                            anyhow!("The key is gone or is no longer a JSON document.")
                        })?,
                        value,
                        key,
                    }
                }
                RedisOp::StreamRemove | RedisOp::GroupDestroy | RedisOp::StreamAck => {
                    return Err(anyhow!("Start this from its row."));
                }
            })
        });
        form._task = Some(cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if let Some(form) = &mut this.redis_write {
                    match result {
                        Ok(planned) => {
                            form.planned = Some(planned);
                            form.message = None;
                        }
                        Err(error) => form.message = Some(format!("{error:#}").into()),
                    }
                    cx.notify();
                }
            })
            .ok();
        }));
        cx.notify();
    }

    fn send_redis_write(&mut self, cx: &mut Context<Self>) {
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
        let generation = self.generation;
        let log_checked = self.write_log_checked;
        let Some(form) = &mut self.redis_write else {
            return;
        };
        let Some(planned) = form.planned.clone() else {
            return;
        };
        if form.sending {
            return;
        }
        // A new key's name is only known now, so its lock is checked here too.
        let locked = self.active.as_ref().is_some_and(|connection| {
            self.unknown_writes
                .contains(&lock_key(connection, database, planned.key()))
        });
        let Some(form) = &mut self.redis_write else {
            return;
        };
        if locked {
            form.message = Some(
                "A write to this key has an unknown outcome. Refresh the key to check it first."
                    .into(),
            );
            cx.notify();
            return;
        }
        if mongo_write::guarded(&connection)
            && form.typed.read(cx).text(cx).trim() != connection.name.trim()
        {
            form.message = Some("Type the connection name exactly to confirm.".into());
            cx.notify();
            return;
        }
        let operation = uuid::Uuid::new_v4().simple().to_string();
        // XACK and XCLAIM cannot be checked by reloading, so they hold their group's lock,
        // shared with every window, until their answer is known or the user checked it.
        let group_lock =
            match &planned {
                RedisWrite::StreamAck { key, group, .. }
                | RedisWrite::StreamClaim { key, group, .. } => Some(
                    crate::redis_stream::group_lock(&connection.id, database, key, group),
                ),
                _ => None,
            };
        // Until the write log was read, an unsettled acknowledgement from before a restart is not known.
        if group_lock.is_some() && !log_checked {
            form.message = Some(
                "Vilo has not finished checking the write log for unsettled writes, so it does not change consumer groups yet."
                    .into(),
            );
            cx.notify();
            return;
        }
        if let Some(lock) = &group_lock
            && let Err(unknown) = crate::redis_pubsub::take_operation_lock(lock, &operation)
        {
            form.message = Some(
                if unknown {
                    "An acknowledgement or claim for this group has an unknown outcome. Check its pending entries, then press I checked it in the group."
                } else {
                    "An acknowledgement or claim for this group is still waiting for its answer."
                }
                .into(),
            );
            cx.notify();
            return;
        }
        form.sending = true;
        form.sent = Some(operation.clone());
        form.message = Some("Sending…".into());
        let intent = serde_json::json!({
            "time": chrono::Utc::now().to_rfc3339(),
            "operation": operation,
            "connection": connection.id,
            "name": connection.name,
            "site": connection.site,
            "environment": connection.environment,
            "db": database,
            "key": quote_bytes(planned.key()),
            "kind": planned.name(),
            // Restores the group's lock after a restart until the outcome is settled.
            "lock": group_lock,
            "phase": "intent",
        });
        let (summary, operation_id) = (mongo_write::summarize_intent(&intent), operation.clone());
        let task = run(cx, {
            let planned = planned.clone();
            async move {
                mongo_write::audit(intent)?;
                let outcome = session.write(&planned).await;
                let result = match &outcome {
                    Ok(true) => "ok",
                    Ok(false) => "unchanged",
                    Err(WriteError::Refused(_)) => "refused",
                    Err(WriteError::Unknown(_)) => "unknown",
                };
                let logged = mongo_write::audit(serde_json::json!({ "operation": operation, "phase": "result", "result": result }))
                    .err()
                    .map(|error| format!("{error:#}"));
                Ok((outcome, logged))
            }
        });
        // Detached, not owned by the form: the outcome must be recorded and the shared lock
        // settled even if the form or its window closes first.
        cx.spawn(async move |this, cx| {
            let (result, logged) = match task.await {
                Ok((outcome, logged)) => (Ok(outcome), logged),
                Err(error) => (Err(error), None),
            };
            let unknown = matches!(result, Ok(Err(WriteError::Unknown(_))));
            // Settled before touching the window, which may be closed by now.
            if let Some(lock) = &group_lock {
                crate::redis_pubsub::settle_operation_lock(lock, &operation_id, unknown);
            }
            // Recorded on the connection it was sent on, so the next write from any window checks.
            if let Some(error) = logged {
                crate::shared_unlogged_writes().push((
                    operation_id.clone(),
                    format!("{summary} · its result is not in the log: {error}"),
                ));
            }
            if unknown && group_lock.is_none() {
                crate::shared_unknown_writes().insert(lock_key(
                    &connection.id,
                    database,
                    planned.key(),
                ));
            }
            this.update(cx, |this, cx| {
                let key = planned.key().to_vec();
                if let Ok(Err(WriteError::Unknown(message))) = &result {
                    if group_lock.is_some() {
                        this.write_notice = Some(
                            format!("The outcome of {summary} is unknown ({message}). Check the group's pending entries, then press I checked it in the group.")
                                .into(),
                        );
                    } else {
                        this.write_notice = Some(
                            format!("The outcome of {summary} is unknown ({message}). Refresh the key to check it before writing again.")
                                .into(),
                        );
                    }
                }
                if this.generation != generation {
                    cx.notify();
                    return;
                }
                // The group's pending list may differ from what was shown; show it as it is.
                let reload_group = matches!(result, Ok(Ok(false)))
                    && matches!(
                        planned,
                        RedisWrite::StreamAck { .. }
                            | RedisWrite::StreamClaim { .. }
                            | RedisWrite::GroupDestroy { .. }
                    );
                if reload_group {
                    this.reload_stream_group(cx);
                }
                let Some(form) = this
                    .redis_write
                    .as_mut()
                    .filter(|form| form.sent.as_deref() == Some(operation_id.as_str()))
                else {
                    cx.notify();
                    return;
                };
                form.sending = false;
                match result {
                    Ok(Ok(true)) => {
                        this.redis_write = None;
                        // A key that moved or went is dropped from the list; others reload in place.
                        if matches!(planned, RedisWrite::Delete { .. } | RedisWrite::Rename { .. }) {
                            this.close_redis_key();
                            this.scan(true, cx);
                        } else if matches!(planned, RedisWrite::AddKey { .. }) {
                            this.scan(true, cx);
                            this.reopen_redis_key(key, cx);
                        } else {
                            this.reopen_redis_key(key, cx);
                        }
                        if matches!(
                            planned,
                            RedisWrite::StreamAck { .. }
                                | RedisWrite::StreamClaim { .. }
                                | RedisWrite::GroupDestroy { .. }
                        ) {
                            this.reload_stream_group(cx);
                        }
                    }
                    Ok(Ok(false)) => {
                        form.message = Some(
                            match planned {
                                RedisWrite::Rename { .. } => "Nothing changed: the new name is already taken, or the key is gone.",
                                RedisWrite::AddKey { .. } => "Nothing changed: a key with this name already exists.",
                                RedisWrite::StreamAdd { .. } => "Nothing changed: the stream is gone.",
                                RedisWrite::StreamDelete { .. } => "Nothing changed: the entry is already gone.",
                                RedisWrite::StreamAck { .. } => "Nothing changed: the entry is no longer pending for the group.",
                                RedisWrite::StreamClaim { .. } => "Nothing changed: the entry is no longer pending, or another consumer touched it since it was shown.",
                                RedisWrite::GroupDestroy { .. } => "Nothing changed: the group is already gone.",
                                RedisWrite::JsonSet { .. } => "Nothing changed: the document changed since it was read, or the key is gone. Refresh and try again.",
                                RedisWrite::ListSet { .. } | RedisWrite::ListRemoveAt { .. } => "Nothing changed: the item is no longer there as shown (the list changed). Refresh and try again.",
                                RedisWrite::ListRemoveFirst { .. } | RedisWrite::SetRemove { .. } | RedisWrite::ZSetRemove { .. } => "Nothing changed: the value is not there.",
                                _ => "Nothing changed: the value changed since it was read, or the key is gone. Refresh and try again.",
                            }
                            .into(),
                        );
                    }
                    Ok(Err(WriteError::Unknown(_))) => this.redis_write = None,
                    Ok(Err(WriteError::Refused(message))) => {
                        form.message = Some(format!("Not written: {message}").into());
                    }
                    Err(error) => form.message = Some(format!("Not sent: {error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    pub(crate) fn render_redis_write(
        &self,
        form: &RedisWriteForm,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(connection) = self.active_connection() else {
            return div().into_any_element();
        };
        let production = mongo_write::guarded(connection);
        let close = |this: &mut DatabaseView, cx: &mut Context<DatabaseView>| {
            if !this.redis_write.as_ref().is_some_and(|form| form.sending) {
                this.redis_write = None;
                cx.notify();
            }
        };
        v_flex()
            .id("database-redis-write")
            .gap_3()
            .on_action(cx.listener(move |this, _: &menu::Cancel, _, cx| close(this, cx)))
            .child(Headline::new(form.op.label()).size(HeadlineSize::Small))
            .child(
                Label::new(format!(
                    "{} · {} · {} · {}",
                    connection.site,
                    connection.environment,
                    connection.name,
                    match form.op {
                        RedisOp::AddKey(kind) => format!("a new {kind} key"),
                        RedisOp::ListSet { index } | RedisOp::ListRemoveAt { index } => {
                            format!("{} item {index}", quote_bytes(&form.key))
                        }
                        _ => quote_bytes(&form.key),
                    }
                ))
                .size(LabelSize::Small)
                .weight(gpui::FontWeight::SEMIBOLD)
                .color(if production {
                    Color::Error
                } else {
                    Color::Muted
                }),
            )
            .map(|this| match &form.planned {
                None => {
                    let fixed = form.op.inputs().len().min(form.inputs.len());
                    let (single, rows) = form.inputs.split_at(fixed);
                    let width = form.op.row_inputs().len().max(1);
                    let labelled = |label: &'static str, editor: &Entity<Editor>| {
                        v_flex()
                            .flex_1()
                            .gap_1()
                            .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
                            .child(boxed(editor.clone(), cx))
                    };
                    let growable = matches!(form.op, RedisOp::AddKey(kind) if kind != "string" && kind != "json")
                        || form.op == RedisOp::StreamAdd;
                    this.children(single.iter().map(|(label, editor)| labelled(label, editor)))
                        .children(rows.chunks(width).map(|row| {
                            h_flex()
                                .gap_2()
                                .children(row.iter().map(|(label, editor)| labelled(label, editor)))
                        }))
                        .when(growable, |this| {
                            // Each row adds up to two arguments; the script takes a bounded number.
                            let full = rows.len() + width > NEW_KEY_ARGUMENTS;
                            this.child(
                                h_flex()
                                    .gap_2()
                                    .child(
                                        Button::new("database-redis-write-add-row", "Add another")
                                            .start_icon(
                                                Icon::new(IconName::Plus).size(IconSize::XSmall),
                                            )
                                            .label_size(LabelSize::Small)
                                            .disabled(full)
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.add_redis_write_row(window, cx)
                                            })),
                                    )
                                    .child(
                                        Label::new("Empty rows are skipped.")
                                            .size(LabelSize::Small)
                                            .color(Color::Muted),
                                    ),
                            )
                        })
                }
                Some(planned) => this
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Label::new("This will be sent once:").size(LabelSize::Small))
                            .child(
                                Button::new(
                                    "database-redis-write-values",
                                    if form.show_values {
                                        "Hide values"
                                    } else {
                                        "Show values"
                                    },
                                )
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        if let Some(form) = &mut this.redis_write {
                                            form.show_values = !form.show_values;
                                        }
                                        cx.notify();
                                    },
                                )),
                            ),
                    )
                    .child(boxed(
                        Label::new(planned.describe(form.show_values))
                            .size(LabelSize::Small)
                            .buffer_font(cx),
                        cx,
                    ))
                    .when(production, |this| {
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
            .when_some(form.message.clone(), |this, message| {
                this.child(
                    Label::new(message)
                        .size(LabelSize::Small)
                        .color(if form.sending {
                            Color::Muted
                        } else {
                            Color::Warning
                        }),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .map(|this| match &form.planned {
                        None => this.child(
                            Button::new("database-redis-write-review", "Review")
                                .style(ButtonStyle::Filled)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.review_redis_write(cx)),
                                ),
                        ),
                        Some(planned) => this
                            .child(
                                Button::new("database-redis-write-send", planned.name())
                                    .style(ButtonStyle::Filled)
                                    .color(if matches!(planned, RedisWrite::Delete { .. }) {
                                        Color::Error
                                    } else {
                                        Color::Default
                                    })
                                    .disabled(form.sending)
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.send_redis_write(cx)),
                                    ),
                            )
                            .when(!form.inputs.is_empty(), |this| {
                                this.child(
                                    Button::new("database-redis-write-back", "Back to editing")
                                        .disabled(form.sending)
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            if let Some(form) = &mut this.redis_write {
                                                form.planned = None;
                                                form.message = None;
                                            }
                                            cx.notify();
                                        })),
                                )
                            }),
                    })
                    .child(
                        Button::new("database-redis-write-cancel", "Cancel")
                            .disabled(form.sending)
                            .tooltip(Tooltip::text("Nothing is sent"))
                            .on_click(cx.listener(move |this, _, _, cx| close(this, cx))),
                    ),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Connection, Kind, Session};
    use gpui_tokio::Tokio;
    use std::sync::Arc;
    use vilo_database_core::redis::RedisSession;

    /// One inline command to a test server, for what the session API does not send (CLIENT PAUSE).
    fn send_inline(address: &str, line: &str) -> std::io::Result<String> {
        use std::io::{BufRead as _, Write as _};
        let limit = std::time::Duration::from_secs(5);
        let address = address
            .parse()
            .map_err(|error| std::io::Error::other(format!("{address}: {error}")))?;
        let mut stream = std::net::TcpStream::connect_timeout(&address, limit)?;
        stream.set_read_timeout(Some(limit))?;
        stream.set_write_timeout(Some(limit))?;
        stream.write_all(format!("{line}\r\n").as_bytes())?;
        let mut reply = String::new();
        std::io::BufReader::new(stream).read_line(&mut reply)?;
        Ok(reply.trim_end().to_string())
    }

    /// Pumps the test executor while Tokio does the network work, until `done` or a minute passes.
    fn wait_until(
        cx: &mut gpui::VisualTestContext,
        what: &str,
        mut done: impl FnMut(&mut gpui::VisualTestContext) -> bool,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            cx.run_until_parked();
            if done(cx) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting: {what}"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// Undoes what the test did to the server, the shared lock table and the disk, also when an
    /// assertion fails halfway.
    struct Cleanup {
        address: String,
        key: String,
        lock: String,
        data_dir: std::path::PathBuf,
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Err(error) = send_inline(&self.address, "CLIENT UNPAUSE") {
                eprintln!("cleanup `CLIENT UNPAUSE` failed: {error}");
            }
            // A write still in flight logs its result once it is answered; removing the data
            // directory before that would let the late log line create it again.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
            while !mongo_write::read_dangling_writes().is_ok_and(|open| open.is_empty()) {
                if std::time::Instant::now() >= deadline {
                    eprintln!("cleanup: a write had no logged result after 40 s");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            if let Err(error) = send_inline(&self.address, &format!("DEL {}", self.key)) {
                eprintln!("cleanup `DEL {}` failed: {error}", self.key);
            }
            let locks = crate::shared_unknown_writes();
            if let Some(version) = locks.version(&self.lock) {
                locks.remove_if(&self.lock, version);
            }
            if let Err(error) = std::fs::remove_dir_all(&self.data_dir) {
                eprintln!("cleanup of {:?} failed: {error}", self.data_dir);
            }
        }
    }

    #[gpui::test]
    async fn test_an_unknown_write_locks_the_key_in_every_window_even_after_its_window_closed(
        app: &mut gpui::TestAppContext,
    ) {
        // A server of its own: the test pauses its writes, which would stall other tests.
        let Ok(url) = std::env::var("VILO_TEST_REDIS_PAUSABLE") else {
            return;
        };
        let address = url
            .trim_start_matches("redis://")
            .trim_end_matches('/')
            .to_string();
        let data_dir = std::env::temp_dir().join(format!("vilo-ui-test-{}", std::process::id()));
        // The write log goes here, not into the real config directory.
        paths::set_custom_data_dir(&data_dir.to_string_lossy());
        let key = format!("vilo_test:ui_unknown:{}", std::process::id());
        let connection = Connection {
            id: "ui-test-pausable".into(),
            name: "UI test".into(),
            kind: Kind::Redis,
            site: String::new(),
            environment: "TEST".into(),
            allow_writes: true,
            redis_tls: None,
            redis_ssh: None,
        };
        let lock = lock_key(&connection.id, 0, key.as_bytes());
        let _cleanup = Cleanup {
            address: address.clone(),
            key: key.clone(),
            lock: lock.clone(),
            data_dir,
        };
        assert_eq!(
            send_inline(&address, &format!("SET {key} before EX 600")).unwrap(),
            "+OK"
        );
        app.executor().allow_parking();
        app.update(|cx| {
            workspace::AppState::test(cx);
            editor::init(cx);
            gpui_tokio::init(cx);
        });
        let project = project::Project::test(fs::FakeFs::new(app.executor()), [], app).await;
        // Window B is a workspace window; window A is a window of its own holding only the
        // database page, since a second test workspace would register its collaboration
        // handlers again.
        let workspace_window = app.add_window({
            let project = project.clone();
            |window, cx| workspace::MultiWorkspace::test_new(project, window, cx)
        });
        let workspace = workspace_window
            .read_with(app, |multi_workspace, _| {
                multi_workspace.workspace().clone()
            })
            .unwrap();
        let window_a = app.add_window({
            let workspace = workspace.downgrade();
            |window, cx| DatabaseView::new(workspace, window, cx)
        });
        let view_a = window_a.root(app).unwrap();
        let mut cx_a = gpui::VisualTestContext::from_window(window_a.into(), app);
        let mut cx_b = gpui::VisualTestContext::from_window(workspace_window.into(), app);
        let view_b = cx_b
            .update(|window, cx| cx.new(|cx| DatabaseView::new(workspace.downgrade(), window, cx)));
        for (view, cx) in [(&view_a, &mut cx_a), (&view_b, &mut cx_b)] {
            let session = cx
                .update(|_, cx| {
                    let url = url.clone();
                    Tokio::spawn(cx, async move { RedisSession::connect(&url).await })
                })
                .await
                .unwrap()
                .unwrap();
            view.update(cx, |view, cx| {
                view.connections.push(connection.clone());
                view.active = Some(connection.id.clone());
                view.session_opened(Session::Redis(Arc::new(session)), cx);
            });
        }

        // Window A edits the string and confirms; the server then holds every write.
        view_a.update_in(&mut cx_a, |view, window, cx| {
            view.start_redis_write(
                RedisOp::EditString,
                key.clone().into_bytes(),
                vec![b"before".to_vec()],
                None,
                window,
                cx,
            );
        });
        view_a.update_in(&mut cx_a, |view, window, cx| {
            let form = view.redis_write.as_ref().expect("the edit form opened");
            form.inputs[0]
                .1
                .update(cx, |editor, cx| editor.set_text("after", window, cx));
            view.review_redis_write(cx);
        });
        wait_until(&mut cx_a, "the write was reviewed", |cx| {
            view_a.read_with(cx, |view, _| {
                view.redis_write
                    .as_ref()
                    .is_some_and(|form| form.planned.is_some())
            })
        });
        // Far longer than the 30 s command timeout; the test unpauses it itself.
        assert_eq!(
            send_inline(&address, "CLIENT PAUSE 120000 WRITE").unwrap(),
            "+OK"
        );
        view_a.update(&mut cx_a, |view, cx| view.send_redis_write(cx));
        assert!(view_a.read_with(&cx_a, |view, _| {
            view.redis_write.as_ref().is_some_and(|form| form.sending)
        }));

        // Window A closes while its write waits for an answer.
        cx_a.update(|window, _| window.remove_window());
        drop(view_a);
        cx_b.run_until_parked();
        assert!(
            window_a.update(app, |_, _, _| ()).is_err(),
            "window A is closed"
        );
        wait_until(&mut cx_b, "the write timed out as unknown", |_| {
            crate::shared_unknown_writes().contains(&lock)
        });

        // Window B may not edit the key until it has read it again.
        view_b.update_in(&mut cx_b, |view, window, cx| {
            view.start_redis_write(
                RedisOp::EditString,
                key.clone().into_bytes(),
                vec![b"before".to_vec()],
                None,
                window,
                cx,
            );
        });
        view_b.read_with(&cx_b, |view, _| {
            assert!(
                view.redis_write.is_none(),
                "the locked key opened an edit form"
            );
            assert!(
                view.write_notice
                    .as_ref()
                    .is_some_and(|notice| notice.contains("unknown outcome")),
                "{:?}",
                view.write_notice
            );
        });

        assert_eq!(send_inline(&address, "CLIENT UNPAUSE").unwrap(), "+OK");
        view_b.update(&mut cx_b, |view, cx| {
            view.open_key_matching(key.clone().into_bytes(), None, None, cx)
        });
        wait_until(&mut cx_b, "reading the key again cleared its lock", |_| {
            !crate::shared_unknown_writes().contains(&lock)
        });
        view_b.update_in(&mut cx_b, |view, window, cx| {
            view.start_redis_write(
                RedisOp::EditString,
                key.clone().into_bytes(),
                vec![b"after".to_vec()],
                None,
                window,
                cx,
            );
        });
        view_b.read_with(&cx_b, |view, _| {
            assert!(
                view.redis_write.is_some(),
                "after the read the key can be edited"
            );
        });
    }

    fn values(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|text| text.to_string()).collect()
    }

    #[test]
    fn test_new_key_from_the_form() {
        let hash = new_key(
            "hash",
            &values(&["user:1", "60", "name", "Ann", "", "", "age", ""]),
        )
        .unwrap();
        assert_eq!(
            hash,
            RedisWrite::AddKey {
                key: b"user:1".to_vec(),
                value: NewValue::Hash(vec![
                    (b"name".to_vec(), b"Ann".to_vec()),
                    (b"age".to_vec(), Vec::new()),
                ]),
                ttl_seconds: Some(60),
            },
            "an empty row is skipped, an empty value is kept"
        );
        let string = new_key("string", &values(&["s", "", ""])).unwrap();
        assert_eq!(
            string,
            RedisWrite::AddKey {
                key: b"s".to_vec(),
                value: NewValue::String(Vec::new()),
                ttl_seconds: None,
            }
        );
        let zset = new_key("zset", &values(&["z", "", "m", " 2.5 ", "n", "-inf"])).unwrap();
        assert_eq!(
            zset,
            RedisWrite::AddKey {
                key: b"z".to_vec(),
                value: NewValue::SortedSet(vec![
                    (2.5, b"m".to_vec()),
                    (f64::NEG_INFINITY, b"n".to_vec())
                ]),
                ttl_seconds: None,
            }
        );
        assert!(
            new_key("list", &values(&[" ", "", "a"])).is_err(),
            "no name"
        );
        assert!(new_key("list", &values(&["l", "0", "a"])).is_err(), "TTL 0");
        assert!(
            new_key("list", &values(&["l", "1.5", "a"])).is_err(),
            "TTL not whole"
        );
        assert!(
            new_key("list", &values(&["l", "18446744073709551615", "a"])).is_err(),
            "TTL Redis would refuse after creating the key"
        );
        assert!(new_key("set", &values(&["s", "", ""])).is_err(), "no value");
        assert!(
            new_key("hash", &values(&["h", "", "", "v"])).is_err(),
            "unnamed field"
        );
        assert!(
            new_key("zset", &values(&["z", "", "m", "NaN"])).is_err(),
            "NaN score"
        );
        assert!(new_key("stream", &values(&["x", "", "a"])).is_err());
        assert_eq!(
            new_key("json", &values(&["j", "", r#"{"a": 1}"#])).unwrap(),
            RedisWrite::AddKey {
                key: b"j".to_vec(),
                value: NewValue::Json(r#"{"a": 1}"#.into()),
                ttl_seconds: None,
            }
        );
        assert!(
            new_key("json", &values(&["j", "", "{oops"])).is_err(),
            "not JSON"
        );
    }

    #[test]
    fn test_stream_entry_from_the_form() {
        assert_eq!(
            stream_entry(&values(&["", "a", "1", "", "", "b", ""])).unwrap(),
            (
                "*".to_string(),
                vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), Vec::new())]
            ),
            "empty ID lets Redis choose; empty rows are skipped; empty values are kept"
        );
        assert_eq!(
            stream_entry(&values(&[" 5-1 ", "a", "1"])).unwrap().0,
            "5-1"
        );
        assert!(stream_entry(&values(&["*", "", ""])).is_err(), "no field");
        assert!(
            stream_entry(&values(&["*", "", "v"])).is_err(),
            "unnamed field"
        );
    }
}
