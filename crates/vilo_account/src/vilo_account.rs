mod callback;
mod meegle;
mod welcome;

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, anyhow};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, EventEmitter, Global, SharedString, Task,
    TaskExt as _, WeakEntity,
};
use serde::{Deserialize, Serialize};
use util::ResultExt as _;
use workspace::Workspace;

pub use meegle::MEEGLE_HOSTS;
pub use welcome::{AccountButton, WelcomeModal};

/// The person signed in to this Vilo installation. Non-secret: written to the
/// config directory. Credentials live in the keychain under `credential_url`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountRecord {
    /// Local to this installation; never equal to the provider's id.
    pub vilo_user_id: String,
    pub provider: String,
    pub issuer: String,
    pub subject: String,
    pub display_name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub verified_at_unix: u64,
    pub verified_with: String,
    pub credential_url: String,
}

#[derive(Default, Serialize, Deserialize)]
struct AccountFile {
    #[serde(default)]
    welcome_seen: bool,
    #[serde(default)]
    account: Option<AccountRecord>,
}

#[derive(Serialize, Deserialize)]
struct StoredCredential {
    issuer: String,
    subject: String,
    client_id: String,
    refresh_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignInStatus {
    Idle,
    InProgress { host: SharedString },
    Failed(SharedString),
}

pub enum AccountEvent {
    SignedIn,
    SignedOut,
}

struct PendingSignIn {
    attempt_id: String,
    _task: Task<()>,
}

pub struct AccountStore {
    path: PathBuf,
    welcome_seen: bool,
    account: Option<AccountRecord>,
    /// Bumped on every sign-in, sign-out and cancel, so work started under an
    /// older epoch can tell it was superseded and must not commit.
    epoch: u64,
    pending: Option<PendingSignIn>,
    status: SignInStatus,
}

struct GlobalAccountStore(Entity<AccountStore>);
impl Global for GlobalAccountStore {}

impl EventEmitter<AccountEvent> for AccountStore {}

pub fn init(cx: &mut App) {
    let path = paths::config_dir().join("account.json");
    let file = std::fs::read(&path)
        .ok()
        .and_then(|bytes| {
            serde_json::from_slice::<AccountFile>(&bytes)
                .map_err(|error| log::error!("ignoring unreadable {path:?}: {error}"))
                .ok()
        })
        .unwrap_or_default();
    let store = cx.new(|_| AccountStore {
        path,
        welcome_seen: file.welcome_seen,
        account: file.account,
        epoch: 0,
        pending: None,
        status: SignInStatus::Idle,
    });
    cx.set_global(GlobalAccountStore(store.clone()));

    cx.observe_new(move |_: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };
        let state = store.read(cx);
        if state.welcome_seen || state.account.is_some() || welcome_disabled() {
            return;
        }
        let store = store.clone();
        cx.defer_in(window, move |workspace, window, cx| {
            workspace.toggle_modal(window, cx, |window, cx| {
                WelcomeModal::new(store, window, cx)
            });
        });
    })
    .detach();
}

/// Lets automated runs of debug builds start without the welcome screen.
fn welcome_disabled() -> bool {
    cfg!(debug_assertions) && std::env::var_os("VILO_SKIP_ACCOUNT_GATE").is_some()
}

impl AccountStore {
    pub fn global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalAccountStore>()
            .map(|global| global.0.clone())
    }

    pub fn account(&self) -> Option<&AccountRecord> {
        self.account.as_ref()
    }

    pub fn status(&self) -> &SignInStatus {
        &self.status
    }

    pub fn mark_welcome_seen(&mut self, cx: &mut Context<Self>) {
        if self.welcome_seen {
            return;
        }
        self.welcome_seen = true;
        self.persist(cx).detach_and_log_err(cx);
    }

    pub fn sign_in_with_meegle(&mut self, host: &str, cx: &mut Context<Self>) {
        self.epoch += 1;
        let epoch = self.epoch;
        let attempt_id = uuid::Uuid::new_v4().simple().to_string();
        let host = host.to_string();
        self.status = SignInStatus::InProgress {
            host: host.clone().into(),
        };
        // Replacing the pending attempt drops its task, which drops its callback listener.
        self.pending = Some(PendingSignIn {
            attempt_id: attempt_id.clone(),
            _task: cx.spawn(async move |this, cx| {
                let result = match meegle::sign_in(&host, cx).await {
                    Ok(identity) => commit(&this, identity, epoch, &attempt_id, cx).await,
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    log::warn!("Meegle sign-in failed: {error:#}");
                    this.update(cx, |this, cx| {
                        if this.epoch == epoch {
                            this.pending = None;
                            this.status = SignInStatus::Failed(error.to_string().into());
                            cx.notify();
                        }
                    })
                    .ok();
                }
            }),
        });
        cx.notify();
    }

    pub fn cancel_sign_in(&mut self, cx: &mut Context<Self>) {
        if self.pending.take().is_some() {
            self.epoch += 1;
        }
        self.status = SignInStatus::Idle;
        cx.notify();
    }

    pub fn sign_out(&mut self, cx: &mut Context<Self>) {
        self.epoch += 1;
        self.pending = None;
        self.status = SignInStatus::Idle;
        let Some(account) = self.account.take() else {
            return;
        };
        let persist = self.persist(cx);
        let delete = cx.delete_credentials(&account.credential_url);
        cx.spawn(async move |_, _| {
            persist.await?;
            delete
                .await
                .context("Vilo could not remove the saved Meegle credentials from the keychain")
        })
        .detach_and_log_err(cx);
        cx.emit(AccountEvent::SignedOut);
        cx.notify();
    }

    fn persist(&self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let path = self.path.clone();
        let file = AccountFile {
            welcome_seen: self.welcome_seen,
            account: self.account.clone(),
        };
        cx.background_spawn(
            async move { write_atomically(&path, &serde_json::to_vec_pretty(&file)?) },
        )
    }
}

/// Saves the credential under a fresh keychain item, reads it back, and only then
/// switches the account record to it. A superseded attempt or any failure leaves
/// the previous account and its credential untouched.
async fn commit(
    this: &WeakEntity<AccountStore>,
    identity: meegle::VerifiedIdentity,
    epoch: u64,
    attempt_id: &str,
    cx: &mut AsyncApp,
) -> Result<()> {
    let is_current = |store: &AccountStore| {
        store.epoch == epoch
            && store
                .pending
                .as_ref()
                .is_some_and(|pending| pending.attempt_id == attempt_id)
    };
    let host = identity.issuer.host_str().unwrap_or_default().to_string();
    let credential_url = format!("vilo-account://{host}/{attempt_id}");
    let credential = serde_json::to_vec(&StoredCredential {
        issuer: identity.issuer.to_string(),
        subject: identity.subject.clone(),
        client_id: identity.client_id.clone(),
        refresh_token: identity.refresh_token.clone(),
    })?;
    if !this.read_with(cx, |store, _| is_current(store))? {
        return Ok(());
    }

    cx.update(|cx| cx.write_credentials(&credential_url, &identity.subject, &credential))
        .await
        .context("Vilo could not save the Meegle credentials to the keychain")?;
    let read_back = cx.update(|cx| cx.read_credentials(&credential_url)).await?;
    if read_back.as_ref().map(|(_, bytes)| bytes) != Some(&credential) {
        cx.update(|cx| cx.delete_credentials(&credential_url))
            .await
            .ok();
        return Err(anyhow!("the keychain did not keep the Meegle credentials"));
    }

    let verified_at_unix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let committed = this.update(cx, |store, cx| {
        if !is_current(store) {
            return None;
        }
        let vilo_user_id = store
            .account
            .as_ref()
            .filter(|account| {
                account.issuer == identity.issuer.as_str() && account.subject == identity.subject
            })
            .map(|account| account.vilo_user_id.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let previous = store.account.replace(AccountRecord {
            vilo_user_id,
            provider: "meegle".to_string(),
            issuer: identity.issuer.to_string(),
            subject: identity.subject.clone(),
            display_name: identity.display_name.clone(),
            email: identity.email.clone(),
            avatar_url: identity.avatar_url.clone(),
            verified_at_unix,
            verified_with: format!("{} {}", identity.resource, "search_user_info"),
            credential_url: credential_url.clone(),
        });
        store.welcome_seen = true;
        Some((previous, store.persist(cx)))
    })?;
    let Some((previous, persist)) = committed else {
        cx.update(|cx| cx.delete_credentials(&credential_url))
            .await
            .ok();
        return Ok(());
    };

    if let Err(error) = persist.await {
        // Keep memory and disk in agreement: fall back to the previous account.
        this.update(cx, |store, cx| {
            store.account = previous;
            cx.notify();
        })?;
        cx.update(|cx| cx.delete_credentials(&credential_url))
            .await
            .ok();
        return Err(error.context("Vilo could not save the account"));
    }

    this.update(cx, |store, cx| {
        // A sign-out while the record was being written already took this account.
        if is_current(store) {
            store.epoch += 1;
            store.pending = None;
            store.status = SignInStatus::Idle;
            cx.emit(AccountEvent::SignedIn);
            cx.notify();
        }
    })?;
    if let Some(previous) = previous.filter(|previous| previous.credential_url != credential_url) {
        cx.update(|cx| cx.delete_credentials(&previous.credential_url))
            .await
            .log_err();
    }
    Ok(())
}

fn write_atomically(path: &PathBuf, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, contents)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}
