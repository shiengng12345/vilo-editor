mod callback;
mod meegle;
mod my_work_panel;
mod welcome;

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, anyhow};
use context_server::oauth::OAuthTokenError;
use futures::{FutureExt as _, future::Shared};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, EventEmitter, Global, SharedString, Task,
    TaskExt as _, WeakEntity,
};
use serde::{Deserialize, Serialize};
use url::Url;
use util::ResultExt as _;
use workspace::Workspace;

pub use meegle::MEEGLE_HOSTS;
pub use my_work_panel::MyWorkPanel;
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
    token_endpoint: Url,
    resource: Url,
}

/// Refresh early so a token never expires between being handed out and used.
const ACCESS_TOKEN_MARGIN: Duration = Duration::from_secs(60);
/// Used when Meegle does not say how long an access token lives.
const DEFAULT_ACCESS_TOKEN_LIFETIME: Duration = Duration::from_secs(5 * 60);

/// The saved credential can no longer be used; only a new sign-in fixes it.
#[derive(Debug)]
struct SignInRequired(&'static str);

impl std::fmt::Display for SignInRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for SignInRequired {}

type AccessTokenTask = Shared<Task<Result<String, SharedString>>>;

struct CachedAccess {
    epoch: u64,
    token: String,
    expires_at: Option<SystemTime>,
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
    access: Option<CachedAccess>,
    refreshing: Option<(u64, AccessTokenTask)>,
    /// The saved credential was rejected; the record stays until the user signs in again.
    needs_sign_in: bool,
    /// Keychain writes and deletes run on background threads; holding this keeps a
    /// late refresh from re-saving a credential that a sign-out just removed.
    credential_lock: Arc<async_lock::Mutex<()>>,
    /// Account-file writes may finish out of order; only the newest snapshot is written.
    persist_sequence: u64,
    persisted_sequence: Arc<AtomicU64>,
    persist_lock: Arc<async_lock::Mutex<()>>,
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
        access: None,
        refreshing: None,
        needs_sign_in: false,
        credential_lock: Arc::default(),
        persist_sequence: 0,
        persisted_sequence: Arc::default(),
        persist_lock: Arc::default(),
    });
    cx.set_global(GlobalAccountStore(store.clone()));
    my_work_panel::init(cx);

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

    pub fn needs_sign_in(&self) -> bool {
        self.needs_sign_in
    }

    /// Changes whenever the signed-in account changes; data fetched under an older
    /// epoch belongs to a previous account and must be discarded.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// A freshly refreshed token was rejected too: the session is gone.
    pub fn reject_access_token(&mut self, token: &str, cx: &mut Context<Self>) {
        self.invalidate_access_token(token);
        self.needs_sign_in = true;
        cx.notify();
    }

    /// Drops a cached token that the server rejected, so the next request refreshes.
    pub fn invalidate_access_token(&mut self, token: &str) {
        if self
            .access
            .as_ref()
            .is_some_and(|access| access.token == token)
        {
            self.access = None;
        }
    }

    /// A Meegle access token for the signed-in account, refreshed from the
    /// keychain when missing or about to expire. Concurrent callers share one refresh.
    pub fn access_token(&mut self, cx: &mut Context<Self>) -> AccessTokenTask {
        let epoch = self.epoch;
        if let Some(access) = &self.access
            && access.epoch == epoch
            && access
                .expires_at
                .is_none_or(|expires_at| expires_at > SystemTime::now() + ACCESS_TOKEN_MARGIN)
        {
            return Task::ready(Ok(access.token.clone())).shared();
        }
        if let Some((refresh_epoch, task)) = &self.refreshing
            && *refresh_epoch == epoch
        {
            return task.clone();
        }
        let Some(account) = self.account.clone() else {
            return Task::ready(Err("Sign in to Meegle first".into())).shared();
        };
        let task = cx
            .spawn(async move |this, cx| {
                let result = refresh_access(&this, &account, epoch, cx).await;
                this.update(cx, |store, cx| {
                    if store.epoch != epoch {
                        return;
                    }
                    store.refreshing = None;
                    match &result {
                        Ok(access) => {
                            store.needs_sign_in = false;
                            store.access = Some(CachedAccess {
                                epoch,
                                token: access.0.clone(),
                                expires_at: Some(access.1.unwrap_or_else(|| {
                                    SystemTime::now() + DEFAULT_ACCESS_TOKEN_LIFETIME
                                })),
                            });
                        }
                        Err(error) if requires_sign_in(error) => store.needs_sign_in = true,
                        Err(_) => {}
                    }
                    cx.notify();
                })
                .ok();
                result
                    .map(|(token, _)| token)
                    .map_err(|error| SharedString::from(format!("{error:#}")))
            })
            .shared();
        self.refreshing = Some((epoch, task.clone()));
        task
    }

    pub fn resource(&self) -> Option<Url> {
        let account = self.account.as_ref()?;
        Url::parse(&account.issuer).ok()?.join("mcp_server/v1").ok()
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
        self.access = None;
        self.refreshing = None;
        self.needs_sign_in = false;
        let Some(account) = self.account.take() else {
            return;
        };
        let persist = self.persist(cx);
        let credential_lock = self.credential_lock.clone();
        cx.spawn(async move |_, cx| {
            persist.await?;
            let _guard = credential_lock.lock().await;
            cx.update(|cx| cx.delete_credentials(&account.credential_url))
                .await
                .context("Vilo could not remove the saved Meegle credentials from the keychain")
        })
        .detach_and_log_err(cx);
        cx.emit(AccountEvent::SignedOut);
        cx.notify();
    }

    fn persist(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        self.persist_sequence += 1;
        let sequence = self.persist_sequence;
        let persisted_sequence = self.persisted_sequence.clone();
        let lock = self.persist_lock.clone();
        let path = self.path.clone();
        let file = AccountFile {
            welcome_seen: self.welcome_seen,
            account: self.account.clone(),
        };
        cx.background_spawn(async move {
            let _guard = lock.lock().await;
            if persisted_sequence.load(Ordering::SeqCst) > sequence {
                return Ok(());
            }
            write_atomically(&path, &serde_json::to_vec_pretty(&file)?)?;
            persisted_sequence.store(sequence, Ordering::SeqCst);
            Ok(())
        })
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
        refresh_token: identity.tokens.refresh_token.clone(),
        token_endpoint: identity.token_endpoint.clone(),
        resource: identity.resource.clone(),
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
            store.needs_sign_in = false;
            store.refreshing = None;
            store.access = Some(CachedAccess {
                epoch: store.epoch,
                token: identity.tokens.access_token.clone(),
                expires_at: Some(
                    identity
                        .tokens
                        .expires_at
                        .unwrap_or_else(|| SystemTime::now() + DEFAULT_ACCESS_TOKEN_LIFETIME),
                ),
            });
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

async fn refresh_access(
    this: &WeakEntity<AccountStore>,
    account: &AccountRecord,
    epoch: u64,
    cx: &mut AsyncApp,
) -> Result<(String, Option<SystemTime>)> {
    let (username, bytes) = cx
        .update(|cx| cx.read_credentials(&account.credential_url))
        .await?
        .ok_or(SignInRequired(
            "The saved Meegle session is missing. Sign in again.",
        ))?;
    let mut credential: StoredCredential = serde_json::from_slice(&bytes)
        .map_err(|_| SignInRequired("Your Meegle session needs to be renewed. Sign in again."))?;
    if credential.issuer != account.issuer || credential.subject != account.subject {
        return Err(SignInRequired("The saved Meegle session belongs to another account.").into());
    }
    let refresh_token = credential.refresh_token.clone().ok_or(SignInRequired(
        "Your Meegle session expired. Sign in again.",
    ))?;
    let tokens = meegle::refresh(
        &credential.token_endpoint,
        &refresh_token,
        &credential.client_id,
        &credential.resource,
        cx,
    )
    .await?;

    let Some(rotated) = tokens
        .refresh_token
        .clone()
        .filter(|token| *token != refresh_token)
    else {
        return Ok((tokens.access_token, tokens.expires_at));
    };
    let credential_lock = this.read_with(cx, |store, _| store.credential_lock.clone())?;
    credential.refresh_token = Some(rotated);
    let bytes = serde_json::to_vec(&credential)?;
    let credential_url = account.credential_url.clone();
    let this = this.clone();
    let (sender, receiver) = futures::channel::oneshot::channel();
    // Detached so that cancelling the refresh cannot release the lock while the
    // keychain write is still running; sign-out's delete waits on the same lock.
    cx.spawn(async move |cx| {
        let _guard = credential_lock.lock().await;
        let result = async {
            if this.read_with(cx, |store, _| store.epoch)? != epoch {
                return Err(anyhow!("signed out while renewing the Meegle session"));
            }
            let stored = cx
                .update(|cx| cx.read_credentials(&credential_url))
                .await?
                .ok_or(SignInRequired(
                    "You signed out of Meegle in another window.",
                ))?;
            // Another window in this process renewed first; keep its token. Separate
            // Vilo processes are not coordinated: if two rotate at once, the loser's
            // next refresh gets invalid_grant and asks the user to sign in again.
            let stored_token = serde_json::from_slice::<StoredCredential>(&stored.1)
                .ok()
                .and_then(|stored| stored.refresh_token);
            if stored_token.is_some_and(|stored_token| stored_token != refresh_token) {
                return Ok(());
            }
            cx.update(|cx| cx.write_credentials(&credential_url, &username, &bytes))
                .await
                .map_err(|_| {
                    // The old refresh token is spent after rotation, so this cannot be retried.
                    SignInRequired("Vilo could not save the renewed Meegle session. Sign in again.")
                        .into()
                })
        }
        .await;
        sender.send(result).ok();
    })
    .detach();
    receiver
        .await
        .map_err(|_| anyhow!("renewing the Meegle session was interrupted"))??;
    Ok((tokens.access_token, tokens.expires_at))
}

fn requires_sign_in(error: &anyhow::Error) -> bool {
    error.downcast_ref::<SignInRequired>().is_some()
        || error
            .downcast_ref::<OAuthTokenError>()
            .is_some_and(|error| error.error == "invalid_grant")
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
