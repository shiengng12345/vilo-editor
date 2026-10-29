use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use futures::{AsyncReadExt as _, lock::Mutex};
use http_client::{AsyncBody, HttpClient, HttpRequestExt as _, RedirectPolicy, Request, Url, http::Method};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

/// The Argo CD root a connection is bound to; requests never leave it.
pub fn normalize_base(address: &str) -> Result<Url> {
    let mut url = Url::parse(address.trim()).context("Enter an address like https://argocd.example.com/")?;
    if url.scheme() != "https" || url.host_str().is_none() {
        bail!("Argo CD connections must use https://");
    }
    url.set_query(None);
    url.set_fragment(None);
    // Pasted page URLs keep only what precedes Argo's own paths.
    let path = url.path().to_string();
    let base = ["/applications", "/settings", "/user-info", "/login", "/api/"]
        .iter()
        .filter_map(|marker| path.find(marker))
        .min()
        .map_or(path.as_str(), |index| &path[..index]);
    url.set_path(&format!("{}/", base.trim_end_matches('/')));
    Ok(url)
}

/// How the saved secret is used. Chosen by the user, never guessed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMode {
    /// Exchanged for a session token; used again only when that session expires.
    Password,
    /// Sent as is; never offered to the password login.
    Token,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArgoError {
    Unauthorized,
    Forbidden,
    NotFound,
    Redirected,
    TooLarge,
    Status(u16),
    Network(String),
    Unexpected,
}

impl std::fmt::Display for ArgoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArgoError::Unauthorized => f.write_str("Argo CD rejected the credentials. Edit the connection to sign in again."),
            ArgoError::Forbidden => f.write_str("This account has no permission for that."),
            ArgoError::NotFound => f.write_str("Not found on Argo CD."),
            ArgoError::Redirected => f.write_str("Argo CD redirected the request; Vilo did not follow it. Check the address."),
            ArgoError::TooLarge => f.write_str(
                "Too many applications to load at once. Limit the connection to some projects.",
            ),
            ArgoError::Status(status) => write!(f, "Argo CD answered with status {status}."),
            ArgoError::Network(reason) => write!(f, "Could not reach Argo CD: {reason}"),
            ArgoError::Unexpected => f.write_str("Argo CD returned an unexpected response."),
        }
    }
}

impl std::error::Error for ArgoError {}

/// One application row. Name is unique only within its namespace.
#[derive(Debug, Clone, PartialEq)]
pub struct AppSummary {
    pub name: String,
    pub namespace: String,
    pub project: String,
    pub destination: String,
    pub sync: String,
    pub health: String,
    pub images: Vec<String>,
    pub operation: Option<String>,
    pub deleting: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub repo: String,
    pub path: String,
    pub target_revision: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEntry {
    pub id: u64,
    pub revisions: Vec<String>,
    pub deployed_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppDetail {
    pub summary: AppSummary,
    pub sources: Vec<Source>,
    /// Synced revision per source.
    pub revisions: Vec<String>,
    pub history: Vec<HistoryEntry>,
    pub conditions: Vec<(String, String)>,
    pub operation_message: Option<String>,
    pub reconciled_at: Option<String>,
    /// Argo's own automated sync; `None` when it is off.
    pub auto_sync: Option<AutoSync>,
    pub out_of_sync: usize,
    /// Another application that keeps this one's definition in Git, so Argo may undo edits to it.
    pub managed_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutoSync {
    pub prune: bool,
    pub self_heal: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Resource {
    pub uid: String,
    pub group: String,
    pub version: String,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub health: Option<String>,
    pub message: Option<String>,
    pub parents: Vec<String>,
    /// Argo's summary lines, e.g. "Status Reason" = "Running", "Containers" = "1/1".
    pub info: Vec<(String, String)>,
    pub images: Vec<String>,
    pub created_at: Option<String>,
}

impl Resource {
    pub fn info(&self, name: &str) -> Option<&str> {
        self.info.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

/// A workload (Deployment, StatefulSet, …) and the pods it runs.
#[derive(Debug, Clone, PartialEq)]
pub struct Workload {
    pub resource: Option<Resource>,
    pub pods: Vec<Resource>,
}

/// Groups pods under the workload they belong to, following parent links
/// (Pod → ReplicaSet → Deployment). Pods without one are grouped last.
pub fn workloads(resources: &[Resource]) -> Vec<Workload> {
    let by_uid = resources.iter().map(|resource| (resource.uid.as_str(), resource)).collect::<collections::HashMap<_, _>>();
    let owner = |pod: &Resource| {
        let mut current = pod;
        // Climb at most a few levels; trees are shallow and this guards against cycles.
        for _ in 0..4 {
            let parent = current.parents.iter().find_map(|uid| by_uid.get(uid.as_str()).copied())?;
            if matches!(parent.kind.as_str(), "Deployment" | "StatefulSet" | "DaemonSet" | "Job" | "CronJob" | "Rollout") {
                return Some(parent.uid.clone());
            }
            current = parent;
        }
        None
    };
    let mut groups: Vec<Workload> = resources
        .iter()
        .filter(|resource| matches!(resource.kind.as_str(), "Deployment" | "StatefulSet" | "DaemonSet" | "Rollout"))
        .map(|resource| Workload { resource: Some(resource.clone()), pods: Vec::new() })
        .collect();
    let mut loose = Vec::new();
    for pod in resources.iter().filter(|resource| resource.kind == "Pod") {
        let group = owner(pod).and_then(|uid| {
            groups.iter_mut().find(|group| group.resource.as_ref().is_some_and(|resource| resource.uid == uid))
        });
        match group {
            Some(group) => group.pods.push(pod.clone()),
            None => loose.push(pod.clone()),
        }
    }
    for group in &mut groups {
        group.pods.sort_by(|a, b| a.name.cmp(&b.name));
    }
    // Workloads with problems first.
    groups.sort_by_key(|group| {
        let healthy = group.resource.as_ref().is_some_and(|resource| resource.health.as_deref() == Some("Healthy"))
            && group.pods.iter().all(|pod| pod.health.as_deref() == Some("Healthy"));
        (healthy, group.resource.as_ref().map(|resource| resource.name.clone()))
    });
    if !loose.is_empty() {
        groups.push(Workload { resource: None, pods: loose });
    }
    groups
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub kind: String,
    pub reason: String,
    pub message: String,
    pub count: u64,
    pub first_seen: String,
    pub last_seen: String,
}

/// An action Argo offers for a resource, e.g. "restart" on a Deployment.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceAction {
    pub name: String,
    pub disabled: bool,
    pub has_params: bool,
}

/// Why a write did not clearly succeed. Writes are sent once and never repeated by Vilo.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteError {
    /// Argo refused it; nothing was changed. Carries Argo's own reason, e.g. a sync window or RBAC.
    Rejected(String),
    /// The session had expired, so the request was not accepted.
    SignInAgain,
    /// The answer was lost; Argo may or may not have done it.
    Unknown(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Rejected(reason) => write!(f, "Argo CD refused: {reason}"),
            WriteError::SignInAgain => f.write_str("The session had expired, so nothing was done. Press again to retry."),
            WriteError::Unknown(reason) => {
                write!(f, "Argo CD did not confirm it ({reason}). It may have run; check the pods before trying again.")
            }
        }
    }
}

#[derive(Clone)]
pub struct ArgoClient {
    http: Arc<dyn HttpClient>,
    base: Url,
    username: String,
    mode: AuthMode,
    secret: String,
    /// The bearer in use. Shared so concurrent requests re-login only once.
    bearer: Arc<Mutex<Option<String>>>,
}

fn segment(text: &str) -> String {
    utf8_percent_encode(text, NON_ALPHANUMERIC).to_string()
}

fn app_path(namespace: &str, name: &str, suffix: &str) -> String {
    let query = if namespace.is_empty() { String::new() } else { format!("appNamespace={}", segment(namespace)) };
    let joiner = if suffix.contains('?') { "&" } else { "?" };
    let path = format!("api/v1/applications/{}{suffix}", segment(name));
    if query.is_empty() { path } else { format!("{path}{joiner}{query}") }
}

impl ArgoClient {
    pub fn new(http: Arc<dyn HttpClient>, base: Url, username: &str, mode: AuthMode, secret: &str) -> Self {
        let bearer = (mode == AuthMode::Token).then(|| secret.to_string());
        Self {
            http,
            base,
            username: username.to_string(),
            mode,
            secret: secret.to_string(),
            bearer: Arc::new(Mutex::new(bearer)),
        }
    }

    /// Signs in if needed and checks Argo knows this user as `username`.
    pub async fn verify(&self) -> Result<String, ArgoError> {
        let body = self.get("api/v1/session/userinfo").await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        let logged_in = value.get("loggedIn").and_then(|logged| logged.as_bool()) == Some(true);
        let username = value.get("username").and_then(|name| name.as_str()).unwrap_or_default();
        // HTTP success alone does not mean the token was accepted.
        if !logged_in || username != self.username {
            return Err(ArgoError::Unauthorized);
        }
        let version = self.get("api/version").await?;
        let version: serde_json::Value = serde_json::from_str(&version).map_err(|_| ArgoError::Unexpected)?;
        Ok(version.get("Version").and_then(|version| version.as_str()).unwrap_or_default().to_string())
    }

    pub async fn applications(&self, projects: &[String]) -> Result<Vec<AppSummary>, ArgoError> {
        let mut path = "api/v1/applications?fields=items.metadata.name,items.metadata.namespace,items.metadata.deletionTimestamp,\
            items.spec.project,items.spec.destination,items.status.sync.status,items.status.health.status,\
            items.status.summary.images,items.status.operationState.phase"
            .to_string();
        for project in projects {
            path.push_str(&format!("&projects={}", segment(project)));
        }
        let body = self.get(&path).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        Ok(value
            .get("items")
            .and_then(|items| items.as_array())
            .map(|items| items.iter().filter_map(parse_summary).collect())
            .unwrap_or_default())
    }

    pub async fn application(&self, namespace: &str, name: &str) -> Result<AppDetail, ArgoError> {
        let body = self.get(&app_path(namespace, name, "")).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        parse_detail(&value).ok_or(ArgoError::Unexpected)
    }

    pub async fn resources(&self, namespace: &str, name: &str) -> Result<Vec<Resource>, ArgoError> {
        let body = self.get(&app_path(namespace, name, "/resource-tree")).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        let mut resources = value
            .get("nodes")
            .and_then(|nodes| nodes.as_array())
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|node| Resource {
                        uid: text(node, "/uid"),
                        group: text(node, "/group"),
                        version: text(node, "/version"),
                        kind: text(node, "/kind"),
                        name: text(node, "/name"),
                        namespace: text(node, "/namespace"),
                        health: node.pointer("/health/status").and_then(|status| status.as_str()).map(str::to_string),
                        message: node.pointer("/health/message").and_then(|message| message.as_str()).map(str::to_string),
                        parents: node
                            .get("parentRefs")
                            .and_then(|parents| parents.as_array())
                            .map(|parents| parents.iter().map(|parent| text(parent, "/uid")).collect())
                            .unwrap_or_default(),
                        info: node
                            .get("info")
                            .and_then(|info| info.as_array())
                            .map(|info| info.iter().map(|item| (text(item, "/name"), text(item, "/value"))).collect())
                            .unwrap_or_default(),
                        images: strings(node.get("images")),
                        created_at: node.get("createdAt").and_then(|time| time.as_str()).map(str::to_string),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        // Problems first.
        resources.sort_by_key(|resource| {
            (
                matches!(resource.health.as_deref(), Some("Healthy") | None),
                resource.kind.clone(),
                resource.name.clone(),
            )
        });
        Ok(resources)
    }

    pub async fn events(&self, namespace: &str, name: &str) -> Result<Vec<Event>, ArgoError> {
        self.events_at(&app_path(namespace, name, "/events")).await
    }

    /// Events about one resource of the application, e.g. a pod.
    pub async fn resource_events(&self, namespace: &str, name: &str, resource: &Resource) -> Result<Vec<Event>, ArgoError> {
        let suffix = format!(
            "/events?resourceNamespace={}&resourceName={}&resourceUID={}",
            segment(&resource.namespace),
            segment(&resource.name),
            segment(&resource.uid)
        );
        self.events_at(&app_path(namespace, name, &suffix)).await
    }

    /// The live object as the cluster has it, through Argo.
    pub async fn pod_manifest(&self, namespace: &str, name: &str, pod: &Resource) -> Result<serde_json::Value, ArgoError> {
        let suffix = format!(
            "/resource?namespace={}&resourceName={}&version=v1&kind=Pod&group=",
            segment(&pod.namespace),
            segment(&pod.name)
        );
        let body = self.get(&app_path(namespace, name, &suffix)).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        let manifest = value.get("manifest").and_then(|manifest| manifest.as_str()).ok_or(ArgoError::Unexpected)?;
        serde_json::from_str(manifest).map_err(|_| ArgoError::Unexpected)
    }

    async fn events_at(&self, path: &str) -> Result<Vec<Event>, ArgoError> {
        let body = self.get(path).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        let mut events = value
            .get("items")
            .and_then(|items| items.as_array())
            .map(|items| {
                items
                    .iter()
                    .map(|event| Event {
                        kind: text(event, "/type"),
                        reason: text(event, "/reason"),
                        message: text(event, "/message"),
                        count: event.get("count").and_then(|count| count.as_u64()).unwrap_or(1),
                        first_seen: event
                            .get("firstTimestamp")
                            .or_else(|| event.get("eventTime"))
                            .and_then(|time| time.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        last_seen: event
                            .get("lastTimestamp")
                            .or_else(|| event.get("eventTime"))
                            .and_then(|time| time.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        events.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
        Ok(events)
    }

    /// The last lines a pod's container wrote, through Argo (no cluster access needed).
    pub async fn pod_logs(
        &self,
        namespace: &str,
        name: &str,
        pod: &Resource,
        container: Option<&str>,
        tail_lines: u32,
        previous: bool,
    ) -> Result<Vec<LogLine>, ArgoError> {
        let mut suffix = format!(
            "/logs?namespace={}&podName={}&tailLines={tail_lines}&follow=false&previous={previous}",
            segment(&pod.namespace),
            segment(&pod.name)
        );
        if let Some(container) = container {
            suffix.push_str(&format!("&container={}", segment(container)));
        }
        let body = self.get(&app_path(namespace, name, &suffix)).await?;
        Ok(parse_log_lines(&body))
    }

    pub async fn resource_actions(&self, namespace: &str, name: &str, resource: &Resource) -> Result<Vec<ResourceAction>, ArgoError> {
        let suffix = format!("/resource/actions?{}", resource_query(resource));
        let body = self.get(&app_path(namespace, name, &suffix)).await?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        Ok(parse_actions(&value))
    }

    /// Runs a resource action once. Never retried: a lost answer is reported as unknown.
    pub async fn run_action(&self, namespace: &str, name: &str, resource: &Resource, action: &str) -> Result<(), WriteError> {
        let body = serde_json::json!({
            "name": name,
            "appNamespace": namespace,
            "namespace": resource.namespace,
            "resourceName": resource.name,
            "group": resource.group,
            "version": resource.version,
            "kind": resource.kind,
            "action": action,
        });
        let path = format!("api/v1/applications/{}/resource/actions/v2", segment(name));
        self.write_once(Method::POST, &path, &body).await
    }

    /// Syncs to the app's own target revisions (each source's, for multi-source apps) with its own sync options.
    pub async fn sync(&self, namespace: &str, name: &str, project: &str, prune: bool) -> Result<(), WriteError> {
        let body = serde_json::json!({ "name": name, "appNamespace": namespace, "project": project, "prune": prune });
        let path = format!("api/v1/applications/{}/sync", segment(name));
        self.write_once(Method::POST, &path, &body).await
    }

    /// Switches Argo's automated sync. Prune and self-heal keep whatever the app has set.
    pub async fn set_auto_sync(&self, namespace: &str, name: &str, project: &str, enabled: bool) -> Result<(), WriteError> {
        let patch = serde_json::json!({ "spec": { "syncPolicy": { "automated": { "enabled": enabled } } } });
        let body = serde_json::json!({
            "name": name,
            "appNamespace": namespace,
            "project": project,
            "patchType": "merge",
            "patch": patch.to_string(),
        });
        let path = format!("api/v1/applications/{}", segment(name));
        self.write_once(Method::PATCH, &path, &body).await
    }

    async fn write_once(&self, method: Method, path: &str, body: &serde_json::Value) -> Result<(), WriteError> {
        let unknown = |error: ArgoError| WriteError::Unknown(error.to_string());
        let current = {
            let mut bearer = self.bearer.lock().await;
            // Signing in before the write is safe: nothing has been sent yet.
            if bearer.is_none() && self.mode == AuthMode::Password {
                *bearer = Some(self.login().await.map_err(|error| WriteError::Rejected(error.to_string()))?);
            }
            bearer.clone()
        };
        let url = self.url(path).map_err(|error| WriteError::Rejected(error.to_string()))?;
        let mut request = Request::builder()
            .method(method)
            .uri(url.as_str())
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        if let Some(bearer) = &current {
            request = request.header("Authorization", format!("Bearer {bearer}"));
        }
        let request = request
            .body(AsyncBody::from(body.to_string()))
            .map_err(|_| WriteError::Rejected("could not build the request".into()))?;
        let (status, body) = match self.send(request).await {
            Ok(answer) => answer,
            // A redirect is an answer: Argo did not run it.
            Err(ArgoError::Redirected) => return Err(WriteError::Rejected(ArgoError::Redirected.to_string())),
            Err(error) => return Err(unknown(error)),
        };
        match status {
            200..=299 => Ok(()),
            401 => {
                if self.mode == AuthMode::Password {
                    let mut bearer = self.bearer.lock().await;
                    if *bearer == current {
                        // The next press signs in again; this one is not repeated.
                        *bearer = None;
                    }
                    Err(WriteError::SignInAgain)
                } else {
                    Err(WriteError::Rejected(ArgoError::Unauthorized.to_string()))
                }
            }
            // Argo may have applied part of it before failing, and gateways answer these when it may still run.
            500 | 502..=504 => Err(WriteError::Unknown(format!("status {status}"))),
            status => Err(WriteError::Rejected(error_message(&body).unwrap_or_else(|| format!("status {status}")))),
        }
    }

    /// Asks Argo to compare the app with Git again. Argo may still be working
    /// when this returns or times out, so the caller reports it as requested.
    pub async fn refresh(&self, namespace: &str, name: &str, hard: bool) -> Result<(), ArgoError> {
        let mode = if hard { "hard" } else { "normal" };
        self.get(&app_path(namespace, name, &format!("?refresh={mode}"))).await.map(|_| ())
    }

    async fn login(&self) -> Result<String, ArgoError> {
        let url = self.url("api/v1/session")?;
        let body = serde_json::to_vec(&serde_json::json!({ "username": self.username, "password": self.secret }))
            .map_err(|_| ArgoError::Unexpected)?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .header("Content-Type", "application/json")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT)
            .body(AsyncBody::from(body))
            .map_err(|_| ArgoError::Unexpected)?;
        let (status, body) = self.send(request).await?;
        match status {
            200..=299 => {}
            401 | 403 => return Err(ArgoError::Unauthorized),
            status => return Err(ArgoError::Status(status)),
        }
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|_| ArgoError::Unexpected)?;
        value
            .get("token")
            .and_then(|token| token.as_str())
            .map(str::to_string)
            .ok_or(ArgoError::Unexpected)
    }

    /// GET with the current bearer. A password connection signs in again once
    /// when its session expired; a token connection never does.
    async fn get(&self, path: &str) -> Result<String, ArgoError> {
        let current = {
            let mut bearer = self.bearer.lock().await;
            if bearer.is_none() && self.mode == AuthMode::Password {
                *bearer = Some(self.login().await?);
            }
            bearer.clone()
        };
        match self.get_with(path, current.as_deref()).await {
            Err(ArgoError::Unauthorized) if self.mode == AuthMode::Password => {
                let renewed = {
                    let mut bearer = self.bearer.lock().await;
                    // Another request may have renewed it while this one waited.
                    if *bearer == current {
                        *bearer = Some(self.login().await?);
                    }
                    bearer.clone()
                };
                self.get_with(path, renewed.as_deref()).await
            }
            result => result,
        }
    }

    fn url(&self, path: &str) -> Result<Url, ArgoError> {
        let url = self.base.join(path).map_err(|_| ArgoError::Unexpected)?;
        if url.origin() != self.base.origin() || !url.path().starts_with(self.base.path()) {
            return Err(ArgoError::Unexpected);
        }
        Ok(url)
    }

    async fn get_with(&self, path: &str, bearer: Option<&str>) -> Result<String, ArgoError> {
        let url = self.url(path)?;
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header("Accept", "application/json")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        if let Some(bearer) = bearer {
            request = request.header("Authorization", format!("Bearer {bearer}"));
        }
        let request = request.body(AsyncBody::empty()).map_err(|_| ArgoError::Unexpected)?;
        let (status, body) = self.send(request).await?;
        match status {
            200..=299 => Ok(body),
            401 => Err(ArgoError::Unauthorized),
            403 => Err(ArgoError::Forbidden),
            404 => Err(ArgoError::NotFound),
            status => Err(ArgoError::Status(status)),
        }
    }

    async fn send(&self, request: Request<AsyncBody>) -> Result<(u16, String), ArgoError> {
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| ArgoError::Network(error.to_string().chars().take(200).collect()))?;
        let status = response.status();
        if status.is_redirection() {
            return Err(ArgoError::Redirected);
        }
        let mut bytes = Vec::new();
        response
            .body_mut()
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| ArgoError::Unexpected)?;
        // A cut-off list must not look like a short one.
        if bytes.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(ArgoError::TooLarge);
        }
        Ok((status.as_u16(), String::from_utf8_lossy(&bytes).into_owned()))
    }
}

/// A container's state, from a pod manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ContainerState {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub restarts: u64,
    /// "running", "waiting: CrashLoopBackOff", "terminated: Error (exit 1)" …
    pub state: String,
    pub message: Option<String>,
}

pub fn container_states(manifest: &serde_json::Value) -> Vec<ContainerState> {
    let statuses = manifest.pointer("/status/containerStatuses").and_then(|statuses| statuses.as_array());
    manifest
        .pointer("/spec/containers")
        .and_then(|containers| containers.as_array())
        .map(|containers| {
            containers
                .iter()
                .map(|container| {
                    let name = text(container, "/name");
                    let status = statuses.and_then(|statuses| statuses.iter().find(|status| text(status, "/name") == name));
                    let (state, message) = match status.and_then(|status| status.get("state")) {
                        Some(state) if state.get("running").is_some() => ("running".to_string(), None),
                        Some(state) if state.get("waiting").is_some() => (
                            format!("waiting: {}", text(state, "/waiting/reason")),
                            state.pointer("/waiting/message").and_then(|message| message.as_str()).map(str::to_string),
                        ),
                        Some(state) if state.get("terminated").is_some() => (
                            format!(
                                "terminated: {} (exit {})",
                                text(state, "/terminated/reason"),
                                state.pointer("/terminated/exitCode").and_then(|code| code.as_i64()).unwrap_or(0)
                            ),
                            state.pointer("/terminated/message").and_then(|message| message.as_str()).map(str::to_string),
                        ),
                        _ => ("unknown".to_string(), None),
                    };
                    ContainerState {
                        image: text(container, "/image"),
                        ready: status.and_then(|status| status.get("ready")).and_then(|ready| ready.as_bool()).unwrap_or(false),
                        restarts: status.and_then(|status| status.get("restartCount")).and_then(|count| count.as_u64()).unwrap_or(0),
                        state,
                        message,
                        name,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

pub struct LogLine {
    /// When the container wrote the line, as RFC 3339; empty if Argo did not say.
    pub time: String,
    pub content: String,
}

/// Argo streams logs as one JSON object per line: `{"result":{"content":…,"timeStampStr":…}}`.
fn parse_log_lines(body: &str) -> Vec<LogLine> {
    body.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|entry| entry.pointer("/result/last").and_then(|last| last.as_bool()) != Some(true))
        .filter_map(|entry| {
            let content = entry.pointer("/result/content")?.as_str()?.to_string();
            let time = ["/result/timeStampStr", "/result/timeStamp"]
                .iter()
                .find_map(|pointer| entry.pointer(pointer).and_then(|time| time.as_str()))
                .unwrap_or_default()
                .to_string();
            Some(LogLine { time, content })
        })
        .collect()
}

fn resource_query(resource: &Resource) -> String {
    format!(
        "namespace={}&resourceName={}&group={}&version={}&kind={}",
        segment(&resource.namespace),
        segment(&resource.name),
        segment(&resource.group),
        segment(&resource.version),
        segment(&resource.kind)
    )
}

fn parse_actions(value: &serde_json::Value) -> Vec<ResourceAction> {
    value
        .get("actions")
        .and_then(|actions| actions.as_array())
        .map(|actions| {
            actions
                .iter()
                .map(|action| ResourceAction {
                    name: text(action, "/name"),
                    disabled: action.get("disabled").and_then(|disabled| disabled.as_bool()).unwrap_or(false),
                    has_params: action.get("params").and_then(|params| params.as_array()).is_some_and(|params| !params.is_empty()),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Argo explains refusals as `{"error": …, "message": …}`.
fn error_message(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    ["/message", "/error"]
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(|message| message.as_str()))
        .filter(|message| !message.is_empty())
        .map(|message| message.chars().take(300).collect())
}

fn text(value: &serde_json::Value, pointer: &str) -> String {
    value.pointer(pointer).and_then(|text| text.as_str()).unwrap_or_default().to_string()
}

fn status_or_unknown(value: &serde_json::Value, pointer: &str) -> String {
    match value.pointer(pointer).and_then(|status| status.as_str()) {
        Some(status) if !status.is_empty() => status.to_string(),
        _ => "Unknown".to_string(),
    }
}

fn parse_summary(app: &serde_json::Value) -> Option<AppSummary> {
    let name = app.pointer("/metadata/name")?.as_str()?.to_string();
    let destination = {
        let server = app
            .pointer("/spec/destination/name")
            .or_else(|| app.pointer("/spec/destination/server"))
            .and_then(|server| server.as_str())
            .unwrap_or_default();
        let namespace = text(app, "/spec/destination/namespace");
        if namespace.is_empty() { server.to_string() } else { format!("{server} · {namespace}") }
    };
    Some(AppSummary {
        name,
        namespace: text(app, "/metadata/namespace"),
        project: text(app, "/spec/project"),
        destination,
        sync: status_or_unknown(app, "/status/sync/status"),
        health: status_or_unknown(app, "/status/health/status"),
        images: app
            .pointer("/status/summary/images")
            .and_then(|images| images.as_array())
            .map(|images| images.iter().filter_map(|image| image.as_str().map(str::to_string)).collect())
            .unwrap_or_default(),
        operation: app.pointer("/status/operationState/phase").and_then(|phase| phase.as_str()).map(str::to_string),
        deleting: app.pointer("/metadata/deletionTimestamp").is_some_and(|time| !time.is_null()),
    })
}

fn strings(value: Option<&serde_json::Value>) -> Vec<String> {
    match value {
        Some(serde_json::Value::String(text)) => vec![text.clone()],
        Some(serde_json::Value::Array(items)) => items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    }
}

fn parse_detail(app: &serde_json::Value) -> Option<AppDetail> {
    let summary = parse_summary(app)?;
    // Single-source apps use `source`/`revision`; multi-source ones the plural forms.
    let source_values = match app.pointer("/spec/sources").and_then(|sources| sources.as_array()) {
        Some(sources) if !sources.is_empty() => sources.clone(),
        _ => app.pointer("/spec/source").cloned().into_iter().collect(),
    };
    let sources = source_values
        .iter()
        .map(|source| Source {
            repo: text(source, "/repoURL"),
            path: source
                .get("path")
                .or_else(|| source.get("chart"))
                .and_then(|path| path.as_str())
                .unwrap_or_default()
                .to_string(),
            target_revision: text(source, "/targetRevision"),
        })
        .collect();
    let mut revisions = strings(app.pointer("/status/sync/revisions"));
    if revisions.is_empty() {
        revisions = strings(app.pointer("/status/sync/revision"));
    }
    let mut history = app
        .pointer("/status/history")
        .and_then(|history| history.as_array())
        .map(|history| {
            history
                .iter()
                .map(|entry| {
                    let mut revisions = strings(entry.get("revisions"));
                    if revisions.is_empty() {
                        revisions = strings(entry.get("revision"));
                    }
                    HistoryEntry {
                        id: entry.get("id").and_then(|id| id.as_u64()).unwrap_or(0),
                        revisions,
                        deployed_at: text(entry, "/deployedAt"),
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    history.sort_by_key(|entry| std::cmp::Reverse(entry.id));
    Some(AppDetail {
        summary,
        sources,
        revisions,
        history,
        conditions: app
            .pointer("/status/conditions")
            .and_then(|conditions| conditions.as_array())
            .map(|conditions| conditions.iter().map(|condition| (text(condition, "/type"), text(condition, "/message"))).collect())
            .unwrap_or_default(),
        operation_message: app.pointer("/status/operationState/message").and_then(|message| message.as_str()).map(str::to_string),
        reconciled_at: app.pointer("/status/reconciledAt").and_then(|time| time.as_str()).map(str::to_string),
        auto_sync: app
            .pointer("/spec/syncPolicy/automated")
            .filter(|automated| automated.is_object())
            // Argo 3 can keep the settings while `enabled: false` switches it off.
            .filter(|automated| automated.get("enabled").and_then(|enabled| enabled.as_bool()) != Some(false))
            .map(|automated| AutoSync {
                prune: automated.get("prune").and_then(|prune| prune.as_bool()).unwrap_or(false),
                self_heal: automated.get("selfHeal").and_then(|heal| heal.as_bool()).unwrap_or(false),
            }),
        out_of_sync: app
            .pointer("/status/resources")
            .and_then(|resources| resources.as_array())
            .map_or(0, |resources| resources.iter().filter(|resource| text(resource, "/status") == "OutOfSync").count()),
        managed_by: managed_by(app),
    })
}

/// The parent app from Argo's tracking annotation ("parent:group/Kind:ns/name") or instance label.
fn managed_by(app: &serde_json::Value) -> Option<String> {
    let own = text(app, "/metadata/name");
    let tracking = app
        .pointer("/metadata/annotations/argocd.argoproj.io~1tracking-id")
        .and_then(|id| id.as_str())
        .and_then(|id| id.split(':').next());
    let label = app.pointer("/metadata/labels/app.kubernetes.io~1instance").and_then(|label| label.as_str());
    tracking.or(label).filter(|parent| !parent.is_empty() && *parent != own).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_normalize_base() {
        assert_eq!(
            normalize_base("https://argocd.example.com/applications/argocd/x?view=tree").unwrap().as_str(),
            "https://argocd.example.com/"
        );
        assert_eq!(normalize_base("https://ci.example.com/argo/settings").unwrap().as_str(), "https://ci.example.com/argo/");
        assert!(normalize_base("http://argocd.example.com").is_err());
    }

    #[test]
    fn test_app_paths_carry_the_namespace() {
        assert_eq!(app_path("team-a", "web", "/resource-tree"), "api/v1/applications/web/resource-tree?appNamespace=team%2Da");
        assert_eq!(
            app_path("argocd", "web", "?refresh=hard"),
            "api/v1/applications/web?refresh=hard&appNamespace=argocd"
        );
        assert_eq!(app_path("", "a/b", ""), "api/v1/applications/a%2Fb");
    }

    #[test]
    fn test_pods_group_under_their_workload() {
        let node = |uid: &str, kind: &str, name: &str, parent: Option<&str>, health: &str| Resource {
            uid: uid.into(),
            group: String::new(),
            version: "v1".into(),
            kind: kind.into(),
            name: name.into(),
            namespace: "ns".into(),
            health: Some(health.into()),
            message: None,
            parents: parent.map(|parent| vec![parent.to_string()]).unwrap_or_default(),
            info: Vec::new(),
            images: Vec::new(),
            created_at: None,
        };
        let resources = vec![
            node("d1", "Deployment", "auth", None, "Healthy"),
            node("r1", "ReplicaSet", "auth-1", Some("d1"), "Healthy"),
            node("p1", "Pod", "auth-1-a", Some("r1"), "Healthy"),
            node("d2", "Deployment", "web", None, "Degraded"),
            node("r2", "ReplicaSet", "web-1", Some("d2"), "Degraded"),
            node("p2", "Pod", "web-1-a", Some("r2"), "Degraded"),
            node("p3", "Pod", "lonely", None, "Healthy"),
        ];
        let groups = workloads(&resources);
        // The unhealthy workload comes first; loose pods last.
        assert_eq!(groups[0].resource.as_ref().unwrap().name, "web");
        assert_eq!(groups[0].pods[0].name, "web-1-a");
        assert_eq!(groups[1].pods[0].name, "auth-1-a");
        assert!(groups[2].resource.is_none());
        assert_eq!(groups[2].pods[0].name, "lonely");
    }

    #[test]
    fn test_container_states() {
        let manifest = json!({
            "spec": {"containers": [{"name": "app", "image": "r/app:v1"}, {"name": "side", "image": "r/side:v2"}]},
            "status": {"containerStatuses": [
                {"name": "app", "ready": true, "restartCount": 0, "state": {"running": {"startedAt": "t"}}},
                {"name": "side", "ready": false, "restartCount": 4, "state": {"waiting": {"reason": "CrashLoopBackOff", "message": "back-off"}}}
            ]}
        });
        let states = container_states(&manifest);
        assert_eq!((states[0].state.as_str(), states[0].ready), ("running", true));
        assert_eq!(states[1].state, "waiting: CrashLoopBackOff");
        assert_eq!((states[1].restarts, states[1].message.as_deref()), (4, Some("back-off")));
    }

    #[test]
    fn test_parse_actions_and_errors() {
        let value = serde_json::json!({"actions": [
            {"name": "restart"},
            {"name": "pause", "disabled": true},
            {"name": "scale", "params": [{"name": "replicas"}]}
        ]});
        let actions = parse_actions(&value);
        assert_eq!((actions[0].name.as_str(), actions[0].disabled, actions[0].has_params), ("restart", false, false));
        assert!(actions[1].disabled);
        assert!(actions[2].has_params);
        assert_eq!(error_message(r#"{"error":"x","message":"blocked by sync window"}"#).as_deref(), Some("blocked by sync window"));
        assert_eq!(error_message("<html>"), None);
    }

    #[test]
    fn test_parse_log_lines() {
        let body = "{\"result\":{\"content\":\"started\",\"timeStamp\":\"t\"}}\n{\"result\":{\"content\":\"ok\"}}\n{\"result\":{\"content\":\"\",\"last\":true}}\n";
        let lines = parse_log_lines(body);
        let lines = lines.iter().map(|line| (line.time.as_str(), line.content.as_str())).collect::<Vec<_>>();
        assert_eq!(lines, vec![("t", "started"), ("", "ok")]);
    }

    #[test]
    fn test_parse_detail_single_and_multi_source() {
        let single = json!({
            "metadata": {"name": "web", "namespace": "argocd"},
            "spec": {"project": "p", "destination": {"server": "https://k8s", "namespace": "uat"},
                     "source": {"repoURL": "git@x:r.git", "path": "charts/web", "targetRevision": "main"}},
            "status": {"sync": {"status": "OutOfSync", "revision": "abc"}, "health": {"status": "Degraded"},
                       "history": [{"id": 1, "revision": "aaa", "deployedAt": "t1"}, {"id": 2, "revision": "abc", "deployedAt": "t2"}],
                       "conditions": [{"type": "ComparisonError", "message": "boom"}]}
        });
        let detail = parse_detail(&single).unwrap();
        assert_eq!((detail.summary.sync.as_str(), detail.summary.health.as_str()), ("OutOfSync", "Degraded"));
        assert_eq!(detail.revisions, vec!["abc".to_string()]);
        assert_eq!(detail.history[0].id, 2);
        assert_eq!(detail.conditions[0].0, "ComparisonError");
        assert_eq!(detail.summary.destination, "https://k8s · uat");

        let multi = json!({
            "metadata": {"name": "web", "namespace": "argocd", "deletionTimestamp": "now"},
            "spec": {"sources": [{"repoURL": "a", "chart": "c", "targetRevision": "1"}, {"repoURL": "b", "path": "p", "targetRevision": "2"}]},
            "status": {"sync": {"revisions": ["r1", "r2"]}, "history": [{"id": 3, "revisions": ["r1", "r2"]}]}
        });
        let detail = parse_detail(&multi).unwrap();
        assert_eq!(detail.sources.len(), 2);
        assert_eq!(detail.revisions, vec!["r1".to_string(), "r2".to_string()]);
        assert_eq!(detail.history[0].revisions.len(), 2);
        assert!(detail.summary.deleting);
        assert_eq!(detail.summary.health, "Unknown");
        assert_eq!(detail.auto_sync, None);

        let automated = json!({
            "metadata": {"name": "web", "annotations": {"argocd.argoproj.io/tracking-id": "apps:argoproj.io/Application:argocd/web"}},
            "spec": {"syncPolicy": {"automated": {"selfHeal": true}}},
            "status": {"resources": [{"status": "OutOfSync"}, {"status": "Synced"}]}
        });
        let detail = parse_detail(&automated).unwrap();
        assert_eq!(detail.auto_sync, Some(AutoSync { prune: false, self_heal: true }));
        assert_eq!(detail.out_of_sync, 1);
        assert_eq!(detail.managed_by.as_deref(), Some("apps"));
        let switched_off = json!({"metadata": {"name": "web"}, "spec": {"syncPolicy": {"automated": {"enabled": false}}}});
        assert_eq!(parse_detail(&switched_off).unwrap().auto_sync, None);
    }
}
