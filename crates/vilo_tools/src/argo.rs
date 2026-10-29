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
}

#[derive(Debug, Clone, PartialEq)]
pub struct Resource {
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub health: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub kind: String,
    pub reason: String,
    pub message: String,
    pub last_seen: String,
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
                        kind: text(node, "/kind"),
                        name: text(node, "/name"),
                        namespace: text(node, "/namespace"),
                        health: node.pointer("/health/status").and_then(|status| status.as_str()).map(str::to_string),
                        message: node.pointer("/health/message").and_then(|message| message.as_str()).map(str::to_string),
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
        let body = self.get(&app_path(namespace, name, "/events")).await?;
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
    })
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
    }
}
