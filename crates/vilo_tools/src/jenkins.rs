use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use futures::AsyncReadExt as _;
use http_client::{
    AsyncBody, HttpClient, HttpRequestExt as _, RedirectPolicy, Request, Url, http::Method,
};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const RATE_LIMIT_RETRIES: u32 = 3;
pub const BUILDS_SHOWN: usize = 30;

/// The Jenkins root a connection is bound to: https, host, port and any base
/// path such as `/jenkins/`. Requests never leave it.
pub fn normalize_base(address: &str) -> Result<Url> {
    let mut url = Url::parse(address.trim()).context("Enter an address like https://jenkins.example.com/")?;
    if url.scheme() != "https" || url.host_str().is_none() {
        bail!("Jenkins connections must use https://");
    }
    url.set_query(None);
    url.set_fragment(None);
    // Pasted page URLs keep only what precedes Jenkins' own paths.
    let path = url.path().to_string();
    let base = ["/job/", "/view/", "/me/", "/login", "/manage"]
        .iter()
        .filter_map(|marker| path.find(marker))
        .min()
        .map_or(path.as_str(), |index| &path[..index]);
    let base = format!("{}/", base.trim_end_matches('/'));
    url.set_path(&base);
    Ok(url)
}

#[derive(Debug, Clone, PartialEq)]
pub enum JenkinsError {
    Unauthorized,
    Forbidden,
    NotFound,
    Redirected,
    RateLimited,
    TooLarge,
    Status(u16),
    Network(String),
    Unexpected,
}

impl std::fmt::Display for JenkinsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JenkinsError::Unauthorized => f.write_str("Jenkins rejected the username or password/token."),
            JenkinsError::Forbidden => f.write_str("This account has no permission for that."),
            JenkinsError::NotFound => f.write_str("Not found on Jenkins."),
            JenkinsError::Redirected => f.write_str("Jenkins redirected the request; Vilo did not follow it. Check the address."),
            JenkinsError::RateLimited => f.write_str("Jenkins kept rejecting requests as too many (429)."),
            JenkinsError::TooLarge => f.write_str("The response is too large."),
            JenkinsError::Status(status) => write!(f, "Jenkins answered with status {status}."),
            JenkinsError::Network(reason) => write!(f, "Could not reach Jenkins: {reason}"),
            JenkinsError::Unexpected => f.write_str("Jenkins returned an unexpected response."),
        }
    }
}

impl std::error::Error for JenkinsError {}

#[derive(Debug, Clone, PartialEq)]
pub struct JobEntry {
    pub name: String,
    /// Folders and multibranch projects contain jobs; they are not built themselves.
    pub is_container: bool,
    /// Jenkins' ball color: blue, red, yellow, aborted, notbuilt, disabled, with `_anime` while running.
    pub color: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Build {
    pub number: u64,
    pub result: Option<String>,
    pub building: bool,
    pub timestamp: u64,
    pub duration: u64,
    pub parameters: Vec<(String, String)>,
    /// `None` when Jenkins does not say who started it (timer, webhook…).
    pub user: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stage {
    pub name: String,
    pub status: String,
    pub duration: u64,
}

/// A job parameter as Jenkins defines it today.
#[derive(Debug, Clone, PartialEq)]
pub struct ParameterDefinition {
    pub name: String,
    pub description: String,
    pub kind: ParameterKind,
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParameterKind {
    Choice(Vec<String>),
    Boolean,
    String,
    Text,
    /// Never prefilled, never shown, never logged.
    Password,
    /// Active Choices and other plugin types whose values Vilo cannot compute yet.
    Unsupported(String),
}

/// What happened to a build request, as far as Vilo can tell.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOutcome {
    /// A new queue item (201) or an identical one already waiting (303).
    Queued { queue_id: u64, reused: bool },
    /// Stop accepted; the build ends when Jenkins gets to it.
    StopRequested,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WriteError {
    /// Jenkins refused before acting.
    Refused(String),
    /// The request may have been acted on; check Jenkins before trying again.
    OutcomeUnknown(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Refused(reason) => f.write_str(reason),
            WriteError::OutcomeUnknown(reason) => write!(
                f,
                "Vilo could not tell whether Jenkins accepted this ({reason}). Check the job's builds before trying again."
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueueState {
    Waiting(Option<String>),
    Started(u64),
    Cancelled,
    /// Jenkins no longer keeps the queue item; look for the build by its queue id.
    Gone,
}

/// The only changes Vilo makes on Jenkins.
pub enum WriteAction<'a> {
    Build {
        job: &'a [String],
        /// `None` for a job without parameters.
        parameters: Option<&'a [(String, String)]>,
    },
    Stop { job: &'a [String], number: u64 },
}

/// An Active Choices cascade parameter as Jenkins' build page wires it up.
#[derive(Debug, Clone, PartialEq)]
pub struct CascadeSpec {
    pub name: String,
    pub referenced: Vec<String>,
    /// Several values (checkboxes, multi-select) joined with commas on submit.
    pub multiple: bool,
    proxy: String,
}

/// One rendering of the build page: its session and the cascades on it.
/// Kept for a single form; the bound proxies only exist in this session.
pub struct FormSession {
    pub cascades: Vec<CascadeSpec>,
    crumb: Option<(String, String)>,
    cookies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChoiceOption {
    pub label: String,
    /// The value Jenkins receives.
    pub key: String,
    pub selected: bool,
    pub disabled: bool,
}

pub struct LogChunk {
    pub text: String,
    /// The byte offset to ask for next, as reported by Jenkins.
    pub next_start: u64,
    pub more: bool,
}

#[derive(Clone)]
pub struct JenkinsClient {
    http: Arc<dyn HttpClient>,
    executor: Option<gpui::BackgroundExecutor>,
    base: Url,
    authorization: String,
}

/// A job's location as folder names from the root, e.g. ["FPMS_NT", "FPMS-NT-SWIM-UPDATE"].
pub fn job_path(segments: &[String]) -> String {
    segments
        .iter()
        // Multibranch branch names may contain `/`; Jenkins expects it encoded.
        .map(|segment| format!("job/{}/", utf8_percent_encode(segment, NON_ALPHANUMERIC)))
        .collect()
}

impl JenkinsClient {
    pub fn new(http: Arc<dyn HttpClient>, base: Url, username: &str, secret: &str) -> Self {
        let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{username}:{secret}"));
        Self {
            http,
            executor: None,
            base,
            authorization: format!("Basic {credentials}"),
        }
    }

    pub fn with_backoff(mut self, executor: gpui::BackgroundExecutor) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Checks the credentials; returns the Jenkins version.
    pub async fn whoami(&self) -> Result<String, JenkinsError> {
        let (_, headers) = self.get("api/json?tree=mode", false).await?;
        Ok(headers.version.unwrap_or_default())
    }

    pub async fn jobs(&self, folder: &[String]) -> Result<Vec<JobEntry>, JenkinsError> {
        #[derive(Deserialize)]
        struct Listing {
            #[serde(default)]
            jobs: Vec<Raw>,
        }
        #[derive(Deserialize)]
        struct Raw {
            name: String,
            #[serde(rename = "_class", default)]
            class: String,
            color: Option<String>,
        }
        let (body, _) = self
            .get(&format!("{}api/json?tree=jobs[name,_class,color]", job_path(folder)), false)
            .await?;
        let listing: Listing = parse_lenient(&body)?;
        Ok(listing
            .jobs
            .into_iter()
            .map(|job| JobEntry {
                is_container: job.class.contains("Folder") || job.class.contains("MultiBranch") || job.class.contains("OrganizationFolder"),
                name: job.name,
                color: job.color,
            })
            .collect())
    }

    pub async fn builds(&self, job: &[String]) -> Result<Vec<Build>, JenkinsError> {
        let (body, _) = self
            .get(
                &format!(
                    "{}api/json?tree=builds[number,result,building,timestamp,duration,actions[parameters[name,value],causes[userId]]]{{0,{BUILDS_SHOWN}}}",
                    job_path(job)
                ),
                false,
            )
            .await?;
        parse_builds(&body)
    }

    pub async fn build(&self, job: &[String], number: u64) -> Result<Build, JenkinsError> {
        let (body, _) = self
            .get(
                &format!(
                    "{}{number}/api/json?tree=number,result,building,timestamp,duration,actions[parameters[name,value],causes[userId]]",
                    job_path(job)
                ),
                false,
            )
            .await?;
        let value: serde_json::Value = parse_lenient(&body)?;
        parse_build(&value).ok_or(JenkinsError::Unexpected)
    }

    /// Stages from the Pipeline Stage View plugin; `None` when the plugin is absent.
    pub async fn stages(&self, job: &[String], number: u64) -> Result<Option<Vec<Stage>>, JenkinsError> {
        let body = match self.get(&format!("{}{number}/wfapi/describe", job_path(job)), false).await {
            Ok((body, _)) => body,
            Err(JenkinsError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let value: serde_json::Value = parse_lenient(&body)?;
        Ok(Some(
            value
                .get("stages")
                .and_then(|stages| stages.as_array())
                .map(|stages| {
                    stages
                        .iter()
                        .map(|stage| Stage {
                            name: stage.get("name").and_then(|name| name.as_str()).unwrap_or_default().to_string(),
                            status: stage.get("status").and_then(|status| status.as_str()).unwrap_or_default().to_string(),
                            duration: stage.get("durationMillis").and_then(|duration| duration.as_u64()).unwrap_or(0),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        ))
    }

    /// Console output from byte `start`. The next request must use `next_start`.
    pub async fn log(&self, job: &[String], number: u64, start: u64) -> Result<LogChunk, JenkinsError> {
        let (text, headers) = self
            .get(&format!("{}{number}/logText/progressiveText?start={start}", job_path(job)), true)
            .await?;
        Ok(LogChunk {
            next_start: headers.text_size.ok_or(JenkinsError::Unexpected)?,
            more: headers.more_data,
            text,
        })
    }

    /// Parameter names, types and static choices. Fast: it leaves out default
    /// values, which Active Choices computes by running scripts.
    pub async fn parameter_definitions(&self, job: &[String]) -> Result<Vec<ParameterDefinition>, JenkinsError> {
        let (body, _) = self
            .get(
                &format!("{}api/json?tree=property[parameterDefinitions[name,type,description,choices]]", job_path(job)),
                false,
            )
            .await?;
        let value: serde_json::Value = parse_lenient(&body)?;
        Ok(parse_definitions(&value))
    }

    /// Each parameter's default value, by name. Slow on jobs with Active Choices.
    pub async fn parameter_defaults(&self, job: &[String]) -> Result<Vec<(String, Option<String>)>, JenkinsError> {
        let (body, _) = self
            .get(
                &format!("{}api/json?tree=property[parameterDefinitions[name,type,defaultParameterValue[value]]]", job_path(job)),
                false,
            )
            .await?;
        let value: serde_json::Value = parse_lenient(&body)?;
        Ok(parse_definitions(&value)
            .into_iter()
            .map(|definition| (definition.name, definition.default))
            .collect())
    }

    /// The build a queue item turned into. Polled after a build request.
    pub async fn queue_item(&self, queue_id: u64) -> Result<QueueState, JenkinsError> {
        let body = match self.get(&format!("queue/item/{queue_id}/api/json"), false).await {
            Ok((body, _)) => body,
            Err(JenkinsError::NotFound) => return Ok(QueueState::Gone),
            Err(error) => return Err(error),
        };
        let value: serde_json::Value = parse_lenient(&body)?;
        if value.get("cancelled").and_then(|cancelled| cancelled.as_bool()) == Some(true) {
            return Ok(QueueState::Cancelled);
        }
        if let Some(number) = value.pointer("/executable/number").and_then(|number| number.as_u64()) {
            return Ok(QueueState::Started(number));
        }
        Ok(QueueState::Waiting(
            value.get("why").and_then(|why| why.as_str()).map(str::to_string),
        ))
    }

    /// The build number that came from a queue item, once its record has expired.
    pub async fn build_for_queue_item(&self, job: &[String], queue_id: u64) -> Result<Option<u64>, JenkinsError> {
        let (body, _) = self
            .get(&format!("{}api/json?tree=builds[number,queueId]{{0,50}}", job_path(job)), false)
            .await?;
        let value: serde_json::Value = parse_lenient(&body)?;
        Ok(value
            .get("builds")
            .and_then(|builds| builds.as_array())
            .and_then(|builds| {
                builds
                    .iter()
                    .find(|build| build.get("queueId").and_then(|id| id.as_u64()) == Some(queue_id))
            })
            .and_then(|build| build.get("number").and_then(|number| number.as_u64())))
    }

    /// Performs one change. Never retried: a repeat could start a second deployment.
    pub async fn write(&self, action: WriteAction<'_>) -> Result<WriteOutcome, WriteError> {
        let (path, body) = match &action {
            WriteAction::Build { job, parameters: None } => (format!("{}build", job_path(job)), String::new()),
            WriteAction::Build { job, parameters: Some(parameters) } => (
                format!("{}buildWithParameters", job_path(job)),
                url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(parameters.iter().map(|(name, value)| (name.as_str(), value.as_str())))
                    .finish(),
            ),
            WriteAction::Stop { job, number } => (format!("{}{number}/stop", job_path(job)), String::new()),
        };
        // Password logins need a crumb tied to a session cookie; token logins accept it too.
        let (crumb, cookies) = self
            .crumb()
            .await
            .map_err(|error| WriteError::Refused(format!("Could not prepare the request: {error}")))?;
        let url = self.base.join(&path).map_err(|_| WriteError::Refused("Invalid job path.".into()))?;
        if !is_within(&self.base, &url) {
            return Err(WriteError::Refused("Invalid job path.".into()));
        }
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .header("Authorization", &self.authorization)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        if let Some((field, value)) = &crumb {
            request = request.header(field.as_str(), value.as_str());
        }
        if !cookies.is_empty() {
            request = request.header("Cookie", cookies.join("; "));
        }
        let request = request
            .body(AsyncBody::from(body.into_bytes()))
            .map_err(|_| WriteError::Refused("Could not build the request.".into()))?;
        let response = self
            .http
            .send(request)
            .await
            .map_err(|error| WriteError::OutcomeUnknown(error.to_string().chars().take(200).collect()))?;
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .and_then(|location| url.join(location).ok());
        classify_write(&action, status, location.as_ref(), &self.base)
    }

    /// Loads the job's build page to drive its Active Choices parameters the
    /// way the page itself does.
    pub async fn open_form(&self, job: &[String]) -> Result<FormSession, JenkinsError> {
        let url = self
            .base
            .join(&format!("{}build?delay=0sec", job_path(job)))
            .map_err(|_| JenkinsError::Unexpected)?;
        let request = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header("Authorization", &self.authorization)
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT)
            .body(AsyncBody::empty())
            .map_err(|_| JenkinsError::Unexpected)?;
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| JenkinsError::Network(error.to_string().chars().take(200).collect()))?;
        // Jenkins renders the form with 405 for a GET of a parameterized job.
        match response.status().as_u16() {
            200 | 405 => {}
            401 => return Err(JenkinsError::Unauthorized),
            403 => return Err(JenkinsError::Forbidden),
            404 => return Err(JenkinsError::NotFound),
            status if (300..400).contains(&status) => return Err(JenkinsError::Redirected),
            status => return Err(JenkinsError::Status(status)),
        }
        let cookies = session_cookies(&response);
        let mut bytes = Vec::new();
        response
            .body_mut()
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| JenkinsError::Unexpected)?;
        if bytes.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(JenkinsError::TooLarge);
        }
        let html = String::from_utf8_lossy(&bytes);
        let crumb = attribute(&html, "data-crumb-header")
            .zip(attribute(&html, "data-crumb-value"));
        Ok(FormSession {
            cascades: parse_cascades(&html),
            crumb,
            cookies,
        })
    }

    /// Recomputes one cascade's choices from the values it references. The
    /// plugin keeps this state per session, so calls for a form must not overlap.
    pub async fn cascade_choices(
        &self,
        form: &FormSession,
        cascade: &CascadeSpec,
        referenced_values: &[(String, String)],
    ) -> Result<Vec<ChoiceOption>, JenkinsError> {
        let text = referenced_values
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("__LESEP__");
        let update = serde_json::to_string(&[text]).map_err(|_| JenkinsError::Unexpected)?;
        self.proxy_call(form, &cascade.proxy, "doUpdate", update).await?;
        let body = self.proxy_call(form, &cascade.proxy, "getChoicesForUI", "[]".into()).await?;
        parse_choices(&body)
    }

    async fn proxy_call(&self, form: &FormSession, proxy: &str, method: &str, body: String) -> Result<String, JenkinsError> {
        // Only the proxies found on the page just loaded, and only these methods.
        if !matches!(method, "doUpdate" | "getChoicesForUI") || !form.cascades.iter().any(|cascade| cascade.proxy == proxy) {
            return Err(JenkinsError::Unexpected);
        }
        let url = self
            .base
            .join(&format!("$stapler/bound/{proxy}/{method}"))
            .map_err(|_| JenkinsError::Unexpected)?;
        if !is_within(&self.base, &url) {
            return Err(JenkinsError::Unexpected);
        }
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .header("Authorization", &self.authorization)
            .header("Content-Type", "application/x-stapler-method-invocation;charset=UTF-8")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        if let Some((field, value)) = &form.crumb {
            request = request.header("Crumb", value.as_str()).header(field.as_str(), value.as_str());
        }
        if !form.cookies.is_empty() {
            request = request.header("Cookie", form.cookies.join("; "));
        }
        let request = request
            .body(AsyncBody::from(body.into_bytes()))
            .map_err(|_| JenkinsError::Unexpected)?;
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| JenkinsError::Network(error.to_string().chars().take(200).collect()))?;
        match response.status().as_u16() {
            200..=299 => {}
            403 => return Err(JenkinsError::Forbidden),
            404 => return Err(JenkinsError::NotFound),
            status => return Err(JenkinsError::Status(status)),
        }
        let mut text = String::new();
        response
            .body_mut()
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_string(&mut text)
            .await
            .map_err(|_| JenkinsError::Unexpected)?;
        Ok(text)
    }

    async fn crumb(&self) -> Result<(Option<(String, String)>, Vec<String>), JenkinsError> {
        let url = self.base.join("crumbIssuer/api/json").map_err(|_| JenkinsError::Unexpected)?;
        let request = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header("Authorization", &self.authorization)
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT)
            .body(AsyncBody::empty())
            .map_err(|_| JenkinsError::Unexpected)?;
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| JenkinsError::Network(error.to_string().chars().take(200).collect()))?;
        // Jenkins without CSRF protection has no crumb issuer.
        if response.status().as_u16() == 404 {
            return Ok((None, Vec::new()));
        }
        if !response.status().is_success() {
            return Err(JenkinsError::Status(response.status().as_u16()));
        }
        let cookies = session_cookies(&response);
        let mut body = String::new();
        response
            .body_mut()
            .take(64 * 1024)
            .read_to_string(&mut body)
            .await
            .map_err(|_| JenkinsError::Unexpected)?;
        let value: serde_json::Value = parse_lenient(&body)?;
        let crumb = match (
            value.get("crumbRequestField").and_then(|field| field.as_str()),
            value.get("crumb").and_then(|crumb| crumb.as_str()),
        ) {
            (Some(field), Some(crumb)) => Some((field.to_string(), crumb.to_string())),
            _ => None,
        };
        Ok((crumb, cookies))
    }

    async fn get(&self, path: &str, raw: bool) -> Result<(String, Headers), JenkinsError> {
        let mut attempt = 0;
        loop {
            match self.get_once(path, raw).await {
                Err(JenkinsError::RateLimited) if attempt < RATE_LIMIT_RETRIES => {
                    let Some(executor) = &self.executor else {
                        return Err(JenkinsError::RateLimited);
                    };
                    executor.timer(Duration::from_millis(500 * 2u64.pow(attempt))).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    /// The only request this client can make: a GET below the bound base URL.
    async fn get_once(&self, path: &str, raw: bool) -> Result<(String, Headers), JenkinsError> {
        let url = self.base.join(path).map_err(|_| JenkinsError::Unexpected)?;
        if !is_within(&self.base, &url) {
            return Err(JenkinsError::Unexpected);
        }
        let request = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header("Authorization", &self.authorization)
            .header("Accept", if raw { "text/plain" } else { "application/json" })
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT)
            .body(AsyncBody::empty())
            .map_err(|_| JenkinsError::Unexpected)?;
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| JenkinsError::Network(error.to_string().chars().take(200).collect()))?;
        let status = response.status();
        if status.is_redirection() {
            return Err(JenkinsError::Redirected);
        }
        match status.as_u16() {
            401 => return Err(JenkinsError::Unauthorized),
            403 => return Err(JenkinsError::Forbidden),
            404 => return Err(JenkinsError::NotFound),
            429 => return Err(JenkinsError::RateLimited),
            200..=299 => {}
            other => return Err(JenkinsError::Status(other)),
        }
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        let headers = Headers {
            version: header("x-jenkins"),
            text_size: header("x-text-size").and_then(|size| size.parse().ok()),
            more_data: header("x-more-data").is_some_and(|more| more == "true"),
        };
        let mut bytes = Vec::new();
        response
            .body_mut()
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| JenkinsError::Unexpected)?;
        if bytes.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(JenkinsError::TooLarge);
        }
        Ok((String::from_utf8_lossy(&bytes).into_owned(), headers))
    }
}

fn session_cookies(response: &http_client::Response<AsyncBody>) -> Vec<String> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|cookie| cookie.split(';').next())
        .map(str::to_string)
        .collect()
}

/// The first value of an HTML attribute, entity-decoded for the few entities Jenkins emits.
fn attribute(html: &str, name: &str) -> Option<String> {
    let start = html.find(&format!("{name}=\""))? + name.len() + 2;
    let end = html[start..].find('"')? + start;
    Some(decode_entities(&html[start..end]))
}

fn decode_entities(text: &str) -> String {
    text.replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// Finds each `cascade-choice-parameter-data-holder` and its bound proxy. A
/// cascade whose markup is not fully recognized is left out, so the caller
/// sees it missing and falls back to Jenkins' own page.
fn parse_cascades(html: &str) -> Vec<CascadeSpec> {
    let mut cascades = Vec::new();
    let mut rest = html;
    while let Some(index) = rest.find("class=\"cascade-choice-parameter-data-holder\"") {
        let tag_start = rest[..index].rfind('<').unwrap_or(0);
        let tag_end = rest[index..].find('>').map_or(rest.len(), |end| index + end);
        let tag = &rest[tag_start..tag_end];
        let after = &rest[tag_end..];
        rest = after;
        let (Some(name), Some(random), Some(proxy_name)) = (
            attribute(tag, "data-name"),
            attribute(tag, "data-random-name"),
            attribute(tag, "data-proxy-name"),
        ) else {
            continue;
        };
        let referenced = attribute(tag, "data-referenced-parameters")
            .unwrap_or_default()
            .split(',')
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        let marker = "$stapler/bound/script/$stapler/bound/";
        let proxy = after.find(marker).and_then(|start| {
            let id_start = start + marker.len();
            let script = &after[id_start..after[id_start..].find('"').map_or(after.len(), |end| id_start + end)];
            let (id, query) = script.split_once('?')?;
            let valid = id.len() == 36 && id.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
            (valid && decode_entities(query).contains(&format!("var={proxy_name}&"))).then(|| id.to_string())
        });
        // The control lives in the element with the random id, before the holder.
        let control_start = html.find(&format!("id=\"{random}\""));
        let control = control_start.map(|start| &html[start..html[start..].find("cascade-choice-parameter-data-holder").map_or(html.len(), |end| start + end)]);
        let multiple = match control {
            Some(control) if control.contains("class=\"dynamic_checkbox\"") => Some(true),
            Some(control) if control.contains("<select") && control.contains("multiple") => Some(true),
            Some(control) if control.contains("<select") || control.contains("type=\"radio\"") => Some(false),
            _ => None,
        };
        if let (Some(proxy), Some(multiple)) = (proxy, multiple) {
            cascades.push(CascadeSpec { name, referenced, multiple, proxy });
        }
    }
    cascades
}

/// `getChoicesForUI` answers `[labels, keys]`; markers like `:selected` and
/// `:disabled` ride on the labels and keys and are stripped here.
fn parse_choices(body: &str) -> Result<Vec<ChoiceOption>, JenkinsError> {
    let value: serde_json::Value = parse_lenient(body)?;
    let arrays = value.as_array().filter(|arrays| arrays.len() == 2).ok_or(JenkinsError::Unexpected)?;
    let labels = arrays[0].as_array().ok_or(JenkinsError::Unexpected)?;
    let keys = arrays[1].as_array().ok_or(JenkinsError::Unexpected)?;
    if labels.len() != keys.len() {
        return Err(JenkinsError::Unexpected);
    }
    let text = |value: &serde_json::Value| match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    Ok(labels
        .iter()
        .zip(keys)
        .map(|(label, key)| {
            let (mut label, mut key) = (text(label), text(key));
            let mut selected = false;
            let mut disabled = false;
            for marked in [&mut label, &mut key] {
                loop {
                    if let Some(stripped) = marked.strip_suffix(":selected") {
                        *marked = stripped.to_string();
                        selected = true;
                    } else if let Some(stripped) = marked.strip_suffix(":disabled") {
                        *marked = stripped.to_string();
                        disabled = true;
                    } else {
                        break;
                    }
                }
            }
            ChoiceOption { label, key, selected, disabled }
        })
        .collect())
}

/// Reads Jenkins' answer to a change. Redirects to the job or login page mean
/// it was not scheduled; only a queue-item location proves a build request.
fn classify_write(
    action: &WriteAction<'_>,
    status: u16,
    location: Option<&Url>,
    base: &Url,
) -> Result<WriteOutcome, WriteError> {
    let refused = |reason: &str| Err(WriteError::Refused(reason.to_string()));
    match status {
        401 => return refused("Jenkins rejected the username or password/token."),
        403 => return refused("This account may not do that, or the request was rejected as forged (crumb)."),
        404 => return refused("Jenkins could not find that job or build."),
        405 => return refused("Jenkins does not accept this action for this job."),
        500..=599 => return Err(WriteError::OutcomeUnknown(format!("status {status}"))),
        _ => {}
    }
    let location = location.filter(|location| is_within(base, location));
    match action {
        WriteAction::Build { .. } => {
            let queue_id = location.and_then(|location| {
                let path = location.path().strip_prefix(base.path())?;
                path.strip_prefix("queue/item/")?.trim_end_matches('/').parse::<u64>().ok()
            });
            match (status, queue_id) {
                (201, Some(queue_id)) => Ok(WriteOutcome::Queued { queue_id, reused: false }),
                (303, Some(queue_id)) => Ok(WriteOutcome::Queued { queue_id, reused: true }),
                (200..=399, None) if location.is_some_and(|location| location.path().contains("login")) => {
                    refused("Jenkins asked to log in again; nothing was started.")
                }
                (200..=399, None) => refused("Jenkins did not schedule the build (no queue item was created)."),
                _ => Err(WriteError::OutcomeUnknown(format!("status {status}"))),
            }
        }
        WriteAction::Stop { .. } => match status {
            200..=399 if !location.is_some_and(|location| location.path().contains("login")) => {
                Ok(WriteOutcome::StopRequested)
            }
            200..=399 => refused("Jenkins asked to log in again; nothing was stopped."),
            _ => Err(WriteError::OutcomeUnknown(format!("status {status}"))),
        },
    }
}

fn parse_definitions(value: &serde_json::Value) -> Vec<ParameterDefinition> {
    value
        .get("property")
        .and_then(|properties| properties.as_array())
        .into_iter()
        .flatten()
        .filter_map(|property| property.get("parameterDefinitions").and_then(|definitions| definitions.as_array()))
        .flatten()
        .filter_map(|definition| {
            let name = definition.get("name")?.as_str()?.to_string();
            let kind_name = definition.get("type").and_then(|kind| kind.as_str()).unwrap_or_default();
            let default = match definition.pointer("/defaultParameterValue/value") {
                Some(serde_json::Value::String(text)) => Some(text.clone()),
                Some(serde_json::Value::Bool(flag)) => Some(flag.to_string()),
                Some(serde_json::Value::Null) | None => None,
                Some(other) => Some(other.to_string()),
            };
            let kind = match kind_name {
                "ChoiceParameterDefinition" => ParameterKind::Choice(
                    definition
                        .get("choices")
                        .and_then(|choices| choices.as_array())
                        .map(|choices| choices.iter().filter_map(|choice| choice.as_str().map(str::to_string)).collect())
                        .unwrap_or_default(),
                ),
                "BooleanParameterDefinition" => ParameterKind::Boolean,
                "StringParameterDefinition" => ParameterKind::String,
                "TextParameterDefinition" => ParameterKind::Text,
                "PasswordParameterDefinition" => ParameterKind::Password,
                other => ParameterKind::Unsupported(other.to_string()),
            };
            Some(ParameterDefinition {
                name,
                description: definition
                    .get("description")
                    .and_then(|description| description.as_str())
                    .unwrap_or_default()
                    .to_string(),
                default: if kind == ParameterKind::Password { None } else { default },
                kind,
            })
        })
        .collect()
}

struct Headers {
    version: Option<String>,
    text_size: Option<u64>,
    more_data: bool,
}

fn is_within(base: &Url, url: &Url) -> bool {
    url.origin() == base.origin() && url.path().starts_with(base.path())
}

/// Jenkins' JSON can carry raw control characters inside strings (job
/// descriptions, parameter values); escape them before parsing.
fn parse_lenient<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, JenkinsError> {
    let mut repaired = String::with_capacity(body.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in body.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            } else if (character as u32) < 0x20 {
                repaired.push_str(&format!("\\u{:04x}", character as u32));
                continue;
            }
        } else if character == '"' {
            in_string = true;
        }
        repaired.push(character);
    }
    serde_json::from_str(&repaired).map_err(|_| JenkinsError::Unexpected)
}

fn parse_build(value: &serde_json::Value) -> Option<Build> {
    let mut parameters = Vec::new();
    let mut user = None;
    for action in value.get("actions").and_then(|actions| actions.as_array()).into_iter().flatten() {
        for parameter in action.get("parameters").and_then(|parameters| parameters.as_array()).into_iter().flatten() {
            let name = parameter.get("name").and_then(|name| name.as_str()).unwrap_or_default();
            let value = match parameter.get("value") {
                Some(serde_json::Value::String(text)) => text.clone(),
                Some(serde_json::Value::Null) | None => String::new(),
                Some(other) => other.to_string(),
            };
            parameters.push((name.to_string(), value));
        }
        for cause in action.get("causes").and_then(|causes| causes.as_array()).into_iter().flatten() {
            if let Some(id) = cause.get("userId").and_then(|id| id.as_str()) {
                user.get_or_insert_with(|| id.to_string());
            }
        }
    }
    Some(Build {
        number: value.get("number")?.as_u64()?,
        result: value.get("result").and_then(|result| result.as_str()).map(str::to_string),
        building: value.get("building").and_then(|building| building.as_bool()).unwrap_or(false),
        timestamp: value.get("timestamp").and_then(|time| time.as_u64()).unwrap_or(0),
        duration: value.get("duration").and_then(|duration| duration.as_u64()).unwrap_or(0),
        parameters,
        user,
    })
}

fn parse_builds(body: &str) -> Result<Vec<Build>, JenkinsError> {
    let value: serde_json::Value = parse_lenient(body)?;
    Ok(value
        .get("builds")
        .and_then(|builds| builds.as_array())
        .map(|builds| builds.iter().filter_map(parse_build).collect())
        .unwrap_or_default())
}

/// Console text for display: terminal escape sequences (colors, but also OSC
/// sequences that could set titles or the clipboard) and control characters
/// other than tabs are removed. Applied to display text only, never to offsets.
pub fn strip_escapes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\u{1b}' => match chars.next() {
                // CSI: parameters, then one final byte in @..~
                Some('[') => {
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                // OSC, DCS, PM, APC: until BEL or ESC \
                Some(']' | 'P' | '^' | '_') => {
                    while let Some(next) = chars.next() {
                        if next == '\u{7}' {
                            break;
                        }
                        if next == '\u{1b}' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\n' | '\t' => out.push(character),
            character if character.is_control() => {}
            character => out.push(character),
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests_support {
    use super::CascadeSpec;

    pub fn spec(name: &str, referenced: &[&str], multiple: bool) -> CascadeSpec {
        CascadeSpec {
            name: name.into(),
            referenced: referenced.iter().map(|name| name.to_string()).collect(),
            multiple,
            proxy: "00000000-0000-0000-0000-000000000000".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_base_keeps_base_path_and_requires_https() {
        assert_eq!(
            normalize_base("https://jenkins.example.com/job/FPMS_NT/job/X/").unwrap().as_str(),
            "https://jenkins.example.com/"
        );
        assert_eq!(
            normalize_base("https://ci.example.com/jenkins/me/security/").unwrap().as_str(),
            "https://ci.example.com/jenkins/"
        );
        assert!(normalize_base("http://jenkins.example.com/").is_err());
    }

    #[test]
    fn test_requests_stay_below_the_base() {
        let base = Url::parse("https://ci.example.com/jenkins/").unwrap();
        assert!(is_within(&base, &base.join("job/a/api/json").unwrap()));
        assert!(!is_within(&base, &base.join("../other/api/json").unwrap()));
        assert!(!is_within(&base, &base.join("https://evil.example.com/jenkins/").unwrap()));
        assert_eq!(
            job_path(&["FPMS_NT".into(), "feature/x y".into()]),
            "job/FPMS%5FNT/job/feature%2Fx%20y/"
        );
    }

    #[test]
    fn test_lenient_json_and_builds() {
        let body = "{\"builds\":[{\"number\":7,\"result\":\"SUCCESS\",\"building\":false,\"timestamp\":1,\"duration\":2,\
            \"actions\":[{\"parameters\":[{\"name\":\"NAMESPACE\",\"value\":\"lane\tx\"},{\"name\":\"CACHE\",\"value\":true}]},\
            {\"causes\":[{\"userId\":\"fpmsdev\"}]}]},{\"number\":6,\"building\":true,\"actions\":[]}]}";
        let builds = parse_builds(body).unwrap();
        assert_eq!(builds[0].parameters[0], ("NAMESPACE".into(), "lane\tx".into()));
        assert_eq!(builds[0].parameters[1].1, "true");
        assert_eq!(builds[0].user.as_deref(), Some("fpmsdev"));
        assert_eq!((builds[1].result.as_deref(), builds[1].building, builds[1].user.as_deref()), (None, true, None));
    }

    #[test]
    fn test_write_classification() {
        let base = Url::parse("https://ci.example.com/jenkins/").unwrap();
        let job = vec!["A".to_string()];
        let build = WriteAction::Build { job: &job, parameters: Some(&[]) };
        let queue = base.join("queue/item/42/").unwrap();
        assert_eq!(
            classify_write(&build, 201, Some(&queue), &base),
            Ok(WriteOutcome::Queued { queue_id: 42, reused: false })
        );
        assert_eq!(
            classify_write(&build, 303, Some(&queue), &base),
            Ok(WriteOutcome::Queued { queue_id: 42, reused: true })
        );
        // Sent back to the job page: Jenkins refused to schedule it.
        let job_page = base.join("job/A/").unwrap();
        assert!(matches!(classify_write(&build, 302, Some(&job_page), &base), Err(WriteError::Refused(_))));
        let login = base.join("login?from=x").unwrap();
        assert!(matches!(classify_write(&build, 302, Some(&login), &base), Err(WriteError::Refused(_))));
        // A queue item on another host is not trusted.
        let elsewhere = Url::parse("https://evil.example.com/jenkins/queue/item/42/").unwrap();
        assert!(matches!(classify_write(&build, 201, Some(&elsewhere), &base), Err(WriteError::Refused(_))));
        assert!(matches!(classify_write(&build, 502, None, &base), Err(WriteError::OutcomeUnknown(_))));
        let stop = WriteAction::Stop { job: &job, number: 3 };
        assert_eq!(classify_write(&stop, 302, Some(&job_page), &base), Ok(WriteOutcome::StopRequested));
    }

    #[test]
    fn test_parameter_definitions() {
        let value = serde_json::json!({"property": [{}, {"parameterDefinitions": [
            {"name": "ENV_TYPE", "type": "ChoiceParameterDefinition", "choices": ["qat", "uat"], "defaultParameterValue": {"value": "qat"}},
            {"name": "NAMESPACE", "type": "CascadeChoiceParameter"},
            {"name": "CACHE", "type": "BooleanParameterDefinition", "defaultParameterValue": {"value": true}},
            {"name": "SECRET", "type": "PasswordParameterDefinition", "defaultParameterValue": {"value": "hunter2"}}
        ]}]});
        let definitions = parse_definitions(&value);
        assert_eq!(definitions[0].kind, ParameterKind::Choice(vec!["qat".into(), "uat".into()]));
        assert_eq!(definitions[1].kind, ParameterKind::Unsupported("CascadeChoiceParameter".into()));
        assert_eq!(definitions[2].default.as_deref(), Some("true"));
        assert_eq!((definitions[3].kind.clone(), definitions[3].default.clone()), (ParameterKind::Password, None));
    }

    #[test]
    fn test_parse_cascades_from_build_page() {
        let html = r#"<div name="parameter" id="choice-parameter-1" class="active-choice"><input name="name" type="hidden" value="NAMESPACE"><select name="value"><option value="a">a</option></select></div>
<span data-referenced-parameters="ENV_TYPE" data-random-name="choice-parameter-1" class="cascade-choice-parameter-data-holder" data-name="NAMESPACE" data-proxy-name="proxy_1"></span><script src="/$stapler/bound/script/$stapler/bound/ffde6e7d-4c57-4324-84a6-13662c630667?var=proxy_1&amp;methods=doUpdate,getChoicesForUI"></script>
<div name="parameter" id="choice-parameter-2" class="active-choice"><input name="name" type="hidden" value="SERVICES"><div id="ecp_choice-parameter-2" class="dynamic_checkbox"></div></div>
<span data-referenced-parameters="NAMESPACE,ENV_TYPE" data-random-name="choice-parameter-2" class="cascade-choice-parameter-data-holder" data-name="SERVICES" data-proxy-name="proxy_2"></span><script src="/$stapler/bound/script/$stapler/bound/e3bd75c4-4c2b-419f-b725-1bdee446abbf?var=proxy_2&amp;methods=doUpdate,getChoicesForUI"></script>
<div name="parameter" id="choice-parameter-3" class="active-choice"><input name="name" type="hidden" value="ODD"><textarea></textarea></div>
<span data-referenced-parameters="ENV_TYPE" data-random-name="choice-parameter-3" class="cascade-choice-parameter-data-holder" data-name="ODD" data-proxy-name="proxy_3"></span><script src="/$stapler/bound/script/$stapler/bound/43c63be4-ec21-4a16-a276-407e566d78f8?var=proxy_3&amp;methods=doUpdate"></script>"#;
        let cascades = parse_cascades(html);
        // The third control is not recognized, so it is left out for the caller to refuse.
        assert_eq!(cascades.len(), 2);
        assert_eq!((cascades[0].name.as_str(), cascades[0].multiple), ("NAMESPACE", false));
        assert_eq!(cascades[0].referenced, vec!["ENV_TYPE".to_string()]);
        assert_eq!((cascades[1].name.as_str(), cascades[1].multiple), ("SERVICES", true));
        assert_eq!(cascades[1].proxy, "e3bd75c4-4c2b-419f-b725-1bdee446abbf");
    }

    #[test]
    fn test_parse_choices_keeps_labels_and_keys_apart() {
        let choices = parse_choices(r#"[["QA env:selected","UAT"],["qat:selected","uat:disabled"]]"#).unwrap();
        assert_eq!(
            choices[0],
            ChoiceOption { label: "QA env".into(), key: "qat".into(), selected: true, disabled: false }
        );
        assert_eq!((choices[1].key.as_str(), choices[1].disabled), ("uat", true));
        assert!(parse_choices(r#"[["a"],["a","b"]]"#).is_err());
    }

    #[test]
    fn test_strip_escapes_removes_colors_osc_and_controls() {
        let text = "\u{1b}[31mERROR\u{1b}[0m done\u{1b}]52;c;Zm9v\u{7}\tok\u{8}\n\u{1b}]0;title\u{1b}\\end";
        assert_eq!(strip_escapes(text), "ERROR done\tok\nend");
    }
}

