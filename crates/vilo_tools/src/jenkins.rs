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
    fn test_strip_escapes_removes_colors_osc_and_controls() {
        let text = "\u{1b}[31mERROR\u{1b}[0m done\u{1b}]52;c;Zm9v\u{7}\tok\u{8}\n\u{1b}]0;title\u{1b}\\end";
        assert_eq!(strip_escapes(text), "ERROR done\tok\nend");
    }
}
