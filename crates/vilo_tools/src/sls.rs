use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::AsyncReadExt as _;
use hmac::{Hmac, Mac};
use http_client::{AsyncBody, HttpClient, HttpRequestExt as _, RedirectPolicy, Request, http::Method};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest, Sha256};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Limits for one response, before and after gzip.
const MAX_COMPRESSED_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// RFC 3986 unreserved characters stay; everything else is escaped, as the official SDK does.
const QUERY_ESCAPE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

#[derive(Debug, Clone, PartialEq)]
pub enum SlsError {
    /// Vilo refused before sending: the project is outside the connection's allowlist.
    NotAllowed(String),
    InvalidName(String),
    Unauthorized(String),
    Forbidden(String),
    ClockSkew(String),
    Throttled,
    Status { status: u16, code: String, message: String },
    Network(String),
    /// The response was larger than Vilo reads; it was not parsed.
    TooLarge,
    Unexpected,
}

impl std::fmt::Display for SlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlsError::NotAllowed(project) => write!(f, "“{project}” is not in this connection's project list, so Vilo did not send the query."),
            SlsError::InvalidName(name) => write!(f, "“{name}” is not a valid project or logstore name."),
            SlsError::Unauthorized(message) => write!(f, "SLS rejected the AccessKey: {message}"),
            SlsError::Forbidden(message) => write!(f, "This AccessKey has no permission for that: {message}"),
            SlsError::ClockSkew(message) => write!(f, "SLS rejected the request time ({message}). Check that this Mac's clock is set automatically."),
            SlsError::Throttled => f.write_str("SLS is limiting requests right now. Wait a moment and try again."),
            SlsError::Status { status, code, message } => write!(f, "SLS answered {status} {code}: {message}"),
            SlsError::Network(reason) => write!(f, "Could not reach SLS: {reason}"),
            SlsError::TooLarge => f.write_str("The answer was too large to read. Narrow the time range or the query."),
            SlsError::Unexpected => f.write_str("SLS returned an unexpected response."),
        }
    }
}

impl std::error::Error for SlsError {}

/// Whether SLS finished computing an answer; an incomplete one is partial.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Progress {
    Complete,
    Incomplete,
}

fn progress(text: &str) -> Progress {
    if text == "Complete" { Progress::Complete } else { Progress::Incomplete }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogsRequest {
    pub query: String,
    /// Unix seconds, left-closed.
    pub from: i64,
    /// Unix seconds, right-open.
    pub to: i64,
    pub line: u32,
    pub offset: u64,
    pub newest_first: bool,
}

/// One page of results. Each row keeps the field order SLS returned.
#[derive(Debug, Clone, PartialEq)]
pub struct LogsPage {
    pub progress: Progress,
    pub has_sql: bool,
    pub keys: Vec<String>,
    pub rows: Vec<Vec<(String, String)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    pub from: i64,
    pub to: i64,
    pub count: u64,
    pub progress: Progress,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexField {
    pub name: String,
    pub kind: String,
}

#[derive(Clone)]
pub struct SlsClient {
    http: Arc<dyn HttpClient>,
    region: String,
    access_key_id: String,
    secret: String,
    /// The only projects Vilo will send a request about.
    allowed_projects: Vec<String>,
}

/// Project and logstore names: lowercase letters, digits, `-` and `_`. Anything else is refused, so names cannot change the host or path.
fn valid_name(name: &str) -> bool {
    (2..=128).contains(&name.len())
        && name.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_')
}

fn valid_region(region: &str) -> bool {
    !region.is_empty() && region.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn utc_stamp(now: time::OffsetDateTime) -> String {
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>, SlsError> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|_| SlsError::Unexpected)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn canonical_query(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(key, value)| {
            if value.is_empty() {
                key.clone()
            } else {
                format!("{key}={}", utf8_percent_encode(value, QUERY_ESCAPE))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// SLS signature V4, matching the official SDK (alibabacloud_gateway_sls): headers starting with `x-log-` or `x-acs-`,
/// plus `host` and `content-type`, are signed; the Authorization header carries no SignedHeaders list.
fn sign_v4(
    method: &str,
    path: &str,
    query: &BTreeMap<String, String>,
    headers: &BTreeMap<String, String>,
    content_sha256: &str,
    stamp: &str,
    region: &str,
    access_key_id: &str,
    secret: &str,
) -> Result<String, SlsError> {
    let signed = headers
        .keys()
        .filter(|name| name.starts_with("x-log-") || name.starts_with("x-acs-") || *name == "host" || *name == "content-type")
        .cloned()
        .collect::<Vec<_>>();
    let canonical_headers = signed
        .iter()
        .map(|name| format!("{name}:{}\n", headers.get(name).map_or("", |value| value.trim())))
        .collect::<String>();
    let canonical_request = format!(
        "{method}\n{path}\n{}\n{canonical_headers}\n{}\n{content_sha256}",
        canonical_query(query),
        signed.join(";")
    );
    let day = &stamp[..8.min(stamp.len())];
    let scope = format!("{day}/{region}/sls/aliyun_v4_request");
    let string_to_sign = format!(
        "SLS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut key = hmac_sha256(format!("aliyun_v4{secret}").as_bytes(), day.as_bytes())?;
    for part in [region, "sls", "aliyun_v4_request"] {
        key = hmac_sha256(&key, part.as_bytes())?;
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes())?);
    Ok(format!("SLS4-HMAC-SHA256 Credential={access_key_id}/{scope},Signature={signature}"))
}

impl SlsClient {
    pub fn new(http: Arc<dyn HttpClient>, region: &str, access_key_id: &str, secret: &str, allowed_projects: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(valid_region(region), "Enter a region like ap-southeast-1");
        Ok(Self {
            http,
            region: region.to_string(),
            access_key_id: access_key_id.trim().to_string(),
            secret: secret.trim().to_string(),
            allowed_projects,
        })
    }

    /// Projects the AccessKey can list. Listing permission is not needed to query an allowed project.
    pub async fn list_projects(&self) -> Result<Vec<String>, SlsError> {
        let query = BTreeMap::from([("offset".into(), "0".into()), ("size".into(), "500".into())]);
        let body = self.send(Method::GET, None, "/", query, None).await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| SlsError::Unexpected)?;
        Ok(value
            .get("projects")
            .and_then(|projects| projects.as_array())
            .map(|projects| projects.iter().filter_map(|project| project.get("projectName")?.as_str().map(str::to_string)).collect())
            .unwrap_or_default())
    }

    pub async fn list_logstores(&self, project: &str) -> Result<Vec<String>, SlsError> {
        let query = BTreeMap::from([("offset".into(), "0".into()), ("size".into(), "500".into())]);
        let body = self.send(Method::GET, Some(project), "/logstores", query, None).await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| SlsError::Unexpected)?;
        let mut logstores = strings(value.get("logstores"));
        logstores.sort();
        Ok(logstores)
    }

    /// Indexed fields, for completion and the field list.
    pub async fn index_fields(&self, project: &str, logstore: &str) -> Result<Vec<IndexField>, SlsError> {
        check_name(logstore)?;
        let body = self.send(Method::GET, Some(project), &format!("/logstores/{logstore}/index"), BTreeMap::new(), None).await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| SlsError::Unexpected)?;
        let mut fields = value
            .get("keys")
            .and_then(|keys| keys.as_object())
            .map(|keys| {
                keys.iter()
                    .map(|(name, spec)| IndexField {
                        name: name.clone(),
                        kind: spec.get("type").and_then(|kind| kind.as_str()).unwrap_or("text").to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(fields)
    }

    /// One page of logs through GetLogsV2. An incomplete page is returned as is; the caller decides whether to ask again.
    pub async fn get_logs(&self, project: &str, logstore: &str, request: &LogsRequest) -> Result<LogsPage, SlsError> {
        check_name(logstore)?;
        let body = serde_json::json!({
            "query": request.query,
            "from": request.from,
            "to": request.to,
            "line": request.line.min(100),
            "offset": request.offset,
            "reverse": request.newest_first,
            "powerSql": false,
            "isAccurate": true,
        });
        let body = self
            .send(Method::POST, Some(project), &format!("/logstores/{logstore}/logs"), BTreeMap::new(), Some(body.to_string().into_bytes()))
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| SlsError::Unexpected)?;
        parse_logs(&value).ok_or(SlsError::Unexpected)
    }

    /// Match counts over the window, for search queries only.
    pub async fn histograms(&self, project: &str, logstore: &str, query: &str, from: i64, to: i64) -> Result<Vec<HistogramBucket>, SlsError> {
        check_name(logstore)?;
        let params = BTreeMap::from([
            ("type".into(), "histogram".into()),
            ("from".into(), from.to_string()),
            ("to".into(), to.to_string()),
            ("query".into(), query.to_string()),
        ]);
        let body = self.send(Method::GET, Some(project), &format!("/logstores/{logstore}/index"), params, None).await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| SlsError::Unexpected)?;
        Ok(value
            .as_array()
            .map(|buckets| {
                buckets
                    .iter()
                    .map(|bucket| HistogramBucket {
                        from: bucket.get("from").and_then(|from| from.as_i64()).unwrap_or(0),
                        to: bucket.get("to").and_then(|to| to.as_i64()).unwrap_or(0),
                        count: bucket.get("count").and_then(|count| count.as_u64()).unwrap_or(0),
                        progress: progress(bucket.get("progress").and_then(|progress| progress.as_str()).unwrap_or("")),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn send(
        &self,
        method: Method,
        project: Option<&str>,
        path: &str,
        query: BTreeMap<String, String>,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, SlsError> {
        let host = match project {
            Some(project) => {
                check_name(project)?;
                if !self.allowed_projects.iter().any(|allowed| allowed == project) {
                    return Err(SlsError::NotAllowed(project.to_string()));
                }
                format!("{project}.{}.log.aliyuncs.com", self.region)
            }
            None => format!("{}.log.aliyuncs.com", self.region),
        };
        let body = body.unwrap_or_default();
        let content_sha256 = if body.is_empty() { EMPTY_SHA256.to_string() } else { hex::encode(Sha256::digest(&body)) };
        let mut headers = BTreeMap::from([
            ("host".to_string(), host.clone()),
            ("content-type".to_string(), "application/json".to_string()),
            ("x-log-apiversion".to_string(), "0.6.0".to_string()),
            ("x-log-bodyrawsize".to_string(), body.len().to_string()),
            ("x-log-content-sha256".to_string(), content_sha256.clone()),
            ("x-log-date".to_string(), utc_stamp(time::OffsetDateTime::now_utc())),
        ]);
        let authorization = sign_v4(
            method.as_str(),
            path,
            &query,
            &headers,
            &content_sha256,
            &headers["x-log-date"],
            &self.region,
            &self.access_key_id,
            &self.secret,
        )?;
        headers.insert("authorization".into(), authorization);
        headers.insert("accept".into(), "application/json".into());
        headers.insert("accept-encoding".into(), "gzip".into());
        let mut url = format!("https://{host}{path}");
        if !query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query(&query));
        }
        let mut request = Request::builder()
            .method(method)
            .uri(url)
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        for (name, value) in &headers {
            // The HTTP stack sets Host from the URL.
            if name != "host" {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let request = request.body(AsyncBody::from(body)).map_err(|_| SlsError::Unexpected)?;
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| SlsError::Network(error.to_string().chars().take(200).collect()))?;
        let status = response.status().as_u16();
        let gzip = response
            .headers()
            .get("content-encoding")
            .and_then(|encoding| encoding.to_str().ok())
            .is_some_and(|encoding| encoding.eq_ignore_ascii_case("gzip"));
        let mut raw = Vec::new();
        response
            .body_mut()
            .take(MAX_COMPRESSED_BYTES + 1)
            .read_to_end(&mut raw)
            .await
            .map_err(|error| SlsError::Network(error.to_string().chars().take(200).collect()))?;
        if raw.len() as u64 > MAX_COMPRESSED_BYTES {
            return Err(SlsError::TooLarge);
        }
        let bytes = if gzip {
            let mut decoded = Vec::new();
            async_compression::futures::bufread::GzipDecoder::new(futures::io::Cursor::new(raw))
                .take(MAX_RESPONSE_BYTES + 1)
                .read_to_end(&mut decoded)
                .await
                .map_err(|_| SlsError::Unexpected)?;
            decoded
        } else {
            raw
        };
        // A cut-off answer is never parsed as if it were whole.
        if bytes.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(SlsError::TooLarge);
        }
        if (200..300).contains(&status) {
            return Ok(bytes);
        }
        Err(status_error(status, &bytes))
    }
}

fn check_name(name: &str) -> Result<(), SlsError> {
    if valid_name(name) { Ok(()) } else { Err(SlsError::InvalidName(name.chars().take(64).collect())) }
}

fn strings(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(|items| items.as_array())
        .map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

/// SLS explains errors as `{"errorCode": …, "errorMessage": …}`.
fn status_error(status: u16, body: &[u8]) -> SlsError {
    let value = serde_json::from_slice::<serde_json::Value>(body).unwrap_or_default();
    let code = value.get("errorCode").and_then(|code| code.as_str()).unwrap_or_default().to_string();
    let message: String = value
        .get("errorMessage")
        .and_then(|message| message.as_str())
        .unwrap_or_default()
        .chars()
        .take(300)
        .collect();
    if code.contains("RequestTime") || code.contains("TimeExpired") || code.contains("Skew") {
        return SlsError::ClockSkew(message);
    }
    match status {
        401 => SlsError::Unauthorized(message),
        403 => SlsError::Forbidden(message),
        429 => SlsError::Throttled,
        _ => SlsError::Status { status, code, message },
    }
}

fn parse_logs(value: &serde_json::Value) -> Option<LogsPage> {
    let meta = value.get("meta")?;
    let rows = value
        .get("data")
        .and_then(|data| data.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.as_object())
                .map(|row| {
                    row.iter()
                        .map(|(key, value)| {
                            let text = match value {
                                serde_json::Value::String(text) => text.clone(),
                                other => other.to_string(),
                            };
                            (key.clone(), text)
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default();
    Some(LogsPage {
        progress: progress(meta.get("progress").and_then(|progress| progress.as_str()).unwrap_or("")),
        has_sql: meta.get("hasSQL").and_then(|has_sql| has_sql.as_bool()).unwrap_or(false),
        keys: strings(meta.get("keys")),
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAMP: &str = "20240902T060000Z";
    const REGION: &str = "ap-southeast-1";
    const HOST: &str = "my-project.ap-southeast-1.log.aliyuncs.com";

    fn headers(extra: &[(&str, &str)], content: &str, raw_size: &str) -> BTreeMap<String, String> {
        let mut headers = BTreeMap::from([
            ("host".to_string(), HOST.to_string()),
            ("content-type".to_string(), "application/json".to_string()),
            ("x-log-apiversion".to_string(), "0.6.0".to_string()),
            ("x-log-bodyrawsize".to_string(), raw_size.to_string()),
            ("x-log-date".to_string(), STAMP.to_string()),
            ("x-log-content-sha256".to_string(), content.to_string()),
        ]);
        for (name, value) in extra {
            headers.insert(name.to_string(), value.to_string());
        }
        headers
    }

    fn signature(method: &str, path: &str, query: &[(&str, &str)], headers: &BTreeMap<String, String>, content: &str) -> String {
        let query = query.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect();
        let authorization = sign_v4(method, path, &query, headers, content, STAMP, REGION, "testId", "testSecret").unwrap();
        authorization.rsplit_once("Signature=").map(|(_, signature)| signature.to_string()).unwrap_or_default()
    }

    /// Expected values come from the official alibabacloud_gateway_sls 0.4 signer run on the same inputs.
    #[test]
    fn test_signature_matches_the_official_sdk() {
        let unsigned_extras = [("accept", "application/json"), ("user-agent", "x"), ("accept-encoding", "gzip")];
        assert_eq!(
            signature("GET", "/logstores", &[], &headers(&unsigned_extras, EMPTY_SHA256, "0"), EMPTY_SHA256),
            "6a75c95adafa81c7f045740b80e871fcc04283f3ddc64ea8e6d5277c1cb79b84"
        );
        assert_eq!(
            signature(
                "GET",
                "/logstores/my-logstore/index",
                &[("type", "histogram"), ("from", "1700000000"), ("to", "1700000900"), ("query", "status: 500 and not path: \"/health\"")],
                &headers(&[], EMPTY_SHA256, "0"),
                EMPTY_SHA256
            ),
            "c14ea0563a48d09772f35f4493b52c32f86be4ccb2838a9fbe010038246e40c9"
        );
        let body = br#"{"from":1700000000,"to":1700000900,"line":20,"offset":0,"reverse":true,"query":"*","isAccurate":true}"#;
        let content = hex::encode(Sha256::digest(body));
        assert_eq!(content, "96a59aebe85da8d759e755e0678eeea1b787c775d8118a15ca8ffc19de9d43a6");
        assert_eq!(
            signature("POST", "/logstores/my-logstore/logs", &[], &headers(&[("accept-encoding", "gzip")], &content, &body.len().to_string()), &content),
            "48d3179e8afcfbc6fe1a41175997904c16d96fba02f1ac9095e2c1f4a7364441"
        );
        let special = [("type", "log"), ("query", "a*b~c (x) 中文 +&=")];
        assert_eq!(
            canonical_query(&special.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect()),
            "query=a%2Ab~c%20%28x%29%20%E4%B8%AD%E6%96%87%20%2B%26%3D&type=log"
        );
        assert_eq!(
            signature("GET", "/logstores/my-logstore", &special, &headers(&[], EMPTY_SHA256, "0"), EMPTY_SHA256),
            "56fdba8a760eb4119b083256e91c02a5f0364d50c8ce4b274187fbcb0a934f78"
        );
    }

    #[test]
    fn test_names_and_stamp() {
        assert!(valid_name("platform-fpms-qat"));
        assert!(valid_name("internal-diagnostic_log"));
        assert!(!valid_name("evil.example.com#"));
        assert!(!valid_name("a/b"));
        assert!(!valid_name("UPPER"));
        assert!(!valid_region("ap-southeast-1.evil"));
        let now = time::OffsetDateTime::from_unix_timestamp(1_725_256_800).unwrap();
        assert_eq!(utc_stamp(now), STAMP);
    }

    type Seen = Arc<std::sync::Mutex<Vec<String>>>;

    fn fake(status: u16, body: Vec<u8>, gzip: bool) -> (SlsClient, Seen) {
        let seen: Seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let http: Arc<dyn HttpClient> = http_client::FakeHttpClient::create({
            let seen = seen.clone();
            move |request| {
                let seen = seen.clone();
                let body = body.clone();
                async move {
                    seen.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.push(request.uri().to_string());
                    let mut response = http_client::Response::builder().status(status);
                    if gzip {
                        response = response.header("content-encoding", "gzip");
                    }
                    Ok(response.body(AsyncBody::from(body))?)
                }
            }
        });
        let client = SlsClient::new(http, REGION, "id", "secret", vec!["platform-qat-aliyun-logs".into()]).unwrap();
        (client, seen)
    }

    fn request() -> LogsRequest {
        LogsRequest { query: "*".into(), from: 1, to: 2, line: 20, offset: 0, newest_first: true }
    }

    #[test]
    fn test_projects_outside_the_allowlist_are_never_sent() {
        let (client, seen) = fake(200, b"{}".to_vec(), false);
        let result = futures::executor::block_on(client.get_logs("platform-prod-aliyun-logs", "platform-fpms-prod", &request()));
        assert_eq!(result, Err(SlsError::NotAllowed("platform-prod-aliyun-logs".into())));
        let result = futures::executor::block_on(client.list_logstores("x.evil"));
        assert!(matches!(result, Err(SlsError::InvalidName(_))));
        assert!(seen.lock().unwrap().is_empty());
    }

    #[test]
    fn test_logs_are_read_from_gzip() {
        let page = serde_json::json!({
            "meta": {"progress": "Incomplete", "hasSQL": false, "keys": ["status", "traceId"]},
            "data": [{"__time__": "1700000000", "status": "500", "traceId": "abc"}]
        });
        let mut compressed = Vec::new();
        futures::executor::block_on(async {
            let mut encoder = async_compression::futures::write::GzipEncoder::new(&mut compressed);
            futures::AsyncWriteExt::write_all(&mut encoder, page.to_string().as_bytes()).await?;
            futures::AsyncWriteExt::close(&mut encoder).await
        })
        .unwrap();
        let (client, seen) = fake(200, compressed, true);
        let page = futures::executor::block_on(client.get_logs("platform-qat-aliyun-logs", "platform-fpms-qat", &request())).unwrap();
        assert_eq!(page.progress, Progress::Incomplete);
        assert_eq!(page.rows[0], vec![("__time__".into(), "1700000000".into()), ("status".into(), "500".into()), ("traceId".into(), "abc".into())]);
        assert_eq!(seen.lock().unwrap()[0], "https://platform-qat-aliyun-logs.ap-southeast-1.log.aliyuncs.com/logstores/platform-fpms-qat/logs");
    }

    #[test]
    fn test_errors_keep_the_reason() {
        let forbidden = br#"{"errorCode":"Unauthorized","errorMessage":"denied by sts or ram"}"#.to_vec();
        let (client, _) = fake(403, forbidden, false);
        let result = futures::executor::block_on(client.list_projects());
        assert_eq!(result, Err(SlsError::Forbidden("denied by sts or ram".into())));
        let skew = br#"{"errorCode":"RequestTimeExpired","errorMessage":"too old"}"#.to_vec();
        let (client, _) = fake(400, skew, false);
        assert_eq!(futures::executor::block_on(client.list_projects()), Err(SlsError::ClockSkew("too old".into())));
        let (client, _) = fake(200, vec![b' '; (MAX_COMPRESSED_BYTES + 10) as usize], false);
        assert_eq!(futures::executor::block_on(client.list_projects()), Err(SlsError::TooLarge));
    }

    #[test]
    fn test_histograms() {
        let body = br#"[{"from":1,"to":2,"count":7,"progress":"Complete"},{"from":2,"to":3,"count":0,"progress":"Incomplete"}]"#.to_vec();
        let (client, seen) = fake(200, body, false);
        let buckets = futures::executor::block_on(client.histograms("platform-qat-aliyun-logs", "platform-fpms-qat", "level: ERROR", 1, 3)).unwrap();
        assert_eq!(buckets.iter().map(|bucket| (bucket.count, bucket.progress)).collect::<Vec<_>>(), vec![(7, Progress::Complete), (0, Progress::Incomplete)]);
        assert_eq!(
            seen.lock().unwrap()[0],
            "https://platform-qat-aliyun-logs.ap-southeast-1.log.aliyuncs.com/logstores/platform-fpms-qat/index?from=1&query=level%3A%20ERROR&to=3&type=histogram"
        );
    }
}
