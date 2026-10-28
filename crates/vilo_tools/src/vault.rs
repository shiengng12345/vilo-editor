use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use collections::BTreeMap;
use futures::AsyncReadExt as _;
use http_client::{
    AsyncBody, HttpClient, HttpRequestExt as _, RedirectPolicy, Request, Url, http::Method,
};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// A KV secret is small; anything larger is not a secret worth rendering.
const MAX_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;
/// Retries after `429 Too Many Requests`, waiting longer each time.
const RATE_LIMIT_RETRIES: u32 = 3;
/// Path segments are user data; keep `/` as the separator and escape the rest.
const SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

/// Why a Vault request failed, in terms the UI can act on.
#[derive(Debug, Clone, PartialEq)]
pub enum VaultError {
    /// 403: the token is valid for something, just not this.
    Forbidden,
    NotFound,
    /// The server answered with a redirect; the token is never sent on to it.
    Redirected,
    Status(u16),
    /// Still `429` after backing off.
    RateLimited,
    TooLarge,
    /// The path has `..`, `.`, backslashes or control characters.
    InvalidPath,
    Network(String),
    Unexpected,
}

impl std::fmt::Display for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultError::Forbidden => f.write_str("This token has no permission for that."),
            VaultError::NotFound => f.write_str("Nothing exists at that path."),
            VaultError::Redirected => f.write_str(
                "Vault redirected the request to another address. Vilo did not follow it; \
                 check the connection address.",
            ),
            VaultError::Status(503) => f.write_str("Vault is sealed or unavailable (503)."),
            VaultError::Status(status) => write!(f, "Vault answered with status {status}."),
            VaultError::RateLimited => f.write_str("Vault kept rejecting requests as too many (429)."),
            VaultError::TooLarge => f.write_str("The response is too large to show."),
            VaultError::InvalidPath => {
                f.write_str("Paths cannot contain “..”, “.”, backslashes or control characters.")
            }
            VaultError::Network(message) => write!(f, "Could not reach Vault: {message}"),
            VaultError::Unexpected => f.write_str("Vault returned an unexpected response."),
        }
    }
}

impl std::error::Error for VaultError {}

/// The scheme, host and port that a token is bound to. Anything else in the
/// address (a path, a trailing slash) does not change where the token goes.
pub fn normalize_origin(address: &str) -> Result<Url> {
    let url = Url::parse(address.trim()).context("Enter an address like http://127.0.0.1:8200")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        anyhow::bail!("The address must start with http:// or https://");
    }
    let mut origin = Url::parse(&url.origin().ascii_serialization())?;
    origin.set_path("/");
    Ok(origin)
}

#[derive(Clone)]
pub struct VaultClient {
    http: Arc<dyn HttpClient>,
    executor: Option<gpui::BackgroundExecutor>,
    origin: Url,
    token: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenInfo {
    pub display_name: String,
    pub policies: Vec<String>,
    /// Seconds; 0 means the token never expires.
    pub ttl: u64,
}

impl TokenInfo {
    pub fn is_root(&self) -> bool {
        self.policies.iter().any(|policy| policy == "root")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvVersion {
    V1,
    V2,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Mount {
    /// Without the trailing slash.
    pub path: String,
    pub version: KvVersion,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionState {
    Live,
    Deleted,
    Destroyed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Secret {
    /// Values keep their JSON type; strings are shown without quotes.
    pub values: BTreeMap<String, serde_json::Value>,
    pub version: Option<u64>,
    pub created_time: Option<String>,
    /// False when a number in the secret cannot be read back exactly (a huge
    /// integer or a long decimal); such secrets are not edited in Vilo.
    pub exact_numbers: bool,
}

/// Why a write did not (provably) happen.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteError {
    /// Someone wrote a newer version after the edit started.
    Conflict,
    Forbidden,
    /// The request may or may not have reached Vault; re-check before writing again.
    OutcomeUnknown(String),
    Failed(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Conflict => f.write_str(
                "This secret changed after you started editing. Your edits are kept; reload to see the newer version.",
            ),
            WriteError::Forbidden => f.write_str("This token cannot write here."),
            WriteError::OutcomeUnknown(reason) => write!(
                f,
                "Vilo could not tell whether the write happened ({reason}). Check before saving again."
            ),
            WriteError::Failed(reason) => f.write_str(reason),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VersionEntry {
    pub version: u64,
    pub created_time: String,
    pub state: VersionState,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SecretRead {
    Found(Secret),
    /// KV v2 keeps the version but its data is gone.
    Unavailable { version: u64, state: VersionState },
}

impl VaultClient {
    pub fn new(http: Arc<dyn HttpClient>, origin: Url, token: String) -> Self {
        Self {
            http,
            executor: None,
            origin,
            token,
        }
    }

    /// Lets the client wait and retry when Vault answers `429`.
    pub fn with_backoff(mut self, executor: gpui::BackgroundExecutor) -> Self {
        self.executor = Some(executor);
        self
    }

    pub async fn lookup_self(&self) -> Result<TokenInfo, VaultError> {
        let body = self.get("v1/auth/token/lookup-self", &[]).await?;
        parse_token_info(&body)
    }

    /// KV mounts the token can see. `sys/mounts` needs a broad policy, so the
    /// UI's own discovery endpoint is tried next; it carries no compatibility
    /// promise, which is why manual path entry exists as well.
    pub async fn kv_mounts(&self) -> Result<Vec<Mount>, VaultError> {
        match self.get("v1/sys/mounts", &[]).await {
            Ok(body) => parse_mounts(body.get("data").unwrap_or(&body)),
            Err(VaultError::Forbidden) => {
                let body = self.get("v1/sys/internal/ui/mounts", &[]).await?;
                parse_mounts(
                    body.pointer("/data/secret")
                        .ok_or(VaultError::Unexpected)?,
                )
            }
            Err(error) => Err(error),
        }
    }

    /// Child names under a folder; folders end with `/`. An empty folder and a
    /// missing one both list as empty, as Vault does not tell them apart.
    pub async fn list(&self, mount: &Mount, folder: &str) -> Result<Vec<String>, VaultError> {
        let path = kv_path(mount, KvOperation::List, folder)?;
        match self.get(&path, &[("list", "true")]).await {
            Ok(body) => Ok(body
                .pointer("/data/keys")
                .and_then(|keys| keys.as_array())
                .ok_or(VaultError::Unexpected)?
                .iter()
                .filter_map(|key| key.as_str().map(str::to_string))
                .collect()),
            Err(VaultError::NotFound) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    pub async fn read(
        &self,
        mount: &Mount,
        path: &str,
        version: Option<u64>,
    ) -> Result<SecretRead, VaultError> {
        let request_path = kv_path(mount, KvOperation::Read, path);
        let version_query = version.map(|version| version.to_string());
        let query = version_query
            .as_deref()
            .map(|version| vec![("version", version)])
            .unwrap_or_default();
        let request_path = request_path?;
        let (status, raw) = self.fetch_with_backoff(&request_path, &query).await?;
        if status == 400 {
            return Err(VaultError::Status(400));
        }
        let body: serde_json::Value = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
        if status == 404 {
            return unavailable_version(&body).ok_or(VaultError::NotFound);
        }
        let mut read = parse_secret(mount.version, &body)?;
        if let SecretRead::Found(secret) = &mut read {
            secret.exact_numbers = !has_inexact_numbers(&raw);
        }
        Ok(read)
    }

    /// The newest version number of a KV v2 secret, or `None` when nothing is stored there.
    pub async fn current_version(&self, mount: &Mount, path: &str) -> Result<Option<u64>, VaultError> {
        match self.get(&kv_path(mount, KvOperation::Metadata, path)?, &[]).await {
            Ok(body) => Ok(body.pointer("/data/current_version").and_then(|version| version.as_u64())),
            Err(VaultError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Writes a KV v2 secret only if its newest version is still `cas`
    /// (0 creates it only if absent). Returns the new version. Never retried.
    pub async fn write_v2(
        &self,
        mount: &Mount,
        path: &str,
        data: &serde_json::Map<String, serde_json::Value>,
        cas: u64,
    ) -> Result<u64, WriteError> {
        if mount.version != KvVersion::V2 {
            return Err(WriteError::Failed("KV v1 secrets are read-only in Vilo.".into()));
        }
        let request_path = kv_path(mount, KvOperation::Read, path)
            .map_err(|error| WriteError::Failed(error.to_string()))?;
        let body = serde_json::json!({ "options": { "cas": cas }, "data": data });
        let body = serde_json::to_vec(&body).map_err(|_| WriteError::Failed("Could not encode the secret.".into()))?;
        let (status, raw) = match self.fetch(Method::POST, &request_path, &[], Some(body)).await {
            Ok(response) => response,
            // Refused before anything was sent.
            Err(error @ (VaultError::Unexpected | VaultError::InvalidPath)) => {
                return Err(WriteError::Failed(error.to_string()));
            }
            Err(VaultError::Forbidden) => return Err(WriteError::Forbidden),
            Err(VaultError::Redirected) => return Err(WriteError::Failed(VaultError::Redirected.to_string())),
            Err(VaultError::RateLimited) => {
                return Err(WriteError::Failed("Vault rejected the write as too many requests (429).".into()));
            }
            Err(error) => return Err(WriteError::OutcomeUnknown(error.to_string())),
        };
        match status {
            200..=299 => serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|body| body.pointer("/data/version").and_then(|version| version.as_u64()))
                .ok_or_else(|| WriteError::OutcomeUnknown("Vault did not return the new version".into())),
            400 if is_cas_mismatch(&raw) => Err(WriteError::Conflict),
            status => Err(WriteError::Failed(format!("Vault refused the write (status {status})."))),
        }
    }

    pub async fn versions(&self, mount: &Mount, path: &str) -> Result<Vec<VersionEntry>, VaultError> {
        let body = self
            .get(&kv_path(mount, KvOperation::Metadata, path)?, &[])
            .await?;
        parse_versions(&body)
    }

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<serde_json::Value, VaultError> {
        let (status, raw) = self.fetch_with_backoff(path, query).await?;
        match status {
            404 => return Err(VaultError::NotFound),
            400 => return Err(VaultError::Status(400)),
            _ => {}
        }
        serde_json::from_str(&raw).map_err(|_| VaultError::Unexpected)
    }

    /// GET with backoff on `429`. Returns 2xx and 404 with their bodies; KV v2
    /// answers a deleted version with 404 and metadata, so 404 is not an error here.
    async fn fetch_with_backoff(&self, path: &str, query: &[(&str, &str)]) -> Result<(u16, String), VaultError> {
        let mut attempt = 0;
        loop {
            match self.fetch(Method::GET, path, query, None).await {
                Err(VaultError::RateLimited) if attempt < RATE_LIMIT_RETRIES => {
                    let Some(executor) = &self.executor else {
                        return Err(VaultError::RateLimited);
                    };
                    executor
                        .timer(Duration::from_millis(500 * 2u64.pow(attempt)))
                        .await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn fetch(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> Result<(u16, String), VaultError> {
        let mut url = self.origin.join(path).map_err(|_| VaultError::Unexpected)?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        // The token only goes to the bound origin, and only to the exact path
        // that was built: URL normalization must not have moved it.
        if url.origin() != self.origin.origin() || url.path() != format!("/{path}") {
            return Err(VaultError::Unexpected);
        }
        let mut request = Request::builder()
            .method(method)
            .uri(url.as_str())
            .header("X-Vault-Token", &self.token)
            .header("Accept", "application/json")
            .follow_redirects(RedirectPolicy::NoFollow)
            .timeout(REQUEST_TIMEOUT);
        if body.is_some() {
            request = request.header("Content-Type", "application/json");
        }
        let request = request
            .body(body.map_or_else(AsyncBody::empty, AsyncBody::from))
            .map_err(|_| VaultError::Unexpected)?;
        // Errors are reported without bodies: they can echo secret values.
        let mut response = self
            .http
            .send(request)
            .await
            .map_err(|error| VaultError::Network(network_reason(&error)))?;
        let status = response.status();
        if status.is_redirection() {
            return Err(VaultError::Redirected);
        }
        if status.as_u16() == 403 {
            return Err(VaultError::Forbidden);
        }
        if status.as_u16() == 429 {
            return Err(VaultError::RateLimited);
        }
        let mut raw = String::new();
        response
            .body_mut()
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_string(&mut raw)
            .await
            .map_err(|_| VaultError::Unexpected)?;
        if raw.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(VaultError::TooLarge);
        }
        match status.as_u16() {
            // Writes report conflicts as 400; callers read the error text.
            status @ (200..=299 | 400 | 404) => Ok((status, raw)),
            status => Err(VaultError::Status(status)),
        }
    }
}

fn is_cas_mismatch(raw: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|body| body.get("errors").cloned())
        .and_then(|errors| errors.as_array().cloned())
        .is_some_and(|errors| {
            errors
                .iter()
                .filter_map(|error| error.as_str())
                .any(|error| error.contains("check-and-set"))
        })
}

/// Only the error chain's outermost message: lower layers can include the URL
/// with its query, never the token, but keep it short for the UI anyway.
fn network_reason(error: &anyhow::Error) -> String {
    error.to_string().chars().take(200).collect()
}

enum KvOperation {
    List,
    Read,
    Metadata,
}

fn kv_path(mount: &Mount, operation: KvOperation, path: &str) -> Result<String, VaultError> {
    let segments = path.trim_matches('/').split('/').filter(|part| !part.is_empty());
    if segments
        .clone()
        .chain(mount.path.trim_matches('/').split('/'))
        .any(|part| part == "." || part == ".." || part.contains('\\') || part.chars().any(char::is_control))
    {
        return Err(VaultError::InvalidPath);
    }
    let segment = match (mount.version, operation) {
        (KvVersion::V1, _) => None,
        (KvVersion::V2, KvOperation::Read) => Some("data"),
        (KvVersion::V2, KvOperation::List | KvOperation::Metadata) => Some("metadata"),
    };
    let encoded = path
        .trim_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .map(|part| utf8_percent_encode(part, SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/");
    let mount_path = utf8_percent_encode(mount.path.trim_matches('/'), SEGMENT).to_string();
    let mut result = format!("v1/{mount_path}");
    if let Some(segment) = segment {
        result.push('/');
        result.push_str(segment);
    }
    if !encoded.is_empty() {
        result.push('/');
        result.push_str(&encoded);
    }
    // Vault lists a folder at its path with a trailing slash.
    if path.ends_with('/') || path.is_empty() {
        result.push('/');
    }
    Ok(result)
}

/// Whether raw JSON holds a number that `serde_json` cannot read back exactly
/// (no arbitrary precision here): integers beyond 64 bits, or decimals with
/// more significant digits than an `f64` keeps.
pub fn has_inexact_numbers(raw: &str) -> bool {
    let mut chars = raw.chars().peekable();
    let mut in_string = false;
    while let Some(character) = chars.next() {
        if in_string {
            match character {
                '\\' => {
                    chars.next();
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        if character == '"' {
            in_string = true;
            continue;
        }
        if character == '-' || character.is_ascii_digit() {
            let mut literal = character.to_string();
            while let Some(next) = chars.peek().copied().filter(|next| next.is_ascii_digit() || "+-.eE".contains(*next)) {
                literal.push(next);
                chars.next();
            }
            if literal.contains(['.', 'e', 'E']) {
                let mantissa = literal.split(['e', 'E']).next().unwrap_or_default();
                let digits = mantissa.chars().filter(char::is_ascii_digit).skip_while(|digit| *digit == '0').count();
                if digits > 15 || literal.parse::<f64>().map_or(true, |number| !number.is_finite()) {
                    return true;
                }
            } else if literal.parse::<i64>().is_err() && literal.parse::<u64>().is_err() {
                return true;
            }
        }
    }
    false
}

/// Parses JSON like `serde_json`, but rejects duplicate keys at any depth and
/// numbers that would not survive a round trip.
pub fn parse_strict_json(text: &str) -> Result<serde_json::Value, String> {
    use serde::de::{DeserializeSeed, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};

    struct Strict;

    impl<'de> DeserializeSeed<'de> for Strict {
        type Value = serde_json::Value;
        fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_any(self)
        }
    }

    impl<'de> Visitor<'de> for Strict {
        type Value = serde_json::Value;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("JSON")
        }
        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
            Ok(value.into())
        }
        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value.into())
        }
        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
            Ok(value.into())
        }
        fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| E::custom("number out of range"))
        }
        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
            Ok(value.into())
        }
        fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
            Ok(value.into())
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(serde_json::Value::Null)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut items = Vec::new();
            while let Some(item) = seq.next_element_seed(Strict)? {
                items.push(item);
            }
            Ok(serde_json::Value::Array(items))
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut object = serde_json::Map::new();
            while let Some(key) = map.next_key::<String>()? {
                if object.contains_key(&key) {
                    return Err(A::Error::custom(format!("duplicate key “{key}”")));
                }
                let value = map.next_value_seed(Strict)?;
                object.insert(key, value);
            }
            Ok(serde_json::Value::Object(object))
        }
    }

    if has_inexact_numbers(text) {
        return Err("A number here has more digits than Vilo can keep exactly.".into());
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = Strict.deserialize(&mut deserializer).map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

fn parse_token_info(body: &serde_json::Value) -> Result<TokenInfo, VaultError> {
    let data = body.get("data").ok_or(VaultError::Unexpected)?;
    Ok(TokenInfo {
        display_name: data
            .get("display_name")
            .and_then(|name| name.as_str())
            .unwrap_or_default()
            .to_string(),
        policies: data
            .get("policies")
            .and_then(|policies| policies.as_array())
            .map(|policies| {
                policies
                    .iter()
                    .filter_map(|policy| policy.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        ttl: data.get("ttl").and_then(|ttl| ttl.as_u64()).unwrap_or(0),
    })
}

fn parse_mounts(mounts: &serde_json::Value) -> Result<Vec<Mount>, VaultError> {
    #[derive(Deserialize)]
    struct MountInfo {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        options: Option<BTreeMap<String, String>>,
    }
    let mounts = mounts.as_object().ok_or(VaultError::Unexpected)?;
    let mut result = mounts
        .iter()
        .filter_map(|(path, info)| {
            let info = serde_json::from_value::<MountInfo>(info.clone()).ok()?;
            // `generic` is the old name of KV v1.
            if info.kind != "kv" && info.kind != "generic" {
                return None;
            }
            // Vault treats a KV mount without a version option as v1.
            let version = match info
                .options
                .as_ref()
                .and_then(|options| options.get("version"))
                .map(String::as_str)
            {
                Some("2") => KvVersion::V2,
                _ => KvVersion::V1,
            };
            Some(Mount {
                path: path.trim_end_matches('/').to_string(),
                version,
            })
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|mount| mount.path.to_lowercase());
    Ok(result)
}

fn parse_secret(version: KvVersion, body: &serde_json::Value) -> Result<SecretRead, VaultError> {
    let (values, metadata) = match version {
        KvVersion::V1 => (body.get("data"), None),
        KvVersion::V2 => (
            body.pointer("/data/data"),
            body.pointer("/data/metadata"),
        ),
    };
    let values = match values {
        Some(serde_json::Value::Object(values)) => values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        // A v2 version whose data was deleted reads back as `null`.
        Some(serde_json::Value::Null) => {
            return unavailable_version(body).ok_or(VaultError::Unexpected);
        }
        _ => return Err(VaultError::Unexpected),
    };
    Ok(SecretRead::Found(Secret {
        exact_numbers: true,
        values,
        version: metadata
            .and_then(|metadata| metadata.get("version"))
            .and_then(|version| version.as_u64()),
        created_time: metadata
            .and_then(|metadata| metadata.get("created_time"))
            .and_then(|time| time.as_str())
            .map(str::to_string),
    }))
}

fn version_state(metadata: &serde_json::Value) -> VersionState {
    if metadata.get("destroyed").and_then(|destroyed| destroyed.as_bool()) == Some(true) {
        VersionState::Destroyed
    } else if metadata
        .get("deletion_time")
        .and_then(|time| time.as_str())
        .is_some_and(|time| !time.is_empty())
    {
        VersionState::Deleted
    } else {
        VersionState::Live
    }
}

fn unavailable_version(body: &serde_json::Value) -> Option<SecretRead> {
    let metadata = body.pointer("/data/metadata")?;
    let state = version_state(metadata);
    if state == VersionState::Live {
        return None;
    }
    Some(SecretRead::Unavailable {
        version: metadata.get("version")?.as_u64()?,
        state,
    })
}

fn parse_versions(body: &serde_json::Value) -> Result<Vec<VersionEntry>, VaultError> {
    let versions = body
        .pointer("/data/versions")
        .and_then(|versions| versions.as_object())
        .ok_or(VaultError::Unexpected)?;
    let mut result = versions
        .iter()
        .filter_map(|(version, metadata)| {
            Some(VersionEntry {
                version: version.parse().ok()?,
                created_time: metadata
                    .get("created_time")
                    .and_then(|time| time.as_str())
                    .unwrap_or_default()
                    .to_string(),
                state: version_state(metadata),
            })
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|entry| std::cmp::Reverse(entry.version));
    Ok(result)
}

/// Shows a value the way it is stored: strings without quotes, the rest as JSON.
pub fn display_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mount(version: KvVersion) -> Mount {
        Mount {
            path: "FPMS-NT-V2".into(),
            version,
        }
    }

    #[test]
    fn test_kv_paths_per_version() {
        let v2 = mount(KvVersion::V2);
        let v1 = mount(KvVersion::V1);
        assert_eq!(kv_path(&v2, KvOperation::List, "").unwrap(), "v1/FPMS-NT-V2/metadata/");
        assert_eq!(
            kv_path(&v2, KvOperation::List, "ccms/").unwrap(),
            "v1/FPMS-NT-V2/metadata/ccms/"
        );
        assert_eq!(
            kv_path(&v2, KvOperation::Read, "ccms/uat").unwrap(),
            "v1/FPMS-NT-V2/data/ccms/uat"
        );
        assert_eq!(
            kv_path(&v2, KvOperation::Metadata, "ccms/uat").unwrap(),
            "v1/FPMS-NT-V2/metadata/ccms/uat"
        );
        assert_eq!(kv_path(&v1, KvOperation::List, "a/").unwrap(), "v1/FPMS-NT-V2/a/");
        assert_eq!(kv_path(&v1, KvOperation::Read, "a/b").unwrap(), "v1/FPMS-NT-V2/a/b");
        // Segments cannot climb out of the mount or smuggle a query.
        assert_eq!(
            kv_path(&v1, KvOperation::Read, "a b/c?x#y").unwrap(),
            "v1/FPMS-NT-V2/a%20b/c%3Fx%23y"
        );
    }

    #[test]
    fn test_kv_path_rejects_escaping_segments() {
        let v2 = mount(KvVersion::V2);
        for path in ["a/../b", "./a", "a\\b", "a/\u{7}b"] {
            assert_eq!(kv_path(&v2, KvOperation::Read, path), Err(VaultError::InvalidPath), "{path}");
        }
    }

    #[test]
    fn test_strict_json_and_number_exactness() {
        assert!(parse_strict_json(r#"{"a":1,"a":2}"#).unwrap_err().contains("duplicate"));
        assert!(parse_strict_json(r#"{"a":{"b":1,"b":1}}"#).is_err());
        assert!(parse_strict_json(r#"{"a":"{\"x\":1}"}"#).unwrap()["a"].is_string());
        assert!(parse_strict_json("{} trailing").is_err());
        assert!(has_inexact_numbers(r#"{"id":123456789012345678901}"#));
        assert!(has_inexact_numbers(r#"{"rate":0.12345678901234567}"#));
        assert!(!has_inexact_numbers(r#"{"id":9007199254740993,"rate":1.5,"s":"123456789012345678901"}"#));
    }

    #[test]
    fn test_cas_mismatch_detection() {
        assert!(is_cas_mismatch(r#"{"errors":["check-and-set parameter did not match the current version"]}"#));
        assert!(!is_cas_mismatch(r#"{"errors":["permission denied"]}"#));
    }

    #[test]
    fn test_normalize_origin() {
        assert_eq!(
            normalize_origin(" http://127.0.0.1:8200/ui/vault/auth ")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:8200/"
        );
        assert!(normalize_origin("ftp://vault").is_err());
        assert!(normalize_origin("vault.local").is_err());
    }

    #[test]
    fn test_parse_mounts_keeps_kv_only_and_defaults_to_v1() {
        let mounts = parse_mounts(&json!({
            "FPMS-NT-V2/": {"type": "kv", "options": {"version": "2"}},
            "secret/": {"type": "kv", "options": {"version": "1"}},
            "legacy/": {"type": "kv", "options": null},
            "old/": {"type": "generic"},
            "sys/": {"type": "system"},
            "cubbyhole/": {"type": "cubbyhole"},
        }))
        .unwrap();
        assert_eq!(
            mounts,
            vec![
                Mount { path: "FPMS-NT-V2".into(), version: KvVersion::V2 },
                Mount { path: "legacy".into(), version: KvVersion::V1 },
                Mount { path: "old".into(), version: KvVersion::V1 },
                Mount { path: "secret".into(), version: KvVersion::V1 },
            ]
        );
    }

    #[test]
    fn test_parse_secret_and_unavailable_versions() {
        let live = json!({"data": {
            "data": {"PORT": 8080, "HOST": "db"},
            "metadata": {"version": 16, "created_time": "2026-10-01T00:00:00Z", "deletion_time": "", "destroyed": false}
        }});
        let SecretRead::Found(secret) = parse_secret(KvVersion::V2, &live).unwrap() else {
            panic!("expected a live secret");
        };
        assert_eq!(secret.version, Some(16));
        assert_eq!(display_value(&secret.values["PORT"]), "8080");
        assert_eq!(display_value(&secret.values["HOST"]), "db");

        let deleted = json!({"data": {"data": null, "metadata": {"version": 3, "deletion_time": "2026-10-02T00:00:00Z", "destroyed": false}}});
        assert_eq!(
            unavailable_version(&deleted),
            Some(SecretRead::Unavailable { version: 3, state: VersionState::Deleted })
        );
        let destroyed = json!({"data": {"data": null, "metadata": {"version": 2, "deletion_time": "", "destroyed": true}}});
        assert_eq!(
            parse_secret(KvVersion::V2, &destroyed).unwrap(),
            SecretRead::Unavailable { version: 2, state: VersionState::Destroyed }
        );

        let v1 = json!({"data": {"KEY": "value"}});
        assert!(matches!(parse_secret(KvVersion::V1, &v1).unwrap(), SecretRead::Found(_)));
    }

    #[test]
    fn test_root_token_detected_from_policies() {
        let info = parse_token_info(&json!({"data": {"display_name": "token", "policies": ["default", "root"], "ttl": 0}})).unwrap();
        assert!(info.is_root());
    }
}
