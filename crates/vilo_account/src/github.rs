use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail};
use futures::AsyncReadExt as _;
use gpui::AsyncApp;
use http_client::{AsyncBody, HttpClient, Request, http::Method};
use serde::Deserialize;

/// Public id of the Vilo OAuth App. Device Flow needs no client secret.
const CLIENT_ID: &str = "Ov23liXNdavVq9qGqI34";
const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const USER_URL: &str = "https://api.github.com/user";
pub const VERIFICATION_URL: &str = "https://github.com/login/device";

pub struct DeviceCode {
    device_code: String,
    pub user_code: String,
    interval: Duration,
    deadline: Instant,
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    error: Option<String>,
    interval: Option<u64>,
}

#[derive(Deserialize)]
pub struct GithubUser {
    pub id: u64,
    pub login: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
}

pub async fn request_device_code(http: &Arc<dyn HttpClient>) -> Result<DeviceCode> {
    let response: DeviceCodeResponse =
        post_form(http, DEVICE_CODE_URL, &[("client_id", CLIENT_ID)]).await?;
    // The browser is only ever sent to GitHub's own page, whatever the response says.
    if response.verification_uri != VERIFICATION_URL {
        bail!("GitHub returned an unexpected verification page");
    }
    Ok(DeviceCode {
        device_code: response.device_code,
        user_code: response.user_code,
        interval: Duration::from_secs(response.interval.max(1)),
        deadline: Instant::now() + Duration::from_secs(response.expires_in),
    })
}

/// Polls until the user approves the code in the browser, following GitHub's
/// `interval` and `slow_down` rules.
pub async fn wait_for_token(
    http: &Arc<dyn HttpClient>,
    code: &DeviceCode,
    cx: &AsyncApp,
) -> Result<String> {
    let mut interval = code.interval;
    loop {
        cx.background_executor().timer(interval).await;
        if Instant::now() >= code.deadline {
            bail!("The GitHub code expired. Try again.");
        }
        let response: TokenResponse = post_form(
            http,
            TOKEN_URL,
            &[
                ("client_id", CLIENT_ID),
                ("device_code", &code.device_code),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ],
        )
        .await?;
        if let Some(token) = response.access_token {
            return Ok(token);
        }
        match response.error.as_deref() {
            Some("authorization_pending") => {}
            Some("slow_down") => {
                interval = response
                    .interval
                    .map_or(interval + Duration::from_secs(5), Duration::from_secs);
            }
            Some("expired_token") => bail!("The GitHub code expired. Try again."),
            Some("access_denied") => bail!("GitHub sign-in was cancelled."),
            Some(error) => bail!("GitHub sign-in failed ({error})."),
            None => bail!("GitHub returned no token"),
        }
    }
}

pub async fn fetch_user(http: &Arc<dyn HttpClient>, token: &str) -> Result<GithubUser> {
    let request = Request::builder()
        .method(Method::GET)
        .uri(USER_URL)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Vilo")
        .body(AsyncBody::empty())?;
    read_json(http, request).await
}

async fn post_form<T: serde::de::DeserializeOwned>(
    http: &Arc<dyn HttpClient>,
    url: &str,
    params: &[(&str, &str)],
) -> Result<T> {
    let body = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish();
    let request = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .body(AsyncBody::from(body.into_bytes()))?;
    read_json(http, request).await
}

async fn read_json<T: serde::de::DeserializeOwned>(
    http: &Arc<dyn HttpClient>,
    request: Request<AsyncBody>,
) -> Result<T> {
    let mut response = http.send(request).await?;
    let status = response.status();
    let mut body = String::new();
    response.body_mut().read_to_string(&mut body).await?;
    // Bodies are never logged: they can carry codes and tokens.
    if !status.is_success() {
        return Err(anyhow!("GitHub request failed with status {status}"));
    }
    serde_json::from_str(&body).context("GitHub returned an unexpected response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use http_client::{FakeHttpClient, Response};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[gpui::test]
    async fn test_wait_for_token_follows_pending_and_slow_down(cx: &mut TestAppContext) {
        let polls = Arc::new(AtomicUsize::new(0));
        let http: Arc<dyn HttpClient> = FakeHttpClient::create({
            let polls = polls.clone();
            move |request| {
                let polls = polls.clone();
                async move {
                    assert_eq!(request.uri().to_string(), TOKEN_URL);
                    let body = match polls.fetch_add(1, Ordering::SeqCst) {
                        0 => r#"{"error":"authorization_pending"}"#,
                        1 => r#"{"error":"slow_down","interval":2}"#,
                        _ => r#"{"access_token":"token"}"#,
                    };
                    Ok(Response::builder().status(200).body(body.into())?)
                }
            }
        });
        let code = DeviceCode {
            device_code: "device".into(),
            user_code: "ABCD-1234".into(),
            interval: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_secs(900),
        };
        let task = cx.update(|cx| {
            cx.spawn(async move |cx| wait_for_token(&http, &code, cx).await)
        });
        cx.executor().advance_clock(Duration::from_secs(10));
        assert_eq!(task.await.unwrap(), "token");
        assert_eq!(polls.load(Ordering::SeqCst), 3);
    }

    #[gpui::test]
    async fn test_wait_for_token_stops_when_denied(cx: &mut TestAppContext) {
        let http: Arc<dyn HttpClient> = FakeHttpClient::create(|_| async {
            Ok(Response::builder()
                .status(200)
                .body(r#"{"error":"access_denied"}"#.into())?)
        });
        let code = DeviceCode {
            device_code: "device".into(),
            user_code: "ABCD-1234".into(),
            interval: Duration::from_secs(1),
            deadline: Instant::now() + Duration::from_secs(900),
        };
        let task = cx.update(|cx| {
            cx.spawn(async move |cx| wait_for_token(&http, &code, cx).await)
        });
        cx.executor().advance_clock(Duration::from_secs(2));
        assert!(task.await.is_err());
    }
}
