use std::{
    io::{ErrorKind, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail};
use futures::channel::oneshot;

const CALLBACK_PATH: &str = "/callback";
const MAX_REQUEST_BYTES: usize = 8 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq)]
pub(crate) enum CallbackOutcome {
    Code(String),
    Denied(String),
}

/// A one-shot loopback listener for an OAuth redirect. It only accepts
/// `GET /callback` carrying the expected `state`; anything else gets an error
/// response and the listener keeps waiting, so a stray request cannot end the attempt.
pub(crate) struct CallbackListener {
    pub redirect_uri: String,
    receiver: oneshot::Receiver<Result<CallbackOutcome>>,
    cancelled: Arc<AtomicBool>,
}

impl CallbackListener {
    pub fn bind(expected_state: String, deadline: Duration) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("binding OAuth callback port")?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let (sender, receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        std::thread::Builder::new()
            .name("vilo-oauth-callback".into())
            .spawn({
                let cancelled = cancelled.clone();
                move || {
                    let result = serve(listener, &expected_state, deadline, &cancelled);
                    sender.send(result).ok();
                }
            })?;
        Ok(Self {
            redirect_uri: format!("http://127.0.0.1:{port}{CALLBACK_PATH}"),
            receiver,
            cancelled,
        })
    }

    pub async fn outcome(mut self) -> Result<CallbackOutcome> {
        (&mut self.receiver)
            .await
            .map_err(|_| anyhow!("sign-in was cancelled"))?
    }
}

impl Drop for CallbackListener {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

fn serve(
    listener: TcpListener,
    expected_state: &str,
    deadline: Duration,
    cancelled: &AtomicBool,
) -> Result<CallbackOutcome> {
    let started = Instant::now();
    loop {
        if cancelled.load(Ordering::Relaxed) {
            bail!("sign-in was cancelled");
        }
        if started.elapsed() > deadline {
            bail!("timed out waiting for the browser sign-in");
        }
        match listener.accept() {
            Ok((stream, _)) => match handle(stream, expected_state) {
                Ok(Some(outcome)) => return Ok(outcome),
                Ok(None) => {}
                Err(error) => log::warn!("rejected OAuth callback request: {error}"),
            },
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(POLL_INTERVAL)
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn handle(mut stream: TcpStream, expected_state: &str) -> Result<Option<CallbackOutcome>> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let request = read_request_head(&mut stream)?;
    let outcome = parse_request(&request, expected_state);
    let (status, body) = match &outcome {
        Ok(CallbackOutcome::Code(_)) => (
            "200 OK",
            "You are signed in to Vilo. You can close this tab.",
        ),
        Ok(CallbackOutcome::Denied(_)) => {
            ("200 OK", "Sign-in was cancelled. You can close this tab.")
        }
        Err(RequestError::NotFound) => ("404 Not Found", "Not found."),
        Err(RequestError::Invalid(_)) => ("400 Bad Request", "Invalid sign-in response."),
    };
    let page = format!(
        "<!doctype html><meta charset=utf-8><title>Vilo</title>\
         <body style=\"font-family:-apple-system,sans-serif;padding:48px\">{body}</body>"
    );
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
        page.len()
    )?;
    match outcome {
        Ok(outcome) => Ok(Some(outcome)),
        Err(RequestError::NotFound) => Ok(None),
        Err(RequestError::Invalid(reason)) => Err(anyhow!(reason)),
    }
}

fn read_request_head(stream: &mut TcpStream) -> Result<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        if buffer.len() > MAX_REQUEST_BYTES {
            bail!("request too large");
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

#[derive(Debug, PartialEq)]
enum RequestError {
    NotFound,
    Invalid(&'static str),
}

fn parse_request(request: &str, expected_state: &str) -> Result<CallbackOutcome, RequestError> {
    let request_line = request.lines().next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Err(RequestError::Invalid("malformed request line"));
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if method != "GET" || path != CALLBACK_PATH {
        return Err(RequestError::NotFound);
    }

    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let slot = match key.as_ref() {
            "code" => &mut code,
            "state" => &mut state,
            "error" => &mut error,
            _ => continue,
        };
        if slot.replace(value.into_owned()).is_some() {
            return Err(RequestError::Invalid("duplicate callback parameter"));
        }
    }

    let Some(state) = state else {
        return Err(RequestError::Invalid("missing state"));
    };
    if !constant_time_eq(state.as_bytes(), expected_state.as_bytes()) {
        return Err(RequestError::Invalid("state mismatch"));
    }
    match (code, error) {
        (Some(code), None) if !code.is_empty() => Ok(CallbackOutcome::Code(code)),
        (None, Some(error)) => Ok(CallbackOutcome::Denied(error)),
        _ => Err(RequestError::Invalid(
            "expected exactly one of code or error",
        )),
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(target: &str) -> String {
        format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    }

    #[test]
    fn test_parse_request() {
        assert_eq!(
            parse_request(&request("/callback?code=abc&state=s1"), "s1"),
            Ok(CallbackOutcome::Code("abc".into()))
        );
        assert_eq!(
            parse_request(&request("/callback?error=access_denied&state=s1"), "s1"),
            Ok(CallbackOutcome::Denied("access_denied".into()))
        );
        assert_eq!(
            parse_request(&request("/favicon.ico"), "s1"),
            Err(RequestError::NotFound)
        );
        assert_eq!(
            parse_request("POST /callback?code=abc&state=s1 HTTP/1.1\r\n\r\n", "s1"),
            Err(RequestError::NotFound)
        );
        for rejected in [
            "/callback?code=abc&state=other",
            "/callback?code=abc",
            "/callback?code=abc&error=x&state=s1",
            "/callback?state=s1",
            "/callback?code=abc&code=def&state=s1",
            "/callback?code=abc&state=s1&state=s1",
        ] {
            assert!(
                matches!(
                    parse_request(&request(rejected), "s1"),
                    Err(RequestError::Invalid(_))
                ),
                "{rejected}"
            );
        }
    }

    #[test]
    fn test_listener_ignores_stray_requests_and_accepts_valid_callback() {
        let listener = CallbackListener::bind("s1".into(), Duration::from_secs(10)).unwrap();
        let address = listener
            .redirect_uri
            .trim_start_matches("http://")
            .trim_end_matches(CALLBACK_PATH)
            .to_string();
        let send = |target: &str| {
            let mut stream = TcpStream::connect(&address).unwrap();
            stream.write_all(request(target).as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        };
        assert!(send("/favicon.ico").starts_with("HTTP/1.1 404"));
        assert!(send("/callback?code=x&state=wrong").starts_with("HTTP/1.1 400"));
        assert!(send("/callback?code=x&state=s1").starts_with("HTTP/1.1 200"));
        assert_eq!(
            futures::executor::block_on(listener.outcome()).unwrap(),
            CallbackOutcome::Code("x".into())
        );
    }
}
