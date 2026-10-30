//! The Profiler, as RedisInsight's: MONITOR on one node for a limited time. MONITOR makes the
//! server copy every command it runs to this connection, which costs it throughput, so a run is
//! short and ends by itself.
//!
//! The connection is Vilo's own: fred's MONITOR parser drops arguments that contain escapes, and
//! reading the lines here bounds each one as it arrives. Closing it ends MONITOR at once.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use fred::types::config::ServerConfig;
use futures::channel::oneshot;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    net::TcpStream,
};

use crate::redis::{Bytes, RedisSession, ScanNode};

pub const DEFAULT_SECONDS: u64 = 10;
pub const MAX_SECONDS: u64 = 60;
/// A line longer than this is cut, and says so; the rest is read past without being kept.
pub const MAX_LINE_BYTES: usize = 16 * 1024;
/// Lines waiting for the view; lines beyond this are dropped and counted as lost.
const QUEUE_BYTES: usize = 4 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// What the allocator keeps beside each allocation, roughly.
const ALLOCATION_OVERHEAD: usize = 16;

#[derive(Clone, Debug, PartialEq)]
pub struct MonitorLine {
    /// Unix seconds with microseconds, as the server wrote it.
    pub time: String,
    pub db: String,
    /// The client's address, `lua` for a script, or `unix:…`.
    pub client: String,
    /// The command name first, then its arguments, unescaped.
    pub args: Vec<Bytes>,
    /// The line was longer than `MAX_LINE_BYTES`; the last argument shown may be partial.
    pub cut: bool,
    /// The line could not be read as MONITOR output; `args` holds it as received.
    pub raw: bool,
}

impl MonitorLine {
    /// What keeping the line costs against the queue and view budgets: the entry itself and
    /// every allocation it holds. Each argument is an allocation of its own, so many empty ones
    /// are not free.
    pub fn cost(&self) -> usize {
        std::mem::size_of::<Self>()
            // The three strings and the argument vector.
            + 4 * ALLOCATION_OVERHEAD
            + self.time.capacity()
            + self.db.capacity()
            + self.client.capacity()
            + self.args.capacity() * std::mem::size_of::<Bytes>()
            + self
                .args
                .iter()
                .map(|arg| arg.capacity() + ALLOCATION_OVERHEAD)
                .sum::<usize>()
    }
}

#[derive(Default)]
struct Shared {
    started: bool,
    lines: VecDeque<MonitorLine>,
    bytes: usize,
    lost: u64,
    ended: Option<Result<String, String>>,
}

#[derive(Debug, Default, PartialEq)]
pub struct MonitorBatch {
    /// The server accepted MONITOR since the last batch.
    pub started: bool,
    pub lines: Vec<MonitorLine>,
    /// Lines dropped since the last batch because the view did not keep up.
    pub lost: u64,
    /// Why the run ended once it has, as an error when it failed; nothing arrives after it.
    pub ended: Option<Result<String, String>>,
}

/// A MONITOR run, from connecting to its end. Dropping it closes the connection, also while it
/// is still being opened.
pub struct Monitor {
    shared: Arc<Mutex<Shared>>,
    _stop: oneshot::Sender<()>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    // The reader only pushes and counts; a panic there leaves nothing half-done worth refusing.
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Monitor {
    pub fn take(&self) -> MonitorBatch {
        let mut shared = lock(&self.shared);
        shared.bytes = 0;
        MonitorBatch {
            started: std::mem::take(&mut shared.started),
            // Taken whole, so the queue does not keep a busy moment's capacity afterwards.
            lines: std::mem::take(&mut shared.lines).into(),
            lost: std::mem::take(&mut shared.lost),
            ended: shared.ended.take(),
        }
    }
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Reads one line into `line`, keeping at most `MAX_LINE_BYTES`. `None` at the end of the
/// stream; otherwise whether the line was cut.
async fn read_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> Result<Option<bool>> {
    line.clear();
    // One byte past the limit is kept, so the `\r` ending a line of exactly the limit does not
    // make it count as cut.
    let keep = MAX_LINE_BYTES + 1;
    let mut overflow = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(None);
        }
        let (end, used) = match available.iter().position(|byte| *byte == b'\n') {
            Some(newline) => (Some(newline), newline + 1),
            None => (None, available.len()),
        };
        let content = &available[..end.unwrap_or(available.len())];
        let room = keep.saturating_sub(line.len());
        if content.len() > room {
            overflow = true;
        }
        line.extend_from_slice(&content[..content.len().min(room)]);
        reader.consume(used);
        if end.is_some() {
            if !overflow && line.last() == Some(&b'\r') {
                line.pop();
            }
            let cut = overflow || line.len() > MAX_LINE_BYTES;
            line.truncate(MAX_LINE_BYTES);
            return Ok(Some(cut));
        }
    }
}

/// One reply line of the handshake: `+…` is fine, `-…` is the server's refusal.
async fn expect_ok<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R, step: &str) -> Result<()> {
    let mut line = Vec::new();
    match read_line(reader, &mut line).await? {
        None => bail!("The server closed the connection during {step}."),
        Some(_) if line.first() == Some(&b'+') => Ok(()),
        Some(_) => bail!(
            "{step} was refused: {}",
            String::from_utf8_lossy(line.strip_prefix(b"-").unwrap_or(&line))
        ),
    }
}

/// Reads a quoted argument as Redis writes it (sdscatrepr): `\\`, `\"`, `\n`, `\r`, `\t`, `\a`,
/// `\b` and `\xHH`. Returns the argument and the rest; `None` when the quote never closes,
/// unless `partial`, where the line was cut: then the argument so far, without an escape the
/// cut left unfinished.
fn quoted(input: &[u8], partial: bool) -> Option<(Bytes, &[u8])> {
    let mut rest = input.strip_prefix(b"\"")?;
    let mut out = Vec::new();
    loop {
        match rest {
            [] => return partial.then_some((out, rest)),
            [b'\\'] | [b'\\', b'x'] | [b'\\', b'x', _] if partial => return Some((out, &[])),
            [b'"', after @ ..] => return Some((out, after)),
            [b'\\', b'x', high, low, after @ ..]
                if high.is_ascii_hexdigit() && low.is_ascii_hexdigit() =>
            {
                let digit = |byte: u8| (byte as char).to_digit(16).unwrap_or(0) as u8;
                out.push(digit(*high) * 16 + digit(*low));
                rest = after;
            }
            [b'\\', escaped, after @ ..] => {
                out.push(match escaped {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'a' => 0x07,
                    b'b' => 0x08,
                    other => *other,
                });
                rest = after;
            }
            [byte, after @ ..] => {
                out.push(*byte);
                rest = after;
            }
        }
    }
}

/// Parses `+1700000000.123456 [0 127.0.0.1:5000] "SET" "k" "v"`. A cut line keeps the
/// arguments read so far plus the partial last one.
pub fn parse_line(line: &[u8], cut: bool) -> MonitorLine {
    let raw = || MonitorLine {
        time: String::new(),
        db: String::new(),
        client: String::new(),
        args: vec![line.to_vec()],
        cut,
        raw: true,
    };
    let Some(body) = line.strip_prefix(b"+") else {
        return raw();
    };
    let Some(space) = body.iter().position(|byte| *byte == b' ') else {
        return raw();
    };
    let (time, rest) = body.split_at(space);
    let Some(rest) = rest.strip_prefix(b" [") else {
        return raw();
    };
    let Some(close) = rest.iter().position(|byte| *byte == b']') else {
        return raw();
    };
    let (inside, rest) = rest.split_at(close);
    let mut rest = rest.strip_prefix(b"]").unwrap_or(rest);
    let inside = String::from_utf8_lossy(inside);
    let Some((db, client)) = inside.split_once(' ') else {
        return raw();
    };
    let mut args = Vec::new();
    loop {
        rest = rest.strip_prefix(b" ").unwrap_or(rest);
        if rest.is_empty() {
            break;
        }
        match quoted(rest, false) {
            Some((arg, after)) => {
                args.push(arg);
                rest = after;
            }
            // Only a cut line may end inside an argument; keep what arrived of it.
            None if cut => {
                if let Some((arg, _)) = quoted(rest, true) {
                    args.push(arg);
                }
                break;
            }
            None => return raw(),
        }
    }
    if args.is_empty() {
        return raw();
    }
    MonitorLine {
        time: String::from_utf8_lossy(time).into_owned(),
        db: db.to_string(),
        client: client.to_string(),
        args,
        cut,
        raw: false,
    }
}

/// A plain or TLS connection to read MONITOR from.
trait Connection: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Connection for T {}

/// TLS settings, the node's own name for the handshake, and the connected host to fall back to
/// when rustls does not take the node's name (certificates are checked against that host anyway).
type Tls = (Arc<rustls::ClientConfig>, String, String);

async fn open(
    host: String,
    port: u16,
    tls: Option<Tls>,
    username: Option<String>,
    password: Option<String>,
) -> Result<BufReader<Box<dyn Connection>>> {
    // As a (host, port) pair, so an IPv6 address needs no brackets.
    let stream = TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("Could not connect to {host} port {port} for the profiler"))?;
    let stream: Box<dyn Connection> = match tls {
        None => Box::new(stream),
        Some((config, node, connected)) => {
            let name = rustls::pki_types::ServerName::try_from(node.clone())
                .or_else(|_| rustls::pki_types::ServerName::try_from(connected.clone()))
                .with_context(|| format!("{node} is not a valid TLS server name"))?;
            Box::new(
                tokio_rustls::TlsConnector::from(config)
                    .connect(name, stream)
                    .await
                    .context("The TLS handshake for the profiler failed")?,
            )
        }
    };
    let mut reader = BufReader::new(stream);
    if let Some(password) = &password {
        let mut args: Vec<&[u8]> = vec![b"AUTH"];
        if let Some(username) = &username {
            args.push(username.as_bytes());
        }
        args.push(password.as_bytes());
        reader.get_mut().write_all(&encode(&args)).await?;
        expect_ok(&mut reader, "Signing in").await?;
    }
    reader.get_mut().write_all(&encode(&[b"MONITOR"])).await?;
    expect_ok(&mut reader, "MONITOR").await?;
    Ok(reader)
}

impl RedisSession {
    /// Starts MONITOR on `node` for `seconds` (1–`MAX_SECONDS`) on a connection of its own.
    /// Returns at once; connecting and the server's answer arrive in the batches.
    pub async fn monitor(&self, node: &ScanNode, seconds: u64) -> Result<Monitor> {
        anyhow::ensure!(
            (1..=MAX_SECONDS).contains(&seconds),
            "A profiler run lasts between 1 and {MAX_SECONDS} seconds."
        );
        let server = match (&node.server, &self.config.server) {
            (Some(server), _) => server.clone(),
            (None, ServerConfig::Centralized { server }) => server.clone(),
            _ => bail!(
                "The profiler works on a single server or one cluster node; this connection is neither."
            ),
        };
        let host = server.host.to_string();
        let port = server.port;
        // The node's own name for the handshake; the certificate is checked against the
        // connected host by the session's verifier.
        let tls = self.tls.clone().map(|(config, connected)| {
            (config, host.trim_matches(['[', ']']).to_string(), connected)
        });
        let username = self.config.username.clone();
        let password = self.config.password.clone();
        let shared = Arc::new(Mutex::new(Shared::default()));
        let (stop, mut stopped) = oneshot::channel::<()>();
        tokio::spawn({
            let shared = shared.clone();
            async move {
                let opened = tokio::select! {
                    _ = &mut stopped => Err(Ok("Stopped.".to_string())),
                    opened = tokio::time::timeout(CONNECT_TIMEOUT, open(host, port, tls, username, password)) => match opened {
                        Ok(Ok(reader)) => Ok(reader),
                        Ok(Err(error)) => Err(Err(format!("{error:#}"))),
                        Err(_) => Err(Err("Starting MONITOR timed out.".to_string())),
                    },
                };
                let mut reader = match opened {
                    Ok(reader) => reader,
                    Err(ended) => {
                        lock(&shared).ended = Some(ended);
                        return;
                    }
                };
                lock(&shared).started = true;
                let deadline = tokio::time::sleep(Duration::from_secs(seconds));
                tokio::pin!(deadline);
                let mut line = Vec::new();
                let ended = loop {
                    tokio::select! {
                        _ = &mut stopped => break Ok("Stopped.".to_string()),
                        _ = &mut deadline => break Ok(format!("Stopped after {seconds} s.")),
                        read = read_line(&mut reader, &mut line) => match read {
                            Ok(Some(cut)) => {
                                let parsed = parse_line(&line, cut);
                                let mut shared = lock(&shared);
                                let cost = parsed.cost();
                                if shared.bytes + cost > QUEUE_BYTES {
                                    shared.lost += 1;
                                } else {
                                    shared.bytes += cost;
                                    shared.lines.push_back(parsed);
                                }
                            }
                            Ok(None) => break Err("The server closed the connection.".to_string()),
                            Err(error) => break Err(format!("The connection failed: {error}")),
                        },
                    }
                };
                // Dropping the reader below closes the socket, which ends MONITOR on the server.
                lock(&shared).ended = Some(ended);
            }
        });
        Ok(Monitor {
            shared,
            _stop: stop,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::prelude::Value;

    #[test]
    fn test_monitor_lines_are_unescaped() {
        let line = parse_line(
            br#"+1700000000.123456 [0 172.17.0.1:49896] "SET" "probe:\"q\"" "a\x00\xff\\b \n""#,
            false,
        );
        assert_eq!(line.time, "1700000000.123456");
        assert_eq!(line.db, "0");
        assert_eq!(line.client, "172.17.0.1:49896");
        assert_eq!(
            line.args,
            vec![
                b"SET".to_vec(),
                b"probe:\"q\"".to_vec(),
                b"a\x00\xff\\b \n".to_vec()
            ]
        );
        assert!(!line.raw && !line.cut);

        let script = parse_line(br#"+1.5 [3 lua] "get" "k""#, false);
        assert_eq!((script.db.as_str(), script.client.as_str()), ("3", "lua"));
        let unix = parse_line(br#"+1.5 [0 unix:/tmp/redis.sock] "PING""#, false);
        assert_eq!(unix.client, "unix:/tmp/redis.sock");

        let partial = parse_line(br#"+1.5 [0 1.2.3.4:5] "SET" "k" "abc"#, true);
        assert_eq!(
            partial.args,
            vec![b"SET".to_vec(), b"k".to_vec(), b"abc".to_vec()]
        );
        assert!(partial.cut && !partial.raw);
        for (cut_inside, kept) in [
            (&br#"+1.5 [0 c] "SET" "ab\x4"#[..], &b"ab"[..]),
            (br#"+1.5 [0 c] "SET" "ab\"#, b"ab"),
            (br#"+1.5 [0 c] "SET" "ab\x"#, b"ab"),
            (br#"+1.5 [0 c] "SET" "a\"b"#, b"a\"b"),
        ] {
            let line = parse_line(cut_inside, true);
            assert_eq!(line.args.get(1).map(Vec::as_slice), Some(kept));
        }

        for bad in [
            &b"-ERR something"[..],
            b"+1.5 no brackets",
            br#"+1.5 [0 c] "open"#,
            b"+1.5 [0 c]",
        ] {
            let line = parse_line(bad, false);
            assert!(line.raw, "{}", String::from_utf8_lossy(bad));
            assert_eq!(line.args, vec![bad.to_vec()]);
        }
    }

    #[tokio::test]
    async fn test_lines_are_read_within_their_limit() {
        let mut long = vec![b'x'; MAX_LINE_BYTES * 3];
        long.extend_from_slice(b"\r\nnext\r\n");
        let mut reader = BufReader::with_capacity(1000, &long[..]);
        let mut line = Vec::new();
        assert_eq!(read_line(&mut reader, &mut line).await.unwrap(), Some(true));
        assert_eq!(line.len(), MAX_LINE_BYTES);
        assert_eq!(
            read_line(&mut reader, &mut line).await.unwrap(),
            Some(false)
        );
        assert_eq!(line, b"next");
        assert_eq!(read_line(&mut reader, &mut line).await.unwrap(), None);

        let mut exact = vec![b'y'; MAX_LINE_BYTES];
        exact.extend_from_slice(b"\r\n");
        let mut reader = BufReader::with_capacity(1000, &exact[..]);
        assert_eq!(
            read_line(&mut reader, &mut line).await.unwrap(),
            Some(false),
            "a line of exactly the limit is not cut"
        );
        assert_eq!(line.len(), MAX_LINE_BYTES);
    }

    #[test]
    fn test_empty_arguments_are_not_free() {
        let many = parse_line(
            &[&b"+1.5 [0 c] \"DEL\""[..], &b" \"\"".repeat(5000)].concat(),
            false,
        );
        assert_eq!(many.args.len(), 5001);
        assert!(many.cost() >= 5001 * (std::mem::size_of::<Bytes>() + ALLOCATION_OVERHEAD));
        let short = parse_line(br#"+1.5 [0 c] "PING""#, false);
        assert!(short.cost() > std::mem::size_of::<MonitorLine>() + 4 * ALLOCATION_OVERHEAD);
    }

    /// Tests that count the single server's monitor clients run one at a time.
    static SINGLE_SERVER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Waits until the server accepted MONITOR, or returns why the run ended instead.
    async fn opened(monitor: &Monitor) -> Result<(), String> {
        for _ in 0..200 {
            let batch = monitor.take();
            if batch.started {
                return Ok(());
            }
            match batch.ended {
                Some(Ok(reason)) | Some(Err(reason)) => return Err(reason),
                None => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        Err("never opened".into())
    }

    async fn wait_for(
        monitor: &Monitor,
        seen: &mut Vec<MonitorLine>,
        done: impl Fn(&[MonitorLine]) -> bool,
    ) {
        for _ in 0..100 {
            seen.extend(monitor.take().lines);
            if done(seen) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn monitors(session: &RedisSession) -> usize {
        let clients = session.clients().await;
        clients
            .items
            .iter()
            .filter(|client| client.get("flags").contains('O'))
            .count()
    }

    #[tokio::test]
    async fn test_monitor_against_redis() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let _serial = SINGLE_SERVER.lock().await;
        let session = RedisSession::connect(&url).await.unwrap();
        let node = session.scan_nodes().remove(0);
        let before = monitors(&session).await;
        let monitor = session.monitor(&node, 30).await.unwrap();
        opened(&monitor).await.unwrap();
        assert_eq!(monitors(&session).await, before + 1);
        let key = b"vilo_test:monitor:\"q\"".to_vec();
        let value = b"a\x00\xff\\b \n".to_vec();
        session
            .call(
                "SET",
                vec![
                    Value::Bytes(key.clone().into()),
                    Value::Bytes(value.clone().into()),
                ],
            )
            .await
            .unwrap();
        let big = vec![b'z'; MAX_LINE_BYTES * 2];
        session
            .call(
                "SET",
                vec!["vilo_test:monitor:big".into(), Value::Bytes(big.into())],
            )
            .await
            .unwrap();
        let mut seen = Vec::new();
        wait_for(&monitor, &mut seen, |seen| {
            seen.iter()
                .any(|line| line.args.get(1).is_some_and(|arg| arg.ends_with(b":big")))
        })
        .await;
        let set = seen
            .iter()
            .find(|line| line.args.get(1) == Some(&key))
            .expect("the SET was seen");
        assert_eq!(set.args, vec![b"SET".to_vec(), key.clone(), value]);
        let cut = seen
            .iter()
            .find(|line| line.args.get(1).is_some_and(|arg| arg.ends_with(b":big")))
            .expect("the big SET was seen");
        assert!(cut.cut && !cut.raw);
        assert!(
            cut.args
                .get(2)
                .is_some_and(|arg| arg.len() < MAX_LINE_BYTES)
        );

        drop(monitor);
        for _ in 0..50 {
            if monitors(&session).await == before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            monitors(&session).await,
            before,
            "stopping closes the connection"
        );
        session
            .call(
                "DEL",
                vec![Value::Bytes(key.into()), "vilo_test:monitor:big".into()],
            )
            .await
            .unwrap();

        let short = session.monitor(&node, 1).await.unwrap();
        let mut ended = None;
        for _ in 0..60 {
            ended = short.take().ended;
            if ended.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ended, Some(Ok("Stopped after 1 s.".to_string())));
        assert!(session.monitor(&node, 0).await.is_err());
        assert!(session.monitor(&node, MAX_SECONDS + 1).await.is_err());
    }

    #[tokio::test]
    async fn test_monitor_signs_in_as_the_connection_does() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let _serial = SINGLE_SERVER.lock().await;
        let admin = RedisSession::connect(&url).await.unwrap();
        let address = url.trim_start_matches("redis://");
        for (user, rules) in [
            ("vilo_test_monitor", &["+@all"][..]),
            ("vilo_test_no_monitor", &["+@all", "-monitor"][..]),
        ] {
            let mut args: Vec<Value> = vec![
                "SETUSER".into(),
                user.into(),
                "reset".into(),
                "on".into(),
                ">secret-pw".into(),
                "~*".into(),
            ];
            args.extend(rules.iter().map(|rule| Value::from(*rule)));
            admin.call("ACL", args).await.unwrap();
        }
        let allowed =
            RedisSession::connect(&format!("redis://vilo_test_monitor:secret-pw@{address}"))
                .await
                .unwrap();
        let node = allowed.scan_nodes().remove(0);
        let run = allowed.monitor(&node, 5).await.unwrap();
        opened(&run).await.unwrap();
        drop(run);

        let refused =
            RedisSession::connect(&format!("redis://vilo_test_no_monitor:secret-pw@{address}"))
                .await
                .unwrap();
        let error = opened(&refused.monitor(&node, 5).await.unwrap())
            .await
            .expect_err("MONITOR is not allowed");
        assert!(error.contains("MONITOR was refused"), "{error}");

        // The open session stays signed in; the profiler's new connection uses the old password.
        admin
            .call(
                "ACL",
                vec![
                    "SETUSER".into(),
                    "vilo_test_monitor".into(),
                    "resetpass".into(),
                    ">changed-pw".into(),
                ],
            )
            .await
            .unwrap();
        let error = opened(&allowed.monitor(&node, 5).await.unwrap())
            .await
            .expect_err("a wrong password");
        assert!(error.contains("Signing in was refused"), "{error}");
        assert!(!error.contains("secret-pw"), "the password is never shown");
        for user in ["vilo_test_monitor", "vilo_test_no_monitor"] {
            admin
                .call("ACL", vec!["DELUSER".into(), user.into()])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_stopping_while_opening_closes_the_connection() {
        use tokio::io::AsyncReadExt as _;
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        // A server that accepts the connection and never answers MONITOR.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let node = ScanNode {
            label: "silent".into(),
            server: Some(fred::prelude::Server::new("127.0.0.1", port)),
        };
        let monitor = session.monitor(&node, 5).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 64];
        let read = socket.read(&mut request).await.unwrap();
        assert!(request[..read].ends_with(b"MONITOR\r\n"));
        drop(monitor);
        let closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut request))
            .await
            .expect("the connection closes when the run is dropped");
        assert_eq!(closed.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_ipv6_addresses_connect() {
        let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await else {
            return;
        };
        let port = listener.local_addr().unwrap().port();
        let accepted = tokio::spawn(async move { listener.accept().await.is_ok() });
        // The handshake fails on this silent listener; only the connection matters here.
        tokio::time::timeout(
            Duration::from_millis(500),
            open("::1".into(), port, None, None, None),
        )
        .await
        .ok();
        assert!(accepted.await.unwrap());
    }

    #[tokio::test]
    async fn test_monitor_one_cluster_node() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let _cluster = crate::CLUSTER_TESTS.lock().await;
        let session = RedisSession::connect(&url).await.unwrap();
        let nodes = session.scan_nodes();
        assert!(nodes.len() > 1);
        let mut runs = Vec::new();
        for node in &nodes {
            runs.push(session.monitor(node, 30).await.unwrap());
        }
        for run in &runs {
            opened(run).await.unwrap();
        }
        let keys = (0..20)
            .map(|number| format!("vilo_test:monitor:{number}"))
            .collect::<Vec<_>>();
        for key in &keys {
            session
                .call("SET", vec![key.as_str().into(), "1".into()])
                .await
                .unwrap();
        }
        let mut seen = vec![Vec::new(); runs.len()];
        for _ in 0..100 {
            for (run, lines) in runs.iter().zip(seen.iter_mut()) {
                lines.extend(run.take().lines);
            }
            let found = keys
                .iter()
                .filter(|key| {
                    seen.iter().flatten().any(|line| {
                        line.args
                            .first()
                            .is_some_and(|name| name.eq_ignore_ascii_case(b"set"))
                            && line.args.get(1) == Some(&key.as_bytes().to_vec())
                    })
                })
                .count();
            if found == keys.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for key in &keys {
            let nodes_seeing = seen
                .iter()
                .filter(|lines| {
                    lines.iter().any(|line| {
                        line.args
                            .first()
                            .is_some_and(|name| name.eq_ignore_ascii_case(b"set"))
                            && line.args.get(1) == Some(&key.as_bytes().to_vec())
                    })
                })
                .count();
            assert_eq!(nodes_seeing, 1, "{key} runs on exactly one primary");
            session
                .call("DEL", vec![key.as_str().into()])
                .await
                .unwrap();
        }
    }
}
