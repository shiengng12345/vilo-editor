//! SSH tunnels for Redis, as RedisInsight's: the connection goes through a jump host. The
//! system's ssh does the work, so keys, the agent and ~/.ssh/config apply as the user has them;
//! it never prompts. One master connection carries a local forward for every Redis node Vilo
//! reaches, so the nodes a cluster reports go through the tunnel too.

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail};
use tokio::process::{Child, Command};

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SshSettings {
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    pub user: String,
    /// A private key; none uses the agent and ~/.ssh/config.
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// Where host keys are remembered; none is ~/.ssh/known_hosts. A host seen for the first
    /// time is remembered, a changed one is refused.
    #[serde(default)]
    pub known_hosts: Option<PathBuf>,
}

const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// A control command asks the local master only; it never needs long.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// What is kept of the master's error output, the end of it, for saying why it failed.
const LOG_BYTES: usize = 8 * 1024;

pub struct SshTunnel {
    /// The options every ssh call for this tunnel shares, ending with the destination.
    arguments: Vec<String>,
    control: PathBuf,
    master: std::sync::Mutex<Option<Child>>,
    /// The end of the master's error output. It is read all along, since a full pipe would
    /// stop ssh, and every forward with it.
    log: Arc<std::sync::Mutex<Vec<u8>>>,
    drain: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Redis host and port to the local port forwarding to it.
    forwards: tokio::sync::Mutex<HashMap<(String, u16), u16>>,
}

/// The options every ssh call of a tunnel shares, ending with `--` and the destination.
fn ssh_arguments(settings: &SshSettings, control: &std::path::Path) -> Result<Vec<String>> {
    let host = settings.host.trim();
    let user = settings.user.trim();
    anyhow::ensure!(!host.is_empty(), "Fill in the SSH host.");
    // ssh would read a leading `-` as an option, such as one that runs a command.
    for (what, value) in [("host", host), ("user", user)] {
        anyhow::ensure!(
            !value.starts_with('-') && !value.chars().any(char::is_whitespace),
            "The SSH {what} cannot start with - or contain spaces."
        );
    }
    let mut arguments = vec![
        "-o".to_string(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        format!("ControlPath={}", control.display()),
    ];
    // No port or user given leaves them to ~/.ssh/config (or ssh's defaults).
    if let Some(port) = settings.port {
        arguments.extend(["-p".into(), port.to_string()]);
    }
    if !user.is_empty() {
        arguments.extend(["-l".into(), user.to_string()]);
    }
    if let Some(key) = &settings.key_file {
        arguments.extend(["-i".into(), key.display().to_string()]);
        arguments.extend(["-o".into(), "IdentitiesOnly=yes".into()]);
    }
    if let Some(known_hosts) = &settings.known_hosts {
        arguments.extend([
            "-o".into(),
            format!("UserKnownHostsFile={}", known_hosts.display()),
        ]);
    }
    // Nothing after `--` is an option, whatever it looks like.
    arguments.extend(["--".into(), host.to_string()]);
    Ok(arguments)
}

impl SshTunnel {
    pub async fn open(settings: &SshSettings) -> Result<Arc<Self>> {
        // Short, as macOS limits socket paths to about 100 bytes.
        let control = PathBuf::from(format!(
            "/tmp/vilo-ssh-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        let arguments = ssh_arguments(settings, &control)?;
        let mut master = Command::new("ssh")
            // The master stays this process, whatever ~/.ssh/config says, so closing the
            // session ends it.
            .args([
                "-M",
                "-N",
                "-o",
                "ControlPersist=no",
                "-o",
                "ForkAfterAuthentication=no",
            ])
            .args(&arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("Could not start ssh")?;
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let stderr = master.stderr.take();
        let drain = tokio::spawn({
            let log = log.clone();
            async move {
                use tokio::io::AsyncReadExt as _;
                let Some(mut stderr) = stderr else {
                    return;
                };
                let mut chunk = [0_u8; 4096];
                // Ends when ssh closes its side, that is when it exits.
                while let Ok(read) = stderr.read(&mut chunk).await
                    && read > 0
                {
                    let mut log = log.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    log.extend_from_slice(chunk.get(..read).unwrap_or_default());
                    let excess = log.len().saturating_sub(LOG_BYTES);
                    log.drain(..excess);
                }
            }
        });
        let tunnel = Arc::new(Self {
            arguments,
            control,
            master: std::sync::Mutex::new(Some(master)),
            log,
            drain: tokio::sync::Mutex::new(Some(drain)),
            forwards: Default::default(),
        });
        tunnel.wait_ready().await?;
        Ok(tunnel)
    }

    fn master(&self) -> std::sync::MutexGuard<'_, Option<Child>> {
        // Only ever replaced whole; a panic elsewhere leaves nothing half-done.
        self.master
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn wait_ready(&self) -> Result<()> {
        let started = Instant::now();
        loop {
            let exited = self
                .master()
                .as_mut()
                .map(|master| master.try_wait())
                .transpose()?
                .flatten();
            if let Some(status) = exited {
                let reason = self.master_error().await;
                bail!("The SSH connection failed ({status}): {reason}");
            }
            // Bounded by what is left of the startup deadline, so a master that stalls while
            // answering cannot hold the connection past it.
            let left = READY_TIMEOUT
                .saturating_sub(started.elapsed())
                .max(Duration::from_millis(100));
            let check = tokio::time::timeout(
                left,
                Command::new("ssh")
                    .args(["-O", "check"])
                    .args(&self.arguments)
                    .stdin(Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await;
            let ready = match check {
                Ok(output) => output.context("Could not run ssh")?.status.success(),
                Err(_) => false,
            };
            if ready {
                return Ok(());
            }
            if started.elapsed() > READY_TIMEOUT {
                let said = self.master_error().await;
                bail!(
                    "The SSH connection did not come up within {} s{}",
                    READY_TIMEOUT.as_secs(),
                    if said.is_empty() {
                        ".".to_string()
                    } else {
                        format!(": {said}")
                    }
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// What ssh said. Once it exited, the drain is waited for (briefly), so its last words are in.
    async fn master_error(&self) -> String {
        let exited = self
            .master()
            .as_mut()
            .is_none_or(|master| master.try_wait().ok().flatten().is_some());
        if exited && let Some(drain) = self.drain.lock().await.take() {
            tokio::time::timeout(Duration::from_secs(1), drain)
                .await
                .ok();
        }
        let log = self
            .log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        String::from_utf8_lossy(&log).trim().to_string()
    }

    /// The local address forwarding to `host:port` as the jump host sees it, set up on first use.
    pub async fn forward(&self, host: &str, port: u16) -> Result<SocketAddr> {
        let mut forwards = self.forwards.lock().await;
        if let Some(local) = forwards.get(&(host.to_string(), port)) {
            return Ok(SocketAddr::from((Ipv4Addr::LOCALHOST, *local)));
        }
        let target = if host.contains(':') {
            format!("[{}]", host.trim_matches(['[', ']']))
        } else {
            host.to_string()
        };
        let mut last = String::new();
        // A free port can be taken between finding it and ssh binding it; try another then.
        for _ in 0..3 {
            let local = match std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .and_then(|listener| listener.local_addr())
            {
                Ok(address) => address.port(),
                Err(error) => {
                    last = format!("no free local port ({error})");
                    continue;
                }
            };
            let output = tokio::time::timeout(
                CONTROL_TIMEOUT,
                Command::new("ssh")
                    .args(["-O", "forward", "-L"])
                    .arg(format!("127.0.0.1:{local}:{target}:{port}"))
                    .args(&self.arguments)
                    .stdin(Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .context("The SSH master did not answer in time")?
            .context("Could not run ssh")?;
            if output.status.success() {
                forwards.insert((host.to_string(), port), local);
                return Ok(SocketAddr::from((Ipv4Addr::LOCALHOST, local)));
            }
            last = String::from_utf8_lossy(&output.stderr).trim().to_string();
        }
        bail!("The SSH tunnel could not forward to {host}:{port}: {last}")
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        if let Some(mut master) = self.master().take() {
            // Closing the master ends every forward. It fails only when ssh already exited.
            master.start_kill().ok();
        }
        // ssh removes its socket when it exits; this only covers a master killed before that.
        std::fs::remove_file(&self.control).ok();
    }
}

/// Resolves every Redis address fred connects to into the tunnel's forward for it. Weak, since
/// fred keeps its resolver alive in background tasks: the session alone owns the tunnel, and
/// closing the session closes ssh with it.
pub(crate) struct ThroughTunnel(pub std::sync::Weak<SshTunnel>);

#[async_trait::async_trait]
impl fred::types::Resolve for ThroughTunnel {
    async fn resolve(
        &self,
        host: fred::bytes_utils::Str,
        port: u16,
    ) -> std::result::Result<Vec<SocketAddr>, fred::error::Error> {
        let failed = |error: String| fred::error::Error::new(fred::error::ErrorKind::IO, error);
        let tunnel = self
            .0
            .upgrade()
            .ok_or_else(|| failed("The SSH tunnel was closed.".into()))?;
        tunnel
            .forward(&host, port)
            .await
            .map(|local| vec![local])
            .map_err(|error| failed(format!("{error:#}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::RedisSession;

    /// The jump host and key from the environment, with a known_hosts file of the test's own.
    fn settings() -> Option<SshSettings> {
        let address = std::env::var("VILO_TEST_SSH").ok()?;
        let (host, port) = address.rsplit_once(':')?;
        let known_hosts =
            std::env::temp_dir().join(format!("vilo-known-hosts-{}", std::process::id()));
        Some(SshSettings {
            host: host.to_string(),
            port: port.parse().ok(),
            user: std::env::var("VILO_TEST_SSH_USER").ok()?,
            key_file: Some(std::env::var("VILO_TEST_SSH_KEY").ok()?.into()),
            known_hosts: Some(known_hosts),
        })
    }

    #[tokio::test]
    async fn test_a_private_server_through_a_jump_host() {
        let (Some(ssh), Ok(url)) = (settings(), std::env::var("VILO_TEST_SSH_REDIS")) else {
            return;
        };
        // The address is the jump host's view of the server, here a name only it can resolve.
        let session = RedisSession::connect_with(&url, None, None, Some(&ssh))
            .await
            .unwrap();
        session
            .call("SET", vec!["vilo_test:ssh".into(), "1".into()])
            .await
            .unwrap();
        assert_eq!(
            session
                .call("GET", vec!["vilo_test:ssh".into()])
                .await
                .unwrap()
                .as_string()
                .as_deref(),
            Some("1")
        );

        let node = session.scan_nodes().remove(0);
        let monitor = session.monitor(&node, 5).await.unwrap();
        let mut started = false;
        for _ in 0..100 {
            let batch = monitor.take();
            assert!(batch.ended.is_none(), "{:?}", batch.ended);
            if batch.started {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(started, "the profiler goes through the tunnel too");
        drop(monitor);

        let subscription = session
            .subscribe(
                crate::redis_pubsub::Subscribe::Channels,
                vec!["vilo_test:ssh".into()],
            )
            .await
            .unwrap();
        let (mut events, _stop) = subscription.into_parts();
        let mut received = false;
        for _ in 0..50 {
            session
                .publish("vilo_test:ssh", b"hi", false)
                .await
                .unwrap();
            if let Ok(Some(crate::redis_pubsub::PubSubEvent::Message(message))) =
                tokio::time::timeout(Duration::from_millis(100), events.next()).await
            {
                received = message.payload == b"hi";
                break;
            }
        }
        assert!(received, "Pub/Sub's own connection goes through the tunnel");

        session
            .call("DEL", vec!["vilo_test:ssh".into()])
            .await
            .unwrap();
        let (host, port) = url.trim_start_matches("redis://").rsplit_once(':').unwrap();
        let tunnel = session.tunnel.clone().unwrap();
        let local = tunnel.forward(host, port.parse().unwrap()).await.unwrap();
        drop(tunnel);
        drop(session);
        let mut closed = false;
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(local).await.is_err() {
                closed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(closed, "closing the session closes its tunnel");
    }

    #[tokio::test]
    async fn test_a_private_cluster_through_a_jump_host() {
        let (Some(ssh), Ok(url)) = (settings(), std::env::var("VILO_TEST_SSH_CLUSTER")) else {
            return;
        };
        // One node's internal address; the others the cluster reports go through the tunnel.
        let session = RedisSession::connect_with(&url, None, None, Some(&ssh))
            .await
            .unwrap();
        assert!(session.is_clustered());
        let nodes = session.scan_nodes();
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        for number in 0..30 {
            session
                .call(
                    "SET",
                    vec![format!("vilo_test:ssh:{number}").into(), "1".into()],
                )
                .await
                .unwrap();
        }
        let mut total = 0;
        for node in &nodes {
            total += session.dbsize(node).await.unwrap();
        }
        assert!(total >= 30, "{total}");
        for number in 0..30 {
            session
                .call("DEL", vec![format!("vilo_test:ssh:{number}").into()])
                .await
                .unwrap();
        }
    }

    #[test]
    fn test_what_is_not_given_is_left_to_ssh_config() {
        let control = std::path::Path::new("/tmp/vilo-ssh-test");
        let bare = ssh_arguments(
            &SshSettings {
                host: "jump".into(),
                ..SshSettings::default()
            },
            control,
        )
        .unwrap();
        assert!(
            !bare
                .iter()
                .any(|argument| argument == "-p" || argument == "-l"),
            "{bare:?}"
        );
        assert_eq!(
            bare[bare.len() - 2..],
            ["--".to_string(), "jump".to_string()]
        );
        let full = ssh_arguments(
            &SshSettings {
                host: "jump".into(),
                port: Some(2222),
                user: "vilo".into(),
                ..SshSettings::default()
            },
            control,
        )
        .unwrap();
        assert!(full.windows(2).any(|pair| pair == ["-p", "2222"]));
        assert!(full.windows(2).any(|pair| pair == ["-l", "vilo"]));
    }

    #[tokio::test]
    async fn test_hosts_and_users_cannot_be_options() {
        for (host, user) in [
            ("-oProxyCommand=touch /tmp/vilo-pwned", "vilo"),
            ("bastion", "-oProxyCommand=x"),
            ("bastion extra", "vilo"),
        ] {
            let settings = SshSettings {
                host: host.into(),
                user: user.into(),
                ..SshSettings::default()
            };
            let error = SshTunnel::open(&settings)
                .await
                .err()
                .expect("refused before ssh runs");
            assert!(
                format!("{error:#}").contains("cannot start with -"),
                "{error:#}"
            );
        }
        assert!(!std::path::Path::new("/tmp/vilo-pwned").exists());
    }

    #[tokio::test]
    async fn test_a_key_the_jump_host_refuses() {
        let (Some(ssh), Ok(url)) = (settings(), std::env::var("VILO_TEST_SSH_REDIS")) else {
            return;
        };
        let other = std::env::temp_dir().join(format!("vilo-other-key-{}", std::process::id()));
        std::fs::remove_file(&other).ok();
        let generated = Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&other)
            .status()
            .await
            .unwrap();
        assert!(generated.success());
        let refused = SshSettings {
            key_file: Some(other.clone()),
            ..ssh
        };
        let error = RedisSession::connect_with(&url, None, None, Some(&refused))
            .await
            .err()
            .expect("an unknown key");
        assert!(format!("{error:#}").contains("SSH"), "{error:#}");
        // ssh's own words, read from its drained output.
        assert!(
            format!("{error:#}").contains("Permission denied"),
            "{error:#}"
        );
        std::fs::remove_file(&other).ok();
        std::fs::remove_file(other.with_extension("pub")).ok();
    }
}
