//! SSH tunnels for Redis, as RedisInsight's: the connection goes through a jump host. The
//! system's ssh does the work, so keys, the agent and ~/.ssh/config apply as the user has them;
//! it never prompts the user, and a password, when given, is answered by Vilo itself. One master
//! connection carries a local forward for every Redis node Vilo reaches, so the nodes a cluster
//! reports go through the tunnel too.

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
    /// Kept in the keychain, never in the saved settings.
    #[serde(skip)]
    pub password: Option<SshPassword>,
    /// The private key's passphrase, kept like the password.
    #[serde(skip)]
    pub passphrase: Option<SshPassword>,
}

/// An SSH password; its Debug output does not show it.
#[derive(Clone, Default, PartialEq)]
pub struct SshPassword(pub String);

impl std::fmt::Debug for SshPassword {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SshPassword(..)")
    }
}

/// ssh's askpass helper, in a directory only this user can open, and the pipes it reads the
/// secrets from: the password, and the passphrase of the key. Each goes through its pipe from a
/// thread of Vilo's and is never written to disk, nor put on ssh's command line or in its
/// environment; if Vilo exits, nothing of them is left behind.
struct Askpass {
    dir: PathBuf,
    /// The pipes made, each with the thread writing its secret.
    writers: Vec<(PathBuf, std::thread::JoinHandle<()>)>,
}

/// The pipe for a key's passphrase; the password has its own.
const PASSPHRASE_PIPE: &str = "passphrase";
const PASSWORD_PIPE: &str = "password";

impl Askpass {
    #[cfg(not(unix))]
    fn create(
        _password: Option<&str>,
        _passphrase: Option<&str>,
        _key_prompt: Option<&str>,
    ) -> Result<Self> {
        bail!("SSH passwords are only supported on macOS and Linux; use a key or the agent.")
    }

    /// `key_prompt` is ssh's prompt for the passphrase, word for word; only it gets the
    /// passphrase.
    #[cfg(unix)]
    fn create(
        password: Option<&str>,
        passphrase: Option<&str>,
        key_prompt: Option<&str>,
    ) -> Result<Self> {
        use std::io::Write as _;
        use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
        for secret in [password, passphrase].into_iter().flatten() {
            // Fits any pipe's buffer, so a writer never waits on a reader once one has opened.
            anyhow::ensure!(
                secret.len() <= MAX_PASSWORD_BYTES,
                "The SSH password or passphrase is longer than {MAX_PASSWORD_BYTES} bytes."
            );
        }
        // Short and fixed in shape, so the helper's path needs no quoting in ssh's environment.
        let dir = std::env::temp_dir().join(format!(
            "vilo-askpass-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .context("Could not prepare the SSH password")?;
        let mut askpass = Self {
            dir,
            writers: Vec::new(),
        };
        remove_stale_askpass(&askpass.dir);
        let mut helper = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(askpass.program())
            .context("Could not prepare the SSH password")?;
        // ssh passes its prompt. Only its prompt for unlocking this key, word for word, gets the
        // passphrase: it names the key's local path, which a server's keyboard-interactive
        // prompt cannot know to copy. Any other key prompt gets nothing, so the password is never
        // spent on a key; everything else gets the password. A secret not given has no pipe, so
        // that ask fails at once.
        helper
            .write_all(
                b"#!/bin/sh\nd=$(dirname \"$0\")\ncase \"$1\" in\n\"Enter passphrase for key '\"*)\n  if [ -f \"$d/key_prompt\" ] && [ \"$1\" = \"$(cat \"$d/key_prompt\")\" ]; then exec cat \"$d/passphrase\"; fi\n  exit 1 ;;\nesac\nexec cat \"$d/password\"\n",
            )
            .context("Could not prepare the SSH password")?;
        drop(helper);
        if let (Some(prompt), Some(_)) = (key_prompt, passphrase) {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(askpass.dir.join("key_prompt"))
                .context("Could not prepare the SSH password")?;
            file.write_all(prompt.as_bytes())
                .context("Could not prepare the SSH password")?;
        }
        for (name, secret) in [(PASSWORD_PIPE, password), (PASSPHRASE_PIPE, passphrase)] {
            let Some(secret) = secret else {
                continue;
            };
            let pipe = askpass.dir.join(name);
            nix::unistd::mkfifo(
                &pipe,
                nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
            )
            .context("Could not prepare the SSH password")?;
            let secret = secret.to_string();
            let writer = std::thread::spawn({
                let pipe = pipe.clone();
                move || {
                    // Waits until ssh runs the helper, or until drop opens the reading end.
                    let written = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&pipe)
                        .and_then(|mut end| end.write_all(secret.as_bytes()));
                    if let Err(error) = written
                        && error.kind() != std::io::ErrorKind::BrokenPipe
                    {
                        log::error!("could not hand an SSH secret to ssh: {error}");
                    }
                }
            });
            askpass.writers.push((pipe, writer));
        }
        Ok(askpass)
    }

    fn program(&self) -> PathBuf {
        self.dir.join("askpass")
    }
}

impl Drop for Askpass {
    fn drop(&mut self) {
        #[cfg(unix)]
        for (pipe, writer) in self.writers.drain(..) {
            use std::os::unix::fs::OpenOptionsExt as _;
            let end = |write: bool| {
                std::fs::OpenOptions::new()
                    .read(!write)
                    .write(write)
                    .custom_flags(nix::libc::O_NONBLOCK)
                    .open(&pipe)
            };
            // Opening an end without waiting releases whoever still waits at the other: the
            // writer when ssh never asked, a helper still reading when ssh asked twice. Either
            // open fails when nobody waits, which is fine.
            let reader = end(false).ok();
            // Without the reading end the writer could wait forever; it is left to the process.
            if reader.is_some() && writer.join().is_err() {
                log::error!("an SSH secret writer panicked");
            }
            drop(end(true).ok());
            drop(reader);
        }
        if let Err(error) = std::fs::remove_dir_all(&self.dir) {
            log::error!(
                "could not remove the SSH askpass helper at {:?}: {error}",
                self.dir
            );
        }
    }
}

/// Removes askpass directories a Vilo that exited while signing in left behind: this user's,
/// older than ten minutes, far past the time a sign-in may take.
fn remove_stale_askpass(ours: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let (Some(parent), Ok(own)) = (ours.parent(), std::fs::metadata(ours)) else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(parent) else {
            return;
        };
        for path in entries.filter_map(|entry| entry.ok().map(|entry| entry.path())) {
            let stale = path != ours
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("vilo-askpass-"))
                && std::fs::symlink_metadata(&path).is_ok_and(|metadata| {
                    metadata.is_dir()
                        && metadata.uid() == own.uid()
                        && metadata
                            .modified()
                            .ok()
                            .and_then(|modified| modified.elapsed().ok())
                            .is_some_and(|age| age > Duration::from_secs(600))
                });
            if stale && let Err(error) = std::fs::remove_dir_all(&path) {
                log::error!("could not remove a stale SSH password at {path:?}: {error}");
            }
        }
    }
}

/// POSIX guarantees pipes at least 4 KiB of buffer.
const MAX_PASSWORD_BYTES: usize = 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// One readiness check of the master; it answers at once once it is up.
const CHECK_TIMEOUT: Duration = Duration::from_secs(2);
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
/// The key file as given, with a leading `~/` made the home directory, so the path ssh is given
/// is the one it names when it asks for the key's passphrase.
fn key_path(key: &std::path::Path) -> String {
    let text = key.display().to_string();
    match (text.strip_prefix("~/"), std::env::var_os("HOME")) {
        (Some(rest), Some(home)) => std::path::Path::new(&home).join(rest).display().to_string(),
        _ => text,
    }
}

/// ssh's prompt for the key's passphrase, word for word (it cuts the path at 100 bytes).
fn key_prompt(key: &std::path::Path) -> Result<String> {
    let path = key_path(key);
    // ssh expands % tokens in key paths, so its prompt would name another path.
    anyhow::ensure!(
        !path.contains('%'),
        "A key with a passphrase needs a path without %."
    );
    let mut end = path.len().min(100);
    while !path.is_char_boundary(end) {
        end -= 1;
    }
    Ok(format!(
        "Enter passphrase for key '{}': ",
        path.get(..end).unwrap_or_default()
    ))
}

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
        arguments.extend(["-i".into(), key_path(key)]);
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
        fn given(secret: &Option<SshPassword>) -> Option<&str> {
            secret
                .as_ref()
                .map(|secret| secret.0.as_str())
                .filter(|secret| !secret.is_empty())
        }
        let (password, passphrase) = (given(&settings.password), given(&settings.passphrase));
        let key_prompt = match (passphrase, &settings.key_file) {
            (Some(_), Some(key)) => Some(key_prompt(key)?),
            (Some(_), None) => bail!("A key passphrase needs the private key file."),
            (None, _) => None,
        };
        let askpass = if password.is_some() || passphrase.is_some() {
            Some(Askpass::create(
                password,
                passphrase,
                key_prompt.as_deref(),
            )?)
        } else {
            None
        };
        let mut master = Command::new("ssh");
        if let Some(askpass) = &askpass {
            // ssh takes the first value it is given for an option, so these win over the
            // shared BatchMode=yes for the master alone; control commands still never ask.
            master
                .args(["-o", "BatchMode=no", "-o", "NumberOfPasswordPrompts=1"])
                // Says how far signing in got, for the error shown when it does not finish.
                .args(["-o", "LogLevel=VERBOSE"])
                .env("SSH_ASKPASS", askpass.program())
                .env("SSH_ASKPASS_REQUIRE", "force");
        }
        let mut master = master
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
        let ready = tunnel.wait_ready().await;
        // Signed in or given up: either way ssh asks for the password no more.
        drop(askpass);
        ready?;
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
            // Short, so one check that stalls is retried (and the master's exit seen) instead
            // of using up the whole startup deadline; never past that deadline either.
            let left = READY_TIMEOUT
                .saturating_sub(started.elapsed())
                .min(CHECK_TIMEOUT)
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
            password: None,
            passphrase: None,
        })
    }

    /// Tests that create or count askpass directories in the shared temp dir take turns.
    static ASKPASS_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn askpass_dirs() -> std::collections::BTreeSet<PathBuf> {
        std::fs::read_dir(std::env::temp_dir())
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .filter(|path| {
                        path.file_name()
                            .is_some_and(|name| name.to_string_lossy().starts_with("vilo-askpass-"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_the_askpass_helper_hands_each_secret_over_its_pipe() {
        use std::os::unix::fs::FileTypeExt as _;
        let _serial = ASKPASS_TESTS.lock().await;
        let ask = |askpass: &Askpass, prompt: &str| {
            let mut command = Command::new(askpass.program());
            command
                .arg(prompt)
                .stdout(Stdio::piped())
                .kill_on_drop(true);
            command
        };
        let key_prompt = "Enter passphrase for key '/k/id': ";
        let askpass =
            Askpass::create(Some("s3cret pass"), Some("key phrase"), Some(key_prompt)).unwrap();
        for name in [PASSWORD_PIPE, PASSPHRASE_PIPE] {
            let pipe = std::fs::metadata(askpass.dir.join(name)).unwrap();
            assert!(
                pipe.file_type().is_fifo() && pipe.len() == 0,
                "nothing of {name} is on disk"
            );
        }
        let password = ask(&askpass, "vboth@passphrase-bastion's password: ")
            .output()
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&password.stdout),
            "s3cret pass",
            "a host name with \"passphrase\" in it still gets the password"
        );
        // A server could word prompts like these. None gets the key's passphrase; one worded as
        // another key's prompt gets nothing at all. Each is asked of a fresh helper, as each pipe
        // hands its secret over once.
        for (phishing, answer) in [
            ("Type your key passphrase: ", "server pw"),
            ("Enter passphrase for key '/home/me/.ssh/id_ed25519': ", ""),
            ("Enter passphrase for key '/k/id': x", ""),
        ] {
            let worded =
                Askpass::create(Some("server pw"), Some("key phrase"), Some(key_prompt)).unwrap();
            let phished =
                tokio::time::timeout(Duration::from_secs(5), ask(&worded, phishing).output())
                    .await
                    .expect("answered at once")
                    .unwrap();
            assert_eq!(
                String::from_utf8_lossy(&phished.stdout),
                answer,
                "{phishing}"
            );
        }
        let passphrase = ask(&askpass, key_prompt).output().await.unwrap();
        assert_eq!(String::from_utf8_lossy(&passphrase.stdout), "key phrase");
        // Asked a second time, the helper waits; dropping releases it with nothing.
        let second = ask(&askpass, "vboth@bastion's password: ").spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let dir = askpass.dir.clone();
        drop(askpass);
        let second = tokio::time::timeout(Duration::from_secs(5), second.wait_with_output())
            .await
            .expect("the waiting helper was released")
            .unwrap();
        assert!(second.stdout.is_empty());
        assert!(!dir.exists());

        // A secret not given is not waited for.
        let askpass = Askpass::create(Some("only a password"), None, None).unwrap();
        let passphrase = tokio::time::timeout(
            Duration::from_secs(5),
            ask(&askpass, "Enter passphrase for key '/k/id': ").output(),
        )
        .await
        .expect("an ask for a secret not given fails at once")
        .unwrap();
        assert!(!passphrase.status.success() && passphrase.stdout.is_empty());
        drop(askpass);
    }

    #[tokio::test]
    async fn test_an_askpass_never_asked_is_removed_without_waiting() {
        let _serial = ASKPASS_TESTS.lock().await;
        assert!(Askpass::create(Some(&"x".repeat(MAX_PASSWORD_BYTES + 1)), None, None).is_err());
        assert!(Askpass::create(None, Some(&"x".repeat(MAX_PASSWORD_BYTES + 1)), None).is_err());
        // The longest password allowed, so its write cannot outgrow the pipe nobody reads.
        let longest = "x".repeat(MAX_PASSWORD_BYTES);
        let askpass = Askpass::create(Some(&longest), Some(&longest), None).unwrap();
        let dir = askpass.dir.clone();
        let dropped = tokio::task::spawn_blocking(move || drop(askpass));
        tokio::time::timeout(Duration::from_secs(5), dropped)
            .await
            .expect("dropping does not wait for a reader")
            .unwrap();
        assert!(!dir.exists());
    }

    #[test]
    fn test_stale_askpass_directories_are_removed() {
        let _serial = ASKPASS_TESTS.blocking_lock();
        let stale = std::env::temp_dir().join(format!("vilo-askpass-stale{}", std::process::id()));
        let fresh = std::env::temp_dir().join(format!("vilo-askpass-fresh{}", std::process::id()));
        for dir in [&stale, &fresh] {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join("password"), "left behind").unwrap();
        }
        let old = std::time::SystemTime::now() - Duration::from_secs(3600);
        std::fs::File::open(&stale)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let askpass = Askpass::create(Some("new"), None, None).unwrap();
        assert!(!stale.exists(), "one left by an exited Vilo goes");
        assert!(fresh.exists(), "one a sign-in may still use stays");
        drop(askpass);
        std::fs::remove_dir_all(&fresh).unwrap();
    }

    #[test]
    fn test_the_key_prompt_is_ssh_s_own_words() {
        assert_eq!(
            key_prompt(std::path::Path::new("/k/id")).unwrap(),
            "Enter passphrase for key '/k/id': "
        );
        let home = std::env::var("HOME").unwrap();
        assert_eq!(
            key_prompt(std::path::Path::new("~/.ssh/id")).unwrap(),
            format!("Enter passphrase for key '{home}/.ssh/id': ")
        );
        let long = format!("/{}", "é".repeat(80));
        let prompt = key_prompt(std::path::Path::new(&long)).unwrap();
        assert!(prompt.len() <= "Enter passphrase for key '': ".len() + 100);
        assert!(key_prompt(std::path::Path::new("/k/%d")).is_err());
    }

    #[test]
    fn test_an_ssh_password_is_never_saved_or_shown() {
        let settings = SshSettings {
            host: "bastion".into(),
            password: Some(SshPassword("s3cret".into())),
            ..SshSettings::default()
        };
        let saved = serde_json::to_string(&settings).unwrap();
        assert!(
            !saved.contains("s3cret") && !saved.contains("password"),
            "{saved}"
        );
        assert!(!format!("{settings:?}").contains("s3cret"));
        let read: SshSettings = serde_json::from_str(&saved).unwrap();
        assert_eq!(read.password, None);
        let arguments = ssh_arguments(&settings, std::path::Path::new("/tmp/c")).unwrap();
        assert!(!arguments.iter().any(|argument| argument.contains("s3cret")));
    }

    #[tokio::test]
    async fn test_a_jump_host_that_takes_a_password() {
        let (Ok(address), Ok(user), Ok(password_file), Ok(url)) = (
            std::env::var("VILO_TEST_SSH_PASSWORD_HOST"),
            std::env::var("VILO_TEST_SSH_PASSWORD_USER"),
            std::env::var("VILO_TEST_SSH_PASSWORD_FILE"),
            std::env::var("VILO_TEST_SSH_REDIS"),
        ) else {
            return;
        };
        let password = std::fs::read_to_string(password_file)
            .unwrap()
            .trim()
            .to_string();
        let _serial = ASKPASS_TESTS.lock().await;
        // Compared with what was there before, so a leftover of another run cannot fail this.
        let before = askpass_dirs();
        let (host, port) = address.rsplit_once(':').unwrap();
        let known_hosts =
            std::env::temp_dir().join(format!("vilo-known-hosts-pw-{}", std::process::id()));
        let settings = |password: &str| SshSettings {
            host: host.to_string(),
            port: port.parse().ok(),
            user: user.clone(),
            key_file: None,
            known_hosts: Some(known_hosts.clone()),
            password: Some(SshPassword(password.to_string())),
            passphrase: None,
        };
        let session = RedisSession::connect_with(&url, None, None, Some(&settings(&password)))
            .await
            .unwrap();
        let pong = session.call("PING", Vec::new()).await.unwrap();
        assert_eq!(pong.as_string().as_deref(), Some("PONG"));
        assert!(
            askpass_dirs().is_subset(&before),
            "nothing of the password stays once signed in"
        );
        let tunnel = session.tunnel.clone().unwrap();
        assert!(
            !tunnel
                .arguments
                .iter()
                .any(|argument| argument.contains(&password))
        );
        let (redis_host, redis_port) = url.trim_start_matches("redis://").rsplit_once(':').unwrap();
        // Forwards after sign-in ask the master only, never for the password again.
        tunnel
            .forward(redis_host, redis_port.parse().unwrap())
            .await
            .unwrap();
        drop(tunnel);
        drop(session);

        let refused = RedisSession::connect_with(&url, None, None, Some(&settings("wrong")))
            .await
            .err()
            .expect("a wrong password is refused");
        assert!(
            format!("{refused:#}").contains("Permission denied"),
            "{refused:#}"
        );
        assert!(
            askpass_dirs().is_subset(&before),
            "removed after a refusal too"
        );

        let no_password = SshSettings {
            password: None,
            ..settings("")
        };
        assert!(
            RedisSession::connect_with(&url, None, None, Some(&no_password))
                .await
                .is_err(),
            "without the password ssh does not prompt and the connection fails"
        );
        std::fs::remove_file(&known_hosts).ok();
    }

    #[tokio::test]
    async fn test_a_key_with_a_passphrase_and_a_host_that_also_wants_a_password() {
        let (
            Ok(key_address),
            Ok(key_user),
            Ok(locked_key),
            Ok(passphrase_file),
            Ok(both_address),
            Ok(password_file),
            Ok(url),
        ) = (
            std::env::var("VILO_TEST_SSH"),
            std::env::var("VILO_TEST_SSH_USER"),
            std::env::var("VILO_TEST_SSH_LOCKED_KEY"),
            std::env::var("VILO_TEST_SSH_PASSPHRASE_FILE"),
            std::env::var("VILO_TEST_SSH_PASSWORD_HOST"),
            std::env::var("VILO_TEST_SSH_PASSWORD_FILE"),
            std::env::var("VILO_TEST_SSH_REDIS"),
        )
        else {
            return;
        };
        let _serial = ASKPASS_TESTS.lock().await;
        let read = |path: &str| std::fs::read_to_string(path).unwrap().trim().to_string();
        let (passphrase, password) = (read(&passphrase_file), read(&password_file));
        let known_hosts =
            std::env::temp_dir().join(format!("vilo-known-hosts-phrase-{}", std::process::id()));
        let settings =
            |address: &str, user: &str, password: Option<&str>, passphrase: Option<&str>| {
                let (host, port) = address.rsplit_once(':').unwrap();
                SshSettings {
                    host: host.to_string(),
                    port: port.parse().ok(),
                    user: user.to_string(),
                    key_file: Some(locked_key.clone().into()),
                    known_hosts: Some(known_hosts.clone()),
                    password: password.map(|password| SshPassword(password.to_string())),
                    passphrase: passphrase.map(|passphrase| SshPassword(passphrase.to_string())),
                }
            };
        let reaches = |settings: SshSettings| {
            let url = url.clone();
            async move {
                let started = Instant::now();
                let session = RedisSession::connect_with(&url, None, None, Some(&settings)).await;
                let reached = match session {
                    Ok(session) => session
                        .call("PING", Vec::new())
                        .await
                        .is_ok_and(|pong| pong.as_string().as_deref() == Some("PONG")),
                    Err(_) => false,
                };
                (reached, started.elapsed())
            }
        };

        // A key with a passphrase on a host that takes keys.
        let (reached, _) =
            reaches(settings(&key_address, &key_user, None, Some(&passphrase))).await;
        assert!(reached, "the passphrase unlocks the key");
        let (reached, took) = reaches(settings(&key_address, &key_user, None, None)).await;
        assert!(
            !reached && took < Duration::from_secs(15),
            "no passphrase fails at once ({took:?})"
        );

        // A host that wants the key and then the password: both are answered.
        let (reached, _) = reaches(settings(
            &both_address,
            "vboth",
            Some(&password),
            Some(&passphrase),
        ))
        .await;
        assert!(reached, "the passphrase and then the password");
        let (reached, took) =
            reaches(settings(&both_address, "vboth", Some(&password), None)).await;
        assert!(
            !reached && took < Duration::from_secs(15),
            "without the passphrase it fails at once ({took:?})"
        );
        let (reached, took) =
            reaches(settings(&both_address, "vboth", None, Some(&passphrase))).await;
        assert!(
            !reached && took < Duration::from_secs(15),
            "without the password it fails at once ({took:?})"
        );
        // A host whose password is asked by keyboard-interactive (PAM): its own prompt gets the
        // password, ssh's prompt for the key the passphrase.
        if let Ok(kbd_address) = std::env::var("VILO_TEST_SSH_KBD_HOST") {
            let (reached, _) = reaches(settings(
                &kbd_address,
                "vkbd",
                Some(&password),
                Some(&passphrase),
            ))
            .await;
            assert!(
                reached,
                "the passphrase, then the password by keyboard-interactive"
            );
            let (reached, took) =
                reaches(settings(&kbd_address, "vkbd", None, Some(&passphrase))).await;
            assert!(
                !reached && took < Duration::from_secs(15),
                "without the password it fails at once ({took:?})"
            );
        }
        std::fs::remove_file(&known_hosts).ok();
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
