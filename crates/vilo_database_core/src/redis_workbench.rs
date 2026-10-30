//! Workbench writes and command files. Only commands on a reviewed list run, each with its keys
//! known, so it can be logged and its keys locked when the outcome is unknown. Scripts,
//! functions, administration and connection-state commands are never on it.

use anyhow::{Context as _, Result, bail};
use fred::{interfaces::ClientLike as _, prelude::Value};

use crate::{
    mongo::WriteError,
    redis::{
        Bytes, CommandKind, RedisSession, bytes_arg, classify_redis, classify_words, cluster_hash,
        split_command,
    },
};

/// A command file larger than this is not read.
pub const MAX_FILE_BYTES: u64 = 1024 * 1024;
pub const MAX_FILE_COMMANDS: usize = 10_000;

/// Where a write command's keys are among its arguments.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Keys {
    /// The first argument.
    First,
    /// The first two arguments (source and destination).
    FirstTwo,
    /// Every argument.
    All,
    /// Every other argument from the first (key value key value).
    Pairs,
}

/// The reviewed write commands. Left out on purpose: COPY (can write another database), SORT
/// (STORE), MOVE, blocking commands, XACK/XCLAIM/XGROUP (consumer state, from the group panel),
/// and anything that runs code or changes the server or the connection.
const WRITE_COMMANDS: &[(&str, Keys)] = &[
    ("SET", Keys::First),
    ("SETNX", Keys::First),
    ("SETEX", Keys::First),
    ("PSETEX", Keys::First),
    ("GETSET", Keys::First),
    ("GETDEL", Keys::First),
    ("GETEX", Keys::First),
    ("APPEND", Keys::First),
    ("INCR", Keys::First),
    ("DECR", Keys::First),
    ("INCRBY", Keys::First),
    ("DECRBY", Keys::First),
    ("INCRBYFLOAT", Keys::First),
    ("SETRANGE", Keys::First),
    ("SETBIT", Keys::First),
    ("MSET", Keys::Pairs),
    ("MSETNX", Keys::Pairs),
    ("DEL", Keys::All),
    ("UNLINK", Keys::All),
    ("EXPIRE", Keys::First),
    ("PEXPIRE", Keys::First),
    ("EXPIREAT", Keys::First),
    ("PEXPIREAT", Keys::First),
    ("PERSIST", Keys::First),
    ("RENAME", Keys::FirstTwo),
    ("RENAMENX", Keys::FirstTwo),
    ("HSET", Keys::First),
    ("HSETNX", Keys::First),
    ("HMSET", Keys::First),
    ("HDEL", Keys::First),
    ("HINCRBY", Keys::First),
    ("HINCRBYFLOAT", Keys::First),
    ("LPUSH", Keys::First),
    ("RPUSH", Keys::First),
    ("LPUSHX", Keys::First),
    ("RPUSHX", Keys::First),
    ("LPOP", Keys::First),
    ("RPOP", Keys::First),
    ("LSET", Keys::First),
    ("LREM", Keys::First),
    ("LTRIM", Keys::First),
    ("LINSERT", Keys::First),
    ("LMOVE", Keys::FirstTwo),
    ("RPOPLPUSH", Keys::FirstTwo),
    ("SADD", Keys::First),
    ("SREM", Keys::First),
    ("SPOP", Keys::First),
    ("SMOVE", Keys::FirstTwo),
    ("ZADD", Keys::First),
    ("ZREM", Keys::First),
    ("ZINCRBY", Keys::First),
    ("ZPOPMIN", Keys::First),
    ("ZPOPMAX", Keys::First),
    ("ZREMRANGEBYSCORE", Keys::First),
    ("ZREMRANGEBYRANK", Keys::First),
    ("ZREMRANGEBYLEX", Keys::First),
    ("XADD", Keys::First),
    ("XDEL", Keys::First),
    ("XTRIM", Keys::First),
    ("PFADD", Keys::First),
    ("GEOADD", Keys::First),
    ("JSON.SET", Keys::First),
    ("JSON.DEL", Keys::First),
    ("JSON.FORGET", Keys::First),
    ("JSON.MERGE", Keys::First),
    ("JSON.CLEAR", Keys::First),
    ("JSON.TOGGLE", Keys::First),
    ("JSON.NUMINCRBY", Keys::First),
    ("JSON.STRAPPEND", Keys::First),
    ("JSON.ARRAPPEND", Keys::First),
    ("JSON.ARRINSERT", Keys::First),
    ("JSON.ARRPOP", Keys::First),
    ("JSON.ARRTRIM", Keys::First),
];

/// Commands that run code; refused with that reason rather than as merely unlisted.
const SCRIPT_COMMANDS: &[&str] = &[
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL",
    "FCALL_RO",
    "SCRIPT",
    "FUNCTION",
];

/// One command line ready to run: a read, or a reviewed write with its keys.
#[derive(Clone, Debug, PartialEq)]
pub enum Planned {
    Read(Vec<Bytes>),
    Write(WorkbenchWrite),
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkbenchWrite {
    /// Upper case.
    pub name: String,
    pub words: Vec<Bytes>,
    pub keys: Vec<Bytes>,
}

/// Decides how a command line may run, or says why it may not.
pub fn plan(words: Vec<Bytes>) -> Result<Planned> {
    let name = words
        .first()
        .map(|name| String::from_utf8_lossy(name).to_ascii_uppercase())
        .context("Type a command")?;
    match classify_words(&words) {
        CommandKind::Read => return Ok(Planned::Read(words)),
        CommandKind::Dangerous if SCRIPT_COMMANDS.contains(&name.as_str()) => {
            bail!("{name} runs code on the server; Vilo never runs scripts or functions.")
        }
        CommandKind::Dangerous => bail!(
            "{name} is blocked: it can stop the server, wipe data, change configuration, change the connection or stream every command."
        ),
        CommandKind::Write => {}
    }
    let Some((_, keys)) = WRITE_COMMANDS.iter().find(|(known, _)| *known == name) else {
        bail!("{name} is not on Vilo's list of reviewed write commands, so it is not run.");
    };
    let args = words.get(1..).unwrap_or_default();
    let keys = match keys {
        Keys::First => args.first().into_iter().cloned().collect::<Vec<_>>(),
        Keys::FirstTwo => {
            anyhow::ensure!(
                args.len() >= 2,
                "{name} needs a source and a destination key."
            );
            args.iter().take(2).cloned().collect()
        }
        Keys::All => args.to_vec(),
        Keys::Pairs => {
            anyhow::ensure!(
                !args.is_empty() && args.len() % 2 == 0,
                "{name} needs keys and values in pairs."
            );
            args.iter().step_by(2).cloned().collect()
        }
    };
    anyhow::ensure!(!keys.is_empty(), "{name} needs a key.");
    Ok(Planned::Write(WorkbenchWrite { name, words, keys }))
}

/// Parses a command file: one command per line as the Workbench takes it; empty lines and lines
/// starting with `#` are skipped. Every line is planned first, so a file with one line Vilo would
/// refuse runs nothing. Returns each command with its line number.
pub fn parse_command_file(text: &str) -> Result<Vec<(usize, String, Planned)>> {
    let mut commands = Vec::new();
    let mut problems = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if commands.len() == MAX_FILE_COMMANDS {
            bail!("The file has more than {MAX_FILE_COMMANDS} commands.");
        }
        match split_command(line).and_then(plan) {
            Ok(planned) => commands.push((number, line.to_string(), planned)),
            Err(error) => problems.push(format!("line {number}: {error:#}")),
        }
    }
    if !problems.is_empty() {
        let shown = problems
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let more = problems.len().saturating_sub(5);
        bail!(
            "Nothing was run. {} line{} cannot be run:\n{shown}{}",
            problems.len(),
            if problems.len() == 1 { "" } else { "s" },
            if more > 0 {
                format!("\n…and {more} more")
            } else {
                String::new()
            }
        );
    }
    anyhow::ensure!(!commands.is_empty(), "The file has no commands.");
    Ok(commands)
}

impl RedisSession {
    /// Sends a reviewed write once; a lost answer is reported as unknown, never resent.
    pub async fn run_write(&self, write: &WorkbenchWrite) -> Result<Value, WriteError> {
        let (name, args) = write
            .words
            .split_first()
            .ok_or_else(|| WriteError::Refused("Type a command".into()))?;
        let name = String::from_utf8_lossy(name).to_ascii_uppercase();
        let args = args.iter().map(|arg| bytes_arg(arg)).collect::<Vec<_>>();
        let hash = cluster_hash(&name, &args);
        let command = fred::types::CustomCommand::new(name, hash, false);
        self.client
            .custom(command, args)
            .await
            .map_err(classify_redis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<Bytes> {
        split_command(line).unwrap()
    }

    fn keys(line: &str) -> Vec<Bytes> {
        match plan(words(line)).unwrap() {
            Planned::Write(write) => write.keys,
            Planned::Read(_) => panic!("{line} is a read"),
        }
    }

    #[test]
    fn test_writes_are_planned_with_their_keys() {
        assert_eq!(keys("set user:1 x EX 10"), vec![b"user:1".to_vec()]);
        assert_eq!(
            keys("DEL a b c"),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(keys("MSET a 1 b 2"), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(keys("RENAME a b"), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(keys("json.set doc $ 1"), vec![b"doc".to_vec()]);
        assert!(matches!(plan(words("GET a")).unwrap(), Planned::Read(_)));

        for (line, reason) in [
            ("EVAL \"return 1\" 0", "runs code"),
            ("FUNCTION LOAD x", "runs code"),
            ("FLUSHALL", "blocked"),
            ("CONFIG SET maxmemory 1", "blocked"),
            ("SELECT 2", "blocked"),
            ("CLIENT KILL ID 1", "blocked"),
            ("CLUSTER RESET", "blocked"),
            ("COPY a b DB 3", "not on Vilo's list"),
            ("SORT a STORE b", "not on Vilo's list"),
            ("XACK s g 1-1", "not on Vilo's list"),
            ("SOMETHING.NEW a", "not on Vilo's list"),
            ("MSET a 1 b", "in pairs"),
            ("RENAME a", "source and a destination"),
            ("DEL", "needs a key"),
            ("SET", "needs a key"),
        ] {
            let error = format!("{:#}", plan(words(line)).unwrap_err());
            assert!(error.contains(reason), "{line}: {error}");
        }
    }

    #[test]
    fn test_command_files_are_checked_whole() {
        let commands = parse_command_file("# seed\nSET a 1\n\n  HSET h f v  \nGET a\n").unwrap();
        assert_eq!(
            commands.iter().map(|(line, ..)| *line).collect::<Vec<_>>(),
            vec![2, 4, 5]
        );
        assert!(matches!(commands[2].2, Planned::Read(_)));

        let error = format!(
            "{:#}",
            parse_command_file("SET a 1\nFLUSHALL\nEVAL x 0\nSET \"open\n").unwrap_err()
        );
        assert!(
            error.starts_with("Nothing was run. 3 lines cannot be run"),
            "{error}"
        );
        assert!(error.contains("line 2") && error.contains("line 3") && error.contains("line 4"));
        assert!(parse_command_file("# only a comment\n").is_err());
        let too_many = "PING\n".repeat(MAX_FILE_COMMANDS + 1);
        assert!(format!("{:#}", parse_command_file(&too_many).unwrap_err()).contains("more than"));
    }

    #[tokio::test]
    async fn test_writes_run_once_against_redis() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect_to_database(&url, Some(7))
            .await
            .unwrap();
        session.call("FLUSHDB", Vec::new()).await.unwrap();
        let run = |line: &str| {
            let Planned::Write(write) = plan(words(line)).unwrap() else {
                panic!("{line} is a read");
            };
            write
        };
        session.run_write(&run("SET vilo_test:wb 1")).await.unwrap();
        let reply = session
            .run_write(&run("INCRBY vilo_test:wb 4"))
            .await
            .unwrap();
        assert_eq!(reply.as_i64(), Some(5));
        assert!(matches!(
            session.run_write(&run("HSET vilo_test:wb f v")).await,
            Err(WriteError::Refused(message)) if message.contains("WRONGTYPE")
        ));
        let deleted = session
            .run_write(&run("DEL vilo_test:wb nope"))
            .await
            .unwrap();
        assert_eq!(deleted.as_i64(), Some(1));
        session.call("FLUSHDB", Vec::new()).await.unwrap();
    }

    #[tokio::test]
    async fn test_cluster_writes_route_by_key() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let run = |line: &str| {
            let Planned::Write(write) = plan(words(line)).unwrap() else {
                panic!("{line} is a read");
            };
            write
        };
        for number in 0..20 {
            session
                .run_write(&run(&format!("SET vilo_test:wb:{number} {number}")))
                .await
                .unwrap();
        }
        assert!(matches!(
            session.run_write(&run("DEL vilo_test:wb:0 vilo_test:wb:1")).await,
            Err(WriteError::Refused(message)) if message.contains("CROSSSLOT")
        ));
        for number in 0..20 {
            session
                .run_write(&run(&format!("DEL vilo_test:wb:{number}")))
                .await
                .unwrap();
        }
    }
}
