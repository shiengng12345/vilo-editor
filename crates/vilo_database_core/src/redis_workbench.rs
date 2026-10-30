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

/// The reviewed write commands with their arity as Redis counts it (the name included; negative
/// means at least that many), from `COMMAND INFO` on Redis 7.4 and, for RedisJSON, which does not
/// declare one, from its documented syntax. Left out on purpose: COPY (can write another database), SORT
/// (STORE), MOVE, blocking commands, XACK/XCLAIM/XGROUP (consumer state, from the group panel),
/// and anything that runs code or changes the server or the connection.
const WRITE_COMMANDS: &[(&str, Keys, i32)] = &[
    ("SET", Keys::First, -3),
    ("SETNX", Keys::First, 3),
    ("SETEX", Keys::First, 4),
    ("PSETEX", Keys::First, 4),
    ("GETSET", Keys::First, 3),
    ("GETDEL", Keys::First, 2),
    ("GETEX", Keys::First, -2),
    ("APPEND", Keys::First, 3),
    ("INCR", Keys::First, 2),
    ("DECR", Keys::First, 2),
    ("INCRBY", Keys::First, 3),
    ("DECRBY", Keys::First, 3),
    ("INCRBYFLOAT", Keys::First, 3),
    ("SETRANGE", Keys::First, 4),
    ("SETBIT", Keys::First, 4),
    ("MSET", Keys::Pairs, -3),
    ("MSETNX", Keys::Pairs, -3),
    ("DEL", Keys::All, -2),
    ("UNLINK", Keys::All, -2),
    ("EXPIRE", Keys::First, -3),
    ("PEXPIRE", Keys::First, -3),
    ("EXPIREAT", Keys::First, -3),
    ("PEXPIREAT", Keys::First, -3),
    ("PERSIST", Keys::First, 2),
    ("RENAME", Keys::FirstTwo, 3),
    ("RENAMENX", Keys::FirstTwo, 3),
    ("HSET", Keys::First, -4),
    ("HSETNX", Keys::First, 4),
    ("HMSET", Keys::First, -4),
    ("HDEL", Keys::First, -3),
    ("HINCRBY", Keys::First, 4),
    ("HINCRBYFLOAT", Keys::First, 4),
    ("LPUSH", Keys::First, -3),
    ("RPUSH", Keys::First, -3),
    ("LPUSHX", Keys::First, -3),
    ("RPUSHX", Keys::First, -3),
    ("LPOP", Keys::First, -2),
    ("RPOP", Keys::First, -2),
    ("LSET", Keys::First, 4),
    ("LREM", Keys::First, 4),
    ("LTRIM", Keys::First, 4),
    ("LINSERT", Keys::First, 5),
    ("LMOVE", Keys::FirstTwo, 5),
    ("RPOPLPUSH", Keys::FirstTwo, 3),
    ("SADD", Keys::First, -3),
    ("SREM", Keys::First, -3),
    ("SPOP", Keys::First, -2),
    ("SMOVE", Keys::FirstTwo, 4),
    ("ZADD", Keys::First, -4),
    ("ZREM", Keys::First, -3),
    ("ZINCRBY", Keys::First, 4),
    ("ZPOPMIN", Keys::First, -2),
    ("ZPOPMAX", Keys::First, -2),
    ("ZREMRANGEBYSCORE", Keys::First, 4),
    ("ZREMRANGEBYRANK", Keys::First, 4),
    ("ZREMRANGEBYLEX", Keys::First, 4),
    ("XADD", Keys::First, -5),
    ("XDEL", Keys::First, -3),
    ("XTRIM", Keys::First, -4),
    ("PFADD", Keys::First, -2),
    ("GEOADD", Keys::First, -5),
    ("JSON.SET", Keys::First, -4),
    ("JSON.DEL", Keys::First, -2),
    ("JSON.FORGET", Keys::First, -2),
    ("JSON.MERGE", Keys::First, 4),
    ("JSON.CLEAR", Keys::First, -2),
    ("JSON.TOGGLE", Keys::First, 3),
    ("JSON.NUMINCRBY", Keys::First, 4),
    ("JSON.STRAPPEND", Keys::First, -3),
    ("JSON.ARRAPPEND", Keys::First, -3),
    ("JSON.ARRINSERT", Keys::First, -5),
    ("JSON.ARRPOP", Keys::First, -2),
    ("JSON.ARRTRIM", Keys::First, 5),
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
    let Some((_, keys, arity)) = WRITE_COMMANDS.iter().find(|(known, ..)| *known == name) else {
        bail!("{name} is not on Vilo's list of reviewed write commands, so it is not run.");
    };
    // Checked here, so a file whose later line is short is refused before any line runs.
    let count = i64::try_from(words.len()).unwrap_or(i64::MAX);
    let arity = i64::from(*arity);
    if (arity > 0 && count != arity) || (arity < 0 && count < -arity) {
        bail!(
            "{name} takes {} argument{}; this line has {}.",
            if arity > 0 {
                format!("{}", arity - 1)
            } else {
                format!("at least {}", -arity - 1)
            },
            if arity.abs() == 2 { "" } else { "s" },
            count - 1
        );
    }
    let args = words.get(1..).unwrap_or_default();
    check_shape(&name, args)?;
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
                !args.is_empty() && args.len().is_multiple_of(2),
                "{name} needs keys and values in pairs."
            );
            args.iter().step_by(2).cloned().collect()
        }
    };
    anyhow::ensure!(!keys.is_empty(), "{name} needs a key.");
    Ok(Planned::Write(WorkbenchWrite { name, words, keys }))
}

/// How arguments group, and how many some commands take at most: errors Redis would refuse a
/// line for, checked before anything runs. Option words themselves are checked by the server
/// when the line runs.
fn check_shape(name: &str, args: &[Bytes]) -> Result<()> {
    let word = |arg: &Bytes| String::from_utf8_lossy(arg).to_ascii_uppercase();
    let at_most = |limit: usize| {
        anyhow::ensure!(
            args.len() <= limit,
            "{name} takes at most {limit} arguments; this line has {}.",
            args.len()
        );
        anyhow::Ok(())
    };
    // The arguments after the key, past any leading option words.
    let after_options = |options: &[&str]| {
        let mut rest = args.get(1..).unwrap_or_default();
        while let Some((first, tail)) = rest.split_first() {
            if !options.contains(&word(first).as_str()) {
                break;
            }
            rest = tail;
        }
        rest
    };
    match name {
        "HSET" | "HMSET" => anyhow::ensure!(
            args.len().saturating_sub(1).is_multiple_of(2),
            "{name} needs fields and values in pairs after the key."
        ),
        "LPOP" | "RPOP" | "SPOP" | "ZPOPMIN" | "ZPOPMAX" | "JSON.DEL" | "JSON.FORGET"
        | "JSON.CLEAR" => at_most(2)?,
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "JSON.STRAPPEND" | "JSON.ARRPOP" => {
            at_most(3)?
        }
        "ZADD" => {
            let pairs = after_options(&["NX", "XX", "GT", "LT", "CH", "INCR"]);
            anyhow::ensure!(
                !pairs.is_empty() && pairs.len().is_multiple_of(2),
                "ZADD needs scores and members in pairs."
            );
        }
        "GEOADD" => {
            let triples = after_options(&["NX", "XX", "CH"]);
            anyhow::ensure!(
                !triples.is_empty() && triples.len().is_multiple_of(3),
                "GEOADD needs longitude, latitude and member in threes."
            );
        }
        "XADD" => {
            let mut rest = after_options(&["NOMKSTREAM"]);
            if let Some((first, tail)) = rest.split_first()
                && matches!(word(first).as_str(), "MAXLEN" | "MINID")
            {
                rest = tail;
                if let Some((mark, tail)) = rest.split_first()
                    && matches!(mark.as_slice(), b"=" | b"~")
                {
                    rest = tail;
                }
                let (_, tail) = rest
                    .split_first()
                    .context("XADD needs a threshold after MAXLEN or MINID.")?;
                rest = tail;
                if let Some((first, tail)) = rest.split_first()
                    && word(first) == "LIMIT"
                {
                    let (_, tail) = tail
                        .split_first()
                        .context("XADD needs a count after LIMIT.")?;
                    rest = tail;
                }
            }
            let (_, fields) = rest
                .split_first()
                .context("XADD needs an entry id, such as *.")?;
            anyhow::ensure!(
                !fields.is_empty() && fields.len().is_multiple_of(2),
                "XADD needs fields and values in pairs after the id."
            );
        }
        _ => {}
    }
    Ok(())
}

/// Parses a command file: one command per line as the Workbench takes it; empty lines and lines
/// starting with `#` are skipped. Every line is planned first, so a file with one line Vilo would
/// refuse runs nothing. Returns each command with its line number.
pub fn parse_command_file(text: &str) -> Result<Vec<(usize, String, Planned)>> {
    anyhow::ensure!(
        text.len() as u64 <= MAX_FILE_BYTES,
        "The file is larger than {MAX_FILE_BYTES} bytes."
    );
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
        for line in [
            "HSET h a 1 b 2",
            "ZADD z NX CH 1 a 2 b",
            "GEOADD g XX 13.3 38.1 palermo",
            "XADD s NOMKSTREAM MAXLEN ~ 100 LIMIT 10 * f v g w",
            "XADD s MINID = 0-1 1-1 f v",
            "LPOP l 3",
            "EXPIRE k 10 GT",
        ] {
            assert!(plan(words(line)).is_ok(), "{line}");
        }
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
            ("RENAME a", "takes 2 arguments"),
            ("DEL", "at least 1 argument"),
            ("SET", "at least 2 arguments"),
            ("SET b", "this line has 1"),
            ("HSET h f", "at least 3 arguments"),
            ("INCR a b", "takes 1 argument; this line has 2"),
            ("LINSERT l BEFORE x", "takes 4 arguments"),
            ("JSON.SET doc $", "at least 3 arguments"),
            ("HSET h field value orphan", "in pairs after the key"),
            ("LPOP l 1 extra", "at most 2 arguments"),
            ("EXPIRE k 10 NX extra", "at most 3 arguments"),
            ("ZADD z NX 1", "scores and members in pairs"),
            ("ZADD z 1 a 2", "scores and members in pairs"),
            ("GEOADD g 1 2 m 3", "in threes"),
            ("XADD s * f v g", "pairs after the id"),
            ("XADD s NOMKSTREAM NOMKSTREAM MAXLEN", "threshold after MAXLEN"),
            ("XADD s MAXLEN ~ 10 LIMIT", "count after LIMIT"),
        ] {
            let error = format!("{:#}", plan(words(line)).unwrap_err());
            assert!(error.contains(reason), "{line}: {error}");
        }
    }

    #[test]
    fn test_no_reviewed_write_passes_as_a_read() {
        for (name, ..) in WRITE_COMMANDS {
            assert_eq!(
                crate::redis::classify(name),
                CommandKind::Write,
                "{name} would skip the confirm step and the log"
            );
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
        let short = format!("{:#}", parse_command_file("SET a 1\nSET b\n").unwrap_err());
        assert!(
            short.contains("line 2"),
            "a short later line refuses the file: {short}"
        );
        let grouped = format!(
            "{:#}",
            parse_command_file("SET a 1\nHSET h field value orphan\n").unwrap_err()
        );
        assert!(grouped.contains("line 2"), "{grouped}");
        assert!(parse_command_file(&"#".repeat(MAX_FILE_BYTES as usize + 1)).is_err());
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
        let _cluster = crate::CLUSTER_TESTS.lock().await;
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
