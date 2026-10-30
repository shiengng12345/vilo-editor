use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use fred::{
    cmd,
    interfaces::ClusterInterface as _,
    prelude::{Builder, Client, ClientLike as _, Config, Server, Value},
};

/// How long any one command may take before its outcome is reported as unknown.
pub(crate) const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Keys and values stay bytes end to end; text, hex and base64 are display choices made elsewhere.
pub type Bytes = Vec<u8>;

pub struct RedisSession {
    pub(crate) client: Client,
    /// Kept so Pub/Sub can open its own connection with the same settings.
    pub(crate) config: Config,
    /// The TLS settings of a `rediss` connection, for the connections Vilo opens itself (MONITOR).
    pub(crate) tls: Option<Arc<rustls::ClientConfig>>,
    /// Something the user should know about how the session was set up.
    notice: Option<String>,
    clustered: bool,
    /// The logical database every connection of this session selects, also after a reconnect.
    database: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanPage {
    pub keys: Vec<Bytes>,
    /// The cursor for the next call; "0" means this node has been walked once. An empty page does not mean done.
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyInfo {
    /// "string", "hash", "list", "set", "zset", "stream", "ReJSON-RL", or "none" when the key is gone.
    pub kind: String,
    /// Milliseconds left; -1 means no expiry, -2 means the key does not exist.
    pub ttl_ms: i64,
}

/// One server to SCAN: the only one, or one primary of a cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanNode {
    pub label: String,
    pub(crate) server: Option<Server>,
}

/// Marks a consumer group whose name is not text, so nothing is sent with its shown name.
pub const GROUP_NAME_NOT_TEXT: &str = "vilo:name-not-text";

#[derive(Debug, Clone, PartialEq)]
pub struct StreamConsumer {
    pub name: String,
    pub pending: i64,
    pub idle_ms: i64,
    pub inactive_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub idle_ms: i64,
    pub deliveries: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(Bytes, Bytes)>,
}

/// What a Workbench command may do. Only reads run in this release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Read,
    Write,
    /// Blocks the server, wipes data, changes configuration or exposes every command; refused.
    Dangerous,
}

const READ_COMMANDS: &[&str] = &[
    "GET",
    "MGET",
    "GETRANGE",
    "STRLEN",
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "DBSIZE",
    "SCAN",
    "HGET",
    "HMGET",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HSTRLEN",
    "HSCAN",
    "HRANDFIELD",
    "LRANGE",
    "LINDEX",
    "LLEN",
    "LPOS",
    "SMEMBERS",
    "SISMEMBER",
    "SMISMEMBER",
    "SCARD",
    "SSCAN",
    "SRANDMEMBER",
    "SINTER",
    "SUNION",
    "SDIFF",
    "SINTERCARD",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZSCORE",
    "ZMSCORE",
    "ZRANK",
    "ZREVRANK",
    "ZCARD",
    "ZCOUNT",
    "ZLEXCOUNT",
    "ZSCAN",
    "ZRANDMEMBER",
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XINFO",
    "XPENDING",
    "GETBIT",
    "BITCOUNT",
    "BITPOS",
    "PFCOUNT",
    "GEOPOS",
    "GEODIST",
    "GEOHASH",
    "GEOSEARCH",
    "OBJECT",
    "MEMORY",
    "INFO",
    "TIME",
    "PING",
    "ECHO",
    "LASTSAVE",
    "ROLE",
    "SLOWLOG",
    "LOLWUT",
    "COMMAND",
    "JSON.GET",
    "JSON.MGET",
    "JSON.TYPE",
    "JSON.STRLEN",
    "JSON.ARRLEN",
    "JSON.OBJKEYS",
    "JSON.OBJLEN",
    "CLUSTER",
];

/// The read-only commands the Workbench and CLI accept, for completion.
pub fn read_commands() -> &'static [&'static str] {
    READ_COMMANDS
}

const DANGEROUS_COMMANDS: &[&str] = &[
    "KEYS",
    "FLUSHALL",
    "FLUSHDB",
    "CONFIG",
    "SHUTDOWN",
    "DEBUG",
    "MONITOR",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "SSUBSCRIBE",
    "SYNC",
    "PSYNC",
    "REPLICAOF",
    "SLAVEOF",
    "FAILOVER",
    "MIGRATE",
    "SCRIPT",
    "FUNCTION",
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL",
    "FCALL_RO",
    "MODULE",
    "ACL",
    "CLIENT",
    "SAVE",
    "BGSAVE",
    "BGREWRITEAOF",
    "SWAPDB",
    "BLPOP",
    "BRPOP",
    "BLMOVE",
    "BZPOPMIN",
    "BZPOPMAX",
    "BLMPOP",
    "BZMPOP",
    "XREAD",
    "XREADGROUP",
    "WAIT",
    "WAITAOF",
    "RESET",
    "QUIT",
    "SELECT",
];

/// Commands whose safety depends on the subcommand, with the read-only subcommands allowed.
const READ_SUBCOMMANDS: &[(&str, &[&str])] = &[
    (
        "CLUSTER",
        &[
            "INFO",
            "NODES",
            "SLOTS",
            "SHARDS",
            "KEYSLOT",
            "COUNTKEYSINSLOT",
            "MYID",
            "MYSHARDID",
            "LINKS",
            "HELP",
        ],
    ),
    ("SLOWLOG", &["GET", "LEN", "HELP"]),
    (
        "OBJECT",
        &["ENCODING", "FREQ", "IDLETIME", "REFCOUNT", "HELP"],
    ),
    (
        "MEMORY",
        &["USAGE", "STATS", "DOCTOR", "MALLOC-STATS", "HELP"],
    ),
    ("XINFO", &["STREAM", "GROUPS", "CONSUMERS", "HELP"]),
    (
        "COMMAND",
        &[
            "COUNT",
            "DOCS",
            "INFO",
            "LIST",
            "GETKEYS",
            "GETKEYSANDFLAGS",
            "HELP",
        ],
    ),
];

/// Classifies a whole command line, so `CLUSTER RESET` or `SLOWLOG RESET` is not let through as a read.
pub fn classify_words(words: &[Bytes]) -> CommandKind {
    let Some(first) = words.first() else {
        return CommandKind::Write;
    };
    let name = String::from_utf8_lossy(first).to_ascii_uppercase();
    match classify(&name) {
        CommandKind::Read => match READ_SUBCOMMANDS
            .iter()
            .find(|(command, _)| *command == name)
        {
            Some((_, allowed)) => {
                let sub = words
                    .get(1)
                    .map(|sub| String::from_utf8_lossy(sub).to_ascii_uppercase())
                    .unwrap_or_default();
                if allowed.contains(&sub.as_str()) {
                    CommandKind::Read
                } else {
                    CommandKind::Dangerous
                }
            }
            None => CommandKind::Read,
        },
        other => other,
    }
}

/// Unknown commands count as writes: a command Vilo cannot place is not assumed safe.
pub fn classify(command: &str) -> CommandKind {
    let command = command.to_ascii_uppercase();
    if DANGEROUS_COMMANDS.contains(&command.as_str()) {
        CommandKind::Dangerous
    } else if READ_COMMANDS.contains(&command.as_str()) {
        CommandKind::Read
    } else {
        CommandKind::Write
    }
}

/// Splits a Workbench line the way redis-cli does: spaces separate words, quotes group them, and inside
/// double quotes `\n`, `\t`, `\"`, `\\` and `\xHH` escape bytes.
pub fn split_command(line: &str) -> Result<Vec<Bytes>> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            chars.next();
        }
        let Some(&first) = chars.peek() else {
            return Ok(words);
        };
        let mut word = Vec::new();
        match first {
            '"' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => bail!("A quoted word is missing its closing \""),
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => word.push(b'\n'),
                            Some('t') => word.push(b'\t'),
                            Some('r') => word.push(b'\r'),
                            Some('x') => {
                                let hex = chars
                                    .next()
                                    .into_iter()
                                    .chain(chars.next())
                                    .collect::<String>();
                                word.push(
                                    u8::from_str_radix(&hex, 16)
                                        .map_err(|_| anyhow!("\\x needs two hex digits"))?,
                                );
                            }
                            Some(other) => word.extend(other.to_string().as_bytes()),
                            None => bail!("A quoted word ends in \\"),
                        },
                        Some(other) => word.extend(other.to_string().as_bytes()),
                    }
                }
            }
            '\'' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => bail!("A quoted word is missing its closing '"),
                        Some('\'') => break,
                        Some(other) => word.extend(other.to_string().as_bytes()),
                    }
                }
            }
            _ => {
                while let Some(&character) = chars.peek() {
                    if character.is_whitespace() {
                        break;
                    }
                    word.extend(character.to_string().as_bytes());
                    chars.next();
                }
            }
        }
        words.push(word);
    }
}

/// Text for bytes in redis-cli's quoted style: printable ASCII as is, everything else escaped.
/// One SLOWLOG entry, with the node it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct SlowEntry {
    pub node: String,
    pub id: u64,
    /// Unix seconds when the command ran.
    pub time: i64,
    pub micros: u64,
    pub args: Vec<Bytes>,
    pub client: String,
    pub client_name: String,
}

/// Parses a `SLOWLOG GET` reply; entries of an unexpected shape are skipped.
pub fn parse_slowlog(node: &str, reply: Value) -> Vec<SlowEntry> {
    reply
        .into_array()
        .into_iter()
        .filter_map(|entry| {
            let mut fields = entry.into_array().into_iter();
            let id = fields.next()?.as_u64()?;
            let time = fields.next()?.as_i64()?;
            let micros = fields.next()?.as_u64()?;
            let args = fields
                .next()?
                .into_array()
                .iter()
                .map(value_bytes)
                .collect();
            let text = |value: Option<Value>| {
                value
                    .and_then(|value| value.as_string())
                    .unwrap_or_default()
            };
            Some(SlowEntry {
                node: node.to_string(),
                id,
                time,
                micros,
                args,
                client: text(fields.next()),
                client_name: text(fields.next()),
            })
        })
        .collect()
}

/// One connection from `CLIENT LIST`, fields in the server's order.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientInfo {
    pub node: String,
    pub fields: Vec<(String, String)>,
}

impl ClientInfo {
    pub fn get(&self, name: &str) -> &str {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map_or("", |(_, value)| value.as_str())
    }
}

/// Parses `CLIENT LIST`: one line per connection of `key=value` pairs.
pub fn parse_client_list(node: &str, text: &str) -> Vec<ClientInfo> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| ClientInfo {
            node: node.to_string(),
            fields: line
                .split(' ')
                .filter_map(|pair| {
                    let (key, value) = pair.split_once('=')?;
                    Some((key.to_string(), value.to_string()))
                })
                .collect(),
        })
        .collect()
}

/// What each node answered; a node that failed is reported, not dropped.
#[derive(Debug, Clone)]
pub struct NodeReport<T> {
    pub items: Vec<T>,
    pub failures: Vec<(String, String)>,
}

impl<T> Default for NodeReport<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            failures: Vec::new(),
        }
    }
}

/// Server administration writes; each runs on one node.
#[derive(Debug, Clone, PartialEq)]
pub enum AdminWrite {
    SlowlogReset,
    SlowlogThreshold { micros: i64 },
}

impl AdminWrite {
    pub fn describe(&self) -> String {
        match self {
            AdminWrite::SlowlogReset => "SLOWLOG RESET   (clears the slow log)".into(),
            AdminWrite::SlowlogThreshold { micros } => {
                format!("CONFIG SET slowlog-log-slower-than {micros}")
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            AdminWrite::SlowlogReset => "SLOWLOG RESET",
            AdminWrite::SlowlogThreshold { .. } => "CONFIG SET slowlog-log-slower-than",
        }
    }
}

/// One node of a cluster as `CLUSTER NODES` reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterNode {
    pub id: String,
    /// host:port clients use; the bus port and hostname are dropped.
    pub address: String,
    pub primary: bool,
    /// For a replica, the id of its primary.
    pub primary_id: Option<String>,
    /// Flags other than master/slave/myself, e.g. fail, fail?, handshake, noaddr.
    pub problems: Vec<String>,
    pub connected: bool,
    /// Slot ranges as written, e.g. "0-5460" or "7000"; importing/migrating entries are skipped.
    pub slots: Vec<String>,
    pub slot_count: u32,
}

/// Parses `CLUSTER NODES`; malformed lines are skipped.
pub fn parse_cluster_nodes(text: &str) -> Vec<ClusterNode> {
    text.lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 8 {
                return None;
            }
            let address = fields[1].split(['@', ',']).next()?.to_string();
            let flags = fields[2].split(',').collect::<Vec<_>>();
            let primary = flags.contains(&"master");
            // A range counts only if it lies within the 16384 slots; others are reported as problems.
            let slot_size = |slot: &str| -> Option<u32> {
                let (start, end) = slot.split_once('-').unwrap_or((slot, slot));
                let (start, end) = (start.parse::<u32>().ok()?, end.parse::<u32>().ok()?);
                (start <= end && end < 16384).then(|| end - start + 1)
            };
            let mut slots = Vec::new();
            let mut slot_count = 0;
            let mut problems = flags
                .iter()
                .filter(|flag| !matches!(**flag, "master" | "slave" | "myself" | "noflags"))
                .map(|flag| flag.to_string())
                .collect::<Vec<_>>();
            for slot in fields[8..].iter().filter(|slot| !slot.starts_with('[')) {
                match slot_size(slot) {
                    Some(size) => {
                        slots.push(slot.to_string());
                        slot_count += size;
                    }
                    None => problems.push(format!("bad slot range {slot}")),
                }
            }
            Some(ClusterNode {
                id: fields[0].to_string(),
                address,
                primary,
                primary_id: (fields[3] != "-").then(|| fields[3].to_string()),
                problems,
                connected: fields[7] == "connected",
                slots,
                slot_count,
            })
        })
        .collect()
}

/// `db3:keys=12,expires=1,avg_ttl=0` lines from INFO keyspace, as (3, 12).
pub fn parse_keyspace(info: &str) -> Vec<(u32, u64)> {
    info.lines()
        .filter_map(|line| {
            let (name, fields) = line.trim().split_once(':')?;
            let database = name.strip_prefix("db")?.parse().ok()?;
            let keys = fields
                .split(',')
                .find_map(|field| field.strip_prefix("keys="))?
                .parse()
                .ok()?;
            Some((database, keys))
        })
        .collect()
}

pub fn quote_bytes(bytes: &[u8]) -> String {
    let mut text = String::from("\"");
    for byte in bytes {
        match byte {
            b'"' => text.push_str("\\\""),
            b'\\' => text.push_str("\\\\"),
            b'\n' => text.push_str("\\n"),
            b' '..=b'~' => text.push(*byte as char),
            _ => text.push_str(&format!("\\x{byte:02x}")),
        }
    }
    text.push('"');
    text
}

/// A reply as redis-cli prints it.
pub fn format_reply(value: &Value) -> Vec<String> {
    fn push(value: &Value, indent: usize, lines: &mut Vec<String>, prefix: String) {
        let pad = " ".repeat(indent);
        match value {
            Value::Null => lines.push(format!("{pad}{prefix}(nil)")),
            Value::Integer(number) => lines.push(format!("{pad}{prefix}(integer) {number}")),
            Value::Double(number) => lines.push(format!("{pad}{prefix}(double) {number}")),
            Value::Boolean(flag) => lines.push(format!("{pad}{prefix}({flag})")),
            Value::String(text) => {
                lines.push(format!("{pad}{prefix}{}", quote_bytes(text.as_bytes())))
            }
            Value::Bytes(bytes) => lines.push(format!("{pad}{prefix}{}", quote_bytes(bytes))),
            Value::Array(items) if items.is_empty() => {
                lines.push(format!("{pad}{prefix}(empty array)"))
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    let marker = format!("{}) ", index + 1);
                    if index == 0 {
                        push(item, indent, lines, format!("{prefix}{marker}"));
                    } else {
                        push(item, indent + prefix.len(), lines, marker);
                    }
                }
            }
            Value::Map(map) => {
                for (index, (key, item)) in map.clone().inner().iter().enumerate() {
                    let key = quote_bytes(key.as_bytes());
                    let marker = format!("{}# {key} => ", index + 1);
                    if index == 0 {
                        push(item, indent, lines, format!("{prefix}{marker}"));
                    } else {
                        push(item, indent + prefix.len(), lines, marker);
                    }
                }
            }
            other => lines.push(format!("{pad}{prefix}{other:?}")),
        }
    }
    let mut lines = Vec::new();
    push(value, 0, &mut lines, String::new());
    lines
}

fn scan_args(key: &[u8], cursor: &str, count: u32, pattern: Option<&[u8]>) -> Vec<Value> {
    let mut args = vec![bytes_arg(key), Value::from(cursor.to_string())];
    if let Some(pattern) = pattern {
        args.push("MATCH".into());
        args.push(bytes_arg(pattern));
    }
    args.push("COUNT".into());
    args.push(Value::from(i64::from(count)));
    args
}

/// A typed search as a Redis glob: a plain word matches anywhere, a glob is used as typed.
pub fn search_pattern(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    if text.is_empty() {
        None
    } else if text.contains(['*', '?', '[']) {
        Some(text.as_bytes().to_vec())
    } else {
        Some(format!("*{text}*").into_bytes())
    }
}

/// How a Cluster routes a command. fred hashes the first argument by default, which for
/// MEMORY USAGE, OBJECT ENCODING, XINFO GROUPS or XGROUP CREATE is the subcommand, not the key, so every such
/// call would go to one node and be redirected until it gives up. fred honours only an explicit
/// slot for custom commands (not an argument offset), so the key's slot is given.
pub(crate) fn cluster_hash(name: &str, args: &[Value]) -> fred::types::ClusterHash {
    let subcommand_first = ["MEMORY", "OBJECT", "XINFO", "XGROUP"]
        .iter()
        .any(|command| command.eq_ignore_ascii_case(name));
    match args.get(1).and_then(|key| key.as_bytes()) {
        Some(key) if subcommand_first => fred::types::ClusterHash::from(key),
        _ => fred::types::ClusterHash::FirstKey,
    }
}

/// The host a connection string names, which TLS certificates are checked against. Taken from
/// the URL itself rather than from fred's seeds, which put `node=` entries first.
fn url_host(url: &str) -> Result<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|url| {
            url.host_str()
                .map(|host| host.trim_matches(['[', ']']).to_string())
        })
        .filter(|host| !host.is_empty())
        .context("The Redis URL has no host")
}

pub(crate) fn bytes_arg(bytes: &[u8]) -> Value {
    Value::Bytes(bytes.to_vec().into())
}

pub(crate) fn value_bytes(value: &Value) -> Bytes {
    value
        .as_bytes()
        .map(<[u8]>::to_vec)
        .unwrap_or_else(|| value.as_string().unwrap_or_default().into_bytes())
}

/// RESP2 sends pairs flat ([a, b, a, b]); RESP3 sends maps or nested pairs. Both come back as pairs.
pub(crate) fn pairs(value: Value) -> Vec<(Value, Value)> {
    match value {
        Value::Map(map) => map
            .inner()
            .into_iter()
            .map(|(key, value)| (Value::from(key), value))
            .collect(),
        Value::Array(items)
            if items
                .iter()
                .all(|item| matches!(item, Value::Array(pair) if pair.len() == 2))
                && !items.is_empty() =>
        {
            items
                .into_iter()
                .filter_map(|item| {
                    let mut pair = item.into_array().into_iter();
                    Some((pair.next()?, pair.next()?))
                })
                .collect()
        }
        Value::Array(items) => {
            let mut iter = items.into_iter();
            let mut out = Vec::new();
            while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                out.push((key, value));
            }
            out
        }
        _ => Vec::new(),
    }
}

impl RedisSession {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_to_database(url, None).await
    }

    /// Connects with `database` in place of the URL's; a cluster only has database 0.
    pub async fn connect_to_database(url: &str, database: Option<u8>) -> Result<Self> {
        Self::connect_with(url, database, None).await
    }

    /// Connects with `database` in place of the URL's and, for a `rediss` URL, these TLS settings
    /// (the system's roots when none are given). A server that turns out to be a cluster node is
    /// connected to again as a cluster, as RedisInsight does, so every node it reports is reached.
    pub async fn connect_with(
        url: &str,
        database: Option<u8>,
        tls: Option<&crate::redis_tls::RedisTls>,
    ) -> Result<Self> {
        // Parsed without its TLS scheme: fred would build its own TLS settings for `rediss`,
        // which needs a process-wide crypto provider; Vilo's own settings are set below.
        let (url, tls_scheme) = crate::redis_tls::split_tls_scheme(url);
        let mut config = Config::from_url(&url).context("The Redis URL is not valid")?;
        let host = url_host(&url)?;
        let tls_config = match (tls_scheme, tls) {
            (true, settings) => Some(Arc::new(crate::redis_tls::client_config(
                &settings.cloned().unwrap_or_default(),
                &host,
            )?)),
            (false, Some(_)) => bail!("TLS settings need a TLS connection; turn Use TLS on."),
            (false, None) => None,
        };
        if let Some(tls_config) = &tls_config {
            config.tls = Some(fred::types::config::TlsConfig {
                connector: fred::types::config::TlsConnector::from(
                    tokio_rustls::TlsConnector::from(tls_config.clone()),
                ),
                // Every node's certificate is checked against the connected host by the
                // verifier itself, so the names a cluster reports are only used to connect.
                hostnames: fred::types::config::TlsHostMapping::None,
            });
        }
        let selects = database
            .or(config.database)
            .is_some_and(|database| database > 0);
        let session = match Self::open(config.clone(), database, tls_config.clone()).await {
            Ok(session) => session,
            // A cluster node refuses SELECT; asked once more without it, the node says whether it
            // is one, whatever words its refusal used.
            Err(error) if selects => {
                let mut plain = config;
                plain.database = None;
                if let Ok(probe) = Self::open(plain, None, tls_config.clone()).await {
                    let cluster = probe.is_cluster_node().await.unwrap_or(false);
                    probe.quit().await.ok();
                    anyhow::ensure!(
                        !cluster,
                        "This server is a cluster node, and a cluster only has database 0."
                    );
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if session.clustered {
            return Ok(session);
        }
        let cluster = match session.is_cluster_node().await {
            Ok(cluster) => cluster,
            // Without INFO there is no telling; a working single connection is kept, and said.
            Err(error) => {
                let mut session = session;
                session.notice = Some(format!(
                    "Vilo could not check whether this server is a cluster node (INFO cluster: {error:#}), so it is used as a single server."
                ));
                return Ok(session);
            }
        };
        let Server { host, port, .. } = match &session.config.server {
            fred::types::config::ServerConfig::Centralized { server } if cluster => server.clone(),
            _ => return Ok(session),
        };
        anyhow::ensure!(
            session.database == 0,
            "This server is a cluster node, and a cluster only has database 0."
        );
        let mut cluster = session.config.clone();
        // The single connection is replaced either way; dropping the session closes it.
        session.quit().await.ok();
        cluster.server =
            fred::types::config::ServerConfig::new_clustered(vec![(host.to_string(), port)]);
        cluster.database = None;
        Self::open(cluster, None, tls_config).await
    }

    /// Whether the server this single connection reached runs in cluster mode.
    async fn is_cluster_node(&self) -> Result<bool> {
        let info = self.call("INFO", vec!["cluster".into()]).await?;
        Ok(info
            .as_string()
            .unwrap_or_default()
            .lines()
            .any(|line| line.trim() == "cluster_enabled:1"))
    }

    async fn open(
        mut config: Config,
        database: Option<u8>,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        let clustered = config.server.is_clustered();
        if let Some(database) = database {
            anyhow::ensure!(
                !clustered || database == 0,
                "A Redis cluster only has database 0"
            );
            config.database = Some(database);
        }
        let selected = config.database.unwrap_or(0);
        let mut builder = Builder::from_config(config.clone());
        builder.with_connection_config(|connection| {
            // A command whose answer was lost is reported as unknown rather than sent again.
            connection.max_command_attempts = 1;
        });
        // Without a deadline a reply lost on an open connection would be awaited forever; a
        // timed-out write is reported as unknown.
        builder.with_performance_config(|performance| {
            performance.default_command_timeout = COMMAND_TIMEOUT;
        });
        let client = builder.build()?;
        client.init().await.context("Could not reach Redis")?;
        Ok(Self {
            client,
            config,
            tls,
            notice: None,
            clustered,
            database: selected,
        })
    }

    pub fn database(&self) -> u8 {
        self.database
    }

    pub fn notice(&self) -> Option<&str> {
        self.notice.as_deref()
    }

    pub fn is_clustered(&self) -> bool {
        self.clustered
    }

    /// How many logical databases the server has, or `None` when CONFIG is not allowed (common on
    /// managed Redis).
    pub async fn database_count(&self) -> Option<u32> {
        let reply = self
            .call("CONFIG", vec!["GET".into(), "databases".into()])
            .await
            .ok()?;
        pairs(reply)
            .into_iter()
            .find_map(|(_, value)| value.as_string()?.parse::<u32>().ok())
    }

    /// The cluster's nodes, or `None` for a single server.
    pub async fn cluster_nodes(&self) -> Result<Option<Vec<ClusterNode>>> {
        if !self.clustered {
            return Ok(None);
        }
        let reply = self.call("CLUSTER", vec!["NODES".into()]).await?;
        Ok(Some(parse_cluster_nodes(
            &reply.as_string().unwrap_or_default(),
        )))
    }

    /// `INFO replication` as key/value pairs (role, connected_slaves, slave0, master_host …).
    pub async fn replication(&self) -> Result<Vec<(String, String)>> {
        let reply = self.call("INFO", vec!["replication".into()]).await?;
        Ok(reply
            .as_string()
            .unwrap_or_default()
            .lines()
            .filter_map(|line| {
                let (key, value) = line.trim().split_once(':')?;
                Some((key.to_string(), value.to_string()))
            })
            .collect())
    }

    /// Databases holding keys, from INFO keyspace, as (database, keys).
    pub async fn keyspace(&self) -> Result<Vec<(u32, u64)>> {
        let reply = self.call("INFO", vec!["keyspace".into()]).await?;
        Ok(parse_keyspace(&reply.as_string().unwrap_or_default()))
    }

    /// The servers a full SCAN must walk: each cluster primary, or the single server.
    pub fn scan_nodes(&self) -> Vec<ScanNode> {
        if self.clustered
            && let Some(state) = self.client.cached_cluster_state()
        {
            return state
                .unique_primary_nodes()
                .into_iter()
                .map(|server| ScanNode {
                    label: server.to_string(),
                    server: Some(server),
                })
                .collect();
        }
        vec![ScanNode {
            label: "server".into(),
            server: None,
        }]
    }

    /// One command routed by its key, keeping fred's error so a write can tell refused from unknown.
    async fn keyed(
        &self,
        name: &'static str,
        args: Vec<Value>,
    ) -> Result<Value, fred::error::Error> {
        let command = fred::types::CustomCommand::new(name, cluster_hash(name, &args), false);
        self.client.custom(command, args).await
    }

    pub(crate) async fn call(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        Ok(self
            .client
            .custom(
                fred::types::CustomCommand::new(name, cluster_hash(name, &args), false),
                args,
            )
            .await?)
    }

    /// One SCAN call on one node. COUNT is a hint, so a page may hold more or fewer keys, or none, and keys may repeat.
    pub async fn scan(
        &self,
        node: &ScanNode,
        cursor: &str,
        pattern: &str,
        count: u32,
        kind: Option<&str>,
    ) -> Result<ScanPage> {
        let mut args = vec![
            Value::from(cursor.to_string()),
            "MATCH".into(),
            bytes_arg(pattern.as_bytes()),
            "COUNT".into(),
            Value::from(count as i64),
        ];
        if let Some(kind) = kind {
            args.push("TYPE".into());
            args.push(Value::from(kind.to_string()));
        }
        let reply: Value = match &node.server {
            Some(server) => {
                self.client
                    .with_cluster_node(server.clone())
                    .custom(cmd!("SCAN"), args)
                    .await?
            }
            None => self.call("SCAN", args).await?,
        };
        let mut parts = reply.into_array().into_iter();
        let cursor = parts
            .next()
            .and_then(|cursor| cursor.as_string())
            .context("SCAN returned no cursor")?;
        let keys = parts
            .next()
            .map(|keys| keys.into_array().iter().map(value_bytes).collect())
            .unwrap_or_default();
        Ok(ScanPage { keys, cursor })
    }

    pub async fn key_info(&self, key: &[u8]) -> Result<KeyInfo> {
        let kind = self.call("TYPE", vec![bytes_arg(key)]).await?;
        let ttl = self.call("PTTL", vec![bytes_arg(key)]).await?;
        Ok(KeyInfo {
            kind: kind.as_string().unwrap_or_else(|| "none".into()),
            ttl_ms: ttl.as_i64().context("PTTL returned no number")?,
        })
    }

    /// Items in the key: string bytes, hash fields, list, set and sorted set members, stream entries.
    pub async fn length(&self, key: &[u8], kind: &str) -> Result<Option<u64>> {
        let command = match kind {
            "string" => "STRLEN",
            "hash" => "HLEN",
            "list" => "LLEN",
            "set" => "SCARD",
            "zset" => "ZCARD",
            "stream" => "XLEN",
            _ => return Ok(None),
        };
        Ok(self.call(command, vec![bytes_arg(key)]).await?.as_u64())
    }

    /// Keys in the current database of one node (`None` for the node the client is connected to).
    /// Runs one command on one node (any node when not clustered).
    async fn node_call(
        &self,
        node: &ScanNode,
        name: &'static str,
        args: Vec<Value>,
    ) -> Result<Value, fred::error::Error> {
        match &node.server {
            Some(server) => {
                self.client
                    .with_cluster_node(server.clone())
                    .custom(cmd!(name), args)
                    .await
            }
            None => self.client.custom(cmd!(name), args).await,
        }
    }

    /// The newest `count` (1–1024) slow commands of every primary, newest first.
    pub async fn slowlog(&self, count: u32) -> NodeReport<SlowEntry> {
        let count = count.clamp(1, 1024);
        let mut report = NodeReport::default();
        for node in self.scan_nodes() {
            match self
                .node_call(
                    &node,
                    "SLOWLOG",
                    vec!["GET".into(), Value::from(i64::from(count))],
                )
                .await
            {
                Ok(reply) => {
                    let total = reply.array_len().unwrap_or(0);
                    let entries = parse_slowlog(&node.label, reply);
                    if entries.len() < total {
                        report.failures.push((
                            node.label.clone(),
                            format!(
                                "{} slow log entries could not be read",
                                total - entries.len()
                            ),
                        ));
                    }
                    report.items.extend(entries);
                }
                Err(error) => report
                    .failures
                    .push((node.label.clone(), error.to_string())),
            }
        }
        report
            .items
            .sort_by(|a, b| b.time.cmp(&a.time).then(b.id.cmp(&a.id)));
        report
    }

    /// `slowlog-log-slower-than` (µs) and `slowlog-max-len` of each primary.
    pub async fn slowlog_config(&self) -> NodeReport<(String, Option<i64>, Option<i64>)> {
        let mut report = NodeReport::default();
        for node in self.scan_nodes() {
            match self
                .node_call(&node, "CONFIG", vec!["GET".into(), "slowlog-*".into()])
                .await
            {
                Ok(reply) => {
                    let pairs = pairs(reply);
                    let read = |name: &str| {
                        pairs
                            .iter()
                            .find(|(key, _)| key.as_string().as_deref() == Some(name))
                            .and_then(|(_, value)| value.as_string()?.parse::<i64>().ok())
                    };
                    report.items.push((
                        node.label.clone(),
                        read("slowlog-log-slower-than"),
                        read("slowlog-max-len"),
                    ));
                }
                Err(error) => report
                    .failures
                    .push((node.label.clone(), error.to_string())),
            }
        }
        report
    }

    /// Every connection of every primary.
    pub async fn clients(&self) -> NodeReport<ClientInfo> {
        let mut report = NodeReport::default();
        for node in self.scan_nodes() {
            match self.node_call(&node, "CLIENT", vec!["LIST".into()]).await {
                Ok(reply) => match reply.as_string() {
                    Some(text) => report.items.extend(parse_client_list(&node.label, &text)),
                    None => report.failures.push((
                        node.label.clone(),
                        "CLIENT LIST did not answer with text".into(),
                    )),
                },
                Err(error) => report
                    .failures
                    .push((node.label.clone(), error.to_string())),
            }
        }
        report
    }

    /// Sends one administration write to one node, once.
    pub async fn admin_write(
        &self,
        node: &ScanNode,
        write: &AdminWrite,
    ) -> Result<(), crate::mongo::WriteError> {
        let reply = match write {
            AdminWrite::SlowlogReset => self.node_call(node, "SLOWLOG", vec!["RESET".into()]).await,
            AdminWrite::SlowlogThreshold { micros } => {
                self.node_call(
                    node,
                    "CONFIG",
                    vec![
                        "SET".into(),
                        "slowlog-log-slower-than".into(),
                        Value::from(micros.to_string()),
                    ],
                )
                .await
            }
        };
        reply.map(|_| ()).map_err(classify_redis)
    }

    pub async fn dbsize(&self, node: &ScanNode) -> Result<u64> {
        let reply: Value = match &node.server {
            Some(server) => {
                self.client
                    .with_cluster_node(server.clone())
                    .custom(cmd!("DBSIZE"), Vec::<Value>::new())
                    .await?
            }
            None => self.call("DBSIZE", Vec::new()).await?,
        };
        reply.as_u64().context("DBSIZE returned no number")
    }

    /// Type, TTL and (when asked) memory for many keys at once. The commands go out together and fred
    /// pipelines them on the connection, instead of one round trip per key.
    pub async fn key_meta(&self, keys: &[Bytes], with_size: bool) -> Vec<(KeyInfo, Option<u64>)> {
        let lookups = keys.iter().map(|key| async move {
            let (kind, ttl, size) = futures::join!(
                self.call("TYPE", vec![bytes_arg(key)]),
                self.call("PTTL", vec![bytes_arg(key)]),
                async {
                    if with_size {
                        self.call("MEMORY", vec!["USAGE".into(), bytes_arg(key)])
                            .await
                            .ok()
                            .and_then(|size| size.as_u64())
                    } else {
                        None
                    }
                }
            );
            let info = KeyInfo {
                kind: kind
                    .ok()
                    .and_then(|kind| kind.as_string())
                    .unwrap_or_else(|| "none".into()),
                ttl_ms: ttl.ok().and_then(|ttl| ttl.as_i64()).unwrap_or(-2),
            };
            (info, size)
        });
        futures::future::join_all(lookups).await
    }

    /// Memory the key takes on the server, which is not the value's length.
    pub async fn memory_usage(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .call("MEMORY", vec!["USAGE".into(), bytes_arg(key)])
            .await?
            .as_u64())
    }

    /// The raw value of a string key, or `None` when it does not exist.
    pub async fn get_string(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.call("GET", vec![bytes_arg(key)]).await?;
        Ok(value.as_bytes().map(<[u8]>::to_vec))
    }

    /// One hash field's value, or `None` when the field or the key is missing.
    pub async fn hash_get(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>> {
        let value = self
            .call("HGET", vec![bytes_arg(key), bytes_arg(field)])
            .await?;
        Ok(value.as_bytes().map(<[u8]>::to_vec))
    }

    /// One sorted-set member's score as the server writes it, or `None` when it is missing.
    pub async fn zset_score(&self, key: &[u8], member: &[u8]) -> Result<Option<String>> {
        let value = self
            .call("ZSCORE", vec![bytes_arg(key), bytes_arg(member)])
            .await?;
        Ok(value
            .as_string()
            .or_else(|| value.as_f64().map(|score| score.to_string())))
    }

    pub async fn hash_page(
        &self,
        key: &[u8],
        cursor: &str,
        count: u32,
        pattern: Option<&[u8]>,
    ) -> Result<(Vec<(Bytes, Bytes)>, String)> {
        let reply = self
            .call("HSCAN", scan_args(key, cursor, count, pattern))
            .await?;
        let mut parts = reply.into_array().into_iter();
        let cursor = parts
            .next()
            .and_then(|cursor| cursor.as_string())
            .context("HSCAN returned no cursor")?;
        let fields = parts
            .next()
            .map(pairs)
            .unwrap_or_default()
            .into_iter()
            .map(|(field, value)| (value_bytes(&field), value_bytes(&value)))
            .collect();
        Ok((fields, cursor))
    }

    pub async fn set_page(
        &self,
        key: &[u8],
        cursor: &str,
        count: u32,
        pattern: Option<&[u8]>,
    ) -> Result<(Vec<Bytes>, String)> {
        let reply = self
            .call("SSCAN", scan_args(key, cursor, count, pattern))
            .await?;
        let mut parts = reply.into_array().into_iter();
        let cursor = parts
            .next()
            .and_then(|cursor| cursor.as_string())
            .context("SSCAN returned no cursor")?;
        let members = parts
            .next()
            .map(|members| members.into_array().iter().map(value_bytes).collect())
            .unwrap_or_default();
        Ok((members, cursor))
    }

    /// One ZSCAN page: members with their scores as the server writes them.
    pub async fn zset_scan(
        &self,
        key: &[u8],
        cursor: &str,
        count: u32,
        pattern: Option<&[u8]>,
    ) -> Result<(Vec<(Bytes, String)>, String)> {
        let reply = self
            .call("ZSCAN", scan_args(key, cursor, count, pattern))
            .await?;
        let mut parts = reply.into_array().into_iter();
        let cursor = parts
            .next()
            .and_then(|cursor| cursor.as_string())
            .context("ZSCAN returned no cursor")?;
        let members = parts
            .next()
            .map(pairs)
            .unwrap_or_default()
            .into_iter()
            .map(|(member, score)| {
                let score = score
                    .as_string()
                    .or_else(|| score.as_f64().map(|score| score.to_string()))
                    .context("ZSCAN returned a score that is not a number")?;
                Ok((value_bytes(&member), score))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((members, cursor))
    }

    /// List items from `start` to `stop`, inclusive, by position.
    pub async fn list_range(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let reply = self
            .call(
                "LRANGE",
                vec![bytes_arg(key), Value::from(start), Value::from(stop)],
            )
            .await?;
        Ok(reply.into_array().iter().map(value_bytes).collect())
    }

    /// Sorted set members with their scores, lowest score first, by position.
    pub async fn zset_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Bytes, String)>> {
        let reply = self
            .call(
                "ZRANGE",
                vec![
                    bytes_arg(key),
                    Value::from(start),
                    Value::from(stop),
                    "WITHSCORES".into(),
                ],
            )
            .await?;
        Ok(pairs(reply)
            .into_iter()
            .map(|(member, score)| {
                (
                    value_bytes(&member),
                    score
                        .as_string()
                        .or_else(|| score.as_f64().map(|score| score.to_string()))
                        .unwrap_or_default(),
                )
            })
            .collect())
    }

    /// Stream entries after `after` (exclusive), oldest first.
    pub async fn stream_range(
        &self,
        key: &[u8],
        after: Option<&str>,
        count: u32,
    ) -> Result<Vec<StreamEntry>> {
        let start = after.map_or_else(|| "-".to_string(), |id| format!("({id}"));
        let reply = self
            .call(
                "XRANGE",
                vec![
                    bytes_arg(key),
                    Value::from(start),
                    "+".into(),
                    "COUNT".into(),
                    Value::from(count as i64),
                ],
            )
            .await?;
        Ok(reply
            .into_array()
            .into_iter()
            .filter_map(|entry| {
                let mut parts = entry.into_array().into_iter();
                let id = parts.next()?.as_string()?;
                let fields = parts
                    .next()
                    .map(pairs)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(field, value)| (value_bytes(&field), value_bytes(&value)))
                    .collect();
                Some(StreamEntry { id, fields })
            })
            .collect())
    }

    /// Consumer groups of a stream: name, consumers, pending, last delivered id, as the server
    /// reports them; a group whose name is not text also carries `GROUP_NAME_NOT_TEXT`.
    pub async fn stream_groups(&self, key: &[u8]) -> Result<Vec<Vec<(String, String)>>> {
        let reply = self
            .call("XINFO", vec!["GROUPS".into(), bytes_arg(key)])
            .await?;
        Ok(reply
            .into_array()
            .into_iter()
            .map(|group| {
                let mut fields = Vec::new();
                for (name, value) in pairs(group) {
                    let name = name.as_string().unwrap_or_default();
                    // A group name that is not text cannot be named back to Redis from here.
                    if name == "name"
                        && !matches!(&value, Value::String(_))
                        && value
                            .as_bytes()
                            .is_none_or(|bytes| std::str::from_utf8(bytes).is_err())
                    {
                        fields.push((GROUP_NAME_NOT_TEXT.to_string(), "true".to_string()));
                    }
                    let text = value
                        .as_string()
                        .or_else(|| value.as_i64().map(|number| number.to_string()))
                        .unwrap_or_else(|| format!("{value:?}"));
                    fields.push((name, text));
                }
                fields
            })
            .collect())
    }

    /// The consumers of a stream's group (XINFO CONSUMERS).
    pub async fn stream_consumers(&self, key: &[u8], group: &str) -> Result<Vec<StreamConsumer>> {
        let reply = self
            .call(
                "XINFO",
                vec![
                    "CONSUMERS".into(),
                    bytes_arg(key),
                    Value::from(group.to_string()),
                ],
            )
            .await?;
        reply
            .into_array()
            .into_iter()
            .map(|consumer| {
                let fields = pairs(consumer)
                    .into_iter()
                    .map(|(name, value)| (name.as_string().unwrap_or_default(), value))
                    .collect::<Vec<_>>();
                let number = |name: &str| {
                    fields
                        .iter()
                        .find(|(field, _)| field == name)
                        .and_then(|(_, value)| value.as_i64())
                };
                let name = fields
                    .iter()
                    .find(|(field, _)| field == "name")
                    .and_then(|(_, value)| value.as_string());
                let read = || -> Option<StreamConsumer> {
                    Some(StreamConsumer {
                        name: name?,
                        pending: number("pending")?,
                        idle_ms: number("idle")?,
                        // Redis 7.2 and later.
                        inactive_ms: number("inactive"),
                    })
                };
                read().context("XINFO CONSUMERS returned a consumer Vilo cannot read")
            })
            .collect()
    }

    /// The first `count` pending entries of a group (XPENDING … - + count), oldest first.
    pub async fn stream_pending(
        &self,
        key: &[u8],
        group: &str,
        count: u32,
    ) -> Result<Vec<PendingEntry>> {
        let reply = self
            .call(
                "XPENDING",
                vec![
                    bytes_arg(key),
                    Value::from(group.to_string()),
                    "-".into(),
                    "+".into(),
                    Value::from(i64::from(count)),
                ],
            )
            .await?;
        // A reply Vilo cannot read is an error, never fewer pending entries than there are.
        reply
            .into_array()
            .into_iter()
            .map(|entry| {
                let mut parts = entry.into_array().into_iter();
                let mut next = || parts.next();
                let mut read = || -> Option<PendingEntry> {
                    Some(PendingEntry {
                        id: next()?.as_string()?,
                        consumer: next()?.as_string()?,
                        idle_ms: next()?.as_i64()?,
                        deliveries: next()?.as_i64()?,
                    })
                };
                read().context("XPENDING returned an entry Vilo cannot read")
            })
            .collect()
    }

    /// A RedisJSON document as JSON text, when the module is loaded.
    pub async fn json_get(&self, key: &[u8]) -> Result<Option<String>> {
        Ok(self
            .call("JSON.GET", vec![bytes_arg(key)])
            .await?
            .as_string())
    }

    pub async fn info(&self) -> Result<String> {
        Ok(self
            .call("INFO", Vec::new())
            .await?
            .as_string()
            .unwrap_or_default())
    }

    /// Runs a Workbench command. Only reads are allowed in this release; the rest is refused before sending.
    pub async fn run_read_command(&self, words: Vec<Bytes>) -> Result<Value> {
        let (name, args) = words.split_first().context("Type a command")?;
        let name = String::from_utf8_lossy(name).to_ascii_uppercase();
        match classify_words(&words) {
            CommandKind::Read => {}
            CommandKind::Write => bail!(
                "{name} changes data or is not known to Vilo; this release only runs read commands."
            ),
            CommandKind::Dangerous => bail!(
                "{name} is blocked: it can stop the server, wipe data, change configuration or stream every command."
            ),
        }
        let args = args.iter().map(|arg| bytes_arg(arg)).collect::<Vec<_>>();
        let hash = cluster_hash(&name, &args);
        let command = fred::types::CustomCommand::new(name, hash, false);
        Ok(self.client.custom(command, args).await?)
    }

    pub async fn quit(&self) -> Result<()> {
        Ok(self.client.quit().await?)
    }
}

/// One single-key write, as the confirm step shows it and as it is sent. Edits that replace a
/// value carry the value they expect to replace; the server checks it atomically.
#[derive(Debug, Clone, PartialEq)]
pub enum RedisWrite {
    SetString {
        key: Bytes,
        expected: Bytes,
        value: Bytes,
    },
    HashSet {
        key: Bytes,
        field: Bytes,
        expected: Option<Bytes>,
        value: Bytes,
    },
    HashDelete {
        key: Bytes,
        field: Bytes,
        expected: Bytes,
    },
    SetAdd {
        key: Bytes,
        member: Bytes,
    },
    SetRemove {
        key: Bytes,
        member: Bytes,
    },
    ZSetScore {
        key: Bytes,
        member: Bytes,
        expected: Option<String>,
        score: f64,
    },
    ZSetRemove {
        key: Bytes,
        member: Bytes,
    },
    ListPush {
        key: Bytes,
        head: bool,
        value: Bytes,
    },
    ListRemoveFirst {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    Rename {
        key: Bytes,
        to: Bytes,
    },
    Expire {
        key: Bytes,
        seconds: u64,
    },
    Persist {
        key: Bytes,
    },
    /// Creates a key that does not exist yet, with its first values and TTL in one step.
    AddKey {
        key: Bytes,
        value: NewValue,
        ttl_seconds: Option<u64>,
    },
    /// Replaces the list item at `index` only if it still holds `expected`, so a list that
    /// shifted since it was read is not edited in the wrong place.
    ListSet {
        key: Bytes,
        index: i64,
        expected: Bytes,
        value: Bytes,
    },
    /// Removes the list item at `index` only if it still holds `expected`.
    ListRemoveAt {
        key: Bytes,
        index: i64,
        expected: Bytes,
    },
    /// XADD NOMKSTREAM with `id` ("*" lets the server choose); a stream deleted meanwhile is
    /// not created again.
    StreamAdd {
        key: Bytes,
        id: String,
        fields: Vec<(Bytes, Bytes)>,
    },
    StreamDelete {
        key: Bytes,
        id: String,
    },
    /// XGROUP CREATE reading from `start` ("$" new entries only, "0" all of them).
    GroupCreate {
        key: Bytes,
        group: String,
        start: String,
    },
    GroupDestroy {
        key: Bytes,
        group: String,
    },
    /// XACK: the entry is no longer pending for the group. It changes what the group's
    /// consumers will process, and reloading cannot show whether it happened.
    StreamAck {
        key: Bytes,
        group: String,
        id: String,
    },
    /// XCLAIM … JUSTID: gives a pending entry to `consumer`, only if it has been idle for at
    /// least `min_idle_ms`, so an entry another consumer just touched is left alone.
    StreamClaim {
        key: Bytes,
        group: String,
        consumer: String,
        min_idle_ms: u64,
        id: String,
    },
    /// The whole RedisJSON document, only if it is still `expected` (as JSON.GET returned it).
    JsonSet {
        key: Bytes,
        expected: String,
        value: String,
    },
}

/// The first values of a new key.
#[derive(Debug, Clone, PartialEq)]
pub enum NewValue {
    String(Bytes),
    Hash(Vec<(Bytes, Bytes)>),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    SortedSet(Vec<(f64, Bytes)>),
    /// A RedisJSON document, as JSON text.
    Json(String),
}

impl NewValue {
    fn command(&self) -> &'static str {
        match self {
            NewValue::String(_) => "SET",
            NewValue::Hash(_) => "HSET",
            NewValue::List(_) => "RPUSH",
            NewValue::Set(_) => "SADD",
            NewValue::SortedSet(_) => "ZADD",
            NewValue::Json(_) => "JSON.SET",
        }
    }

    fn arguments(&self) -> Vec<Bytes> {
        match self {
            NewValue::String(value) => vec![value.clone()],
            NewValue::Hash(pairs) => pairs
                .iter()
                .flat_map(|(field, value)| [field.clone(), value.clone()])
                .collect(),
            NewValue::List(items) | NewValue::Set(items) => items.clone(),
            NewValue::SortedSet(members) => members
                .iter()
                .flat_map(|(score, member)| [score.to_string().into_bytes(), member.clone()])
                .collect(),
            NewValue::Json(json) => vec![b"$".to_vec(), json.clone().into_bytes()],
        }
    }
}

/// Lua's unpack stops near 8000 values; new keys stay well under it.
pub const NEW_KEY_ARGUMENTS: usize = 4000;
/// A hundred years. Checked before sending, because a TTL Redis refuses would fail after the
/// script already created the key, and scripts do not roll back.
pub const MAX_TTL_SECONDS: u64 = 100 * 365 * 24 * 60 * 60;

/// Replaces the value only if it is still `ARGV[1]`; XX and KEEPTTL keep an expired key gone
/// and its expiry unchanged (Redis 6 and later).
const SET_STRING_GUARDED: &str = "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end \
     redis.call('SET', KEYS[1], ARGV[2], 'XX', 'KEEPTTL') return 1";
/// ARGV: field, expected, has-expected ('1' or '0'), value. Never creates a missing key.
const HASH_SET_GUARDED: &str = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end \
     local v = redis.call('HGET', KEYS[1], ARGV[1]) \
     if ARGV[3] == '1' then if v ~= ARGV[2] then return 0 end elseif v then return 0 end \
     redis.call('HSET', KEYS[1], ARGV[1], ARGV[4]) return 1";
const HASH_DELETE_GUARDED: &str = "if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end \
     return redis.call('HDEL', KEYS[1], ARGV[1])";
/// ARGV: member, expected score, has-expected, new score. Never creates a missing key.
const ZSET_SCORE_GUARDED: &str = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end \
     local s = redis.call('ZSCORE', KEYS[1], ARGV[1]) \
     if ARGV[3] == '1' then if not s or tonumber(s) ~= tonumber(ARGV[2]) then return 0 end \
     elseif s then return 0 end \
     redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1]) return 1";
/// A script that changes data in two steps returns this, having changed nothing, when the user's
/// ACL denies a later step: scripts do not roll back, so every step is checked first (Redis 7).
const NOT_PERMITTED: i64 = -2;
/// ARGV: command, TTL seconds ('0' for none), then the command's arguments. Never touches an
/// existing key.
const ADD_KEY: &str = "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end \
     if redis.acl_check_cmd and (not redis.acl_check_cmd(ARGV[1], KEYS[1], unpack(ARGV, 3)) \
       or (ARGV[2] ~= '0' and not redis.acl_check_cmd('EXPIRE', KEYS[1], ARGV[2]))) then return -2 end \
     redis.call(ARGV[1], KEYS[1], unpack(ARGV, 3)) \
     if ARGV[2] ~= '0' then redis.call('EXPIRE', KEYS[1], ARGV[2]) end return 1";
/// ARGV: index, expected, new value.
const LIST_SET_AT: &str = "if redis.call('LINDEX', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end \
     redis.call('LSET', KEYS[1], ARGV[1], ARGV[3]) return 1";
/// ARGV: index, expected, a marker. The item becomes the marker and LREM removes that one
/// marker, so an equal value elsewhere in the list stays; a list already holding the marker is
/// left alone (LPOS needs Redis 6.0.6).
const LIST_REMOVE_AT: &str = "if redis.call('LINDEX', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end \
     if redis.call('LPOS', KEYS[1], ARGV[3]) then return 0 end \
     if redis.acl_check_cmd and not (redis.acl_check_cmd('LSET', KEYS[1], ARGV[1], ARGV[3]) \
       and redis.acl_check_cmd('LREM', KEYS[1], 1, ARGV[3])) then return -2 end \
     redis.call('LSET', KEYS[1], ARGV[1], ARGV[3]) \
     return redis.call('LREM', KEYS[1], 1, ARGV[3])";
/// ARGV: group, consumer, min idle, id. XCLAIM has side effects even when it claims nothing:
/// it creates the receiving consumer (and refreshes its idle) before checking the entry, and
/// drops an entry deleted from the stream from the pending list (Redis 7). So the script first
/// checks the entry is in the stream (-3 when not) and still pending and idle long enough
/// (0 when not), and only then claims; a 0 then means nothing changed at all.
const STREAM_CLAIM_GUARDED: &str = "if #redis.call('XRANGE', KEYS[1], ARGV[4], ARGV[4]) == 0 then return -3 end \
     local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[4], ARGV[4], 1) \
     if #pending == 0 or tonumber(pending[1][3]) < tonumber(ARGV[3]) then return 0 end \
     return #redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], 'JUSTID')";
const ENTRY_GONE: i64 = -3;
/// ARGV: the document as read, the new document.
const JSON_SET_GUARDED: &str = "if redis.call('JSON.GET', KEYS[1]) ~= ARGV[1] then return 0 end \
     redis.call('JSON.SET', KEYS[1], '$', ARGV[2]) return 1";
const SET_ADD_EXISTING: &str = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end \
     redis.call('SADD', KEYS[1], ARGV[1]) return 1";

impl RedisWrite {
    pub fn key(&self) -> &[u8] {
        match self {
            RedisWrite::SetString { key, .. }
            | RedisWrite::HashSet { key, .. }
            | RedisWrite::HashDelete { key, .. }
            | RedisWrite::SetAdd { key, .. }
            | RedisWrite::SetRemove { key, .. }
            | RedisWrite::ZSetScore { key, .. }
            | RedisWrite::ZSetRemove { key, .. }
            | RedisWrite::ListPush { key, .. }
            | RedisWrite::ListRemoveFirst { key, .. }
            | RedisWrite::Delete { key }
            | RedisWrite::Rename { key, .. }
            | RedisWrite::Expire { key, .. }
            | RedisWrite::Persist { key }
            | RedisWrite::AddKey { key, .. }
            | RedisWrite::ListSet { key, .. }
            | RedisWrite::ListRemoveAt { key, .. }
            | RedisWrite::StreamAdd { key, .. }
            | RedisWrite::StreamDelete { key, .. }
            | RedisWrite::GroupCreate { key, .. }
            | RedisWrite::GroupDestroy { key, .. }
            | RedisWrite::StreamAck { key, .. }
            | RedisWrite::StreamClaim { key, .. }
            | RedisWrite::JsonSet { key, .. } => key,
        }
    }

    /// The command name, for the write log.
    pub fn name(&self) -> &'static str {
        match self {
            RedisWrite::SetString { .. } => "SET",
            RedisWrite::HashSet { .. } => "HSET",
            RedisWrite::HashDelete { .. } => "HDEL",
            RedisWrite::SetAdd { .. } => "SADD",
            RedisWrite::SetRemove { .. } => "SREM",
            RedisWrite::ZSetScore { .. } => "ZADD",
            RedisWrite::ZSetRemove { .. } => "ZREM",
            RedisWrite::ListPush { head: true, .. } => "LPUSHX",
            RedisWrite::ListPush { head: false, .. } => "RPUSHX",
            RedisWrite::ListRemoveFirst { .. } => "LREM",
            RedisWrite::Delete { .. } => "UNLINK",
            RedisWrite::Rename { .. } => "RENAMENX",
            RedisWrite::Expire { .. } => "EXPIRE",
            RedisWrite::Persist { .. } => "PERSIST",
            RedisWrite::AddKey { value, .. } => value.command(),
            RedisWrite::ListSet { .. } => "LSET",
            RedisWrite::ListRemoveAt { .. } => "LREM",
            RedisWrite::StreamAdd { .. } => "XADD",
            RedisWrite::StreamDelete { .. } => "XDEL",
            RedisWrite::GroupCreate { .. } => "XGROUP CREATE",
            RedisWrite::GroupDestroy { .. } => "XGROUP DESTROY",
            RedisWrite::StreamAck { .. } => "XACK",
            RedisWrite::StreamClaim { .. } => "XCLAIM",
            RedisWrite::JsonSet { .. } => "JSON.SET",
        }
    }

    /// The command as the confirm step shows it; values are hidden unless asked for.
    pub fn describe(&self, values: bool) -> String {
        let value = |bytes: &[u8]| {
            if values {
                quote_bytes(bytes)
            } else {
                format!("<{} bytes>", bytes.len())
            }
        };
        let key = quote_bytes(self.key());
        let name = self.name();
        match self {
            RedisWrite::SetString { value: new, .. } => {
                format!(
                    "{name} {key} {} XX KEEPTTL   (only if the value is unchanged)",
                    value(new)
                )
            }
            RedisWrite::HashSet {
                field,
                value: new,
                expected,
                ..
            } => format!(
                "{name} {key} {} {}   ({})",
                quote_bytes(field),
                value(new),
                if expected.is_some() {
                    "only if the field is unchanged"
                } else {
                    "only if the field is new"
                }
            ),
            RedisWrite::HashDelete { field, .. } => {
                format!(
                    "{name} {key} {}   (only if the field is unchanged)",
                    quote_bytes(field)
                )
            }
            RedisWrite::SetAdd { member, .. }
            | RedisWrite::SetRemove { member, .. }
            | RedisWrite::ZSetRemove { member, .. } => format!("{name} {key} {}", value(member)),
            RedisWrite::ZSetScore {
                member,
                expected,
                score,
                ..
            } => format!(
                "{name} {key} {score} {}   ({})",
                value(member),
                match expected {
                    Some(old) => format!("score {old} → {score}, only if unchanged"),
                    None => "only if the member is new".to_string(),
                }
            ),
            RedisWrite::ListPush { value: new, .. } => format!("{name} {key} {}", value(new)),
            RedisWrite::ListRemoveFirst { value: old, .. } => {
                format!(
                    "{name} {key} 1 {}   (removes the first equal value)",
                    value(old)
                )
            }
            RedisWrite::Delete { .. } | RedisWrite::Persist { .. } => format!("{name} {key}"),
            RedisWrite::Rename { to, .. } => {
                format!(
                    "{name} {key} {}   (refused if the new name exists)",
                    quote_bytes(to)
                )
            }
            RedisWrite::Expire { seconds, .. } => format!("{name} {key} {seconds}"),
            RedisWrite::AddKey {
                value: new,
                ttl_seconds,
                ..
            } => {
                let arguments = new.arguments();
                let shown = if values {
                    arguments
                        .iter()
                        .map(|argument| quote_bytes(argument))
                        .collect::<Vec<_>>()
                        .join(" ")
                } else {
                    format!("<{} values>", arguments.len())
                };
                let ttl = ttl_seconds
                    .map(|seconds| format!(" · EXPIRE {key} {seconds}"))
                    .unwrap_or_default();
                format!("{name} {key} {shown}{ttl}   (only if the key does not exist)")
            }
            RedisWrite::ListSet {
                index, value: new, ..
            } => format!(
                "{name} {key} {index} {}   (only if item {index} is unchanged)",
                value(new)
            ),
            RedisWrite::ListRemoveAt { index, .. } => {
                format!("{name} {key} item {index}   (only if item {index} is unchanged)")
            }
            RedisWrite::StreamAdd { id, fields, .. } => format!(
                "{name} {key} NOMKSTREAM {id} {}   (not if the stream is gone)",
                fields
                    .iter()
                    .map(|(field, new)| format!("{} {}", quote_bytes(field), value(new)))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            RedisWrite::StreamDelete { id, .. } => format!("{name} {key} {id}"),
            RedisWrite::GroupCreate { group, start, .. } => {
                format!("{name} {key} {} {start}", quote_bytes(group.as_bytes()))
            }
            RedisWrite::GroupDestroy { group, .. } => format!(
                "{name} {key} {}   (its consumers and pending entries go with it)",
                quote_bytes(group.as_bytes())
            ),
            RedisWrite::StreamAck { group, id, .. } => format!(
                "{name} {key} {} {id}   (the group stops treating this entry as pending; its consumers will not get it again)",
                quote_bytes(group.as_bytes())
            ),
            RedisWrite::StreamClaim {
                group,
                consumer,
                min_idle_ms,
                id,
                ..
            } => format!(
                "{name} {key} {} {} {min_idle_ms} {id} JUSTID   (only if it is still idle for {min_idle_ms} ms or more)",
                quote_bytes(group.as_bytes()),
                quote_bytes(consumer.as_bytes())
            ),
            RedisWrite::JsonSet { value: new, .. } => format!(
                "{name} {key} $ {}   (only if the document is unchanged)",
                value(new.as_bytes())
            ),
        }
    }
}

pub(crate) fn classify_redis(error: fred::error::Error) -> crate::mongo::WriteError {
    use fred::error::ErrorKind;
    let message = error.details().to_string();
    match error.kind() {
        // The command may have reached the server without an answer coming back.
        ErrorKind::IO | ErrorKind::Timeout | ErrorKind::Canceled | ErrorKind::Protocol => {
            crate::mongo::WriteError::Unknown(message)
        }
        _ => crate::mongo::WriteError::Refused(message),
    }
}

impl RedisSession {
    async fn eval(
        &self,
        script: &str,
        key: &[u8],
        args: Vec<Value>,
    ) -> Result<Value, fred::error::Error> {
        let mut all = vec![Value::from(script), Value::from(1_i64), bytes_arg(key)];
        all.extend(args);
        // Routed by the key's slot, not by the script text that comes first.
        let command =
            fred::types::CustomCommand::new("EVAL", fred::types::ClusterHash::from(key), false);
        self.client.custom(command, all).await
    }

    /// Sends one write once. `Ok(false)` means nothing changed: a guard found a different value,
    /// the key is gone, or (for rename) the new name is taken.
    pub async fn write(&self, write: &RedisWrite) -> Result<bool, crate::mongo::WriteError> {
        let flag = |present: bool| Value::from(if present { "1" } else { "0" });
        // These change data in two steps; an error between them may leave the first applied.
        let two_steps = matches!(
            write,
            RedisWrite::AddKey { .. } | RedisWrite::ListRemoveAt { .. }
        );
        let reply = match write {
            RedisWrite::SetString {
                key,
                expected,
                value,
            } => {
                self.eval(
                    SET_STRING_GUARDED,
                    key,
                    vec![bytes_arg(expected), bytes_arg(value)],
                )
                .await
            }
            RedisWrite::HashSet {
                key,
                field,
                expected,
                value,
            } => {
                self.eval(
                    HASH_SET_GUARDED,
                    key,
                    vec![
                        bytes_arg(field),
                        bytes_arg(expected.as_deref().unwrap_or_default()),
                        flag(expected.is_some()),
                        bytes_arg(value),
                    ],
                )
                .await
            }
            RedisWrite::HashDelete {
                key,
                field,
                expected,
            } => {
                self.eval(
                    HASH_DELETE_GUARDED,
                    key,
                    vec![bytes_arg(field), bytes_arg(expected)],
                )
                .await
            }
            RedisWrite::SetAdd { key, member } => {
                self.eval(SET_ADD_EXISTING, key, vec![bytes_arg(member)])
                    .await
            }
            RedisWrite::SetRemove { key, member } => {
                self.client
                    .custom(cmd!("SREM"), vec![bytes_arg(key), bytes_arg(member)])
                    .await
            }
            RedisWrite::ZSetScore {
                key,
                member,
                expected,
                score,
            } => {
                self.eval(
                    ZSET_SCORE_GUARDED,
                    key,
                    vec![
                        bytes_arg(member),
                        Value::from(expected.clone().unwrap_or_default()),
                        flag(expected.is_some()),
                        Value::from(score.to_string()),
                    ],
                )
                .await
            }
            RedisWrite::ZSetRemove { key, member } => {
                self.client
                    .custom(cmd!("ZREM"), vec![bytes_arg(key), bytes_arg(member)])
                    .await
            }
            RedisWrite::ListPush { key, head, value } => {
                let name = if *head { "LPUSHX" } else { "RPUSHX" };
                self.client
                    .custom(cmd!(name), vec![bytes_arg(key), bytes_arg(value)])
                    .await
            }
            RedisWrite::ListRemoveFirst { key, value } => {
                self.client
                    .custom(
                        cmd!("LREM"),
                        vec![bytes_arg(key), Value::from(1_i64), bytes_arg(value)],
                    )
                    .await
            }
            RedisWrite::Delete { key } => {
                self.client
                    .custom(cmd!("UNLINK"), vec![bytes_arg(key)])
                    .await
            }
            RedisWrite::Rename { key, to } => {
                self.client
                    .custom(cmd!("RENAMENX"), vec![bytes_arg(key), bytes_arg(to)])
                    .await
            }
            RedisWrite::Expire { key, seconds } => {
                if *seconds == 0 {
                    return Err(crate::mongo::WriteError::Refused(
                        "A time to live of 0 deletes the key; use Delete instead.".into(),
                    ));
                }
                self.client
                    .custom(
                        cmd!("EXPIRE"),
                        vec![bytes_arg(key), Value::from(*seconds as i64)],
                    )
                    .await
            }
            RedisWrite::Persist { key } => {
                self.client
                    .custom(cmd!("PERSIST"), vec![bytes_arg(key)])
                    .await
            }
            RedisWrite::AddKey {
                key,
                value,
                ttl_seconds,
            } => {
                let arguments = value.arguments();
                if arguments.is_empty() || arguments.len() > NEW_KEY_ARGUMENTS {
                    return Err(crate::mongo::WriteError::Refused(format!(
                        "A new key takes 1 to {NEW_KEY_ARGUMENTS} values."
                    )));
                }
                if ttl_seconds.is_some_and(|seconds| !(1..=MAX_TTL_SECONDS).contains(&seconds)) {
                    return Err(crate::mongo::WriteError::Refused(format!(
                        "The TTL must be 1 to {MAX_TTL_SECONDS} seconds."
                    )));
                }
                let mut all = vec![
                    Value::from(value.command()),
                    Value::from(ttl_seconds.unwrap_or(0).to_string()),
                ];
                all.extend(arguments.iter().map(|argument| bytes_arg(argument)));
                self.eval(ADD_KEY, key, all).await
            }
            RedisWrite::ListSet {
                key,
                index,
                expected,
                value,
            } => {
                self.eval(
                    LIST_SET_AT,
                    key,
                    vec![Value::from(*index), bytes_arg(expected), bytes_arg(value)],
                )
                .await
            }
            RedisWrite::ListRemoveAt {
                key,
                index,
                expected,
            } => {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_nanos());
                let marker = format!("\0vilo-remove-{}-{nanos}", std::process::id());
                self.eval(
                    LIST_REMOVE_AT,
                    key,
                    vec![
                        Value::from(*index),
                        bytes_arg(expected),
                        Value::from(marker),
                    ],
                )
                .await
            }
            RedisWrite::StreamAdd { key, id, fields } => {
                if fields.is_empty() {
                    return Err(crate::mongo::WriteError::Refused(
                        "An entry needs at least one field.".into(),
                    ));
                }
                let mut args = vec![bytes_arg(key), "NOMKSTREAM".into(), Value::from(id.clone())];
                for (field, value) in fields {
                    args.push(bytes_arg(field));
                    args.push(bytes_arg(value));
                }
                self.client.custom(cmd!("XADD"), args).await
            }
            RedisWrite::StreamDelete { key, id } => {
                self.client
                    .custom(cmd!("XDEL"), vec![bytes_arg(key), Value::from(id.clone())])
                    .await
            }
            RedisWrite::GroupCreate { key, group, start } => {
                self.keyed(
                    "XGROUP",
                    vec![
                        "CREATE".into(),
                        bytes_arg(key),
                        Value::from(group.clone()),
                        Value::from(start.clone()),
                    ],
                )
                .await
            }
            RedisWrite::GroupDestroy { key, group } => {
                self.keyed(
                    "XGROUP",
                    vec!["DESTROY".into(), bytes_arg(key), Value::from(group.clone())],
                )
                .await
            }
            RedisWrite::StreamAck { key, group, id } => {
                self.client
                    .custom(
                        cmd!("XACK"),
                        vec![
                            bytes_arg(key),
                            Value::from(group.clone()),
                            Value::from(id.clone()),
                        ],
                    )
                    .await
            }
            RedisWrite::StreamClaim {
                key,
                group,
                consumer,
                min_idle_ms,
                id,
            } => {
                self.eval(
                    STREAM_CLAIM_GUARDED,
                    key,
                    vec![
                        Value::from(group.clone()),
                        Value::from(consumer.clone()),
                        Value::from(*min_idle_ms as i64),
                        Value::from(id.clone()),
                    ],
                )
                .await
            }
            RedisWrite::JsonSet {
                key,
                expected,
                value,
            } => {
                if serde_json::from_str::<serde_json::Value>(value).is_err() {
                    return Err(crate::mongo::WriteError::Refused(
                        "The new document is not valid JSON.".into(),
                    ));
                }
                self.eval(
                    JSON_SET_GUARDED,
                    key,
                    vec![Value::from(expected.clone()), Value::from(value.clone())],
                )
                .await
            }
        }
        .map_err(|error| match classify_redis(error) {
            crate::mongo::WriteError::Refused(message) if two_steps => {
                crate::mongo::WriteError::Unknown(format!(
                    "{message} (part of it may have been applied)"
                ))
            }
            other => other,
        })?;
        match (write, reply) {
            // XADD answers the new id (nil when the stream is gone) and XGROUP CREATE "OK".
            (RedisWrite::StreamAdd { .. } | RedisWrite::GroupCreate { .. }, reply) => {
                Ok(!matches!(reply, Value::Null))
            }
            (RedisWrite::StreamClaim { .. }, reply) if reply.as_i64() == Some(ENTRY_GONE) => {
                Err(crate::mongo::WriteError::Refused(
                    "The entry was deleted from the stream; acknowledge it to clear it from the pending list. Nothing was claimed."
                        .into(),
                ))
            }
            (_, reply) => match reply.as_i64() {
                Some(NOT_PERMITTED) => Err(crate::mongo::WriteError::Refused(
                    "This connection's ACL does not allow every step of this write; nothing was changed."
                        .into(),
                )),
                reply => Ok(reply.unwrap_or(0) > 0),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_certificates_are_checked_against_the_url_host() {
        assert_eq!(
            url_host("redis-cluster://trusted.example:6379?node=other.example:6379").unwrap(),
            "trusted.example"
        );
        assert_eq!(url_host("redis://[::1]:6379").unwrap(), "::1");
        assert_eq!(url_host("redis://user:pw@h:1/2").unwrap(), "h");
        assert!(url_host("redis://").is_err());
    }

    #[test]
    fn test_cluster_hash_uses_the_key() {
        use fred::types::ClusterHash;
        let args = |words: &[&str]| {
            words
                .iter()
                .map(|word| bytes_arg(word.as_bytes()))
                .collect::<Vec<_>>()
        };
        let slot = |hash: ClusterHash| match hash {
            ClusterHash::Custom(slot) => Some(slot),
            _ => None,
        };
        let key = ClusterHash::from("user:1".as_bytes());
        assert_eq!(
            slot(cluster_hash("MEMORY", &args(&["USAGE", "user:1"]))),
            slot(key)
        );
        assert_eq!(
            slot(cluster_hash("xinfo", &args(&["GROUPS", "user:1"]))),
            slot(ClusterHash::from("user:1".as_bytes()))
        );
        assert!(
            matches!(
                cluster_hash("MEMORY", &args(&["STATS"])),
                ClusterHash::FirstKey
            ),
            "no key"
        );
        assert!(matches!(
            cluster_hash("GET", &args(&["user:1"])),
            ClusterHash::FirstKey
        ));
    }

    #[test]
    fn test_commands_are_split_and_classified() {
        assert_eq!(
            split_command(r#"GET "user:\x00\xff" 'a b'"#).unwrap(),
            vec![b"GET".to_vec(), b"user:\x00\xff".to_vec(), b"a b".to_vec()]
        );
        assert_eq!(
            split_command("  HGETALL   h  ").unwrap(),
            vec![b"HGETALL".to_vec(), b"h".to_vec()]
        );
        assert!(split_command("GET \"open").is_err());
        assert_eq!(classify("get"), CommandKind::Read);
        assert_eq!(classify("SET"), CommandKind::Write);
        assert_eq!(classify("FLUSHALL"), CommandKind::Dangerous);
        assert_eq!(classify("KEYS"), CommandKind::Dangerous);
        assert_eq!(classify("SOMETHING.NEW"), CommandKind::Write);
        let words = |line: &str| split_command(line).unwrap();
        assert_eq!(classify_words(&words("CLUSTER INFO")), CommandKind::Read);
        assert_eq!(
            classify_words(&words("cluster reset hard")),
            CommandKind::Dangerous
        );
        assert_eq!(
            classify_words(&words("SLOWLOG RESET")),
            CommandKind::Dangerous
        );
        assert_eq!(classify_words(&words("SLOWLOG GET 10")), CommandKind::Read);
        assert_eq!(
            classify_words(&words("MEMORY PURGE")),
            CommandKind::Dangerous
        );
        assert_eq!(classify_words(&words("XINFO GROUPS s")), CommandKind::Read);
        assert_eq!(quote_bytes(b"a\"\x00"), "\"a\\\"\\x00\"");
        let reply = Value::Array(vec![Value::from("one"), Value::Integer(2), Value::Null]);
        assert_eq!(
            format_reply(&reply),
            vec!["1) \"one\"", "2) (integer) 2", "3) (nil)"]
        );
        assert_eq!(
            format_reply(&Value::Array(Vec::new())),
            vec!["(empty array)"]
        );
    }

    /// Runs against a real server: `VILO_TEST_REDIS=redis://127.0.0.1:57379 cargo test -p vilo_database_core`.
    #[test]
    fn test_parse_cluster_nodes() {
        // Real output from a local 3 primary + 3 replica cluster, plus a failing node and an
        // importing slot as Redis documents them.
        let text = "247790d65b6faa09daea9d2242fd0d25e05174f3 127.0.0.1:47102@57102 master - 0 1793371854000 3 connected 10923-16383\n\
7798d999ab1e64a74b816bc3046bfe9ea89fd255 127.0.0.1:47103@57103 slave d0a1d0332440fa9054fca14876d4e4abdd690162 0 1793371855547 2 connected\n\
40a7385ee006410f5bfd54906534a2d858f3e43f 127.0.0.1:47100@57100,node-a myself,master - 0 0 1 connected 0-5460 [5461->-d0a1d0332440fa9054fca14876d4e4abdd690162]\n\
aaaa 10.0.0.9:6379@16379 master,fail - 1 2 4 disconnected 7000 9000-8000 0-4294967295\n\
broken line";
        let nodes = parse_cluster_nodes(text);
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].slot_count, 16384 - 10923);
        assert!(nodes[0].primary && nodes[0].problems.is_empty());
        assert_eq!(
            nodes[1].primary_id.as_deref(),
            Some("d0a1d0332440fa9054fca14876d4e4abdd690162")
        );
        assert!(!nodes[1].primary);
        assert_eq!(nodes[2].address, "127.0.0.1:47100");
        assert_eq!(nodes[2].slots, vec!["0-5460"]);
        assert_eq!(nodes[2].slot_count, 5461);
        assert_eq!(
            nodes[3].problems,
            vec![
                "fail",
                "bad slot range 9000-8000",
                "bad slot range 0-4294967295"
            ]
        );
        assert_eq!(nodes[3].slots, vec!["7000"]);
        assert!(!nodes[3].connected);
        assert_eq!(nodes[3].slot_count, 1);
    }

    #[tokio::test]
    async fn test_cluster_against_a_server() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let _cluster = crate::CLUSTER_TESTS.lock().await;
        let session = RedisSession::connect(&url).await.unwrap();
        assert!(session.is_clustered());
        let nodes = session.cluster_nodes().await.unwrap().unwrap();
        assert_eq!(nodes.iter().filter(|node| node.primary).count(), 3);
        assert_eq!(nodes.iter().filter(|node| !node.primary).count(), 3);
        assert_eq!(nodes.iter().map(|node| node.slot_count).sum::<u32>(), 16384);
        assert_eq!(session.scan_nodes().len(), 3, "SCAN walks every primary");
        assert!(
            RedisSession::connect_to_database(&url, Some(1))
                .await
                .is_err()
        );
        assert_eq!(session.database_count().await.unwrap_or(1), 1);
        // Every primary answers SLOWLOG and CLIENT LIST, tagged with its node.
        let slow = session.slowlog(5).await;
        assert!(slow.failures.is_empty(), "{:?}", slow.failures);
        let clients = session.clients().await;
        assert!(clients.failures.is_empty());
        let nodes = clients
            .items
            .iter()
            .map(|client| client.node.clone())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(nodes.len(), 3, "one CLIENT LIST per primary");
        assert_eq!(session.slowlog_config().await.items.len(), 3);
    }

    #[test]
    fn test_parse_slowlog_skips_malformed_entries() {
        let entry = |id: i64, args: Vec<Value>| {
            Value::Array(vec![
                Value::from(id),
                Value::from(1_793_000_000_i64),
                Value::from(1500_i64),
                Value::Array(args),
                Value::from("127.0.0.1:1"),
                Value::from("worker"),
            ])
        };
        let reply = Value::Array(vec![
            entry(7, vec![Value::from("GET"), Value::from("k")]),
            Value::Array(vec![Value::from("not an id")]),
            entry(6, vec![Value::from("PING")]),
        ]);
        assert_eq!(reply.array_len(), Some(3));
        let entries = parse_slowlog("n", reply);
        assert_eq!(
            entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            [7, 6]
        );
        assert_eq!(entries[0].args, vec![b"GET".to_vec(), b"k".to_vec()]);
        assert_eq!(
            (entries[0].client.as_str(), entries[0].client_name.as_str()),
            ("127.0.0.1:1", "worker")
        );
    }

    #[test]
    fn test_parse_client_list() {
        let text = "id=3 addr=127.0.0.1:52555 laddr=127.0.0.1:6379 fd=8 name=worker age=12 idle=0 flags=N db=2 cmd=client|list user=default\nid=4 addr=10.0.0.1:1 name= age=1 idle=9 flags=P db=0 cmd=subscribe user=app\n";
        let clients = parse_client_list("server", text);
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].get("name"), "worker");
        assert_eq!(clients[0].get("cmd"), "client|list");
        assert_eq!(clients[1].get("name"), "");
        assert_eq!(clients[1].get("missing"), "");
        assert_eq!(clients[1].node, "server");
    }

    async fn slowlog_and_clients(url: &str) {
        let session = RedisSession::connect(url).await.unwrap();
        let node = session.scan_nodes().remove(0);
        // Logs every command for the duration of the test, then restores the server's setting.
        let before = session.slowlog_config().await;
        assert!(before.failures.is_empty(), "{:?}", before.failures);
        let threshold = before.items[0].1.unwrap();
        session
            .admin_write(&node, &AdminWrite::SlowlogReset)
            .await
            .unwrap();
        session
            .admin_write(&node, &AdminWrite::SlowlogThreshold { micros: 0 })
            .await
            .unwrap();
        session
            .call("GET", vec!["vilo_test:slow".into()])
            .await
            .unwrap();
        let report = session.slowlog(10).await;
        assert!(report.failures.is_empty());
        assert!(
            report
                .items
                .iter()
                .any(|entry| entry.args.first().map(Vec::as_slice) == Some(b"GET")),
            "{:?}",
            report.items
        );
        assert!(
            report
                .items
                .windows(2)
                .all(|pair| pair[0].time >= pair[1].time)
        );
        session
            .admin_write(&node, &AdminWrite::SlowlogThreshold { micros: threshold })
            .await
            .unwrap();
        assert_eq!(session.slowlog_config().await.items[0].1, Some(threshold));
        session
            .admin_write(&node, &AdminWrite::SlowlogReset)
            .await
            .unwrap();
        let clients = session.clients().await;
        assert!(clients.failures.is_empty());
        assert!(
            clients
                .items
                .iter()
                .any(|client| client.get("cmd").starts_with("client"))
        );
    }

    #[test]
    fn test_parse_keyspace() {
        let info = "# Keyspace\r\ndb0:keys=3331,expires=12,avg_ttl=0\r\ndb5:keys=7,expires=0,avg_ttl=0\r\n";
        assert_eq!(parse_keyspace(info), vec![(0, 3331), (5, 7)]);
        assert!(parse_keyspace("# Keyspace\r\n").is_empty());
    }

    async fn databases_are_separate(url: &str) {
        let first = RedisSession::connect_to_database(url, Some(1))
            .await
            .unwrap();
        assert_eq!(first.database(), 1);
        first
            .call(
                "SET",
                vec![
                    "vilo_test:db".into(),
                    "one".into(),
                    "PX".into(),
                    Value::from(60_000_i64),
                ],
            )
            .await
            .unwrap();
        let zero = RedisSession::connect_to_database(url, Some(0))
            .await
            .unwrap();
        assert_eq!(zero.get_string(b"vilo_test:db").await.unwrap(), None);
        assert_eq!(
            first.get_string(b"vilo_test:db").await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(zero.database_count().await, Some(16));
        assert!(
            first
                .keyspace()
                .await
                .unwrap()
                .iter()
                .any(|(database, keys)| *database == 1 && *keys >= 1)
        );
        first
            .call("DEL", vec!["vilo_test:db".into()])
            .await
            .unwrap();
    }

    async fn streams_and_groups(session: &RedisSession) {
        let key = b"vilo_test:stream".to_vec();
        session.call("DEL", vec![bytes_arg(&key)]).await.unwrap();
        let write = |write: RedisWrite| async move { session.write(&write).await };
        let add = |id: &str| RedisWrite::StreamAdd {
            key: key.clone(),
            id: id.to_string(),
            fields: vec![(b"f".to_vec(), b"v".to_vec())],
        };
        assert!(
            !write(add("*")).await.unwrap(),
            "NOMKSTREAM: a missing stream is not created"
        );
        session
            .call(
                "XADD",
                vec![bytes_arg(&key), "1-1".into(), "f".into(), "a".into()],
            )
            .await
            .unwrap();
        assert!(write(add("2-1")).await.unwrap());
        assert!(write(add("*")).await.unwrap());
        let group = |start: &str| RedisWrite::GroupCreate {
            key: key.clone(),
            group: "g".into(),
            start: start.into(),
        };
        assert!(write(group("0")).await.unwrap());
        assert!(
            matches!(
                write(group("0")).await,
                Err(crate::mongo::WriteError::Refused(_))
            ),
            "a group that exists is refused"
        );
        // Two entries delivered to consumer c1 and not acknowledged become pending.
        session
            .call(
                "XREADGROUP",
                vec![
                    "GROUP".into(),
                    "g".into(),
                    "c1".into(),
                    "COUNT".into(),
                    2.into(),
                    "STREAMS".into(),
                    bytes_arg(&key),
                    ">".into(),
                ],
            )
            .await
            .unwrap();
        let pending = session.stream_pending(&key, "g", 100).await.unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["1-1", "2-1"]
        );
        assert_eq!(
            (pending[0].consumer.as_str(), pending[0].deliveries),
            ("c1", 1)
        );
        let consumers = session.stream_consumers(&key, "g").await.unwrap();
        assert_eq!(
            (consumers[0].name.as_str(), consumers[0].pending),
            ("c1", 2)
        );

        let ack = |id: &str| RedisWrite::StreamAck {
            key: key.clone(),
            group: "g".into(),
            id: id.into(),
        };
        assert!(write(ack("1-1")).await.unwrap());
        assert!(!write(ack("1-1")).await.unwrap(), "already acknowledged");
        let claim = |min_idle_ms| RedisWrite::StreamClaim {
            key: key.clone(),
            group: "g".into(),
            consumer: "c2".into(),
            min_idle_ms,
            id: "2-1".into(),
        };
        assert!(
            !write(claim(3_600_000)).await.unwrap(),
            "not idle long enough: left with its consumer"
        );
        assert!(
            session
                .stream_consumers(&key, "g")
                .await
                .unwrap()
                .iter()
                .all(|consumer| consumer.name != "c2"),
            "a refused claim does not create its receiving consumer"
        );
        assert!(write(claim(0)).await.unwrap());
        // On a stream of its own: an entry deleted from the stream is refused, and stays pending.
        let other = b"vilo_test:stream_gone".to_vec();
        session.call("DEL", vec![bytes_arg(&other)]).await.unwrap();
        let call = |name: &'static str, args: Vec<Value>| session.call(name, args);
        call(
            "XADD",
            vec![bytes_arg(&other), "1-1".into(), "f".into(), "c".into()],
        )
        .await
        .unwrap();
        call(
            "XGROUP",
            vec!["CREATE".into(), bytes_arg(&other), "g".into(), "0".into()],
        )
        .await
        .unwrap();
        call(
            "XREADGROUP",
            vec![
                "GROUP".into(),
                "g".into(),
                "c1".into(),
                "STREAMS".into(),
                bytes_arg(&other),
                ">".into(),
            ],
        )
        .await
        .unwrap();
        call("XDEL", vec![bytes_arg(&other), "1-1".into()])
            .await
            .unwrap();
        let gone = RedisWrite::StreamClaim {
            key: other.clone(),
            group: "g".into(),
            consumer: "c2".into(),
            min_idle_ms: 0,
            id: "1-1".into(),
        };
        assert!(matches!(
            write(gone).await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert_eq!(
            session
                .stream_pending(&other, "g", 100)
                .await
                .unwrap()
                .len(),
            1,
            "a refused claim leaves the pending list alone"
        );
        session.call("DEL", vec![bytes_arg(&other)]).await.unwrap();
        let pending = session.stream_pending(&key, "g", 100).await.unwrap();
        assert_eq!((pending.len(), pending[0].consumer.as_str()), (1, "c2"));

        assert!(
            write(RedisWrite::StreamDelete {
                key: key.clone(),
                id: "1-1".into()
            })
            .await
            .unwrap()
        );
        assert!(
            !write(RedisWrite::StreamDelete {
                key: key.clone(),
                id: "1-1".into()
            })
            .await
            .unwrap()
        );
        assert!(
            write(RedisWrite::GroupDestroy {
                key: key.clone(),
                group: "g".into()
            })
            .await
            .unwrap()
        );
        assert!(session.stream_groups(&key).await.unwrap().is_empty());
        session
            .call(
                "XGROUP",
                vec![
                    "CREATE".into(),
                    bytes_arg(&key),
                    bytes_arg(b"\xff"),
                    "0".into(),
                ],
            )
            .await
            .unwrap();
        let groups = session.stream_groups(&key).await.unwrap();
        assert!(
            groups[0]
                .iter()
                .any(|(field, _)| field == GROUP_NAME_NOT_TEXT),
            "a group name that is not text is marked"
        );
        session
            .call(
                "XGROUP",
                vec!["CREATE".into(), bytes_arg(&key), "text".into(), "0".into()],
            )
            .await
            .unwrap();
        let groups = session.stream_groups(&key).await.unwrap();
        assert_eq!(
            groups
                .iter()
                .filter(|group| group.iter().any(|(field, _)| field == GROUP_NAME_NOT_TEXT))
                .count(),
            1,
            "a text name is not marked"
        );
        session.call("DEL", vec![bytes_arg(&key)]).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_against_redis_stack() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_STACK") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let key = b"vilo_test:json".to_vec();
        session.call("DEL", vec![bytes_arg(&key)]).await.unwrap();
        let add = RedisWrite::AddKey {
            key: key.clone(),
            value: NewValue::Json(r#"{"name":"Ann","tags":[1,2]}"#.into()),
            ttl_seconds: Some(60),
        };
        assert!(session.write(&add).await.unwrap());
        assert!(
            !session.write(&add).await.unwrap(),
            "an existing key is left alone"
        );
        assert_eq!(session.key_info(&key).await.unwrap().kind, "ReJSON-RL");
        let shown = session.json_get(&key).await.unwrap().unwrap();
        let set = |expected: &str, value: &str| RedisWrite::JsonSet {
            key: key.clone(),
            expected: expected.into(),
            value: value.into(),
        };
        assert!(
            session
                .write(&set(&shown, r#"{"name":"Bo"}"#))
                .await
                .unwrap()
        );
        assert!(
            !session
                .write(&set(&shown, r#"{"name":"Cy"}"#))
                .await
                .unwrap(),
            "the document changed since it was read"
        );
        assert!(matches!(
            session.write(&set(r#"{"name":"Bo"}"#, "{not json")).await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        let now: serde_json::Value =
            serde_json::from_str(&session.json_get(&key).await.unwrap().unwrap()).unwrap();
        assert_eq!(now, serde_json::json!({ "name": "Bo" }));
        assert!(
            session.key_info(&key).await.unwrap().ttl_ms > 0,
            "JSON.SET keeps the TTL"
        );
        session.call("DEL", vec![bytes_arg(&key)]).await.unwrap();
    }

    async fn new_keys_and_list_rows(session: &RedisSession) {
        let names = [
            "vilo_test:new_s",
            "vilo_test:new_h",
            "vilo_test:new_z",
            "vilo_test:rows",
        ];
        session
            .call("DEL", names.iter().map(|name| Value::from(*name)).collect())
            .await
            .unwrap();
        let add = |key: &str, value: NewValue, ttl_seconds| RedisWrite::AddKey {
            key: key.as_bytes().to_vec(),
            value,
            ttl_seconds,
        };
        let hash = NewValue::Hash(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"".to_vec()),
        ]);
        assert!(
            session
                .write(&add(names[1], hash.clone(), Some(60)))
                .await
                .unwrap()
        );
        assert!(
            !session.write(&add(names[1], hash, None)).await.unwrap(),
            "an existing key is left alone"
        );
        let info = session.key_info(names[1].as_bytes()).await.unwrap();
        assert_eq!(info.kind, "hash");
        assert!(info.ttl_ms > 0 && info.ttl_ms <= 60_000);
        assert_eq!(
            session.hash_get(names[1].as_bytes(), b"b").await.unwrap(),
            Some(Vec::new())
        );
        assert!(
            session
                .write(&add(names[0], NewValue::String(b"x".to_vec()), None))
                .await
                .unwrap()
        );
        assert_eq!(
            session.key_info(names[0].as_bytes()).await.unwrap().ttl_ms,
            -1
        );
        let zset = NewValue::SortedSet(vec![(1.5, b"m".to_vec()), (f64::INFINITY, b"n".to_vec())]);
        assert!(session.write(&add(names[2], zset, None)).await.unwrap());
        assert_eq!(
            session.zset_score(names[2].as_bytes(), b"m").await.unwrap(),
            Some("1.5".to_string())
        );
        assert!(matches!(
            session
                .write(&add(names[3], NewValue::List(Vec::new()), None))
                .await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert!(matches!(
            session
                .write(&add(
                    names[3],
                    NewValue::List(vec![b"a".to_vec()]),
                    Some(u64::MAX)
                ))
                .await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert_eq!(
            session.key_info(names[3].as_bytes()).await.unwrap().kind,
            "none",
            "a refused TTL creates nothing"
        );

        let items = |values: &[&str]| {
            values
                .iter()
                .map(|value| value.as_bytes().to_vec())
                .collect::<Vec<_>>()
        };
        assert!(
            session
                .write(&add(
                    names[3],
                    NewValue::List(items(&["a", "b", "a"])),
                    None
                ))
                .await
                .unwrap()
        );
        let key = names[3].as_bytes().to_vec();
        let set = |index, expected: &str, value: &str| RedisWrite::ListSet {
            key: key.clone(),
            index,
            expected: expected.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        };
        assert!(session.write(&set(1, "b", "c")).await.unwrap());
        assert!(
            !session.write(&set(1, "b", "d")).await.unwrap(),
            "the item changed"
        );
        assert!(
            !session.write(&set(9, "a", "d")).await.unwrap(),
            "no such item"
        );
        let remove = |index, expected: &str| RedisWrite::ListRemoveAt {
            key: key.clone(),
            index,
            expected: expected.as_bytes().to_vec(),
        };
        assert!(!session.write(&remove(1, "a")).await.unwrap());
        // The second "a" goes; the first, equal one stays.
        assert!(session.write(&remove(2, "a")).await.unwrap());
        assert_eq!(
            session.list_range(&key, 0, -1).await.unwrap(),
            items(&["a", "c"])
        );
        session
            .call("DEL", names.iter().map(|name| Value::from(*name)).collect())
            .await
            .unwrap();
    }

    async fn steps_denied_by_acl_change_nothing(admin: &RedisSession, url: &str) {
        let user = "vilo_test_limited";
        admin
            .call(
                "ACL",
                [
                    "SETUSER",
                    user,
                    "reset",
                    "on",
                    ">vilo-test",
                    "~vilo_test:*",
                    "+@all",
                    "-expire",
                    "-lrem",
                ]
                .into_iter()
                .map(Value::from)
                .collect(),
            )
            .await
            .unwrap();
        admin
            .call(
                "DEL",
                vec!["vilo_test:acl_new".into(), "vilo_test:acl_list".into()],
            )
            .await
            .unwrap();
        let limited = RedisSession::connect(&url.replacen(
            "redis://",
            &format!("redis://{user}:vilo-test@"),
            1,
        ))
        .await
        .unwrap();
        let add = RedisWrite::AddKey {
            key: b"vilo_test:acl_new".to_vec(),
            value: NewValue::Set(vec![b"m".to_vec()]),
            ttl_seconds: Some(60),
        };
        assert!(matches!(
            limited.write(&add).await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert_eq!(
            admin.key_info(b"vilo_test:acl_new").await.unwrap().kind,
            "none",
            "denied EXPIRE: the key is not created either"
        );
        limited
            .call(
                "RPUSH",
                vec!["vilo_test:acl_list".into(), "a".into(), "b".into()],
            )
            .await
            .unwrap();
        let remove = RedisWrite::ListRemoveAt {
            key: b"vilo_test:acl_list".to_vec(),
            index: 0,
            expected: b"a".to_vec(),
        };
        assert!(matches!(
            limited.write(&remove).await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert_eq!(
            admin
                .list_range(b"vilo_test:acl_list", 0, -1)
                .await
                .unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()],
            "denied LREM: the item is not replaced by the marker"
        );
        limited.quit().await.unwrap();
        admin
            .call(
                "DEL",
                vec!["vilo_test:acl_new".into(), "vilo_test:acl_list".into()],
            )
            .await
            .unwrap();
        admin
            .call("ACL", vec!["DELUSER".into(), user.into()])
            .await
            .unwrap();
    }

    async fn values_can_be_searched(session: &RedisSession) {
        let (hash, set, zset) = (b"vilo_test:sh", b"vilo_test:ss", b"vilo_test:sz");
        session
            .call(
                "DEL",
                vec![bytes_arg(hash), bytes_arg(set), bytes_arg(zset)],
            )
            .await
            .unwrap();
        let mut hash_args = vec![bytes_arg(hash)];
        let mut set_args = vec![bytes_arg(set)];
        let mut zset_args = vec![bytes_arg(zset)];
        for index in 0..300 {
            let name = format!(
                "{}:{index}",
                if index % 100 == 7 { "needle" } else { "hay" }
            );
            hash_args.extend([Value::from(name.clone()), Value::from("v")]);
            set_args.push(Value::from(name.clone()));
            zset_args.extend([Value::from(index as i64), Value::from(name)]);
        }
        session.call("HSET", hash_args).await.unwrap();
        session.call("SADD", set_args).await.unwrap();
        session.call("ZADD", zset_args).await.unwrap();
        let pattern = search_pattern("needle");
        // Walk every page; matches can be spread over pages and a page can be empty.
        let mut found = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let (page, next) = session
                .hash_page(hash, &cursor, 50, pattern.as_deref())
                .await
                .unwrap();
            found.extend(page.into_iter().map(|(field, _)| field));
            if next == "0" {
                break;
            }
            cursor = next;
        }
        found.sort();
        assert_eq!(
            found,
            vec![
                b"needle:107".to_vec(),
                b"needle:207".to_vec(),
                b"needle:7".to_vec()
            ]
        );
        let mut members = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let (page, next) = session
                .set_page(set, &cursor, 50, pattern.as_deref())
                .await
                .unwrap();
            members.extend(page);
            if next == "0" {
                break;
            }
            cursor = next;
        }
        assert_eq!(members.len(), 3);
        let mut scored = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let (page, next) = session
                .zset_scan(zset, &cursor, 50, pattern.as_deref())
                .await
                .unwrap();
            scored.extend(page);
            if next == "0" {
                break;
            }
            cursor = next;
        }
        scored.sort();
        assert_eq!(scored[0], (b"needle:107".to_vec(), "107".to_string()));
        assert_eq!(scored.len(), 3);
        assert_eq!(search_pattern(" "), None);
        assert_eq!(search_pattern("a*b"), Some(b"a*b".to_vec()));
        session
            .call(
                "DEL",
                vec![bytes_arg(hash), bytes_arg(set), bytes_arg(zset)],
            )
            .await
            .unwrap();
    }

    async fn writes_are_guarded(session: &RedisSession) {
        let key = b"vilo_test:w".to_vec();
        session
            .call(
                "DEL",
                vec![
                    bytes_arg(&key),
                    "vilo_test:w2".into(),
                    "vilo_test:wh".into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "SET",
                vec![
                    bytes_arg(&key),
                    "one".into(),
                    "PX".into(),
                    Value::from(60_000_i64),
                ],
            )
            .await
            .unwrap();
        let set = |expected: &[u8], value: &[u8]| RedisWrite::SetString {
            key: key.clone(),
            expected: expected.to_vec(),
            value: value.to_vec(),
        };
        assert!(session.write(&set(b"one", b"two")).await.unwrap());
        assert!(
            !session.write(&set(b"one", b"three")).await.unwrap(),
            "stale value"
        );
        assert_eq!(
            session.get_string(&key).await.unwrap(),
            Some(b"two".to_vec())
        );
        let ttl = session
            .call("PTTL", vec![bytes_arg(&key)])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert!(ttl > 0, "KEEPTTL keeps the expiry");
        session
            .call("HSET", vec!["vilo_test:wh".into(), "f".into(), "v".into()])
            .await
            .unwrap();
        let hash = |expected: Option<&[u8]>, value: &[u8]| RedisWrite::HashSet {
            key: b"vilo_test:wh".to_vec(),
            field: b"f".to_vec(),
            expected: expected.map(<[u8]>::to_vec),
            value: value.to_vec(),
        };
        assert!(
            !session.write(&hash(None, b"x")).await.unwrap(),
            "f already exists"
        );
        assert!(session.write(&hash(Some(b"v"), b"w")).await.unwrap());
        assert!(
            !session
                .write(&RedisWrite::HashDelete {
                    key: b"vilo_test:wh".to_vec(),
                    field: b"f".to_vec(),
                    expected: b"v".to_vec(),
                })
                .await
                .unwrap()
        );
        assert!(
            session
                .write(&RedisWrite::Rename {
                    key: key.clone(),
                    to: b"vilo_test:w2".to_vec()
                })
                .await
                .unwrap()
        );
        assert!(
            !session
                .write(&RedisWrite::Rename {
                    key: b"vilo_test:wh".to_vec(),
                    to: b"vilo_test:w2".to_vec()
                })
                .await
                .unwrap(),
            "target exists"
        );
        assert!(
            !session.write(&set(b"two", b"back")).await.unwrap(),
            "a renamed key is not recreated"
        );
        assert!(
            session
                .write(&RedisWrite::Delete {
                    key: b"vilo_test:w2".to_vec()
                })
                .await
                .unwrap()
        );
        assert!(
            !session
                .write(&RedisWrite::SetAdd {
                    key: b"vilo_test:gone".to_vec(),
                    member: b"m".to_vec()
                })
                .await
                .unwrap()
        );
        assert!(matches!(
            session
                .write(&RedisWrite::Expire {
                    key: b"vilo_test:wh".to_vec(),
                    seconds: 0
                })
                .await,
            Err(crate::mongo::WriteError::Refused(_))
        ));
        assert!(
            matches!(
                session
                    .write(&RedisWrite::ListPush {
                        key: b"vilo_test:wh".to_vec(),
                        head: true,
                        value: b"x".to_vec()
                    })
                    .await,
                Err(crate::mongo::WriteError::Refused(_))
            ),
            "WRONGTYPE is a refusal, not an unknown outcome"
        );
        session
            .call("DEL", vec!["vilo_test:wh".into()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_reading_every_type_against_a_server() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        writes_are_guarded(&session).await;
        new_keys_and_list_rows(&session).await;
        streams_and_groups(&session).await;
        steps_denied_by_acl_change_nothing(&session, &url).await;
        values_can_be_searched(&session).await;
        databases_are_separate(&url).await;
        slowlog_and_clients(&url).await;
        assert!(session.cluster_nodes().await.unwrap().is_none());
        assert!(
            session
                .replication()
                .await
                .unwrap()
                .iter()
                .any(|(key, value)| key == "role" && value == "master")
        );
        let key = b"vilo_test:\x00\xff\xe4\xb8\xad".to_vec();
        let value = b"\x01\x02 not utf-8 \xff".to_vec();
        session
            .call(
                "SET",
                vec![
                    bytes_arg(&key),
                    bytes_arg(&value),
                    "PX".into(),
                    Value::from(60_000_i64),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "DEL",
                vec![
                    "vilo_test:h".into(),
                    "vilo_test:l".into(),
                    "vilo_test:s".into(),
                    "vilo_test:z".into(),
                    "vilo_test:x".into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "HSET",
                vec![
                    "vilo_test:h".into(),
                    "f1".into(),
                    "v1".into(),
                    "f2".into(),
                    "v2".into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "RPUSH",
                vec!["vilo_test:l".into(), "a".into(), "b".into(), "c".into()],
            )
            .await
            .unwrap();
        session
            .call("SADD", vec!["vilo_test:s".into(), "m1".into(), "m2".into()])
            .await
            .unwrap();
        session
            .call(
                "ZADD",
                vec![
                    "vilo_test:z".into(),
                    "1.5".into(),
                    "low".into(),
                    "9".into(),
                    "high".into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "XADD",
                vec![
                    "vilo_test:x".into(),
                    "*".into(),
                    "event".into(),
                    "login".into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "XGROUP",
                vec![
                    "CREATE".into(),
                    "vilo_test:x".into(),
                    "workers".into(),
                    "0".into(),
                ],
            )
            .await
            .unwrap();
        for key in [
            "vilo_test:h",
            "vilo_test:l",
            "vilo_test:s",
            "vilo_test:z",
            "vilo_test:x",
        ] {
            session
                .call("PEXPIRE", vec![key.into(), Value::from(60_000_i64)])
                .await
                .unwrap();
        }

        let nodes = session.scan_nodes();
        assert_eq!(nodes.len(), 1);
        let mut cursor = "0".to_string();
        let mut found = Vec::new();
        loop {
            let page = session
                .scan(&nodes[0], &cursor, "vilo_test:*", 100, None)
                .await
                .unwrap();
            found.extend(page.keys);
            cursor = page.cursor;
            if cursor == "0" {
                break;
            }
        }
        assert!(
            found.contains(&key),
            "SCAN must return the key with its exact bytes"
        );
        let info = session.key_info(&key).await.unwrap();
        assert_eq!(info.kind, "string");
        assert!((1..=60_000).contains(&info.ttl_ms));
        assert_eq!(session.get_string(&key).await.unwrap(), Some(value.clone()));
        assert_eq!(
            session.length(&key, "string").await.unwrap(),
            Some(value.len() as u64)
        );
        assert!(session.memory_usage(&key).await.unwrap().is_some());
        assert!(session.dbsize(&nodes[0]).await.unwrap() >= 6);
        let meta = session
            .key_meta(
                &[
                    key.clone(),
                    b"vilo_test:h".to_vec(),
                    b"vilo_test:missing".to_vec(),
                ],
                true,
            )
            .await;
        assert_eq!(
            meta.iter()
                .map(|(info, _)| info.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["string", "hash", "none"]
        );
        assert!(meta[0].1.is_some() && meta[2].0.ttl_ms == -2);
        assert!(
            session
                .run_read_command(split_command("SLOWLOG RESET").unwrap())
                .await
                .is_err()
        );

        let (mut fields, _) = session
            .hash_page(b"vilo_test:h", "0", 100, None)
            .await
            .unwrap();
        fields.sort();
        assert_eq!(
            fields,
            vec![
                (b"f1".to_vec(), b"v1".to_vec()),
                (b"f2".to_vec(), b"v2".to_vec())
            ]
        );
        assert_eq!(
            session.list_range(b"vilo_test:l", 1, 2).await.unwrap(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        let (mut members, _) = session
            .set_page(b"vilo_test:s", "0", 100, None)
            .await
            .unwrap();
        members.sort();
        assert_eq!(members, vec![b"m1".to_vec(), b"m2".to_vec()]);
        assert_eq!(
            session.zset_range(b"vilo_test:z", 0, -1).await.unwrap(),
            vec![
                (b"low".to_vec(), "1.5".to_string()),
                (b"high".to_vec(), "9".to_string())
            ]
        );
        let entries = session
            .stream_range(b"vilo_test:x", None, 10)
            .await
            .unwrap();
        assert_eq!(
            entries[0].fields,
            vec![(b"event".to_vec(), b"login".to_vec())]
        );
        assert!(
            session
                .stream_range(b"vilo_test:x", Some(&entries[0].id), 10)
                .await
                .unwrap()
                .is_empty()
        );
        let groups = session.stream_groups(b"vilo_test:x").await.unwrap();
        assert!(
            groups[0]
                .iter()
                .any(|(name, value)| name == "name" && value == "workers")
        );
        assert!(session.info().await.unwrap().contains("redis_version"));

        let reply = session
            .run_read_command(split_command("LRANGE vilo_test:l 0 -1").unwrap())
            .await
            .unwrap();
        assert_eq!(
            format_reply(&reply),
            vec!["1) \"a\"", "2) \"b\"", "3) \"c\""]
        );
        assert!(
            session
                .run_read_command(split_command("SET vilo_test:new 1").unwrap())
                .await
                .is_err()
        );
        assert!(
            session
                .run_read_command(split_command("FLUSHALL").unwrap())
                .await
                .is_err()
        );
        assert_eq!(
            session.key_info(b"vilo_test:new").await.unwrap().kind,
            "none",
            "a refused write must not reach the server"
        );
        session.quit().await.unwrap();
    }
}
