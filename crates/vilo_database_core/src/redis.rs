use anyhow::{Context as _, Result, anyhow, bail};
use fred::{
    cmd,
    interfaces::ClusterInterface as _,
    prelude::{Builder, Client, ClientLike as _, Config, Server, Value},
};

/// Keys and values stay bytes end to end; text, hex and base64 are display choices made elsewhere.
pub type Bytes = Vec<u8>;

pub struct RedisSession {
    client: Client,
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
    server: Option<Server>,
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

fn bytes_arg(bytes: &[u8]) -> Value {
    Value::Bytes(bytes.to_vec().into())
}

fn value_bytes(value: &Value) -> Bytes {
    value
        .as_bytes()
        .map(<[u8]>::to_vec)
        .unwrap_or_else(|| value.as_string().unwrap_or_default().into_bytes())
}

/// RESP2 sends pairs flat ([a, b, a, b]); RESP3 sends maps or nested pairs. Both come back as pairs.
fn pairs(value: Value) -> Vec<(Value, Value)> {
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
        let mut config = Config::from_url(url).context("The Redis URL is not valid")?;
        let clustered = config.server.is_clustered();
        if let Some(database) = database {
            anyhow::ensure!(
                !clustered || database == 0,
                "A Redis cluster only has database 0"
            );
            config.database = Some(database);
        }
        let selected = config.database.unwrap_or(0);
        let mut builder = Builder::from_config(config);
        builder.with_connection_config(|connection| {
            // A command whose answer was lost is reported as unknown rather than sent again.
            connection.max_command_attempts = 1;
        });
        let client = builder.build()?;
        client.init().await.context("Could not reach Redis")?;
        Ok(Self {
            client,
            clustered,
            database: selected,
        })
    }

    pub fn database(&self) -> u8 {
        self.database
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

    async fn call(&self, name: &'static str, args: Vec<Value>) -> Result<Value> {
        Ok(self.client.custom(cmd!(name), args).await?)
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
    ) -> Result<(Vec<(Bytes, Bytes)>, String)> {
        let reply = self
            .call(
                "HSCAN",
                vec![
                    bytes_arg(key),
                    Value::from(cursor.to_string()),
                    "COUNT".into(),
                    Value::from(count as i64),
                ],
            )
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
    ) -> Result<(Vec<Bytes>, String)> {
        let reply = self
            .call(
                "SSCAN",
                vec![
                    bytes_arg(key),
                    Value::from(cursor.to_string()),
                    "COUNT".into(),
                    Value::from(count as i64),
                ],
            )
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

    /// Consumer groups of a stream: name, consumers, pending, last delivered id, as the server reports them.
    pub async fn stream_groups(&self, key: &[u8]) -> Result<Vec<Vec<(String, String)>>> {
        let reply = self
            .call("XINFO", vec!["GROUPS".into(), bytes_arg(key)])
            .await?;
        Ok(reply
            .into_array()
            .into_iter()
            .map(|group| {
                pairs(group)
                    .into_iter()
                    .map(|(name, value)| {
                        let text = value
                            .as_string()
                            .or_else(|| value.as_i64().map(|number| number.to_string()))
                            .unwrap_or_else(|| format!("{value:?}"));
                        (name.as_string().unwrap_or_default(), text)
                    })
                    .collect()
            })
            .collect())
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
        let command =
            fred::types::CustomCommand::new(name, fred::types::ClusterHash::FirstKey, false);
        Ok(self
            .client
            .custom(
                command,
                args.iter().map(|arg| bytes_arg(arg)).collect::<Vec<_>>(),
            )
            .await?)
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
}

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
            | RedisWrite::Persist { key } => key,
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
        }
    }
}

fn classify_redis(error: fred::error::Error) -> crate::mongo::WriteError {
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
        self.client.custom(cmd!("EVAL"), all).await
    }

    /// Sends one write once. `Ok(false)` means nothing changed: a guard found a different value,
    /// the key is gone, or (for rename) the new name is taken.
    pub async fn write(&self, write: &RedisWrite) -> Result<bool, crate::mongo::WriteError> {
        let flag = |present: bool| Value::from(if present { "1" } else { "0" });
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
        }
        .map_err(classify_redis)?;
        Ok(reply.as_i64().unwrap_or(0) > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        databases_are_separate(&url).await;
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

        let (mut fields, _) = session.hash_page(b"vilo_test:h", "0", 100).await.unwrap();
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
        let (mut members, _) = session.set_page(b"vilo_test:s", "0", 100).await.unwrap();
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
