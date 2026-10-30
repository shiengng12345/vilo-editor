//! Search, as RedisInsight's: RediSearch indexes, their definition, and pages of FT.SEARCH
//! results. Read only. Every call opens a RESP3 connection of its own and closes it, because only
//! RESP3 replies say when a query stopped at its time limit and returned part of its results.

use std::collections::HashMap;

use anyhow::{Context as _, Result, anyhow, bail};
use fred::{
    interfaces::ClientLike as _,
    prelude::{Builder, Client, Value},
    types::{ClusterHash, CustomCommand, RespVersion},
};

use crate::redis::{Bytes, COMMAND_TIMEOUT, RedisSession, bytes_arg, pairs, value_bytes};

pub const DEFAULT_PAGE: u64 = 20;
pub const MAX_PAGE: u64 = 100;
pub const DEFAULT_TIMEOUT_MS: u64 = 1000;
/// Sent as FT.SEARCH's TIMEOUT, so the server stops the query; 0 would mean no limit at all.
pub const MAX_TIMEOUT_MS: u64 = 10_000;
pub const MAX_INDEXES: usize = 1000;
/// Field values are kept up to this many bytes for display, and say when they were cut.
pub const MAX_VALUE_BYTES: usize = 64 * 1024;
/// What one page's documents may take in memory, by MEMORY USAGE, for their content to be fetched.
pub const PAGE_BYTES: u64 = 4 * 1024 * 1024;
/// A document heavier than this is listed without its content.
pub const DOCUMENT_BYTES: u64 = 1024 * 1024;
/// Hashes with more fields are listed without content: measuring them exactly walks every field.
pub const MAX_FIELDS: u64 = 1000;
/// Content kept per page. A document may grow between being measured and fetched; past this
/// the rest of the page is not kept.
/// RediSearch's words when a query stops at its time limit. Even with ON_TIMEOUT RETURN some
/// queries answer with this error instead of partial results (seen on 2.10.5).
const TIMED_OUT: &str = "Timeout limit was reached";
const KEPT_BYTES: usize = 2 * PAGE_BYTES as usize;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    pub index: Bytes,
    pub query: String,
    pub offset: u64,
    pub count: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: Bytes,
    /// At most `MAX_VALUE_BYTES`.
    pub value: Bytes,
    /// The value's full size, larger than `value` when it was cut.
    pub size: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Fields(Vec<Field>),
    /// Why the document's content is not shown; the key can still be opened.
    Missing(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub key: Bytes,
    pub content: Content,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchPage {
    /// As the server counted it; only a lower bound when `partial` is set.
    pub total: u64,
    pub hits: Vec<SearchHit>,
    /// Why the results may be incomplete: the server's warnings, such as its time limit.
    pub partial: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexAttribute {
    pub identifier: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexInfo {
    pub key_type: String,
    pub prefixes: Vec<String>,
    pub filter: Option<String>,
    pub attributes: Vec<IndexAttribute>,
    pub documents: Option<u64>,
    pub percent_indexed: Option<f64>,
    pub failures: Option<u64>,
}

struct Reply {
    total: u64,
    results: Vec<(Bytes, Option<Vec<(Bytes, Bytes)>>)>,
    /// `None` when the reply came in RESP2's shape, which never carries warnings.
    warnings: Option<Vec<String>>,
}

impl Reply {
    fn partial(&self) -> Option<String> {
        match &self.warnings {
            None => Some(
                "This server's RediSearch does not report when a query stops at its time limit, \
                 so these results may be incomplete."
                    .into(),
            ),
            Some(warnings) if !warnings.is_empty() => Some(warnings.join("; ")),
            Some(_) => None,
        }
    }
}

fn text(value: &Value) -> String {
    String::from_utf8_lossy(&value_bytes(value)).into_owned()
}

fn name_of(value: &Value) -> String {
    text(value).to_ascii_lowercase()
}

fn parse_search(reply: Value) -> Result<Reply> {
    match reply {
        Value::Map(_) => {
            let mut total = None;
            let mut results = Vec::new();
            let mut warnings = Vec::new();
            for (name, value) in pairs(reply) {
                match name_of(&name).as_str() {
                    "total_results" => total = value.as_u64(),
                    "results" => {
                        for result in value.into_array() {
                            let mut key = None;
                            let mut fields = None;
                            for (name, value) in pairs(result) {
                                match name_of(&name).as_str() {
                                    "id" => key = Some(value_bytes(&value)),
                                    "extra_attributes" => fields = Some(field_pairs(value)),
                                    _ => {}
                                }
                            }
                            results.push((
                                key.context("A search result came without its key")?,
                                fields,
                            ));
                        }
                    }
                    "warning" => warnings = value.into_array().iter().map(text).collect(),
                    _ => {}
                }
            }
            Ok(Reply {
                total: total.context("The search reply has no total")?,
                results,
                warnings: Some(warnings),
            })
        }
        // Older RediSearch answers RESP3 in RESP2's shape: the total, then each key, followed by
        // an array of its fields unless NOCONTENT was asked for.
        Value::Array(items) => {
            let mut items = items.into_iter();
            let total = items
                .next()
                .and_then(|total| total.as_u64())
                .context("The search reply has no total")?;
            let mut results: Vec<(Bytes, Option<Vec<(Bytes, Bytes)>>)> = Vec::new();
            for item in items {
                match item {
                    Value::Array(_) => {
                        let last = results
                            .last_mut()
                            .filter(|(_, fields)| fields.is_none())
                            .context("The search reply has fields without a key")?;
                        last.1 = Some(field_pairs(item));
                    }
                    key => results.push((value_bytes(&key), None)),
                }
            }
            Ok(Reply {
                total,
                results,
                warnings: None,
            })
        }
        other => bail!("Unexpected search reply: {other:?}"),
    }
}

fn field_pairs(value: Value) -> Vec<(Bytes, Bytes)> {
    let mut fields = pairs(value)
        .into_iter()
        .map(|(name, value)| (value_bytes(&name), value_bytes(&value)))
        .collect::<Vec<_>>();
    // RESP3 maps arrive unordered.
    fields.sort();
    fields
}

fn parse_info(reply: Value) -> IndexInfo {
    let mut info = IndexInfo::default();
    for (name, value) in pairs(reply) {
        match name_of(&name).as_str() {
            "index_definition" => {
                for (name, value) in pairs(value) {
                    match name_of(&name).as_str() {
                        "key_type" => info.key_type = text(&value),
                        "prefixes" => info.prefixes = value.into_array().iter().map(text).collect(),
                        "filter" => info.filter = Some(text(&value)),
                        _ => {}
                    }
                }
            }
            "attributes" => {
                info.attributes = value
                    .into_array()
                    .into_iter()
                    .map(|attribute| {
                        let mut found = IndexAttribute {
                            identifier: String::new(),
                            name: String::new(),
                            kind: String::new(),
                        };
                        let entries = if matches!(attribute, Value::Map(_)) {
                            pairs(attribute)
                        } else {
                            attribute_entries(attribute.into_array())
                        };
                        for (name, value) in entries {
                            match name_of(&name).as_str() {
                                "identifier" => found.identifier = text(&value),
                                "attribute" => found.name = text(&value),
                                "type" => found.kind = text(&value),
                                _ => {}
                            }
                        }
                        found
                    })
                    .collect();
            }
            "num_docs" => info.documents = value.as_u64(),
            "percent_indexed" => info.percent_indexed = value.as_f64(),
            "hash_indexing_failures" => info.failures = value.as_u64(),
            _ => {}
        }
    }
    info
}

/// RESP2 lists flags such as SORTABLE without values, so its attribute array cannot be read as
/// pairs: each name Vilo reads takes the item after it as its value.
fn attribute_entries(items: Vec<Value>) -> Vec<(Value, Value)> {
    let mut entries = Vec::new();
    let mut items = items.into_iter();
    while let Some(item) = items.next() {
        if matches!(name_of(&item).as_str(), "identifier" | "attribute" | "type")
            && let Some(value) = items.next()
        {
            entries.push((item, value));
        }
    }
    entries
}

async fn command(client: &Client, name: &'static str, args: Vec<Value>) -> Result<Value> {
    client
        .custom(CustomCommand::new(name, ClusterHash::FirstKey, false), args)
        .await
        .map_err(|error| {
            let details = error.details();
            if details.to_ascii_lowercase().contains("unknown command") {
                anyhow!("Search (RediSearch) is not available on this server.")
            } else {
                anyhow!("{details}")
            }
        })
}

fn cut(value: Bytes) -> (Bytes, usize) {
    let size = value.len();
    if size <= MAX_VALUE_BYTES {
        return (value, size);
    }
    (value[..MAX_VALUE_BYTES].to_vec(), size)
}

impl RedisSession {
    async fn search_connection(&self) -> Result<Client> {
        anyhow::ensure!(
            !self.is_clustered(),
            "Search on a Redis Cluster is not supported yet: without a search coordinator each \
             shard indexes only its own keys, so the results would be incomplete."
        );
        // Indexes only cover database 0, and their results name keys there.
        anyhow::ensure!(
            self.database() == 0,
            "Search indexes only cover database 0. Switch to database 0 to search."
        );
        let mut config = self.config.clone();
        config.version = RespVersion::RESP3;
        let mut builder = Builder::from_config(config);
        builder.with_connection_config(|connection| connection.max_command_attempts = 1);
        builder.with_performance_config(|performance| {
            performance.default_command_timeout = COMMAND_TIMEOUT;
        });
        let client = builder.build()?;
        client
            .init()
            .await
            .context("Could not open the search connection")?;
        Ok(client)
    }

    /// The index names, sorted, at most `MAX_INDEXES`; `true` when the server has more.
    pub async fn search_indexes(&self) -> Result<(Vec<Bytes>, bool)> {
        let client = self.search_connection().await?;
        let reply = command(&client, "FT._LIST", Vec::new()).await;
        client.quit().await.ok();
        let mut names = reply?
            .into_array()
            .iter()
            .map(value_bytes)
            .collect::<Vec<_>>();
        names.sort();
        let more = names.len() > MAX_INDEXES;
        names.truncate(MAX_INDEXES);
        Ok((names, more))
    }

    pub async fn search_index_info(&self, index: &[u8]) -> Result<IndexInfo> {
        let client = self.search_connection().await?;
        let reply = command(&client, "FT.INFO", vec![bytes_arg(index)]).await;
        client.quit().await.ok();
        Ok(parse_info(reply?))
    }

    /// One page of FT.SEARCH. Keys are found first without content, then content is fetched
    /// for those whose MEMORY USAGE fits the page's byte budget, so a few huge documents cannot
    /// make Vilo read megabytes.
    pub async fn search(&self, request: &SearchRequest) -> Result<SearchPage> {
        anyhow::ensure!(!request.index.is_empty(), "Choose an index.");
        anyhow::ensure!(
            !request.query.trim().is_empty(),
            "Type a query, e.g. * for every document."
        );
        anyhow::ensure!(
            (1..=MAX_PAGE).contains(&request.count),
            "Show between 1 and {MAX_PAGE} results per page."
        );
        anyhow::ensure!(
            (1..=MAX_TIMEOUT_MS).contains(&request.timeout_ms),
            "The time limit must be between 1 and {MAX_TIMEOUT_MS} ms."
        );
        let client = self.search_connection().await?;
        let page = search_on(&client, request).await;
        client.quit().await.ok();
        page
    }
}

/// Each key's memory, or why its content will not be fetched. Hashes are measured exactly
/// (SAMPLES 0), which is why only those with at most `MAX_FIELDS` fields are; JSON documents are
/// measured by the module. The reply is then at most a few times the memory: JSON escapes a
/// control character as six bytes.
async fn measure(client: &Client, keys: &[Bytes]) -> Result<Vec<Result<u64, String>>> {
    let unreadable = || "Its size could not be read; open the key.".to_string();
    let pipeline = client.pipeline();
    for key in keys {
        for name in ["TYPE", "HLEN"] {
            let () = pipeline
                .custom(
                    CustomCommand::new(name, ClusterHash::FirstKey, false),
                    vec![bytes_arg(key)],
                )
                .await?;
        }
    }
    let kinds = pipeline.try_all::<Value>().await;
    let pipeline = client.pipeline();
    let mut sizes = Vec::with_capacity(keys.len());
    let mut asked = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        // The reply, or why the key's size cannot be known, with the server's reason.
        let read = |offset: usize| match kinds.get(index * 2 + offset) {
            Some(Ok(reply)) => Ok(reply),
            Some(Err(error)) => Err(format!(
                "Its size could not be read ({}); open the key.",
                error.details()
            )),
            None => Err(unreadable()),
        };
        let kind = match read(0) {
            Ok(kind) => text(kind),
            Err(reason) => {
                sizes.push(Err(reason));
                continue;
            }
        };
        let args = match kind.as_str() {
            "hash" => match read(1).map(Value::as_u64) {
                Ok(Some(fields)) if fields <= MAX_FIELDS => {
                    vec!["USAGE".into(), bytes_arg(key), "SAMPLES".into(), "0".into()]
                }
                Ok(Some(fields)) => {
                    sizes.push(Err(format!(
                        "It has {fields} fields, more than {MAX_FIELDS}; open the key."
                    )));
                    continue;
                }
                Ok(None) => {
                    sizes.push(Err(unreadable()));
                    continue;
                }
                Err(reason) => {
                    sizes.push(Err(reason));
                    continue;
                }
            },
            "ReJSON-RL" => vec!["USAGE".into(), bytes_arg(key)],
            "none" => {
                sizes.push(Err("The key no longer exists.".to_string()));
                continue;
            }
            other => {
                sizes.push(Err(format!(
                    "It is a {other}, not a hash or JSON document; open the key."
                )));
                continue;
            }
        };
        let () = pipeline
            .custom(
                CustomCommand::new("MEMORY", ClusterHash::FirstKey, false),
                args,
            )
            .await?;
        asked.push(index);
        sizes.push(Err(unreadable()));
    }
    if asked.is_empty() {
        return Ok(sizes);
    }
    let measured = pipeline.try_all::<Value>().await;
    for (reply, index) in measured.into_iter().zip(asked) {
        let size = match reply {
            Ok(Value::Null) => Err("The key no longer exists.".to_string()),
            Ok(size) => size.as_u64().ok_or_else(unreadable),
            Err(error) => Err(format!(
                "Its size could not be read ({}); open the key.",
                error.details()
            )),
        };
        if let Some(slot) = sizes.get_mut(index) {
            *slot = size;
        }
    }
    Ok(sizes)
}

async fn search_on(client: &Client, request: &SearchRequest) -> Result<SearchPage> {
    let base = || {
        vec![
            bytes_arg(&request.index),
            Value::from(request.query.as_str()),
        ]
    };
    let options = |offset: u64, count: u64| -> Vec<Value> {
        vec![
            "LIMIT".into(),
            offset.to_string().into(),
            count.to_string().into(),
            "TIMEOUT".into(),
            request.timeout_ms.to_string().into(),
        ]
    };
    let mut args = base();
    args.push("NOCONTENT".into());
    args.extend(options(request.offset, request.count));
    let found = match command(client, "FT.SEARCH", args).await {
        Err(error) if error.to_string().contains(TIMED_OUT) => bail!(
            "The query stopped at its time limit before it returned any results. Raise the time \
             limit or narrow the query."
        ),
        reply => parse_search(reply?)?,
    };
    let mut partial = found.partial();
    let keys = found
        .results
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(SearchPage {
            total: found.total,
            hits: Vec::new(),
            partial,
        });
    }

    let sizes = measure(client, &keys).await?;
    let mut budget = PAGE_BYTES;
    let mut missing = HashMap::new();
    let mut fetch = Vec::new();
    for (key, size) in keys.iter().zip(sizes) {
        let reason = match size {
            Ok(size) if size > DOCUMENT_BYTES => Some(format!(
                "Too large to show here (about {size} bytes); open the key."
            )),
            Ok(size) if size > budget => Some(
                "Not shown: this page reached its size limit. Open the key, or show fewer per page."
                    .to_string(),
            ),
            Ok(size) => {
                budget -= size;
                fetch.push(key.clone());
                None
            }
            Err(reason) => Some(reason),
        };
        if let Some(reason) = reason {
            missing.insert(key.clone(), reason);
        }
    }

    let mut contents = HashMap::new();
    let mut content_partial = false;
    if !fetch.is_empty() {
        let mut args = base();
        args.push("INKEYS".into());
        args.push(fetch.len().to_string().into());
        args.extend(fetch.iter().map(|key| bytes_arg(key)));
        args.extend(options(0, fetch.len() as u64));
        let results = match command(client, "FT.SEARCH", args).await {
            // The keys are already found; their rows say the content was not fetched.
            Err(error) if error.to_string().contains(TIMED_OUT) => {
                content_partial = true;
                partial.get_or_insert(TIMED_OUT.to_string());
                Vec::new()
            }
            reply => {
                let fetched = parse_search(reply?)?;
                if let Some(reason) = fetched.partial() {
                    content_partial = true;
                    partial.get_or_insert(reason);
                }
                fetched.results
            }
        };
        let mut kept = 0_usize;
        for (key, fields) in results {
            let Some(fields) = fields else {
                continue;
            };
            let weight = fields
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum::<usize>();
            if kept + weight > KEPT_BYTES {
                missing.insert(
                    key,
                    "Not shown: this page reached its size limit. Open the key, or show fewer per page."
                        .to_string(),
                );
                continue;
            }
            kept += weight;
            let fields = fields
                .into_iter()
                .map(|(name, value)| {
                    let (value, size) = cut(value);
                    Field { name, value, size }
                })
                .collect();
            contents.insert(key, fields);
        }
    }

    let hits = keys
        .into_iter()
        .map(|key| {
            let content = match contents.remove(&key) {
                Some(fields) => Content::Fields(fields),
                None => Content::Missing(missing.remove(&key).unwrap_or_else(|| {
                    if content_partial {
                        "Not fetched: the query stopped at its time limit.".to_string()
                    } else {
                        "It changed after it was found and no longer matches.".to_string()
                    }
                })),
            };
            SearchHit { key, content }
        })
        .collect();
    Ok(SearchPage {
        total: found.total,
        hits,
        partial,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(text: &str) -> Value {
        Value::Bytes(text.as_bytes().to_vec().into())
    }

    #[test]
    fn test_resp2_shaped_replies_are_read_and_marked_unsure() {
        let reply = parse_search(Value::Array(vec![
            Value::Integer(2),
            bulk("a"),
            Value::Array(vec![bulk("title"), bulk("x"), bulk("n"), bulk("1")]),
            bulk("b"),
            Value::Array(Vec::new()),
        ]))
        .unwrap();
        assert_eq!(reply.total, 2);
        assert_eq!(
            reply.results,
            vec![
                (
                    b"a".to_vec(),
                    Some(vec![
                        (b"n".to_vec(), b"1".to_vec()),
                        (b"title".to_vec(), b"x".to_vec())
                    ])
                ),
                (b"b".to_vec(), Some(Vec::new())),
            ]
        );
        assert!(
            reply.partial().is_some(),
            "RESP2 cannot say it was complete"
        );

        let keys_only =
            parse_search(Value::Array(vec![Value::Integer(5), bulk("a"), bulk("b")])).unwrap();
        assert_eq!(keys_only.results.len(), 2);
        assert!(
            parse_search(Value::Array(vec![
                Value::Integer(1),
                Value::Array(Vec::new())
            ]))
            .is_err(),
            "fields before any key"
        );
        assert!(
            parse_search(Value::Array(vec![
                Value::Integer(1),
                bulk("a"),
                Value::Array(Vec::new()),
                Value::Array(Vec::new())
            ]))
            .is_err(),
            "two field arrays for one key"
        );
    }

    #[test]
    fn test_resp2_attributes_are_read_by_name() {
        let info = parse_info(Value::Array(vec![
            bulk("attributes"),
            Value::Array(vec![
                Value::Array(
                    [
                        "identifier",
                        "n",
                        "attribute",
                        "n",
                        "type",
                        "NUMERIC",
                        "SORTABLE",
                        "UNF",
                    ]
                    .map(bulk)
                    .to_vec(),
                ),
                Value::Array(
                    [
                        "SORTABLE",
                        "attribute",
                        "type",
                        "type",
                        "TAG",
                        "identifier",
                        "$.type",
                    ]
                    .map(bulk)
                    .to_vec(),
                ),
            ]),
        ]));
        assert_eq!(
            info.attributes,
            vec![
                IndexAttribute {
                    identifier: "n".into(),
                    name: "n".into(),
                    kind: "NUMERIC".into()
                },
                IndexAttribute {
                    identifier: "$.type".into(),
                    name: "type".into(),
                    kind: "TAG".into()
                },
            ]
        );
    }

    #[test]
    fn test_values_are_cut_for_display() {
        let (value, size) = cut(vec![b'x'; MAX_VALUE_BYTES + 5]);
        assert_eq!((value.len(), size), (MAX_VALUE_BYTES, MAX_VALUE_BYTES + 5));
        assert_eq!(cut(b"ab".to_vec()), (b"ab".to_vec(), 2));
    }

    async fn stack() -> Option<RedisSession> {
        let url = std::env::var("VILO_TEST_REDIS_STACK").ok()?;
        Some(RedisSession::connect(&url).await.unwrap())
    }

    fn request(index: &str, query: &str) -> SearchRequest {
        SearchRequest {
            index: index.as_bytes().to_vec(),
            query: query.into(),
            offset: 0,
            count: DEFAULT_PAGE,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    #[tokio::test]
    async fn test_search_against_redis_stack() {
        let Some(session) = stack().await else {
            return;
        };
        let index = "vilo_test_search";
        session.call("FT.DROPINDEX", vec![index.into()]).await.ok();
        let prefix = "vilo_test:search:";
        for (key, title) in [("a", "red apple"), ("b", "green apple"), ("c", "blue sky")] {
            session
                .call(
                    "HSET",
                    vec![
                        format!("{prefix}{key}").into(),
                        "title".into(),
                        title.into(),
                    ],
                )
                .await
                .unwrap();
        }
        // Over DOCUMENT_BYTES, so it is listed without content.
        session
            .call(
                "HSET",
                vec![
                    format!("{prefix}huge").into(),
                    "title".into(),
                    "huge apple".into(),
                    "body".into(),
                    Value::Bytes(vec![b'x'; DOCUMENT_BYTES as usize + 1].into()),
                ],
            )
            .await
            .unwrap();
        // Too many fields to measure exactly, so also listed without content.
        let mut wide = vec![
            Value::from(format!("{prefix}wide")),
            Value::from("title"),
            Value::from("wide apple"),
        ];
        for field in 0..MAX_FIELDS {
            wide.push(Value::from(format!("f{field}")));
            wide.push(Value::from("1"));
        }
        session.call("HSET", wide).await.unwrap();
        session
            .call(
                "FT.CREATE",
                vec![
                    index.into(),
                    "ON".into(),
                    "HASH".into(),
                    "PREFIX".into(),
                    "1".into(),
                    prefix.into(),
                    "SCHEMA".into(),
                    "title".into(),
                    "TEXT".into(),
                ],
            )
            .await
            .unwrap();
        for _ in 0..50 {
            let info = session.search_index_info(index.as_bytes()).await.unwrap();
            if info.percent_indexed == Some(1.0) && info.documents == Some(5) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let (indexes, more) = session.search_indexes().await.unwrap();
        assert!(indexes.contains(&index.as_bytes().to_vec()));
        assert!(!more);
        let info = session.search_index_info(index.as_bytes()).await.unwrap();
        assert_eq!(info.key_type, "HASH");
        assert_eq!(info.prefixes, vec![prefix.to_string()]);
        assert_eq!(
            info.attributes,
            vec![IndexAttribute {
                identifier: "title".into(),
                name: "title".into(),
                kind: "TEXT".into()
            }]
        );
        assert_eq!(info.documents, Some(5));

        let page = session.search(&request(index, "apple")).await.unwrap();
        assert_eq!(page.total, 4);
        assert_eq!(page.partial, None);
        let mut shown = page
            .hits
            .iter()
            .map(|hit| {
                (
                    String::from_utf8_lossy(&hit.key).into_owned(),
                    match &hit.content {
                        Content::Fields(fields) => fields
                            .iter()
                            .map(|field| String::from_utf8_lossy(&field.value).into_owned())
                            .collect::<Vec<_>>()
                            .join(","),
                        Content::Missing(reason) => format!("missing: {reason}"),
                    },
                )
            })
            .collect::<Vec<_>>();
        shown.sort();
        assert_eq!(shown[0], (format!("{prefix}a"), "red apple".into()));
        assert_eq!(shown[1], (format!("{prefix}b"), "green apple".into()));
        assert!(
            shown[2].0 == format!("{prefix}huge") && shown[2].1.starts_with("missing: Too large"),
            "{shown:?}"
        );
        assert!(
            shown[3].0 == format!("{prefix}wide") && shown[3].1.contains("more than 1000"),
            "{shown:?}"
        );

        let mut second = request(index, "apple");
        second.offset = 2;
        second.count = 2;
        assert_eq!(session.search(&second).await.unwrap().hits.len(), 2);
        let mut empty = request(index, "apple");
        empty.offset = 10;
        assert!(session.search(&empty).await.unwrap().hits.is_empty());

        assert!(session.search(&request(index, "  ")).await.is_err());
        let mut too_many = request(index, "*");
        too_many.count = MAX_PAGE + 1;
        assert!(session.search(&too_many).await.is_err());
        let mut no_limit = request(index, "*");
        no_limit.timeout_ms = 0;
        assert!(
            session.search(&no_limit).await.is_err(),
            "TIMEOUT 0 means none"
        );
        assert!(
            session
                .search(&request("vilo_test_missing", "*"))
                .await
                .is_err()
        );
        assert!(session.search(&request(index, "@@@(")).await.is_err());

        session
            .call("FT.DROPINDEX", vec![index.into(), "DD".into()])
            .await
            .unwrap();

        let json_index = "vilo_test_search_json";
        session
            .call("FT.DROPINDEX", vec![json_index.into(), "DD".into()])
            .await
            .ok();
        session
            .call(
                "JSON.SET",
                vec![
                    "vilo_test:search_json:1".into(),
                    "$".into(),
                    r#"{"name":"apple pie","n":1}"#.into(),
                ],
            )
            .await
            .unwrap();
        session
            .call(
                "FT.CREATE",
                vec![
                    json_index.into(),
                    "ON".into(),
                    "JSON".into(),
                    "PREFIX".into(),
                    "1".into(),
                    "vilo_test:search_json:".into(),
                    "SCHEMA".into(),
                    "$.name".into(),
                    "AS".into(),
                    "name".into(),
                    "TEXT".into(),
                ],
            )
            .await
            .unwrap();
        for _ in 0..50 {
            let info = session
                .search_index_info(json_index.as_bytes())
                .await
                .unwrap();
            if info.documents == Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let info = session
            .search_index_info(json_index.as_bytes())
            .await
            .unwrap();
        assert_eq!(info.key_type, "JSON");
        assert_eq!(
            info.attributes,
            vec![IndexAttribute {
                identifier: "$.name".into(),
                name: "name".into(),
                kind: "TEXT".into()
            }]
        );
        let page = session
            .search(&request(json_index, "@name:apple"))
            .await
            .unwrap();
        session
            .call("FT.DROPINDEX", vec![json_index.into(), "DD".into()])
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(
            page.hits[0].content,
            Content::Fields(vec![Field {
                name: b"$".to_vec(),
                value: br#"{"name":"apple pie","n":1}"#.to_vec(),
                size: 26,
            }])
        );
    }

    #[tokio::test]
    async fn test_search_refuses_other_databases_and_clusters() {
        if let Ok(url) = std::env::var("VILO_TEST_REDIS_STACK") {
            let session = RedisSession::connect_to_database(&url, Some(1))
                .await
                .unwrap();
            let error = session.search_indexes().await.unwrap_err();
            assert!(format!("{error:#}").contains("database 0"), "{error:#}");
        }
        if let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") {
            let session = RedisSession::connect(&url).await.unwrap();
            let error = session.search(&request("any", "*")).await.unwrap_err();
            assert!(format!("{error:#}").contains("Cluster"), "{error:#}");
        }
        if let Ok(url) = std::env::var("VILO_TEST_REDIS") {
            let session = RedisSession::connect(&url).await.unwrap();
            let error = session.search_indexes().await.unwrap_err();
            assert!(format!("{error:#}").contains("not available"), "{error:#}");
        }
    }

    #[tokio::test]
    async fn test_a_query_stopped_by_its_time_limit_is_marked_partial() {
        let Some(session) = stack().await else {
            return;
        };
        let index = "vilo_test_search_slow";
        session
            .call("FT.DROPINDEX", vec![index.into(), "DD".into()])
            .await
            .ok();
        let words = ["alpha", "beta", "gamma", "delta", "kappa", "theta"];
        let pipeline = session.client.pipeline();
        for number in 0..60_000_u64 {
            let title = (0..6)
                .map(|offset| {
                    let word = words[((number + offset) % words.len() as u64) as usize];
                    format!("{word}{}", (number * 7 + offset * 13) % 400)
                })
                .collect::<Vec<_>>()
                .join(" ");
            let () = pipeline
                .custom(
                    CustomCommand::new("HSET", ClusterHash::FirstKey, false),
                    vec![
                        Value::from(format!("vilo_test:slow:{number}")),
                        Value::from("title"),
                        Value::from(title),
                    ],
                )
                .await
                .unwrap();
        }
        pipeline.all::<Value>().await.unwrap();
        session
            .call(
                "FT.CREATE",
                vec![
                    index.into(),
                    "ON".into(),
                    "HASH".into(),
                    "PREFIX".into(),
                    "1".into(),
                    "vilo_test:slow:".into(),
                    "SCHEMA".into(),
                    "title".into(),
                    "TEXT".into(),
                ],
            )
            .await
            .unwrap();
        for _ in 0..300 {
            let info = session.search_index_info(index.as_bytes()).await.unwrap();
            if info.percent_indexed == Some(1.0) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let mut slow = request(index, "al* | be* | ga* | de* | ka* | th*");
        slow.timeout_ms = 1;
        let page = session.search(&slow).await;
        let fast = session.search(&request(index, "alpha1")).await.unwrap();
        session
            .call("FT.DROPINDEX", vec![index.into(), "DD".into()])
            .await
            .unwrap();
        // The server either returns part of the results with a warning or refuses the query;
        // both must be said.
        match page {
            Ok(page) => assert!(
                page.partial
                    .as_deref()
                    .is_some_and(|reason| reason.contains("Timeout")),
                "{:?}",
                page.partial
            ),
            Err(error) => assert!(
                format!("{error:#}").contains("stopped at its time limit"),
                "{error:#}"
            ),
        }
        assert_eq!(fast.partial, None);
    }
}
