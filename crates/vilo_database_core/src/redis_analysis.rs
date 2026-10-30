//! Database Analysis, as RedisInsight's: a bounded sample of keys from every primary, summarized
//! by type, expiry and namespace, with the largest keys of the sample. It is a sample: figures
//! describe the keys scanned, and the largest keys are the largest of those, not of the database.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use crate::redis::{Bytes, RedisSession};

/// The sample sizes offered per node.
pub const KEYS_PER_NODE_CHOICES: [u64; 3] = [1_000, 10_000, 50_000];
/// The whole analysis stops after this long, wherever it got to.
pub const TIME_LIMIT: Duration = Duration::from_secs(60);
/// Keys asked of SCAN per call, and keys whose TYPE, PTTL, MEMORY USAGE and length are
/// pipelined together, so the server sees bounded bursts.
const PAGE: u32 = 500;
const BATCH: usize = 100;
/// A breather between pages, so a production server is not kept busy.
const PAUSE: Duration = Duration::from_millis(20);
const TOP: usize = 15;

#[derive(Clone, Debug, PartialEq)]
pub struct KeyStat {
    pub key: Bytes,
    pub kind: String,
    /// -1 for no expiry.
    pub ttl_ms: i64,
    /// `None` when MEMORY USAGE is not allowed or not answered.
    pub memory: Option<u64>,
    pub length: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    pub name: String,
    pub keys: u64,
    pub memory: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeCoverage {
    pub label: String,
    pub scanned: u64,
    /// DBSIZE when it answered.
    pub total: Option<u64>,
    /// The node's SCAN came back to cursor 0, so every key it held was visited once.
    pub finished: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
    pub nodes: Vec<NodeCoverage>,
    pub sampled: u64,
    pub memory: u64,
    /// Keys whose memory is unknown, so the memory figures leave them out.
    pub memory_unknown: u64,
    pub by_type: Vec<Bucket>,
    pub by_ttl: Vec<Bucket>,
    pub namespaces: Vec<Bucket>,
    pub top_memory: Vec<KeyStat>,
    pub top_length: Vec<KeyStat>,
    /// Why the scan stopped before its sample size, if it did.
    pub stopped: Option<String>,
    pub elapsed: Duration,
}

/// RedisInsight's expiry groups, for "memory likely to be freed over time".
fn ttl_bucket(ttl_ms: i64) -> &'static str {
    const HOUR: i64 = 3_600_000;
    match ttl_ms {
        ..0 => "No expiry",
        ttl if ttl < HOUR => "Under 1 hour",
        ttl if ttl < 4 * HOUR => "1–4 hours",
        ttl if ttl < 12 * HOUR => "4–12 hours",
        ttl if ttl < 24 * HOUR => "12–24 hours",
        ttl if ttl < 7 * 24 * HOUR => "1–7 days",
        _ => "Over 7 days",
    }
}

const TTL_ORDER: [&str; 7] = [
    "Under 1 hour",
    "1–4 hours",
    "4–12 hours",
    "12–24 hours",
    "1–7 days",
    "Over 7 days",
    "No expiry",
];

fn add(buckets: &mut Vec<Bucket>, name: &str, memory: Option<u64>) {
    let bucket = match buckets.iter().position(|bucket| bucket.name == name) {
        Some(index) => &mut buckets[index],
        None => {
            buckets.push(Bucket {
                name: name.to_string(),
                ..Bucket::default()
            });
            let last = buckets.len() - 1;
            &mut buckets[last]
        }
    };
    bucket.keys += 1;
    bucket.memory += memory.unwrap_or(0);
}

/// Sums up a sample. Namespaces are the part of a key before its first `delimiter`.
pub fn summarize(
    stats: &[KeyStat],
    nodes: Vec<NodeCoverage>,
    delimiter: &str,
    stopped: Option<String>,
    elapsed: Duration,
) -> Analysis {
    let mut by_type = Vec::new();
    let mut by_ttl = Vec::new();
    let mut namespaces = Vec::new();
    for stat in stats {
        add(&mut by_type, &stat.kind, stat.memory);
        add(&mut by_ttl, ttl_bucket(stat.ttl_ms), stat.memory);
        let key = String::from_utf8_lossy(&stat.key);
        if !delimiter.is_empty()
            && let Some((namespace, _)) = key.split_once(delimiter)
        {
            add(
                &mut namespaces,
                &format!("{namespace}{delimiter}"),
                stat.memory,
            );
        }
    }
    by_type.sort_by(|left, right| {
        right
            .memory
            .cmp(&left.memory)
            .then(right.keys.cmp(&left.keys))
    });
    by_ttl.sort_by_key(|bucket| TTL_ORDER.iter().position(|name| *name == bucket.name));
    namespaces.sort_by(|left, right| {
        right
            .memory
            .cmp(&left.memory)
            .then(right.keys.cmp(&left.keys))
    });
    namespaces.truncate(TOP);
    let mut top_memory = stats
        .iter()
        .filter(|stat| stat.memory.is_some())
        .cloned()
        .collect::<Vec<_>>();
    top_memory.sort_by_key(|stat| std::cmp::Reverse(stat.memory));
    top_memory.truncate(TOP);
    let mut top_length = stats
        .iter()
        .filter(|stat| stat.length.is_some())
        .cloned()
        .collect::<Vec<_>>();
    top_length.sort_by_key(|stat| std::cmp::Reverse(stat.length));
    top_length.truncate(TOP);
    Analysis {
        nodes,
        sampled: stats.len() as u64,
        memory: stats.iter().filter_map(|stat| stat.memory).sum(),
        memory_unknown: stats.iter().filter(|stat| stat.memory.is_none()).count() as u64,
        by_type,
        by_ttl,
        namespaces,
        top_memory,
        top_length,
        stopped,
        elapsed,
    }
}

impl RedisSession {
    /// Samples up to `keys_per_node` keys from every primary, stopping early at `TIME_LIMIT` or
    /// when `cancel` is set (checked between pages, so a dropped report stops scanning soon).
    pub async fn analyze(
        &self,
        keys_per_node: u64,
        delimiter: &str,
        cancel: &AtomicBool,
    ) -> Analysis {
        let started = Instant::now();
        let mut stats = Vec::new();
        let mut nodes = Vec::new();
        let mut stopped = None;
        for node in self.scan_nodes() {
            let mut coverage = NodeCoverage {
                label: node.label.clone(),
                scanned: 0,
                total: self.dbsize(&node).await.ok(),
                finished: false,
                error: None,
            };
            let mut cursor = "0".to_string();
            while stopped.is_none() && coverage.scanned < keys_per_node {
                if cancel.load(Ordering::Relaxed) {
                    stopped = Some("Cancelled.".to_string());
                    break;
                }
                if started.elapsed() >= TIME_LIMIT {
                    stopped = Some(format!(
                        "Stopped at the {} second limit.",
                        TIME_LIMIT.as_secs()
                    ));
                    break;
                }
                let page = match self.scan(&node, &cursor, "*", PAGE, None).await {
                    Ok(page) => page,
                    Err(error) => {
                        coverage.error = Some(format!("{error:#}"));
                        break;
                    }
                };
                let room = usize::try_from(keys_per_node - coverage.scanned).unwrap_or(usize::MAX);
                let taken = page.keys.len().min(room);
                for batch in page.keys[..taken].chunks(BATCH) {
                    let meta = self.key_meta(batch, true).await;
                    let lengths = futures::future::join_all(
                        batch
                            .iter()
                            .zip(&meta)
                            .map(|(key, (info, _))| self.length(key, &info.kind)),
                    )
                    .await;
                    for ((key, (info, memory)), length) in batch.iter().zip(meta).zip(lengths) {
                        coverage.scanned += 1;
                        // Gone between SCAN and TYPE.
                        if info.kind == "none" {
                            continue;
                        }
                        stats.push(KeyStat {
                            key: key.clone(),
                            kind: info.kind,
                            ttl_ms: info.ttl_ms,
                            memory,
                            length: length.ok().flatten(),
                        });
                    }
                }
                cursor = page.cursor;
                if cursor == "0" {
                    // Only a page taken whole completes the node.
                    coverage.finished = taken == page.keys.len();
                    break;
                }
                tokio::time::sleep(PAUSE).await;
            }
            nodes.push(coverage);
        }
        summarize(&stats, nodes, delimiter, stopped, started.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(
        key: &str,
        kind: &str,
        ttl_ms: i64,
        memory: Option<u64>,
        length: Option<u64>,
    ) -> KeyStat {
        KeyStat {
            key: key.as_bytes().to_vec(),
            kind: kind.to_string(),
            ttl_ms,
            memory,
            length,
        }
    }

    #[test]
    fn test_summarize() {
        let stats = [
            stat("user:1", "hash", -1, Some(100), Some(3)),
            stat("user:2", "hash", 30 * 60_000, Some(300), Some(9)),
            stat("queue:a", "list", 5 * 3_600_000, Some(50), Some(40)),
            stat("plain", "string", 8 * 24 * 3_600_000, None, Some(5)),
        ];
        let analysis = summarize(&stats, Vec::new(), ":", None, Duration::ZERO);
        assert_eq!(
            (analysis.sampled, analysis.memory, analysis.memory_unknown),
            (4, 450, 1)
        );
        assert_eq!(
            analysis.by_type[0],
            Bucket {
                name: "hash".into(),
                keys: 2,
                memory: 400
            }
        );
        assert_eq!(
            analysis
                .by_ttl
                .iter()
                .map(|bucket| bucket.name.as_str())
                .collect::<Vec<_>>(),
            ["Under 1 hour", "4–12 hours", "Over 7 days", "No expiry"]
        );
        assert_eq!(
            analysis
                .namespaces
                .iter()
                .map(|bucket| (bucket.name.as_str(), bucket.keys))
                .collect::<Vec<_>>(),
            [("user:", 2), ("queue:", 1)],
            "a key without the delimiter has no namespace"
        );
        assert_eq!(analysis.top_memory[0].key, b"user:2");
        assert_eq!(analysis.top_memory.len(), 3, "unknown memory is not ranked");
        assert_eq!(analysis.top_length[0].key, b"queue:a");
    }

    #[tokio::test]
    async fn test_analysis_against_a_server() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        // Its own database, so the sample holds exactly the keys made here.
        let session = RedisSession::connect_to_database(&url, Some(5))
            .await
            .unwrap();
        // A run that failed midway leaves its keys; start from none of them.
        let leftovers = (0..10)
            .flat_map(|index| [format!("an:h:{index}"), format!("an:l:{index}")])
            .chain((0..5).map(|index| format!("plain{index}")))
            .map(Into::into)
            .collect();
        session.call("DEL", leftovers).await.unwrap();
        let mut keys = Vec::new();
        for index in 0..10 {
            keys.push(format!("an:h:{index}"));
            session
                .call(
                    "HSET",
                    vec![format!("an:h:{index}").into(), "f".into(), "v".into()],
                )
                .await
                .unwrap();
            keys.push(format!("an:l:{index}"));
            session
                .call(
                    "RPUSH",
                    vec![format!("an:l:{index}").into(), "a".into(), "b".into()],
                )
                .await
                .unwrap();
        }
        for index in 0..5 {
            keys.push(format!("plain{index}"));
            session
                .call(
                    "SET",
                    vec![
                        format!("plain{index}").into(),
                        "x".into(),
                        "EX".into(),
                        600.into(),
                    ],
                )
                .await
                .unwrap();
        }
        let never = AtomicBool::new(false);
        let full = session.analyze(1_000, ":", &never).await;
        assert_eq!(full.sampled, 25);
        assert!(full.nodes[0].finished && full.nodes[0].error.is_none());
        assert_eq!(full.nodes[0].total, Some(25));
        assert!(full.stopped.is_none());
        let hashes = full
            .by_type
            .iter()
            .find(|bucket| bucket.name == "hash")
            .unwrap();
        assert_eq!(hashes.keys, 10);
        assert_eq!(full.namespaces[0].name, "an:");
        assert_eq!(full.namespaces[0].keys, 20);
        assert!(
            full.by_ttl
                .iter()
                .any(|bucket| bucket.name == "Under 1 hour" && bucket.keys == 5)
        );
        assert_eq!(full.top_length[0].length, Some(2));
        assert!(full.memory > 0);

        let small = session.analyze(5, ":", &never).await;
        assert_eq!(small.nodes[0].scanned, 5);
        assert!(!small.nodes[0].finished, "the sample size stops the node");
        let cancelled = session.analyze(1_000, ":", &AtomicBool::new(true)).await;
        assert_eq!(cancelled.sampled, 0);
        assert_eq!(cancelled.stopped.as_deref(), Some("Cancelled."));

        session
            .call("DEL", keys.into_iter().map(Into::into).collect())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_analysis_of_a_cluster() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let analysis = session.analyze(1_000, ":", &AtomicBool::new(false)).await;
        assert_eq!(analysis.nodes.len(), 3, "every primary is sampled");
        assert!(
            analysis
                .nodes
                .iter()
                .all(|node| node.error.is_none() && node.total.is_some())
        );
    }

    #[test]
    fn test_ttl_buckets() {
        assert_eq!(ttl_bucket(-1), "No expiry");
        assert_eq!(ttl_bucket(0), "Under 1 hour");
        assert_eq!(ttl_bucket(3_600_000), "1–4 hours");
        assert_eq!(ttl_bucket(7 * 24 * 3_600_000), "Over 7 days");
    }
}
