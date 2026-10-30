//! Database Analysis, as RedisInsight's: a bounded sample of keys from every primary, summarized
//! by type, expiry and namespace, with the largest keys of the sample. It is a sample: figures
//! describe the keys scanned, and the largest keys are the largest of those, not of the database.

use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use crate::redis::{Bytes, RedisSession, ScanNode, bytes_arg};

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
    /// -1 for no expiry; `None` when PTTL was not answered.
    pub ttl_ms: Option<i64>,
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
    /// DBSIZE when it answered, else why not.
    pub total: Option<u64>,
    pub total_error: Option<String>,
    /// The node's SCAN came back to cursor 0, so every key it held was visited once.
    pub finished: bool,
    /// The node stopped because it reached the sample size.
    pub limit_reached: bool,
    pub error: Option<String>,
    /// Keys whose TYPE was not answered, so they are left out of the report.
    pub unread: u64,
    pub first_unread_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
    /// The sample size this report was made with.
    pub keys_per_node: u64,
    pub nodes: Vec<NodeCoverage>,
    pub sampled: u64,
    pub memory: u64,
    /// Keys whose memory is unknown, so the memory figures leave them out.
    pub memory_unknown: u64,
    pub length_unknown: u64,
    /// Keys deleted or expired between SCAN and reading them.
    pub vanished: u64,
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
fn ttl_bucket(ttl_ms: Option<i64>) -> &'static str {
    const HOUR: i64 = 3_600_000;
    let Some(ttl_ms) = ttl_ms else {
        return "Expiry unknown";
    };
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

const TTL_ORDER: [&str; 8] = [
    "Under 1 hour",
    "1–4 hours",
    "4–12 hours",
    "12–24 hours",
    "1–7 days",
    "Over 7 days",
    "No expiry",
    "Expiry unknown",
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
    vanished: u64,
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
        keys_per_node: 0,
        nodes,
        sampled: stats.len() as u64,
        memory: stats.iter().filter_map(|stat| stat.memory).sum(),
        memory_unknown: stats.iter().filter(|stat| stat.memory.is_none()).count() as u64,
        length_unknown: stats.iter().filter(|stat| stat.length.is_none()).count() as u64,
        vanished,
        by_type,
        by_ttl,
        namespaces,
        top_memory,
        top_length,
        stopped,
        elapsed,
    }
}

/// How one key read went.
enum Read {
    Stat(KeyStat),
    Vanished,
    Unread(String),
}

impl RedisSession {
    /// TYPE, PTTL and MEMORY USAGE (its default sampling, never SAMPLES 0) of one key, sent
    /// together. A missing answer is kept as unknown, never taken as a real value.
    async fn read_key(&self, key: &Bytes) -> Read {
        let (kind, ttl, memory) = futures::join!(
            self.call("TYPE", vec![bytes_arg(key)]),
            self.call("PTTL", vec![bytes_arg(key)]),
            self.call("MEMORY", vec!["USAGE".into(), bytes_arg(key)]),
        );
        let kind = match kind.map(|kind| kind.as_string()) {
            Ok(Some(kind)) => kind,
            Ok(None) => return Read::Unread("TYPE gave no answer".to_string()),
            Err(error) => return Read::Unread(format!("{error:#}")),
        };
        let ttl_ms = ttl.ok().and_then(|ttl| ttl.as_i64());
        if kind == "none" || ttl_ms == Some(-2) {
            return Read::Vanished;
        }
        Read::Stat(KeyStat {
            key: key.clone(),
            kind,
            ttl_ms,
            memory: memory.ok().and_then(|memory| memory.as_u64()),
            length: None,
        })
    }

    /// A batch of keys in two pipelined rounds: TYPE, PTTL and MEMORY USAGE of every key, then
    /// the lengths, whose command depends on the type.
    async fn read_batch(&self, batch: &[Bytes]) -> Vec<Read> {
        let mut reads = futures::future::join_all(batch.iter().map(|key| self.read_key(key))).await;
        let lengths = futures::future::join_all(reads.iter().map(|read| async move {
            match read {
                Read::Stat(stat) => self.length(&stat.key, &stat.kind).await.ok().flatten(),
                _ => None,
            }
        }))
        .await;
        for (read, length) in reads.iter_mut().zip(lengths) {
            if let Read::Stat(stat) = read {
                stat.length = length;
            }
        }
        reads
    }

    /// Samples up to `keys_per_node` distinct keys from every primary. Every server call waits
    /// at most until the overall `TIME_LIMIT`, and `cancel` is checked before each call, so a
    /// dropped report stops sending soon.
    pub async fn analyze(
        &self,
        keys_per_node: u64,
        delimiter: &str,
        cancel: &AtomicBool,
    ) -> Analysis {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + TIME_LIMIT;
        let mut stats = Vec::new();
        let mut nodes = Vec::new();
        let mut vanished = 0;
        let mut stopped: Option<String> = None;
        for node in self.scan_nodes() {
            let mut coverage = NodeCoverage {
                label: node.label.clone(),
                scanned: 0,
                total: None,
                total_error: None,
                finished: false,
                limit_reached: false,
                error: None,
                unread: 0,
                first_unread_error: None,
            };
            // Once stopped, the remaining primaries are listed but not asked anything.
            if stopped.is_none()
                && let Err(reason) = self
                    .sample_node(
                        &node,
                        keys_per_node,
                        deadline,
                        cancel,
                        &mut coverage,
                        &mut stats,
                        &mut vanished,
                    )
                    .await
            {
                stopped = Some(reason.unwrap_or_else(|| {
                    format!("Stopped at the {} second limit.", TIME_LIMIT.as_secs())
                }));
            }
            nodes.push(coverage);
        }
        Analysis {
            keys_per_node,
            ..summarize(
                &stats,
                nodes,
                delimiter,
                vanished,
                stopped,
                started.elapsed(),
            )
        }
    }

    /// One primary's part of the sample. `Err(None)` means the time limit ran out,
    /// `Err(Some(reason))` another reason to stop the whole analysis.
    #[allow(clippy::too_many_arguments)]
    async fn sample_node(
        &self,
        node: &ScanNode,
        keys_per_node: u64,
        deadline: tokio::time::Instant,
        cancel: &AtomicBool,
        coverage: &mut NodeCoverage,
        stats: &mut Vec<KeyStat>,
        vanished: &mut u64,
    ) -> Result<(), Option<String>> {
        let check = || {
            if cancel.load(Ordering::Relaxed) {
                Err(Some("Cancelled.".to_string()))
            } else {
                Ok(())
            }
        };
        check()?;
        match tokio::time::timeout_at(deadline, self.dbsize(node))
            .await
            .map_err(|_| None)?
        {
            Ok(total) => coverage.total = Some(total),
            Err(error) => coverage.total_error = Some(format!("{error:#}")),
        }
        // SCAN may return a key more than once while the server rehashes.
        let mut seen = HashSet::new();
        let mut cursor = "0".to_string();
        while coverage.scanned < keys_per_node {
            check()?;
            let scanned =
                tokio::time::timeout_at(deadline, self.scan(node, &cursor, "*", PAGE, None))
                    .await
                    .map_err(|_| None)?;
            let page = match scanned {
                Ok(page) => page,
                Err(error) => {
                    coverage.error = Some(format!("{error:#}"));
                    return Ok(());
                }
            };
            let fresh = page
                .keys
                .iter()
                .filter(|key| seen.insert((*key).clone()))
                .cloned()
                .collect::<Vec<_>>();
            let room = usize::try_from(keys_per_node - coverage.scanned).unwrap_or(usize::MAX);
            let taken = fresh.len().min(room);
            for batch in fresh[..taken].chunks(BATCH) {
                check()?;
                let reads = tokio::time::timeout_at(deadline, self.read_batch(batch))
                    .await
                    .map_err(|_| None)?;
                for read in reads {
                    coverage.scanned += 1;
                    match read {
                        Read::Stat(stat) => stats.push(stat),
                        Read::Vanished => *vanished += 1,
                        Read::Unread(error) => {
                            coverage.unread += 1;
                            coverage.first_unread_error.get_or_insert(error);
                        }
                    }
                }
            }
            cursor = page.cursor;
            if cursor == "0" {
                // Only a page taken whole completes the node; a page cut short hit the sample size.
                coverage.finished = taken == fresh.len();
                coverage.limit_reached = !coverage.finished;
                return Ok(());
            }
            tokio::time::sleep(PAUSE).await;
        }
        coverage.limit_reached = true;
        Ok(())
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
            ttl_ms: Some(ttl_ms),
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
        let analysis = summarize(&stats, Vec::new(), ":", 0, None, Duration::ZERO);
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
        assert_eq!(small.keys_per_node, 5);
        assert!(small.nodes[0].limit_reached);
        assert!(!full.nodes[0].limit_reached);
        assert_eq!(small.nodes[0].scanned, 5);
        assert!(!small.nodes[0].finished, "the sample size stops the node");
        let cancelled = session.analyze(1_000, ":", &AtomicBool::new(true)).await;
        assert_eq!(cancelled.sampled, 0);
        assert_eq!(cancelled.stopped.as_deref(), Some("Cancelled."));

        // A user who may not run TYPE or PTTL: keys are reported unread or of unknown expiry,
        // never as gone or as never expiring.
        let user = "vilo_test_analysis";
        session
            .call(
                "ACL",
                [
                    "SETUSER",
                    user,
                    "reset",
                    "on",
                    ">vilo-test",
                    "~*",
                    "+@all",
                    "-pttl",
                ]
                .into_iter()
                .map(Into::into)
                .collect(),
            )
            .await
            .unwrap();
        let limited = RedisSession::connect_to_database(
            &url.replacen("redis://", &format!("redis://{user}:vilo-test@"), 1),
            Some(5),
        )
        .await
        .unwrap();
        let no_ttl = limited.analyze(1_000, ":", &never).await;
        assert_eq!(no_ttl.sampled, 25);
        assert!(
            no_ttl
                .by_ttl
                .iter()
                .all(|bucket| bucket.name == "Expiry unknown")
        );
        session
            .call(
                "ACL",
                ["SETUSER", user, "-type"]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            )
            .await
            .unwrap();
        let no_type = limited.analyze(1_000, ":", &never).await;
        assert_eq!(no_type.sampled, 0);
        assert_eq!(no_type.vanished, 0);
        assert_eq!(no_type.nodes[0].unread, 25);
        session
            .call(
                "ACL",
                ["SETUSER", user, "+type", "-dbsize"]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            )
            .await
            .unwrap();
        let no_total = limited.analyze(1_000, ":", &never).await;
        assert_eq!(no_total.nodes[0].total, None);
        assert!(
            no_total.nodes[0].total_error.is_some(),
            "a DBSIZE failure is kept"
        );
        assert_eq!(no_total.sampled, 25, "sampling goes on without DBSIZE");
        assert!(no_type.nodes[0].first_unread_error.is_some());
        limited.quit().await.unwrap();
        session
            .call("ACL", vec!["DELUSER".into(), user.into()])
            .await
            .unwrap();

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
        let keys = (0..30)
            .map(|index| format!("vilo_test:an:{index}"))
            .collect::<Vec<_>>();
        for key in &keys {
            session
                .call(
                    "SET",
                    vec![key.clone().into(), "x".into(), "EX".into(), 600.into()],
                )
                .await
                .unwrap();
        }
        let analysis = session.analyze(1_000, ":", &AtomicBool::new(false)).await;
        assert_eq!(analysis.nodes.len(), 3, "every primary is sampled");
        assert!(
            analysis.nodes.iter().all(|node| node.error.is_none()
                && node.finished
                && node.total == Some(node.scanned))
        );
        assert!(
            analysis.nodes.iter().map(|node| node.scanned).sum::<u64>() >= 30,
            "keys on every primary are counted once"
        );
        for key in keys {
            session.call("DEL", vec![key.into()]).await.unwrap();
        }
    }

    #[test]
    fn test_ttl_buckets() {
        assert_eq!(ttl_bucket(Some(-1)), "No expiry");
        assert_eq!(ttl_bucket(None), "Expiry unknown");
        assert_eq!(ttl_bucket(Some(0)), "Under 1 hour");
        assert_eq!(ttl_bucket(Some(3_600_000)), "1–4 hours");
        assert_eq!(ttl_bucket(Some(7 * 24 * 3_600_000)), "Over 7 days");
    }
}
