//! Bulk delete, as RedisInsight's: find the keys a pattern matches, then delete exactly those
//! keys in batches. The set is frozen when it is previewed; deleting never scans again, so it
//! cannot reach keys the preview did not show.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use anyhow::Result;
use fred::{
    interfaces::ClientLike as _,
    types::{ClusterHash, CustomCommand},
};

use crate::{
    mongo::WriteError,
    redis::{Bytes, RedisSession, bytes_arg, classify_redis},
};

pub const MAX_KEYS: usize = 100_000;
/// Keys per UNLINK; in a cluster every batch also lies in one hash slot.
pub const BATCH_KEYS: usize = 100;
const SCAN_COUNT: u32 = 1000;
const PREVIEW_TIME: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq)]
pub struct BulkPreview {
    /// Sorted, each once.
    pub keys: Vec<Bytes>,
    /// Why the scan stopped before walking every node, when it did; the keys are then only part
    /// of what matches.
    pub incomplete: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// The hash slot every key lies in, on a cluster.
    pub slot: Option<u16>,
    pub keys: Vec<Bytes>,
}

/// Splits the frozen keys into batches of at most `BATCH_KEYS`; on a cluster each batch holds
/// keys of one hash slot only, since a multi-key command cannot cross slots.
pub fn batches(keys: &[Bytes], clustered: bool) -> Vec<Batch> {
    if !clustered {
        return keys
            .chunks(BATCH_KEYS)
            .map(|keys| Batch {
                slot: None,
                keys: keys.to_vec(),
            })
            .collect();
    }
    let mut by_slot = BTreeMap::<u16, Vec<Bytes>>::new();
    for key in keys {
        by_slot
            .entry(fred::util::redis_keyslot(key))
            .or_default()
            .push(key.clone());
    }
    by_slot
        .into_iter()
        .flat_map(|(slot, keys)| {
            keys.chunks(BATCH_KEYS)
                .map(|keys| Batch {
                    slot: Some(slot),
                    keys: keys.to_vec(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

impl RedisSession {
    /// SCANs every node (every primary of a cluster) for keys matching `pattern`, and `kind`
    /// when given, keeping at most `limit`. Stops at `PREVIEW_TIME` or when `cancel` is set.
    pub async fn bulk_preview(
        &self,
        pattern: &str,
        kind: Option<&str>,
        limit: usize,
        cancel: &AtomicBool,
    ) -> Result<BulkPreview> {
        anyhow::ensure!(!pattern.is_empty(), "Type a pattern, such as user:*.");
        let started = Instant::now();
        let mut keys = BTreeSet::new();
        let mut incomplete = None;
        'nodes: for node in self.scan_nodes() {
            let mut cursor = "0".to_string();
            loop {
                if cancel.load(Ordering::Relaxed) {
                    incomplete = Some("You stopped the scan.".to_string());
                    break 'nodes;
                }
                if started.elapsed() > PREVIEW_TIME {
                    incomplete = Some(format!(
                        "The scan stopped after {} s.",
                        PREVIEW_TIME.as_secs()
                    ));
                    break 'nodes;
                }
                let page = self.scan(&node, &cursor, pattern, SCAN_COUNT, kind).await?;
                for key in page.keys {
                    keys.insert(key);
                    if keys.len() > limit {
                        keys.pop_last();
                        incomplete = Some(format!(
                            "More than {limit} keys match; only {limit} were kept."
                        ));
                        break 'nodes;
                    }
                }
                cursor = page.cursor;
                if cursor == "0" {
                    break;
                }
            }
        }
        Ok(BulkPreview {
            keys: keys.into_iter().collect(),
            incomplete,
        })
    }

    /// UNLINKs one batch, sent once. Returns how many of its keys still existed.
    pub async fn delete_batch(&self, batch: &Batch) -> Result<u64, WriteError> {
        let Some(first) = batch.keys.first() else {
            return Ok(0);
        };
        // Routed by the batch's slot, which every key of a cluster batch shares.
        let command = CustomCommand::new("UNLINK", ClusterHash::from(first.as_slice()), false);
        let args = batch
            .keys
            .iter()
            .map(|key| bytes_arg(key))
            .collect::<Vec<_>>();
        let deleted = self
            .client
            .custom::<fred::prelude::Value, _>(command, args)
            .await
            .map_err(classify_redis)?;
        deleted
            .as_u64()
            .ok_or_else(|| WriteError::Unknown(format!("UNLINK answered {deleted:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_batches_keep_to_one_slot() {
        let keys = (0..1000)
            .map(|number| format!("user:{number}").into_bytes())
            .chain((0..250).map(|number| format!("{{same}}:{number}").into_bytes()))
            .collect::<Vec<_>>();
        let batches = batches(&keys, true);
        assert_eq!(
            batches.iter().map(|batch| batch.keys.len()).sum::<usize>(),
            keys.len()
        );
        for batch in &batches {
            assert!(!batch.keys.is_empty() && batch.keys.len() <= BATCH_KEYS);
            assert!(
                batch
                    .keys
                    .iter()
                    .all(|key| Some(fred::util::redis_keyslot(key)) == batch.slot)
            );
        }
        let same = batches
            .iter()
            .filter(|batch| batch.keys[0].starts_with(b"{same}"))
            .count();
        assert_eq!(same, 3, "250 keys of one slot make three batches");

        let single = batches_of(&keys[..250]);
        assert_eq!(single, vec![100, 100, 50]);
    }

    fn batches_of(keys: &[Bytes]) -> Vec<usize> {
        batches(keys, false)
            .iter()
            .map(|batch| batch.keys.len())
            .collect()
    }

    async fn fill(session: &RedisSession, prefix: &str, count: usize) {
        for number in 0..count {
            session
                .call("SET", vec![format!("{prefix}{number}").into(), "1".into()])
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_bulk_delete_against_redis() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect_to_database(&url, Some(6))
            .await
            .unwrap();
        session.call("FLUSHDB", Vec::new()).await.unwrap();
        fill(&session, "vilo_test:bulk:", 250).await;
        fill(&session, "vilo_test:keep:", 3).await;
        session
            .call(
                "HSET",
                vec!["vilo_test:bulk:hash".into(), "f".into(), "v".into()],
            )
            .await
            .unwrap();
        let never = AtomicBool::new(false);

        let hashes = session
            .bulk_preview("vilo_test:bulk:*", Some("hash"), MAX_KEYS, &never)
            .await
            .unwrap();
        assert_eq!(hashes.keys, vec![b"vilo_test:bulk:hash".to_vec()]);
        let limited = session
            .bulk_preview("vilo_test:bulk:*", None, 10, &never)
            .await
            .unwrap();
        assert_eq!(limited.keys.len(), 10);
        assert!(limited.incomplete.is_some());
        let stopped = session
            .bulk_preview("vilo_test:bulk:*", None, MAX_KEYS, &AtomicBool::new(true))
            .await
            .unwrap();
        assert_eq!(stopped.incomplete.as_deref(), Some("You stopped the scan."));
        assert!(
            session
                .bulk_preview("", None, MAX_KEYS, &never)
                .await
                .is_err()
        );

        let preview = session
            .bulk_preview("vilo_test:bulk:*", None, MAX_KEYS, &never)
            .await
            .unwrap();
        assert_eq!(preview.keys.len(), 251);
        assert_eq!(preview.incomplete, None);
        // A key created after the preview is not deleted: the set is frozen.
        fill(&session, "vilo_test:bulk:late", 1).await;
        let mut deleted = 0;
        for batch in batches(&preview.keys, false) {
            deleted += session.delete_batch(&batch).await.unwrap();
        }
        assert_eq!(deleted, 251);
        let left = session
            .bulk_preview("vilo_test:*", None, MAX_KEYS, &never)
            .await
            .unwrap();
        assert_eq!(left.keys.len(), 4, "{:?}", left.keys);
        assert!(left.keys.contains(&b"vilo_test:bulk:late0".to_vec()));
        let again = Batch {
            slot: None,
            keys: vec![b"vilo_test:bulk:0".to_vec()],
        };
        assert_eq!(session.delete_batch(&again).await.unwrap(), 0);
        session.call("FLUSHDB", Vec::new()).await.unwrap();
    }

    #[tokio::test]
    async fn test_bulk_delete_against_a_cluster() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        fill(&session, "vilo_test:bulk:", 300).await;
        let preview = session
            .bulk_preview("vilo_test:bulk:*", None, MAX_KEYS, &AtomicBool::new(false))
            .await
            .unwrap();
        assert_eq!(preview.keys.len(), 300, "every primary was scanned");
        let mut deleted = 0;
        for batch in batches(&preview.keys, true) {
            deleted += session.delete_batch(&batch).await.unwrap();
        }
        assert_eq!(deleted, 300);
        let left = session
            .bulk_preview("vilo_test:bulk:*", None, MAX_KEYS, &AtomicBool::new(false))
            .await
            .unwrap();
        assert!(left.keys.is_empty());
        let refused = session
            .delete_batch(&Batch {
                slot: None,
                keys: vec![b"a".to_vec(), b"b".to_vec()],
            })
            .await;
        assert!(
            matches!(refused, Err(WriteError::Refused(_))),
            "keys of two slots are refused, not sent blind: {refused:?}"
        );
    }
}
