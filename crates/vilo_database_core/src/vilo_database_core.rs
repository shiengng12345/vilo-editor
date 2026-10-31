//! Vilo Database core: connections and operations for MongoDB and Redis, with no GPUI dependency
//! so the same code can later run behind a worker process.

pub use mongodb::bson;

pub mod display;
pub mod edit;
pub mod formats;
pub mod mongo;
pub mod redis;
pub mod redis_analysis;
pub mod redis_bulk;
pub mod redis_docs;
pub mod redis_insight;
pub mod redis_monitor;
pub mod redis_pubsub;
pub mod redis_search;
pub mod redis_ssh;
pub mod redis_tls;
pub mod redis_workbench;
pub mod shell;

/// A cluster has only database 0, so tests that change or count its keys run one at a time.
#[cfg(test)]
pub(crate) static CLUSTER_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
