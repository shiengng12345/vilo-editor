//! Vilo Database core: connections and operations for MongoDB and Redis, with no GPUI dependency
//! so the same code can later run behind a worker process.

pub use mongodb::bson;

pub mod mongo;
pub mod redis;
