//! Pub/Sub, as RedisInsight's: a subscription on its own connection, so the main session keeps
//! answering, and PUBLISH. Nothing is replayed: messages missed while the view lagged or the
//! connection dropped are counted and reported as lost.

use anyhow::{Context as _, Result};
use fred::{
    interfaces::{ClientLike as _, EventInterface as _, PubsubInterface as _},
    prelude::{Builder, Value},
    types::MessageKind,
};
use futures::channel::{mpsc, oneshot};
use tokio::sync::broadcast::error::RecvError;

use crate::{
    mongo::WriteError,
    redis::{Bytes, RedisSession},
};

/// A message larger than this is cut, and says so.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;
/// Events waiting for the view; beyond this they are dropped and counted as lost.
const QUEUE: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Subscribe {
    /// PSUBSCRIBE: glob patterns such as `*` or `news.*`.
    Patterns,
    /// SUBSCRIBE: exact channel names.
    Channels,
    /// SSUBSCRIBE: sharded channels (Redis 7), delivered by the shard that owns them.
    Shard,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PubSubMessage {
    pub time: std::time::SystemTime,
    pub channel: String,
    pub sharded: bool,
    /// At most `MAX_MESSAGE_BYTES`.
    pub payload: Bytes,
    /// The message's full size, larger than the payload when it was cut.
    pub size: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PubSubEvent {
    Message(PubSubMessage),
    /// Messages that never reached the view: the connection's buffer or the view's queue was
    /// full.
    Lost(u64),
    /// The connection ended; the subscription is over and nothing after this was received.
    Ended(String),
}

/// A live subscription. Dropping it (or its `Stop` once split) unsubscribes and closes its
/// connection.
pub struct Subscription {
    pub events: mpsc::Receiver<PubSubEvent>,
    stop: Stop,
}

pub struct Stop {
    _sender: oneshot::Sender<()>,
}

impl Subscription {
    pub fn into_parts(self) -> (mpsc::Receiver<PubSubEvent>, Stop) {
        (self.events, self.stop)
    }
}

fn message(message: fred::types::Message) -> PubSubMessage {
    let bytes = match &message.value {
        Value::Bytes(bytes) => bytes.to_vec(),
        Value::String(text) => text.as_bytes().to_vec(),
        other => other.as_string().unwrap_or_default().into_bytes(),
    };
    let size = bytes.len();
    let mut payload = bytes;
    payload.truncate(MAX_MESSAGE_BYTES);
    PubSubMessage {
        time: std::time::SystemTime::now(),
        channel: message.channel.to_string(),
        sharded: matches!(message.kind, MessageKind::SMessage),
        payload,
        size,
    }
}

impl RedisSession {
    /// Subscribes on a new connection made with this session's settings.
    pub async fn subscribe(&self, kind: Subscribe, names: Vec<String>) -> Result<Subscription> {
        anyhow::ensure!(!names.is_empty(), "Name at least one channel or pattern.");
        let mut builder = Builder::from_config(self.config.clone());
        builder.with_connection_config(|connection| connection.max_command_attempts = 1);
        let client = builder.build()?;
        client
            .init()
            .await
            .context("Could not open the Pub/Sub connection")?;
        let mut messages = client.message_rx();
        let mut errors = client.error_rx();
        let subscribed = match kind {
            Subscribe::Patterns => client.psubscribe(names).await,
            Subscribe::Channels => client.subscribe(names).await,
            Subscribe::Shard => client.ssubscribe(names).await,
        };
        if let Err(error) = subscribed {
            client.quit().await.ok();
            return Err(error).context("Redis refused the subscription");
        }
        let (mut sender, events) = mpsc::channel(QUEUE);
        let (stop, mut stopped) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let mut lost = 0_u64;
            let ended = loop {
                // Report losses as soon as the view has room again.
                if lost > 0 && sender.try_send(PubSubEvent::Lost(lost)).is_ok() {
                    lost = 0;
                }
                tokio::select! {
                    _ = &mut stopped => break None,
                    received = messages.recv() => match received {
                        Ok(received) => {
                            if sender.try_send(PubSubEvent::Message(message(received))).is_err() {
                                if sender.is_closed() {
                                    break None;
                                }
                                lost += 1;
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => lost += skipped,
                        Err(RecvError::Closed) => break Some("The connection closed.".to_string()),
                    },
                    error = errors.recv() => match error {
                        Ok((error, _)) => break Some(format!("The connection dropped: {error}")),
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => break Some("The connection closed.".to_string()),
                    },
                }
            };
            if let Some(reason) = ended {
                if lost > 0 {
                    sender.try_send(PubSubEvent::Lost(lost)).ok();
                }
                sender.try_send(PubSubEvent::Ended(reason)).ok();
            }
            // Unsubscribing happens by closing; there is no one left to tell if that fails.
            client.quit().await.ok();
        });
        Ok(Subscription {
            events,
            stop: Stop { _sender: stop },
        })
    }

    /// PUBLISH (or SPUBLISH for a sharded channel), sent once. Returns how many subscribers
    /// received it, which may be 0.
    pub async fn publish(
        &self,
        channel: &str,
        payload: &[u8],
        sharded: bool,
    ) -> Result<i64, WriteError> {
        let payload = Value::Bytes(payload.to_vec().into());
        let sent = if sharded {
            self.client.spublish::<i64, _, _>(channel, payload).await
        } else {
            self.client.publish::<i64, _, _>(channel, payload).await
        };
        sent.map_err(crate::redis::classify_redis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;

    async fn next_message(subscription: &mut Subscription) -> PubSubMessage {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            subscription.events.next(),
        )
        .await
        .expect("a message within 5 seconds")
        .expect("the subscription is open");
        match event {
            PubSubEvent::Message(message) => message,
            other => panic!("expected a message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_against_a_server() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let mut patterns = session
            .subscribe(Subscribe::Patterns, vec!["vilo_test.ps.*".into()])
            .await
            .unwrap();
        let mut channels = session
            .subscribe(Subscribe::Channels, vec!["vilo_test.ps.exact".into()])
            .await
            .unwrap();
        assert_eq!(
            session
                .publish("vilo_test.ps.exact", b"hello", false)
                .await
                .unwrap(),
            2
        );
        let seen = next_message(&mut patterns).await;
        assert_eq!(
            (seen.channel.as_str(), seen.payload.as_slice()),
            ("vilo_test.ps.exact", &b"hello"[..])
        );
        assert_eq!(next_message(&mut channels).await.payload, b"hello");

        let big = vec![b'x'; MAX_MESSAGE_BYTES + 10];
        session
            .publish("vilo_test.ps.big", &big, false)
            .await
            .unwrap();
        let cut = next_message(&mut patterns).await;
        assert_eq!(
            (cut.payload.len(), cut.size),
            (MAX_MESSAGE_BYTES, MAX_MESSAGE_BYTES + 10)
        );

        // Dropping a subscription closes its connection: nobody is left on that channel.
        drop(channels);
        drop(patterns);
        let mut receivers = -1;
        for _ in 0..50 {
            receivers = session
                .publish("vilo_test.ps.exact", b"bye", false)
                .await
                .unwrap();
            if receivers == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(receivers, 0);

        let mut shard = session
            .subscribe(Subscribe::Shard, vec!["vilo_test.shard".into()])
            .await
            .unwrap();
        assert_eq!(
            session
                .publish("vilo_test.shard", b"s", true)
                .await
                .unwrap(),
            1
        );
        let sharded = next_message(&mut shard).await;
        assert!(sharded.sharded);
        assert!(
            session
                .subscribe(Subscribe::Channels, Vec::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_pubsub_on_a_cluster() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS_CLUSTER") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let mut patterns = session
            .subscribe(Subscribe::Patterns, vec!["vilo_test.cps.*".into()])
            .await
            .unwrap();
        session
            .publish("vilo_test.cps.a", b"one", false)
            .await
            .unwrap();
        assert_eq!(next_message(&mut patterns).await.payload, b"one");
        let mut shard = session
            .subscribe(Subscribe::Shard, vec!["vilo_test.cshard".into()])
            .await
            .unwrap();
        assert_eq!(
            session
                .publish("vilo_test.cshard", b"two", true)
                .await
                .unwrap(),
            1
        );
        assert_eq!(next_message(&mut shard).await.payload, b"two");
    }
}
