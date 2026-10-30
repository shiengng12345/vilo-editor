//! Pub/Sub, as RedisInsight's: a subscription on its own connection, so the main session keeps
//! answering, and PUBLISH. Nothing is replayed: messages missed while the view lagged or the
//! connection dropped are counted and reported as lost.

use anyhow::{Context as _, Result};
use fred::{
    interfaces::{ClientLike as _, EventInterface as _, PubsubInterface as _},
    prelude::{Builder, Value},
    types::MessageKind,
};
use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::broadcast::error::RecvError;

use crate::{
    mongo::WriteError,
    redis::{Bytes, RedisSession},
};

/// A message larger than this is cut, and says so.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;
/// Message bytes waiting for the view; messages beyond this are dropped and counted as lost.
/// Notes about losses and the end are always delivered, and are tiny.
const QUEUE_BYTES: usize = 8 * 1024 * 1024;
/// What a note (losses, the end) counts against the queue budget.
const NOTE_COST: usize = 64;
/// How soon losses are reported when no further message comes to carry them.
const LOSS_REPORT: std::time::Duration = std::time::Duration::from_millis(250);

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

impl PubSubMessage {
    /// What keeping the message costs, counted against the queue and view budgets: its payload
    /// and channel plus a fixed share for the entry itself, so empty messages are not free.
    pub fn cost(&self) -> usize {
        self.payload.len() + self.channel.len() + 64
    }
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
    pub events: Events,
    stop: Stop,
}

/// The subscription's events in order. Taking a message frees its bytes in the queue budget.
pub struct Events {
    receiver: mpsc::UnboundedReceiver<PubSubEvent>,
    queued: Arc<AtomicUsize>,
}

impl Events {
    fn taken(&self, event: Option<PubSubEvent>) -> Option<PubSubEvent> {
        let cost = match &event {
            Some(PubSubEvent::Message(message)) => message.cost(),
            Some(PubSubEvent::Lost(_)) => NOTE_COST,
            _ => 0,
        };
        self.queued.fetch_sub(cost, Ordering::Relaxed);
        event
    }

    /// The next event; `None` once the subscription has ended and everything was taken.
    pub async fn next(&mut self) -> Option<PubSubEvent> {
        let event = self.receiver.next().await;
        self.taken(event)
    }

    /// An event already waiting, if any.
    pub fn try_next(&mut self) -> Option<PubSubEvent> {
        let event = self.receiver.try_recv().ok();
        self.taken(event)
    }
}

pub struct Stop {
    _sender: oneshot::Sender<()>,
}

impl Subscription {
    pub fn into_parts(self) -> (Events, Stop) {
        (self.events, self.stop)
    }
}

fn message(message: fred::types::Message) -> PubSubMessage {
    let bytes = match &message.value {
        Value::Bytes(bytes) => bytes.to_vec(),
        Value::String(text) => text.as_bytes().to_vec(),
        other => other
            .as_string()
            .unwrap_or_else(|| format!("{other:?}"))
            .into_bytes(),
    };
    let size = bytes.len();
    // A copy of the kept part, so a cut message does not hold on to its full allocation.
    let payload = bytes[..size.min(MAX_MESSAGE_BYTES)].to_vec();
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
        anyhow::ensure!(
            !names.is_empty() && names.iter().all(|name| !name.is_empty()),
            "Name at least one channel or pattern."
        );
        let mut builder = Builder::from_config(self.config.clone());
        builder.with_connection_config(|connection| connection.max_command_attempts = 1);
        builder.with_performance_config(|performance| {
            performance.default_command_timeout = crate::redis::COMMAND_TIMEOUT;
        });
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
        let (sender, receiver) = mpsc::unbounded();
        let queued = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = oneshot::channel::<()>();
        tokio::spawn({
            let queued = queued.clone();
            async move {
                let mut lost = 0_u64;
                let mut report = tokio::time::interval(LOSS_REPORT);
                // A loss note waits, still counting, while the queue is full, so a stalled view
                // cannot pile notes up. `false` means the view dropped the subscription.
                let flush = |lost: &mut u64| {
                    if *lost > 0 && queued.load(Ordering::Relaxed) + NOTE_COST <= QUEUE_BYTES {
                        queued.fetch_add(NOTE_COST, Ordering::Relaxed);
                        let sent = sender.unbounded_send(PubSubEvent::Lost(*lost)).is_ok();
                        *lost = 0;
                        return sent;
                    }
                    true
                };
                let ended = loop {
                    tokio::select! {
                        _ = &mut stopped => break None,
                        _ = report.tick() => if !flush(&mut lost) {
                            break None;
                        },
                        received = messages.recv() => match received {
                            Ok(received) => {
                                let received = message(received);
                                let size = received.cost();
                                if queued.load(Ordering::Relaxed) + size > QUEUE_BYTES {
                                    lost += 1;
                                    continue;
                                }
                                // Losses go before the message that follows them.
                                if !flush(&mut lost) {
                                    break None;
                                }
                                queued.fetch_add(size, Ordering::Relaxed);
                                if sender.unbounded_send(PubSubEvent::Message(received)).is_err() {
                                    break None;
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
                    // The last losses and the end always go out, so the view never shows a
                    // finished subscription as live or hides what it missed.
                    // A failed send here means the view is gone; the connection closes below anyway.
                    if lost > 0 {
                        sender.unbounded_send(PubSubEvent::Lost(lost)).ok();
                    }
                    sender.unbounded_send(PubSubEvent::Ended(reason)).ok();
                }
                // Unsubscribing happens by closing; there is no one left to tell if that fails.
                client.quit().await.ok();
            }
        });
        let events = Events { receiver, queued };
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
        if channel.is_empty() {
            return Err(WriteError::Refused("Name the channel.".into()));
        }
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

    /// Subscriptions are registered by the server a moment after fred returns; wait for them.
    async fn wait_for_subscribers(session: &RedisSession, channel: &str, count: i64) {
        for _ in 0..100 {
            let reply = session
                .call("PUBSUB", vec!["NUMSUB".into(), channel.into()])
                .await
                .unwrap();
            let found = reply.into_array().get(1).and_then(|count| count.as_i64());
            if found >= Some(count) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("{channel} never had {count} subscribers");
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
        wait_for_subscribers(&session, "vilo_test.ps.exact", 1).await;
        for _ in 0..100 {
            let patterns = session.call("PUBSUB", vec!["NUMPAT".into()]).await.unwrap();
            if patterns.as_i64() >= Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
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
        assert!(
            session
                .subscribe(Subscribe::Channels, vec![String::new()])
                .await
                .is_err()
        );
        assert!(matches!(
            session.publish("", b"x", false).await,
            Err(WriteError::Refused(_))
        ));
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
        // In a Cluster the pattern is registered on one node a moment later; publish until it
        // arrives instead of guessing how long that takes.
        let mut received = None;
        for _ in 0..25 {
            session
                .publish("vilo_test.cps.a", b"one", false)
                .await
                .unwrap();
            if let Ok(Some(PubSubEvent::Message(message))) = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                patterns.events.next(),
            )
            .await
            {
                received = Some(message);
                break;
            }
        }
        assert_eq!(
            received.map(|message| message.payload),
            Some(b"one".to_vec())
        );
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
