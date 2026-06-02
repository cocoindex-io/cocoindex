//! Kafka target connector — the Rust analogue of Python's
//! `cocoindex.connectors.kafka` target.
//!
//! A [`KafkaTopicTarget`] is a *declarative* target: messages you
//! [`declare_message`](KafkaTopicTarget::declare_message) are reconciled against
//! the previous run by CocoIndex's target-state engine:
//! * new or changed messages are produced,
//! * unchanged messages are skipped (nothing re-produced),
//! * messages declared in a previous run but **not** this run (their source was
//!   deleted) produce a *tombstone* (a record with a null value), or a custom
//!   deletion value via [`KafkaTopicOptions::deletion_value_fn`].
//!
//! The topic itself is **user-managed** — like the Python connector, CocoIndex
//! never creates or drops topics during reconciliation. [`KafkaProducer::ensure_topic`]
//! is provided as an explicit, idempotent convenience (e.g. for examples/tests).
//!
//! Uses [`rskafka`] — a pure-Rust, async Kafka client with no `librdkafka`/C
//! dependency.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use cocoindex_core::engine::target_state::{TargetReconcileOutput, TargetStateProvider};
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

// ---------------------------------------------------------------------------
// KafkaProducer — a connection handle (mirrors `postgres::Database`)
// ---------------------------------------------------------------------------

/// A Kafka connection handle. Clone-cheap (the underlying client is shared).
///
/// `state_id` (the bootstrap-server list) is used as the *stable identity* for
/// target-state keys, decoupling target identity from the live connection.
#[derive(Clone)]
pub struct KafkaProducer {
    client: Arc<Client>,
    state_id: Arc<str>,
}

impl KafkaProducer {
    /// Connect to a Kafka (or Redpanda) cluster given its bootstrap servers,
    /// e.g. `["localhost:9092"]`.
    pub async fn connect(bootstrap_servers: &[&str]) -> Result<Self> {
        let boot: Vec<String> = bootstrap_servers.iter().map(|s| s.to_string()).collect();
        if boot.is_empty() {
            return Err(Error::engine(
                "kafka: at least one bootstrap server is required",
            ));
        }
        let state_id = boot.join(",");
        let client = ClientBuilder::new(boot)
            .build()
            .await
            .map_err(|e| Error::engine(format!("kafka connect: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            state_id: Arc::from(state_id),
        })
    }

    /// Stable identity used in target-state keys (the bootstrap-server list).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// Create `topic` with `num_partitions` if it does not already exist.
    ///
    /// Idempotent and explicit — this is *not* part of reconciliation (CocoIndex
    /// treats topics as user-managed). Replication factor is 1 (suitable for a
    /// single-broker dev cluster).
    pub async fn ensure_topic(&self, topic: &str, num_partitions: i32) -> Result<()> {
        if self.topic_exists(topic).await? {
            return Ok(());
        }
        let controller = self
            .client
            .controller_client()
            .map_err(|e| Error::engine(format!("kafka controller: {e}")))?;
        match controller
            .create_topic(topic, num_partitions, 1, 5_000)
            .await
        {
            Ok(_) => Ok(()),
            // Tolerate a concurrent create (another client won the race).
            Err(e) => {
                if self.topic_exists(topic).await? {
                    Ok(())
                } else {
                    Err(Error::engine(format!("kafka create_topic {topic:?}: {e}")))
                }
            }
        }
    }

    async fn topic_exists(&self, topic: &str) -> Result<bool> {
        let topics = self
            .client
            .list_topics()
            .await
            .map_err(|e| Error::engine(format!("kafka list_topics: {e}")))?;
        Ok(topics.iter().any(|t| t.name == topic))
    }
}

// ---------------------------------------------------------------------------
// Public API: options + target
// ---------------------------------------------------------------------------

/// Callback producing the value of a deletion record for a given message key.
pub type DeletionValueFn = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;

/// Options for [`mount_kafka_topic_target`].
#[derive(Clone, Default)]
pub struct KafkaTopicOptions {
    /// How to represent a deletion. `None` (default) produces a *tombstone* — a
    /// record with the message key and a null value. When set, the callback's
    /// return value is used as the record value instead.
    pub deletion_value_fn: Option<DeletionValueFn>,
}

/// A declarative Kafka topic target. See the [module docs](self).
#[derive(Clone)]
pub struct KafkaTopicTarget {
    provider: TargetStateProvider<RustProfile>,
    topic: Arc<str>,
}

/// Mount a declarative Kafka topic target. Declared messages are produced on
/// commit; unchanged messages are skipped; orphaned messages produce tombstones.
///
/// Must be called inside an `App::update()`/`App::run()` pipeline.
pub fn mount_kafka_topic_target(
    ctx: &Ctx,
    producer: &KafkaProducer,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    let topic = topic.into();
    let provider = ctx.register_root_target_provider(
        format!("cocoindex/kafka/topic/{}/{}", producer.state_id(), topic),
        message_handler(
            producer.client.clone(),
            topic.clone(),
            options.deletion_value_fn,
        ),
    )?;
    Ok(KafkaTopicTarget {
        provider,
        topic: Arc::from(topic),
    })
}

impl KafkaTopicTarget {
    /// The target topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Declare that the topic should contain a message with `key` and `value`.
    ///
    /// The actual produce/skip/tombstone decision is made by the engine during
    /// reconciliation: a message is (re)produced only when its value changed
    /// since the last run.
    pub fn declare_message(&self, ctx: &Ctx, key: &str, value: impl AsRef<[u8]>) -> Result<()> {
        if key.is_empty() {
            return Err(Error::engine(
                "kafka declare_message: key must be non-empty",
            ));
        }
        ctx.declare_target_state(
            self.provider.clone(),
            StableKey::Str(Arc::from(key)),
            Value::from_serializable(&value.as_ref().to_vec())?,
        )
    }
}

// ---------------------------------------------------------------------------
// Reconcile logic (pure — unit-testable without a broker)
// ---------------------------------------------------------------------------

/// What the sink should do for one message: produce a record with this value
/// (`None` value = tombstone).
#[derive(Serialize, Deserialize)]
struct MessageAction {
    key: String,
    value: Option<Vec<u8>>,
}

/// The reconcile outcome for a single message, independent of the SDK's
/// `Value`/sink machinery so it can be unit-tested directly.
#[derive(Debug, PartialEq)]
struct MessageDecision {
    /// Record value to produce: `Some(bytes)` for an upsert or a custom deletion
    /// value, `None` for a tombstone.
    value: Option<Vec<u8>>,
    /// Tracking record to persist: `Some(fp)` for an upsert, `None` for a delete.
    tracking: Option<Fingerprint>,
}

/// Decide whether (and how) to act on one message. Returns `None` to skip.
///
/// Mirrors the Python `_MessageHandler.reconcile` semantics:
/// * unchanged (all prev fingerprints match, and prev is known-present) → skip
/// * upsert (new/changed) → produce the value, track its fingerprint
/// * delete with no previous record and prev known-present → skip
/// * delete otherwise → tombstone (or `deletion_value_fn(key)`), no tracking
fn decide_message(
    key: &str,
    desired_value: Option<&[u8]>,
    prev_fps: &[Fingerprint],
    prev_may_be_missing: bool,
    deletion_value_fn: Option<&DeletionValueFn>,
) -> Result<Option<MessageDecision>> {
    let desired_fp = match desired_value {
        Some(v) => Some(Fingerprint::from(&v.to_vec()).map_err(Error::from)?),
        None => None,
    };

    // Upsert path.
    if let Some(value) = desired_value {
        // Skip only when we are certain the message is present and every previous
        // tracking record matches the desired fingerprint.
        let fp = desired_fp
            .as_ref()
            .expect("desired_fp set when value present");
        if !prev_may_be_missing && !prev_fps.is_empty() && prev_fps.iter().all(|p| p == fp) {
            return Ok(None);
        }
        return Ok(Some(MessageDecision {
            value: Some(value.to_vec()),
            tracking: desired_fp,
        }));
    }

    // Delete path.
    if prev_fps.is_empty() && !prev_may_be_missing {
        return Ok(None);
    }
    let value = deletion_value_fn.map(|f| f(key));
    Ok(Some(MessageDecision {
        value,
        tracking: None,
    }))
}

fn message_handler(
    client: Arc<Client>,
    topic: String,
    deletion_value_fn: Option<DeletionValueFn>,
) -> BoxedHandler {
    let sink = message_sink(client, topic);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let StableKey::Str(key) = &key else {
            return Err(cocoindex_utils::error::Error::internal_msg(format!(
                "unexpected kafka message key: {key:?}"
            )));
        };
        let key = key.to_string();

        let desired_value: Option<Vec<u8>> = desired
            .map(Value::deserialize::<Vec<u8>>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev_fps: Vec<Fingerprint> = prev
            .iter()
            .filter_map(|v| v.deserialize::<Fingerprint>().ok())
            .collect();

        let decision = decide_message(
            &key,
            desired_value.as_deref(),
            &prev_fps,
            prev_may_be_missing,
            deletion_value_fn.as_ref(),
        )
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;

        let Some(decision) = decision else {
            return Ok(None);
        };

        let tracking_record = match &decision.tracking {
            Some(fp) => Some(
                Value::from_serializable(fp)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            None => None,
        };
        let action = MessageAction {
            key,
            value: decision.value,
        };
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&action)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record,
            child_invalidation: None,
        }))
    })
}

fn message_sink(client: Arc<Client>, topic: String) -> BoxedSink {
    // One PartitionClient per sink, opened lazily on first apply.
    let partition: Arc<OnceCell<Arc<PartitionClient>>> = Arc::new(OnceCell::new());
    BoxedSink::new(move |actions| {
        let client = client.clone();
        let topic = topic.clone();
        let partition = partition.clone();
        Box::pin(async move {
            if actions.is_empty() {
                return Ok(None);
            }
            let pc = partition
                .get_or_try_init(|| async {
                    client
                        .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
                        .await
                        .map(Arc::new)
                })
                .await
                .map_err(|e| {
                    cocoindex_utils::error::Error::internal_msg(format!(
                        "kafka partition_client: {e}"
                    ))
                })?;

            let timestamp = current_timestamp();
            let mut records = Vec::with_capacity(actions.len());
            for action in actions {
                let inner = match action {
                    Action::Create(v) | Action::Update(v) | Action::Delete(v) => v,
                };
                let msg: MessageAction = inner
                    .deserialize()
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
                records.push(Record {
                    key: Some(msg.key.into_bytes()),
                    value: msg.value,
                    headers: BTreeMap::new(),
                    timestamp,
                });
            }
            pc.produce(records, Compression::NoCompression)
                .await
                .map_err(|e| {
                    cocoindex_utils::error::Error::internal_msg(format!("kafka produce: {e}"))
                })?;
            Ok(None)
        }) as Pin<Box<_>>
    })
}

/// Current wall-clock time as a chrono UTC timestamp, without requiring chrono's
/// `clock` feature.
fn current_timestamp() -> chrono::DateTime<chrono::Utc> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    chrono::DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(bytes: &[u8]) -> Fingerprint {
        Fingerprint::from(&bytes.to_vec()).unwrap()
    }

    // U1: upsert new state (no previous record, prev_may_be_missing).
    #[test]
    fn upsert_new_state() {
        let d = decide_message("k1", Some(b"v1"), &[], true, None)
            .unwrap()
            .expect("should produce");
        assert_eq!(d.value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(d.tracking, Some(fp(b"v1")));
    }

    // U2: unchanged value with a matching previous fingerprint → skip.
    #[test]
    fn upsert_unchanged_skips() {
        let d = decide_message("k1", Some(b"v1"), &[fp(b"v1")], false, None).unwrap();
        assert_eq!(d, None);
    }

    // U3: changed value → produce the new value.
    #[test]
    fn upsert_changed_value() {
        let d = decide_message("k1", Some(b"v2"), &[fp(b"v1")], false, None)
            .unwrap()
            .expect("should produce");
        assert_eq!(d.value.as_deref(), Some(&b"v2"[..]));
        assert_eq!(d.tracking, Some(fp(b"v2")));
    }

    // U4: prev_may_be_missing forces a produce even when the fingerprint matches.
    #[test]
    fn upsert_prev_may_be_missing_forces_produce() {
        let d = decide_message("k1", Some(b"v1"), &[fp(b"v1")], true, None).unwrap();
        assert!(d.is_some());
    }

    // U5: delete without a callback → tombstone (null value), no tracking record.
    #[test]
    fn delete_without_callback_is_tombstone() {
        let d = decide_message("k1", None, &[fp(b"v1")], false, None)
            .unwrap()
            .expect("should tombstone");
        assert_eq!(d.value, None);
        assert_eq!(d.tracking, None);
    }

    // U6: delete with a deletion_value_fn → produce that value.
    #[test]
    fn delete_with_callback_uses_value() {
        let f: DeletionValueFn = Arc::new(|k: &str| format!("deleted:{k}").into_bytes());
        let d = decide_message("k1", None, &[fp(b"v1")], false, Some(&f))
            .unwrap()
            .expect("should produce deletion value");
        assert_eq!(d.value.as_deref(), Some(&b"deleted:k1"[..]));
        assert_eq!(d.tracking, None);
    }

    // U7: delete with no previous record and prev known-present → skip.
    #[test]
    fn delete_no_prev_skips() {
        let d = decide_message("k1", None, &[], false, None).unwrap();
        assert_eq!(d, None);
    }
}
