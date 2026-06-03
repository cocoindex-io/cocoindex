//! Kafka target connector — the Rust analogue of Python's
//! `cocoindex.connectors.kafka` target.
//!
//! A two-level declarative managed target built **on the public target-state
//! facade** ([`crate::target_state`]): a *topic* container (user-managed —
//! CocoIndex never creates/drops it) whose child *messages* you
//! [`declare_message`](KafkaTopicTarget::declare_message) are reconciled against
//! the previous run:
//! * new or changed messages are produced,
//! * unchanged messages are skipped (fingerprint tracking — nothing re-produced),
//! * messages declared in a previous run but **not** this run produce a
//!   *tombstone* (a record with a null value), or a custom deletion value via
//!   [`KafkaTopicOptions::deletion_value_fn`].
//!
//! Mirroring Python, the connector exposes the constructor/declaration/mount
//! split: [`kafka_topic_target`] builds the (composable) [`TargetState`],
//! [`declare_kafka_topic_target`] declares it in the current component, and
//! [`mount_kafka_topic_target`] is the compatibility convenience for the same
//! declaration path. [`KafkaProducer::ensure_topic`] is an explicit, idempotent
//! topic-creation convenience (not part of reconciliation).
//!
//! Uses [`rskafka`] — a pure-Rust, async Kafka client with no `librdkafka`/C
//! dependency.

use std::collections::BTreeMap;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    register_root_target_states_provider,
};

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
// Public API: options + target + the constructor/declaration/mount split
// ---------------------------------------------------------------------------

/// Callback producing the value of a deletion record for a given message key.
pub type DeletionValueFn = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;

/// Options for the Kafka topic target.
#[derive(Clone, Default)]
pub struct KafkaTopicOptions {
    /// How to represent a deletion. `None` (default) produces a *tombstone* — a
    /// record with the message key and a null value. When set, the callback's
    /// return value is used as the record value instead.
    pub deletion_value_fn: Option<DeletionValueFn>,
}

/// A declarative Kafka topic target — a handle to declare messages on. See the
/// [module docs](self).
#[derive(Clone)]
pub struct KafkaTopicTarget {
    messages: TargetStateProvider<Vec<u8>>,
    topic: Arc<str>,
}

/// Build a composable [`TargetState`] for a Kafka topic (the spec constructor,
/// analogous to Python's `kafka_topic_target`). Pass it to
/// [`declare_kafka_topic_target`]/[`mount_kafka_topic_target`], or to the generic
/// [`declare_target_state_with_child`]/[`mount_target`].
pub fn kafka_topic_target(
    ctx: &Ctx,
    producer: &KafkaProducer,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<TargetState<TopicSpec>> {
    let topic = topic.into();
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/kafka/topic_spec/{}/{}",
            producer.state_id(),
            topic
        ),
        TopicHandler::new(producer.client.clone(), options.deletion_value_fn),
    )?;
    Ok(provider.target_state("default", TopicSpec { topic }))
}

/// Declare a Kafka topic target and return a ready same-component handle.
/// Kept synchronous for compatibility; internally this uses the same immediate
/// provider path as [`mount_kafka_topic_target`].
pub fn declare_kafka_topic_target(
    ctx: &Ctx,
    producer: &KafkaProducer,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    mount_kafka_topic_target(ctx, producer, topic, options)
}

/// Compatibility convenience for declaring a Kafka topic target in the current
/// component and returning a handle for declaring messages.
pub fn mount_kafka_topic_target(
    ctx: &Ctx,
    producer: &KafkaProducer,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    let topic = topic.into();
    let messages = register_root_target_states_provider(
        ctx,
        format!("cocoindex/kafka/topic/{}/{}", producer.state_id(), topic),
        MessageHandler::new(
            producer.client.clone(),
            topic.clone(),
            options.deletion_value_fn,
        ),
    )?;
    Ok(KafkaTopicTarget {
        messages,
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
        declare_target_state(
            ctx,
            self.messages.target_state(key, value.as_ref().to_vec()),
        )
    }
}

// ---------------------------------------------------------------------------
// Topic container handler (root)
// ---------------------------------------------------------------------------

/// The topic container spec (the topic name). Tracking record + spec are the
/// same: the topic is user-managed, so the container action never creates/drops.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSpec {
    topic: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TopicAction {
    topic: String,
}

struct TopicHandler {
    client: Arc<Client>,
    deletion_value_fn: Option<DeletionValueFn>,
}

impl TopicHandler {
    fn new(client: Arc<Client>, deletion_value_fn: Option<DeletionValueFn>) -> Self {
        Self {
            client,
            deletion_value_fn,
        }
    }
}

impl TargetHandler<TopicSpec> for TopicHandler {
    type TrackingRecord = TopicSpec;
    type Action = TopicAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<TopicSpec>,
        _prev: Vec<TopicSpec>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TopicAction, TopicSpec>>> {
        // Always emit when the topic is declared, so the sink runs and fulfills
        // the message child provider. The topic itself is user-managed (no
        // create/drop), so there is nothing to do on un-declare.
        let Some(spec) = desired else {
            return Ok(None);
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(TopicAction {
                topic: spec.topic.clone(),
            }),
            sink: self.topic_sink(),
            tracking_record: Some(spec),
            child_invalidation: None,
        }))
    }
}

impl TopicHandler {
    /// Container sink: fulfills each declared topic with a fresh message child
    /// handler bound to that topic.
    fn topic_sink(&self) -> TargetActionSink<TopicAction> {
        let client = self.client.clone();
        let deletion_value_fn = self.deletion_value_fn.clone();
        TargetActionSink::from_async_fn_with_children(
            move |actions: Vec<TargetAction<TopicAction>>| {
                let client = client.clone();
                let deletion_value_fn = deletion_value_fn.clone();
                async move {
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        match action {
                            TargetAction::Create(a) | TargetAction::Update(a) => {
                                out.push(Some(ChildTargetDef::new::<Vec<u8>, _>(
                                    MessageHandler::new(
                                        client.clone(),
                                        a.topic,
                                        deletion_value_fn.clone(),
                                    ),
                                )));
                            }
                            // Topic un-declared: user-managed, nothing to drop.
                            TargetAction::Delete(_) => out.push(None),
                        }
                    }
                    Ok(out)
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Message handler (child)
// ---------------------------------------------------------------------------

/// What the sink should do for one message: produce a record with this value
/// (`None` value = tombstone).
#[derive(Serialize, Deserialize)]
struct MessageAction {
    key: String,
    value: Option<Vec<u8>>,
}

struct MessageHandler {
    client: Arc<Client>,
    topic: String,
    deletion_value_fn: Option<DeletionValueFn>,
}

impl MessageHandler {
    fn new(client: Arc<Client>, topic: String, deletion_value_fn: Option<DeletionValueFn>) -> Self {
        Self {
            client,
            topic,
            deletion_value_fn,
        }
    }
}

impl TargetHandler<Vec<u8>> for MessageHandler {
    type TrackingRecord = Fingerprint;
    type Action = MessageAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<Vec<u8>>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<MessageAction, Fingerprint>>> {
        let StableKey::Str(key) = &key else {
            return Err(Error::engine(format!(
                "unexpected kafka message key: {key:?}"
            )));
        };
        let key = key.to_string();

        let Some(decision) = decide_message(
            &key,
            desired.as_deref(),
            &prev,
            prev_may_be_missing,
            self.deletion_value_fn.as_ref(),
        )?
        else {
            return Ok(None);
        };

        let action = MessageAction {
            key,
            value: decision.value,
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(action),
            sink: self.message_sink(),
            tracking_record: decision.tracking,
            child_invalidation: None,
        }))
    }
}

impl MessageHandler {
    fn message_sink(&self) -> TargetActionSink<MessageAction> {
        let client = self.client.clone();
        let topic = self.topic.clone();
        // One PartitionClient per sink, opened lazily on first apply.
        let partition: Arc<OnceCell<Arc<PartitionClient>>> = Arc::new(OnceCell::new());
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<MessageAction>>| {
            let client = client.clone();
            let topic = topic.clone();
            let partition = partition.clone();
            async move {
                if actions.is_empty() {
                    return Ok(());
                }
                let pc = partition
                    .get_or_try_init(|| async {
                        client
                            .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
                            .await
                            .map(Arc::new)
                    })
                    .await
                    .map_err(|e| Error::engine(format!("kafka partition_client: {e}")))?;

                let timestamp = current_timestamp();
                let mut records = Vec::with_capacity(actions.len());
                for action in actions {
                    let msg = match action {
                        TargetAction::Create(m)
                        | TargetAction::Update(m)
                        | TargetAction::Delete(m) => m,
                    };
                    records.push(Record {
                        key: Some(msg.key.into_bytes()),
                        value: msg.value,
                        headers: BTreeMap::new(),
                        timestamp,
                    });
                }
                pc.produce(records, Compression::NoCompression)
                    .await
                    .map_err(|e| Error::engine(format!("kafka produce: {e}")))?;
                Ok(())
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Reconcile logic (pure — unit-testable without a broker)
// ---------------------------------------------------------------------------

/// The reconcile outcome for a single message, independent of the SDK machinery
/// so it can be unit-tested directly.
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
