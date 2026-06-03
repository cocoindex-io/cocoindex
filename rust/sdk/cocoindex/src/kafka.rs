//! Kafka topic target connector.
//!
//! Topics are user-managed: CocoIndex never creates or drops them during target
//! reconciliation. Messages declared with
//! [`declare_message`](KafkaTopicTarget::declare_message) are reconciled against
//! the previous run:
//! * new or changed messages are produced,
//! * unchanged messages are skipped,
//! * messages declared in a previous run but not this run produce a tombstone
//!   record, or a custom deletion value via
//!   [`KafkaTopicOptions::deletion_value_fn`].
//!
//! Use [`declare_kafka_topic_target`] / [`mount_kafka_topic_target`] to get a
//! handle for declaring messages. [`KafkaProducer::ensure_topic`] is an
//! explicit, idempotent topic-creation helper outside reconciliation.
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
use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};
use crate::target_state::{
    StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    TargetStateProvider, declare_target_state, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// KafkaProducer — connection handle
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

/// Declare a Kafka topic target and return a ready same-component handle.
/// No external setup is needed, so this uses the same immediate provider path as
/// [`mount_kafka_topic_target`].
pub fn declare_kafka_topic_target(
    ctx: &Ctx,
    producer: &KafkaProducer,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    mount_kafka_topic_target(ctx, producer, topic, options)
}

/// Declare a Kafka topic target in the current component and return a handle
/// for declaring messages.
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
// Message handler (root)
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
/// Reconcile one message:
/// * unchanged known-present values are skipped,
/// * new or changed values are produced and fingerprinted,
/// * known-absent deletes are skipped,
/// * other deletes produce a tombstone or `deletion_value_fn(key)`.
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

// ---------------------------------------------------------------------------
// Kafka source — a live map feed over a topic (the Rust analogue of Python's
// `topic_as_map`).
// ---------------------------------------------------------------------------

/// A Kafka consumer handle. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct KafkaConsumer {
    client: Arc<Client>,
    state_id: Arc<str>,
}

impl KafkaConsumer {
    /// Connect to a Kafka (or Redpanda) cluster given its bootstrap servers.
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

    /// Stable identity (the bootstrap-server list).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

/// Decides whether a consumed record is a *deletion* for its key. Receives the
/// key bytes and the value (`None` for a tombstone). The default treats a
/// `None` value (tombstone) as a deletion.
pub type IsDeletionFn = Arc<dyn Fn(&[u8], Option<&[u8]>) -> bool + Send + Sync>;

/// Options for [`topic_as_map_with_options`].
#[derive(Clone, Default)]
pub struct KafkaSourceOptions {
    /// Custom deletion predicate; defaults to "value is a tombstone (`None`)".
    pub is_deletion: Option<IsDeletionFn>,
}

/// A keyed change feed over **all partitions** of a Kafka topic: the latest
/// value per key, with tombstones (or [`KafkaSourceOptions::is_deletion`])
/// removing keys. The Rust analogue of Python's `topic_as_map`.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads every partition up to
/// its high-watermark (the catch-up snapshot, compacted per key) and
/// [`watch`](LiveMapFeed::watch) tails new records across partitions from there.
/// Feed it to [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). Offsets are
/// tracked per partition, so readiness reflects every partition being caught up.
pub struct KafkaTopicMap {
    client: Arc<Client>,
    topic: String,
    is_deletion: Option<IsDeletionFn>,
    /// Per-partition offset `watch` resumes from — set to each partition's
    /// high-watermark reached by the most recent `scan`, so the tail neither
    /// gaps nor double-reads.
    watch_start: Arc<tokio::sync::Mutex<BTreeMap<i32, i64>>>,
}

/// Build a [`KafkaTopicMap`] over `topic` (tombstone deletes).
pub fn topic_as_map(consumer: &KafkaConsumer, topic: impl Into<String>) -> KafkaTopicMap {
    topic_as_map_with_options(consumer, topic, KafkaSourceOptions::default())
}

/// [`topic_as_map`] with a custom deletion predicate.
pub fn topic_as_map_with_options(
    consumer: &KafkaConsumer,
    topic: impl Into<String>,
    options: KafkaSourceOptions,
) -> KafkaTopicMap {
    KafkaTopicMap {
        client: consumer.client.clone(),
        topic: topic.into(),
        is_deletion: options.is_deletion,
        watch_start: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
    }
}

impl KafkaTopicMap {
    /// The topic's partition IDs (from cluster metadata).
    async fn partition_ids(&self) -> Result<Vec<i32>> {
        let topics = self
            .client
            .list_topics()
            .await
            .map_err(|e| Error::engine(format!("kafka list_topics: {e}")))?;
        let topic = topics
            .into_iter()
            .find(|t| t.name == self.topic)
            .ok_or_else(|| Error::engine(format!("kafka topic {:?} not found", self.topic)))?;
        let ids: Vec<i32> = topic.partitions.into_iter().collect();
        if ids.is_empty() {
            return Err(Error::engine(format!(
                "kafka topic {:?} has no partitions",
                self.topic
            )));
        }
        Ok(ids)
    }

    async fn partition_client(&self, id: i32) -> Result<PartitionClient> {
        self.client
            .partition_client(self.topic.clone(), id, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| Error::engine(format!("kafka partition_client {}/{id}: {e}", self.topic)))
    }

    /// Classify a record into `(key, Some(value))` upsert or `(key, None)`
    /// delete. Returns `None` for a record with no key (skipped, as in Python).
    fn classify(&self, record: &Record) -> Option<(String, Option<Vec<u8>>)> {
        let key_bytes = record.key.as_ref()?;
        let key = String::from_utf8_lossy(key_bytes).into_owned();
        let is_delete = match &self.is_deletion {
            Some(f) => f(key_bytes, record.value.as_deref()),
            None => record.value.is_none(),
        };
        if is_delete {
            Some((key, None))
        } else {
            Some((key, record.value.clone()))
        }
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for KafkaTopicMap {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        let mut offsets = self.watch_start.lock().await.clone();
        let ids = self.partition_ids().await?;
        let mut clients = Vec::with_capacity(ids.len());
        for id in &ids {
            clients.push((*id, self.partition_client(*id).await?));
        }
        // Round-robin all partitions on one loop (one subscriber, used
        // sequentially). Each partition fetch uses a short max-wait so the
        // cycle stays responsive; an empty full cycle backs off briefly.
        loop {
            let mut got_any = false;
            for (id, pc) in &clients {
                let offset = *offsets.get(id).unwrap_or(&0);
                let (records, _hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 250)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                for r in &records {
                    got_any = true;
                    offsets.insert(*id, r.offset + 1);
                    let Some((key, value)) = self.classify(&r.record) else {
                        continue;
                    };
                    match value {
                        Some(v) => subscriber.update(key, v).await?,
                        None => subscriber.delete(key).await?,
                    }
                }
            }
            if !got_any {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for KafkaTopicMap {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let ids = self.partition_ids().await?;
        let mut latest: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut ends: BTreeMap<i32, i64> = BTreeMap::new();
        for id in ids {
            let pc = self.partition_client(id).await?;
            let mut offset = 0i64;
            loop {
                let (records, hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 500)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                for r in &records {
                    offset = r.offset + 1;
                    if let Some((key, value)) = self.classify(&r.record) {
                        match value {
                            Some(v) => {
                                latest.insert(key, v);
                            }
                            None => {
                                latest.remove(&key);
                            }
                        }
                    }
                }
                if records.is_empty() || offset >= hwm {
                    break;
                }
            }
            ends.insert(id, offset);
        }
        // `watch` resumes exactly where this per-partition snapshot ended.
        *self.watch_start.lock().await = ends;
        Ok(latest.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------
// Keyless payload stream (the Rust analogue of Python's
// `topic_as_stream(...).payloads()`).
// ---------------------------------------------------------------------------

/// A **keyless, append-only** payload feed over **all partitions** of a Kafka
/// topic — the Rust analogue of Python's `topic_as_stream(...).payloads()`.
///
/// Unlike [`KafkaTopicMap`], nothing is compacted or deleted: every record is
/// delivered exactly once, keyed by its `"{partition}:{offset}"` position, so
/// each message becomes a distinct child under
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). Records with a `None`
/// value (tombstones) are skipped, matching Python's payloads view. No message
/// key or deletion predicate is needed.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads every partition up to
/// its high-watermark and [`watch`](LiveMapFeed::watch) tails new records from
/// there. Re-running is idempotent: the offset keys are stable, so a replayed
/// catch-up reconciles to no-ops.
pub struct KafkaTopicStream {
    client: Arc<Client>,
    topic: String,
    /// Per-partition offset `watch` resumes from (the high-watermark `scan`
    /// reached), mirroring [`KafkaTopicMap`].
    watch_start: Arc<tokio::sync::Mutex<BTreeMap<i32, i64>>>,
}

/// Build a [`KafkaTopicStream`] over `topic` (keyless payload stream).
pub fn topic_as_stream(consumer: &KafkaConsumer, topic: impl Into<String>) -> KafkaTopicStream {
    KafkaTopicStream {
        client: consumer.client.clone(),
        topic: topic.into(),
        watch_start: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
    }
}

impl KafkaTopicStream {
    async fn partition_ids(&self) -> Result<Vec<i32>> {
        let topics = self
            .client
            .list_topics()
            .await
            .map_err(|e| Error::engine(format!("kafka list_topics: {e}")))?;
        let topic = topics
            .into_iter()
            .find(|t| t.name == self.topic)
            .ok_or_else(|| Error::engine(format!("kafka topic {:?} not found", self.topic)))?;
        let ids: Vec<i32> = topic.partitions.into_iter().collect();
        if ids.is_empty() {
            return Err(Error::engine(format!(
                "kafka topic {:?} has no partitions",
                self.topic
            )));
        }
        Ok(ids)
    }

    async fn partition_client(&self, id: i32) -> Result<PartitionClient> {
        self.client
            .partition_client(self.topic.clone(), id, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| Error::engine(format!("kafka partition_client {}/{id}: {e}", self.topic)))
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for KafkaTopicStream {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let ids = self.partition_ids().await?;
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        let mut ends: BTreeMap<i32, i64> = BTreeMap::new();
        for id in ids {
            let pc = self.partition_client(id).await?;
            let mut offset = 0i64;
            loop {
                let (records, hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 500)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                for r in &records {
                    offset = r.offset + 1;
                    if let Some(v) = &r.record.value {
                        out.push((format!("{id}:{}", r.offset), v.clone()));
                    }
                }
                if records.is_empty() || offset >= hwm {
                    break;
                }
            }
            ends.insert(id, offset);
        }
        *self.watch_start.lock().await = ends;
        Ok(out)
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for KafkaTopicStream {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        let mut offsets = self.watch_start.lock().await.clone();
        let ids = self.partition_ids().await?;
        let mut clients = Vec::with_capacity(ids.len());
        for id in &ids {
            clients.push((*id, self.partition_client(*id).await?));
        }
        loop {
            let mut got_any = false;
            for (id, pc) in &clients {
                let offset = *offsets.get(id).unwrap_or(&0);
                let (records, _hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 250)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                for r in &records {
                    got_any = true;
                    offsets.insert(*id, r.offset + 1);
                    if let Some(v) = &r.record.value {
                        subscriber
                            .update(format!("{id}:{}", r.offset), v.clone())
                            .await?;
                    }
                }
            }
            if !got_any {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
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
