//! Apache Iggy topic target connector.
//!
//! Streams and topics are user-managed: CocoIndex never creates or drops them
//! during target reconciliation. Messages declared with
//! [`declare_message`](IggyTopicTarget::declare_message) are reconciled against
//! the previous run:
//! * new or changed messages are sent,
//! * unchanged messages are skipped,
//! * messages declared in a previous run but not this run are deleted by sending
//!   a custom deletion value via [`IggyTopicOptions::deletion_value_fn`].
//!
//! Unlike Kafka, Iggy has **no tombstone** (null-value) concept, so a delete
//! requires a `deletion_value_fn`; deleting a declared message without one is an
//! error (mirroring the Python `iggy` connector).
//!
//! Use [`declare_iggy_topic_target`] / [`mount_iggy_topic_target`] to get a handle
//! for declaring messages.
//!
//! This is the Rust analogue of Python's `cocoindex.connectors.iggy` target,
//! plus a keyed-map source over one topic partition.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use cocoindex_utils::fingerprint::Fingerprint;
use iggy::prelude::{
    Client, Consumer, Identifier, IggyClient, IggyMessage, MessageClient, Partitioning,
    PollingStrategy, TopicClient,
};
use serde::{Deserialize, Serialize};

/// Re-export of the upstream [`iggy`] crate prelude. Streams and topics are
/// user-managed, so callers use this to create/manage them and to poll messages
/// back — without having to depend on the `iggy` crate directly. e.g.
/// `cocoindex::iggy::prelude::{StreamClient, TopicClient}`.
pub use ::iggy::prelude;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};
use crate::target_state::{
    StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    TargetStateProvider, declare_target_state, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// IggyProducer — connection handle
// ---------------------------------------------------------------------------

/// An Iggy connection handle. Clone-cheap (the underlying client is shared).
///
/// `state_id` (the server address, credentials stripped) is used as the *stable
/// identity* for target-state keys, decoupling target identity from the live
/// connection.
#[derive(Clone)]
pub struct IggyProducer {
    client: Arc<IggyClient>,
    state_id: Arc<str>,
}

impl IggyProducer {
    /// Connect to an Iggy server from a connection string, e.g.
    /// `iggy://iggy:iggy@localhost:8090`. Credentials embedded in the string are
    /// used to auto-login on connect.
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let client = IggyClient::from_connection_string(connection_string)
            .map_err(|e| Error::engine(format!("iggy connection string: {e}")))?;
        client
            .connect()
            .await
            .map_err(|e| Error::engine(format!("iggy connect: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            state_id: Arc::from(sanitize_state_id(connection_string)),
        })
    }

    /// Stable identity used in target-state keys (the server address, with any
    /// embedded credentials removed).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// Access the underlying Iggy client (e.g. to manage streams/topics, or poll
    /// messages back).
    pub fn client(&self) -> &IggyClient {
        &self.client
    }
}

/// Strip `user:pass@` userinfo from a connection string so credentials never
/// leak into stable target-state keys.
fn sanitize_state_id(connection_string: &str) -> String {
    let (scheme, rest) = match connection_string.split_once("://") {
        Some((s, r)) => (Some(s), r),
        None => (None, connection_string),
    };
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    match scheme {
        Some(s) => format!("{s}://{host}"),
        None => host.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Public API: options + target + the constructor/declaration/mount split
// ---------------------------------------------------------------------------

/// Callback producing the deletion value for a given message key. Iggy has no
/// tombstone, so this is required to delete a previously-declared message.
pub type DeletionValueFn = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;

/// Options for the Iggy topic target.
#[derive(Clone)]
pub struct IggyTopicOptions {
    /// Partition to send messages to (default `0`).
    pub partition: u32,
    /// Value to send when a previously-declared message is removed. Iggy has no
    /// tombstone, so deleting a declared message without this is an error.
    pub deletion_value_fn: Option<DeletionValueFn>,
}

impl Default for IggyTopicOptions {
    fn default() -> Self {
        Self {
            partition: 0,
            deletion_value_fn: None,
        }
    }
}

/// A declarative Iggy topic target — a handle to declare messages on. See the
/// [module docs](self).
#[derive(Clone)]
pub struct IggyTopicTarget {
    messages: TargetStateProvider<Vec<u8>>,
    stream: Arc<str>,
    topic: Arc<str>,
}

/// Declare an Iggy topic target and return a ready same-component handle.
/// No external setup is needed, so this uses the same immediate provider path as
/// [`mount_iggy_topic_target`].
pub fn declare_iggy_topic_target(
    ctx: &Ctx,
    producer: &IggyProducer,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    mount_iggy_topic_target(ctx, producer, stream, topic, options)
}

/// Declare an Iggy topic target in the current component and return a handle for
/// declaring messages.
pub fn mount_iggy_topic_target(
    ctx: &Ctx,
    producer: &IggyProducer,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    let stream = stream.into();
    let topic = topic.into();
    let messages = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/iggy/topic/{}/{}/{}/{}",
            producer.state_id(),
            stream,
            topic,
            options.partition
        ),
        MessageHandler::new(
            producer.client.clone(),
            stream.clone(),
            topic.clone(),
            options.partition,
            options.deletion_value_fn,
        ),
    )?;
    Ok(IggyTopicTarget {
        messages,
        stream: Arc::from(stream),
        topic: Arc::from(topic),
    })
}

impl IggyTopicTarget {
    /// The target stream name/id.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// The target topic name/id.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Declare that the topic should contain a message with `key` and `value`.
    ///
    /// The actual send/skip/delete decision is made by the engine during
    /// reconciliation: a message is (re)sent only when its value changed since
    /// the last run.
    pub fn declare_message(&self, ctx: &Ctx, key: &str, value: impl AsRef<[u8]>) -> Result<()> {
        if key.is_empty() {
            return Err(Error::engine("iggy declare_message: key must be non-empty"));
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

/// What the sink should send for one message: a record with this value.
#[derive(Serialize, Deserialize)]
struct MessageAction {
    value: Vec<u8>,
}

struct MessageHandler {
    client: Arc<IggyClient>,
    stream: String,
    topic: String,
    partition: u32,
    deletion_value_fn: Option<DeletionValueFn>,
}

impl MessageHandler {
    fn new(
        client: Arc<IggyClient>,
        stream: String,
        topic: String,
        partition: u32,
        deletion_value_fn: Option<DeletionValueFn>,
    ) -> Self {
        Self {
            client,
            stream,
            topic,
            partition,
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
                "unexpected iggy message key: {key:?}"
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

        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(MessageAction {
                value: decision.value,
            }),
            sink: self.message_sink(),
            tracking_record: decision.tracking,
            child_invalidation: None,
        }))
    }
}

impl MessageHandler {
    fn message_sink(&self) -> TargetActionSink<MessageAction> {
        let client = self.client.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<MessageAction>>| {
            let client = client.clone();
            let stream = stream.clone();
            let topic = topic.clone();
            async move {
                if actions.is_empty() {
                    return Ok(());
                }
                let stream_id = Identifier::from_str_value(&stream)
                    .map_err(|e| Error::engine(format!("iggy stream id {stream:?}: {e}")))?;
                let topic_id = Identifier::from_str_value(&topic)
                    .map_err(|e| Error::engine(format!("iggy topic id {topic:?}: {e}")))?;
                let partitioning = Partitioning::partition_id(partition);

                let mut messages = Vec::with_capacity(actions.len());
                for action in actions {
                    let msg = match action {
                        TargetAction::Create(m)
                        | TargetAction::Update(m)
                        | TargetAction::Delete(m) => m,
                    };
                    let message = IggyMessage::builder()
                        .payload(Bytes::from(msg.value))
                        .build()
                        .map_err(|e| Error::engine(format!("iggy build message: {e}")))?;
                    messages.push(message);
                }
                client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await
                    .map_err(|e| Error::engine(format!("iggy send_messages: {e}")))?;
                Ok(())
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Reconcile logic (pure — unit-testable without a server)
// ---------------------------------------------------------------------------

/// The reconcile outcome for a single message, independent of the SDK machinery
/// so it can be unit-tested directly.
#[derive(Debug, PartialEq)]
struct MessageDecision {
    /// Record value to send (an upsert value, or the deletion value).
    value: Vec<u8>,
    /// Tracking record to persist: `Some(fp)` for an upsert, `None` for a delete.
    tracking: Option<Fingerprint>,
}

/// Decide whether (and how) to act on one message. Returns `None` to skip.
///
/// Reconcile one message:
/// * unchanged known-present values are skipped,
/// * new or changed values are sent and fingerprinted,
/// * known-absent deletes are skipped,
/// * other deletes send `deletion_value_fn(key)`, or error if none is set
///   (Iggy has no tombstone).
fn decide_message(
    key: &str,
    desired_value: Option<&[u8]>,
    prev_fps: &[Fingerprint],
    prev_may_be_missing: bool,
    deletion_value_fn: Option<&DeletionValueFn>,
) -> Result<Option<MessageDecision>> {
    // Upsert path.
    if let Some(value) = desired_value {
        let fp = Fingerprint::from(&value.to_vec()).map_err(Error::from)?;
        if !prev_may_be_missing && !prev_fps.is_empty() && prev_fps.iter().all(|p| *p == fp) {
            return Ok(None);
        }
        return Ok(Some(MessageDecision {
            value: value.to_vec(),
            tracking: Some(fp),
        }));
    }

    // Delete path.
    if prev_fps.is_empty() && !prev_may_be_missing {
        return Ok(None);
    }
    let Some(deletion_value_fn) = deletion_value_fn else {
        return Err(Error::engine(format!(
            "iggy: cannot delete message {key:?} — Iggy has no tombstone; \
             set IggyTopicOptions::deletion_value_fn or encode deletes in the payload"
        )));
    };
    Ok(Some(MessageDecision {
        value: deletion_value_fn(key),
        tracking: None,
    }))
}

// ---------------------------------------------------------------------------
// Iggy source — a live map feed over a topic partition (the Rust analogue of
// Python's `topic_as_map`).
// ---------------------------------------------------------------------------

/// An Iggy consumer handle. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct IggyConsumer {
    client: Arc<IggyClient>,
    state_id: Arc<str>,
}

impl IggyConsumer {
    /// Connect to an Iggy server from a connection string, e.g.
    /// `iggy://iggy:iggy@localhost:8090`.
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let client = IggyClient::from_connection_string(connection_string)
            .map_err(|e| Error::engine(format!("iggy connection string: {e}")))?;
        client
            .connect()
            .await
            .map_err(|e| Error::engine(format!("iggy connect: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            state_id: Arc::from(sanitize_state_id(connection_string)),
        })
    }

    /// Stable identity (the server address, credentials stripped).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

/// Extracts a map key from a message payload. Iggy messages have no native key,
/// so a key function is required (returning `None` skips the message).
pub type IggyKeyFn = Arc<dyn Fn(&[u8]) -> Option<String> + Send + Sync>;

/// Decides whether a message payload represents a *deletion* of its key
/// (defaults to "never" — Iggy has no tombstone).
pub type IggyIsDeletionFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// Options for [`topic_as_map_with_options`].
#[derive(Clone, Default)]
pub struct IggySourceOptions {
    /// Partition to consume (default `0`).
    pub partition: u32,
    /// Custom deletion predicate over the payload (default: never delete).
    pub is_deletion: Option<IggyIsDeletionFn>,
}

/// A keyed change feed over an Iggy topic partition: the latest payload per key
/// (as derived by the key function), with [`IggySourceOptions::is_deletion`]
/// removing keys. The Rust analogue of Python's `topic_as_map`.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads the partition log up
/// to its current offset (compacted to the latest payload per key) and
/// [`watch`](LiveMapFeed::watch) tails new messages from there. Feed it to
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). Single-partition only.
pub struct IggyTopicMap {
    client: Arc<IggyClient>,
    stream: String,
    topic: String,
    partition: u32,
    key_fn: IggyKeyFn,
    is_deletion: Option<IggyIsDeletionFn>,
    /// Offset `watch` resumes from — the offset `scan` reached.
    watch_start: Arc<tokio::sync::Mutex<u64>>,
}

/// Build an [`IggyTopicMap`] over `stream`/`topic` keyed by `key_fn`.
pub fn topic_as_map(
    consumer: &IggyConsumer,
    stream: impl Into<String>,
    topic: impl Into<String>,
    key_fn: IggyKeyFn,
) -> IggyTopicMap {
    topic_as_map_with_options(
        consumer,
        stream,
        topic,
        key_fn,
        IggySourceOptions::default(),
    )
}

/// [`topic_as_map`] with explicit [`IggySourceOptions`] (partition, deletion).
pub fn topic_as_map_with_options(
    consumer: &IggyConsumer,
    stream: impl Into<String>,
    topic: impl Into<String>,
    key_fn: IggyKeyFn,
    options: IggySourceOptions,
) -> IggyTopicMap {
    IggyTopicMap {
        client: consumer.client.clone(),
        stream: stream.into(),
        topic: topic.into(),
        partition: options.partition,
        key_fn,
        is_deletion: options.is_deletion,
        watch_start: Arc::new(tokio::sync::Mutex::new(0)),
    }
}

impl IggyTopicMap {
    fn ids(&self) -> Result<(Identifier, Identifier)> {
        let stream = Identifier::from_str_value(&self.stream)
            .map_err(|e| Error::engine(format!("iggy stream id {:?}: {e}", self.stream)))?;
        let topic = Identifier::from_str_value(&self.topic)
            .map_err(|e| Error::engine(format!("iggy topic id {:?}: {e}", self.topic)))?;
        Ok((stream, topic))
    }

    fn consumer() -> Result<Consumer> {
        let id = Identifier::from_str_value("cocoindex")
            .map_err(|e| Error::engine(format!("iggy consumer id: {e}")))?;
        Ok(Consumer::new(id))
    }

    /// Classify a payload into `(key, Some(value))` upsert or `(key, None)`
    /// delete; `None` when `key_fn` yields no key (message skipped).
    fn classify(&self, payload: &[u8]) -> Option<(String, Option<Vec<u8>>)> {
        let key = (self.key_fn)(payload)?;
        let is_delete = self.is_deletion.as_ref().is_some_and(|f| f(payload));
        Some((
            key,
            if is_delete {
                None
            } else {
                Some(payload.to_vec())
            },
        ))
    }

    async fn poll_from(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        consumer: &Consumer,
        offset: u64,
    ) -> Result<iggy::prelude::PolledMessages> {
        self.client
            .poll_messages(
                stream,
                topic,
                Some(self.partition),
                consumer,
                &PollingStrategy::offset(offset),
                1000,
                false,
            )
            .await
            .map_err(|e| Error::engine(format!("iggy poll_messages: {e}")))
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for IggyTopicMap {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let (stream, topic) = self.ids()?;
        let consumer = Self::consumer()?;
        let mut latest: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut offset = 0u64;
        loop {
            let polled = self.poll_from(&stream, &topic, &consumer, offset).await?;
            if polled.messages.is_empty() {
                break;
            }
            let hwm = polled.current_offset;
            for m in &polled.messages {
                offset = m.header.offset + 1;
                if let Some((key, value)) = self.classify(m.payload.as_ref()) {
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
            if offset > hwm {
                break;
            }
        }
        *self.watch_start.lock().await = offset;
        Ok(latest.into_iter().collect())
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for IggyTopicMap {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        let (stream, topic) = self.ids()?;
        let consumer = Self::consumer()?;
        let mut offset = *self.watch_start.lock().await;
        loop {
            let polled = self.poll_from(&stream, &topic, &consumer, offset).await?;
            if polled.messages.is_empty() {
                // Iggy poll returns immediately; sleep to avoid a busy loop.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                continue;
            }
            for m in &polled.messages {
                offset = m.header.offset + 1;
                if let Some((key, value)) = self.classify(m.payload.as_ref()) {
                    match value {
                        Some(v) => subscriber.update(key, v).await?,
                        None => subscriber.delete(key).await?,
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Keyless payload stream (the Rust analogue of Python's
// `topic_as_stream(...).payloads()`).
// ---------------------------------------------------------------------------

/// A **keyless, append-only** payload feed over **all partitions** of an Iggy
/// topic — the Rust analogue of Python's `topic_as_stream(...).payloads()`,
/// extended to read every partition (like the Rust Kafka stream) instead of one.
///
/// Unlike [`IggyTopicMap`] (which is keyed and per-partition), no key function or
/// deletion predicate is needed and nothing is compacted: every message is
/// delivered exactly once, keyed by its `"{partition}:{offset}"` position, so
/// each becomes a distinct child under
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live).
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads every partition up to
/// its current offset (so readiness reflects all partitions being caught up) and
/// [`watch`](LiveMapFeed::watch) round-robins new messages across partitions.
/// Re-running is idempotent: the `partition:offset` keys are stable.
pub struct IggyTopicStream {
    client: Arc<IggyClient>,
    stream: String,
    topic: String,
    /// Per-partition offset `watch` resumes from — the offset `scan` reached for
    /// each partition.
    watch_start: Arc<tokio::sync::Mutex<BTreeMap<u32, u64>>>,
}

/// Build an [`IggyTopicStream`] over all partitions of `stream`/`topic` (keyless
/// payload stream).
pub fn topic_as_stream(
    consumer: &IggyConsumer,
    stream: impl Into<String>,
    topic: impl Into<String>,
) -> IggyTopicStream {
    IggyTopicStream {
        client: consumer.client.clone(),
        stream: stream.into(),
        topic: topic.into(),
        watch_start: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
    }
}

impl IggyTopicStream {
    fn ids(&self) -> Result<(Identifier, Identifier)> {
        let stream = Identifier::from_str_value(&self.stream)
            .map_err(|e| Error::engine(format!("iggy stream id {:?}: {e}", self.stream)))?;
        let topic = Identifier::from_str_value(&self.topic)
            .map_err(|e| Error::engine(format!("iggy topic id {:?}: {e}", self.topic)))?;
        Ok((stream, topic))
    }

    /// The topic's partition IDs (from topic details).
    async fn partition_ids(&self, stream: &Identifier, topic: &Identifier) -> Result<Vec<u32>> {
        let details = self
            .client
            .get_topic(stream, topic)
            .await
            .map_err(|e| Error::engine(format!("iggy get_topic: {e}")))?
            .ok_or_else(|| {
                Error::engine(format!(
                    "iggy topic {:?}/{:?} not found",
                    self.stream, self.topic
                ))
            })?;
        let mut ids: Vec<u32> = details.partitions.iter().map(|p| p.id).collect();
        // Some servers report `partitions_count` without enumerating partitions;
        // fall back to the conventional 1..=count range.
        if ids.is_empty() && details.partitions_count > 0 {
            ids = (1..=details.partitions_count).collect();
        }
        if ids.is_empty() {
            return Err(Error::engine(format!(
                "iggy topic {:?}/{:?} has no partitions",
                self.stream, self.topic
            )));
        }
        ids.sort_unstable();
        Ok(ids)
    }

    async fn poll_from(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        consumer: &Consumer,
        partition: u32,
        offset: u64,
    ) -> Result<iggy::prelude::PolledMessages> {
        self.client
            .poll_messages(
                stream,
                topic,
                Some(partition),
                consumer,
                &PollingStrategy::offset(offset),
                1000,
                false,
            )
            .await
            .map_err(|e| Error::engine(format!("iggy poll_messages: {e}")))
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for IggyTopicStream {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let (stream, topic) = self.ids()?;
        let consumer = IggyTopicMap::consumer()?;
        let ids = self.partition_ids(&stream, &topic).await?;
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        let mut ends: BTreeMap<u32, u64> = BTreeMap::new();
        for pid in ids {
            let mut offset = 0u64;
            loop {
                let polled = self
                    .poll_from(&stream, &topic, &consumer, pid, offset)
                    .await?;
                if polled.messages.is_empty() {
                    break;
                }
                let hwm = polled.current_offset;
                for m in &polled.messages {
                    offset = m.header.offset + 1;
                    out.push((format!("{pid}:{}", m.header.offset), m.payload.to_vec()));
                }
                if offset > hwm {
                    break;
                }
            }
            ends.insert(pid, offset);
        }
        *self.watch_start.lock().await = ends;
        Ok(out)
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for IggyTopicStream {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        let (stream, topic) = self.ids()?;
        let consumer = IggyTopicMap::consumer()?;
        let mut offsets = self.watch_start.lock().await.clone();
        let ids = self.partition_ids(&stream, &topic).await?;
        loop {
            let mut got_any = false;
            for &pid in &ids {
                let offset = *offsets.get(&pid).unwrap_or(&0);
                let polled = self
                    .poll_from(&stream, &topic, &consumer, pid, offset)
                    .await?;
                for m in &polled.messages {
                    got_any = true;
                    offsets.insert(pid, m.header.offset + 1);
                    subscriber
                        .update(format!("{pid}:{}", m.header.offset), m.payload.to_vec())
                        .await?;
                }
            }
            if !got_any {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
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

    // Upsert new state (no previous record, prev_may_be_missing).
    #[test]
    fn upsert_new_state() {
        let d = decide_message("k1", Some(b"v1"), &[], true, None)
            .unwrap()
            .expect("should send");
        assert_eq!(d.value, b"v1");
        assert_eq!(d.tracking, Some(fp(b"v1")));
    }

    // Unchanged value with a matching previous fingerprint → skip.
    #[test]
    fn upsert_unchanged_skips() {
        let d = decide_message("k1", Some(b"v1"), &[fp(b"v1")], false, None).unwrap();
        assert_eq!(d, None);
    }

    // Changed value → send the new value.
    #[test]
    fn upsert_changed_value() {
        let d = decide_message("k1", Some(b"v2"), &[fp(b"v1")], false, None)
            .unwrap()
            .expect("should send");
        assert_eq!(d.value, b"v2");
        assert_eq!(d.tracking, Some(fp(b"v2")));
    }

    // prev_may_be_missing forces a send even when the fingerprint matches.
    #[test]
    fn upsert_prev_may_be_missing_forces_send() {
        let d = decide_message("k1", Some(b"v1"), &[fp(b"v1")], true, None).unwrap();
        assert!(d.is_some());
    }

    // Delete without a deletion_value_fn → error (Iggy has no tombstone).
    #[test]
    fn delete_without_callback_errors() {
        let err = decide_message("k1", None, &[fp(b"v1")], false, None).unwrap_err();
        assert!(err.to_string().contains("tombstone"));
    }

    // Delete with a deletion_value_fn → send that value, no tracking record.
    #[test]
    fn delete_with_callback_uses_value() {
        let f: DeletionValueFn = Arc::new(|k: &str| format!("deleted:{k}").into_bytes());
        let d = decide_message("k1", None, &[fp(b"v1")], false, Some(&f))
            .unwrap()
            .expect("should send deletion value");
        assert_eq!(d.value, b"deleted:k1");
        assert_eq!(d.tracking, None);
    }

    // Delete with no previous record and prev known-present → skip.
    #[test]
    fn delete_no_prev_skips() {
        let d = decide_message("k1", None, &[], false, None).unwrap();
        assert_eq!(d, None);
    }

    #[test]
    fn sanitize_state_id_strips_credentials() {
        assert_eq!(
            sanitize_state_id("iggy://iggy:iggy@localhost:8090"),
            "iggy://localhost:8090"
        );
        assert_eq!(
            sanitize_state_id("iggy://localhost:8090"),
            "iggy://localhost:8090"
        );
        assert_eq!(sanitize_state_id("localhost:8090"), "localhost:8090");
    }
}
