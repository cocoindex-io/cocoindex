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
use std::time::Duration;

use bytes::Bytes;
use cocoindex_utils::fingerprint::Fingerprint;
use iggy::prelude::{
    Client, Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient, IggyError,
    IggyMessage, MessageClient, Partitioning, PolledMessages, PollingStrategy, TopicClient,
};
use serde::{Deserialize, Serialize};

/// Re-export of the upstream [`iggy`] crate prelude. Streams and topics are
/// user-managed, so callers use this to create/manage them and to poll messages
/// back — without having to depend on the `iggy` crate directly. e.g.
/// `cocoindex::connectors::iggy::prelude::{StreamClient, TopicClient}`.
pub use ::iggy::prelude;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::live_component::{
    LiveComponentOperator, LiveMapFeed, LiveMapSubscriber, LiveMapView, SingleWatcherGuard,
};
use crate::target_state::{
    SinkCache, StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    TargetStateProvider, declare_target_state, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// IggyProducer — connection handle
// ---------------------------------------------------------------------------

/// An Iggy connection handle for the topic target. Clone-cheap (the underlying
/// client is shared). Provide it under a [`ContextKey`] with
/// `EnvironmentBuilder::provide_key`; the target names the key.
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

    /// Stable identity (the server address, with any embedded credentials
    /// removed).
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
#[derive(Clone, Default)]
pub struct IggyTopicOptions {
    /// Partition to send messages to (default `0`).
    pub partition: u32,
    /// Value to send when a previously-declared message is removed. Iggy has no
    /// tombstone, so deleting a declared message without this is an error.
    pub deletion_value_fn: Option<DeletionValueFn>,
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
    producer: &ContextKey<IggyProducer>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    mount_iggy_topic_target(ctx, producer, stream, topic, options)
}

/// Declare an Iggy topic target in the current component and return a handle for
/// declaring messages. `producer` names the [`IggyProducer`] provided in the
/// environment; it is resolved when the messages are sent.
pub fn mount_iggy_topic_target(
    ctx: &Ctx,
    producer: &ContextKey<IggyProducer>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    let stream = stream.into();
    let topic = topic.into();
    let messages = register_root_target_states_provider(
        ctx,
        // Stable connection identity = the ContextKey's name; the client is
        // resolved at apply time from the host context, as the other targets do.
        format!(
            "cocoindex/iggy/topic/{}/{}/{}/{}",
            producer.name(),
            stream,
            topic,
            options.partition
        ),
        MessageHandler::new(
            producer.name().to_string(),
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
    /// The `ContextKey` name the producer was provided under.
    producer_key: String,
    stream: String,
    topic: String,
    partition: u32,
    deletion_value_fn: Option<DeletionValueFn>,
    sink: SinkCache<MessageAction>,
}

impl MessageHandler {
    fn new(
        producer_key: String,
        stream: String,
        topic: String,
        partition: u32,
        deletion_value_fn: Option<DeletionValueFn>,
    ) -> Self {
        Self {
            producer_key,
            stream,
            topic,
            partition,
            deletion_value_fn,
            sink: SinkCache::default(),
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
            sink: self.sink.get(|| self.message_sink()),
            tracking_record: decision.tracking,
            child_invalidation: None,
        }))
    }
}

/// Resolve the live [`IggyProducer`] from the host context by the `ContextKey`
/// name it was provided under.
fn resolve_producer(host_ctx: &Arc<ContextStore>, producer_key: &str) -> Result<Arc<IggyProducer>> {
    host_ctx
        .resolve::<IggyProducer>(producer_key)
        .ok_or_else(|| {
            Error::engine(format!(
                "iggy target: producer `{producer_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, producer))"
            ))
        })
}

impl MessageHandler {
    fn message_sink(&self) -> TargetActionSink<MessageAction> {
        let producer_key = self.producer_key.clone();
        let stream = self.stream.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<MessageAction>>| {
                let producer_key = producer_key.clone();
                let stream = stream.clone();
                let topic = topic.clone();
                async move {
                    if actions.is_empty() {
                        return Ok(());
                    }
                    let client = resolve_producer(&host_ctx, &producer_key)?.client.clone();
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
            },
        )
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
// Iggy sources — a keyed map over a topic partition and a keyless stream over
// a topic (the Rust analogues of Python's `topic_as_map` / `topic_as_stream`).
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

/// How the sources poll a topic; the same knobs as the Python connector's
/// `topic_as_stream` / `topic_as_map`.
#[derive(Clone)]
pub struct IggyPollOptions {
    /// Messages per poll (default `100`).
    pub batch_length: u32,
    /// How long `watch` waits after an empty poll before polling again
    /// (default 300 ms).
    pub poll_interval: Duration,
    /// How long to wait after a failed poll before retrying it, or `None`
    /// (the default) to fail the watch on the first poll error.
    pub polling_retry_interval: Option<Duration>,
    /// How many times to look again for a stream or topic that does not exist
    /// yet when the source starts, waiting `init_retry_interval` between looks
    /// (default `0`: a missing topic is an error at once). Useful when the
    /// producer creates the topic.
    pub init_retries: u32,
    /// The wait between `init_retries` (default 1 s).
    pub init_retry_interval: Duration,
    /// Whether the consumer group's stored offset may move backwards. Off (the
    /// default), an offset lower than the one stored is not stored.
    pub allow_replay: bool,
}

impl Default for IggyPollOptions {
    fn default() -> Self {
        Self {
            batch_length: 100,
            poll_interval: Duration::from_millis(300),
            polling_retry_interval: None,
            init_retries: 0,
            init_retry_interval: Duration::from_secs(1),
            allow_replay: false,
        }
    }
}

/// Options for [`topic_as_map_with_options`].
#[derive(Clone, Default)]
pub struct IggySourceOptions {
    /// Partition to consume (default `0`).
    pub partition: u32,
    pub poll: IggyPollOptions,
    /// Custom deletion predicate over the payload (default: never delete).
    pub is_deletion: Option<IggyIsDeletionFn>,
}

/// The stream and topic identifiers of a source.
fn topic_ids(stream: &str, topic: &str) -> Result<(Identifier, Identifier)> {
    let stream_id = Identifier::from_str_value(stream)
        .map_err(|e| Error::engine(format!("iggy stream id {stream:?}: {e}")))?;
    let topic_id = Identifier::from_str_value(topic)
        .map_err(|e| Error::engine(format!("iggy topic id {topic:?}: {e}")))?;
    Ok((stream_id, topic_id))
}

/// The polling shared by [`IggyTopicMap`] and [`IggyTopicStream`]: a catch-up
/// scan of each partition from offset 0, a tail from where the scan ended, and
/// the consumer group's offsets on the server, stored once a polled batch has
/// been delivered and resumed from on the next live run.
struct TopicPoller {
    client: Arc<IggyClient>,
    stream: String,
    topic: String,
    consumer_group: String,
    options: IggyPollOptions,
    /// Per-partition offset `watch` resumes from: where the most recent `scan`
    /// ended, or the stored offset when the scan was skipped.
    watch_start: tokio::sync::Mutex<BTreeMap<u32, u64>>,
    watch_guard: SingleWatcherGuard,
}

impl TopicPoller {
    fn new(
        consumer: &IggyConsumer,
        consumer_group: String,
        stream: String,
        topic: String,
        options: IggyPollOptions,
        label: &str,
    ) -> Self {
        Self {
            client: consumer.client.clone(),
            stream,
            topic,
            consumer_group,
            options,
            watch_start: tokio::sync::Mutex::new(BTreeMap::new()),
            watch_guard: SingleWatcherGuard::new(label),
        }
    }

    fn ids(&self) -> Result<(Identifier, Identifier)> {
        topic_ids(&self.stream, &self.topic)
    }

    fn consumer(&self) -> Result<Consumer> {
        let id = Identifier::from_str_value(&self.consumer_group)
            .map_err(|e| Error::engine(format!("iggy consumer group id: {e}")))?;
        Ok(Consumer::group(id))
    }

    /// The topic's details, looking again `init_retries` times when the stream
    /// or topic does not exist yet.
    async fn topic_details(&self) -> Result<iggy::prelude::TopicDetails> {
        let (stream, topic) = self.ids()?;
        let mut looked = 0;
        loop {
            let details = self
                .client
                .get_topic(&stream, &topic)
                .await
                .map_err(|e| Error::engine(format!("iggy get_topic: {e}")))?;
            if let Some(details) = details {
                return Ok(details);
            }
            if looked >= self.options.init_retries {
                return Err(Error::engine(format!(
                    "iggy topic {:?}/{:?} not found",
                    self.stream, self.topic
                )));
            }
            looked += 1;
            tokio::time::sleep(self.options.init_retry_interval).await;
        }
    }

    /// The topic's partition IDs (from topic details).
    async fn partition_ids(&self) -> Result<Vec<u32>> {
        let details = self.topic_details().await?;
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

    /// Create the consumer group when it does not exist, and join it, so its
    /// offsets can be stored and read.
    async fn ensure_group(&self) -> Result<Identifier> {
        let (stream, topic) = self.ids()?;
        let group = Identifier::from_str_value(&self.consumer_group)
            .map_err(|e| Error::engine(format!("iggy consumer group id: {e}")))?;
        let exists = self
            .client
            .get_consumer_group(&stream, &topic, &group)
            .await
            .map_err(|e| Error::engine(format!("iggy get_consumer_group: {e}")))?
            .is_some();
        if !exists {
            match self
                .client
                .create_consumer_group(&stream, &topic, &self.consumer_group)
                .await
            {
                Ok(_) | Err(IggyError::ConsumerGroupNameAlreadyExists(..)) => {}
                Err(e) => {
                    return Err(Error::engine(format!(
                        "iggy create_consumer_group {:?}: {e}",
                        self.consumer_group
                    )));
                }
            }
        }
        self.client
            .join_consumer_group(&stream, &topic, &group)
            .await
            .map_err(|e| {
                Error::engine(format!(
                    "iggy join_consumer_group {:?}: {e}",
                    self.consumer_group
                ))
            })?;
        Ok(group)
    }

    /// The group's stored (last consumed) offset for `partition`, if any.
    async fn stored_offset(&self, partition: u32) -> Result<Option<u64>> {
        let (stream, topic) = self.ids()?;
        let consumer = self.consumer()?;
        let info = self
            .client
            .get_consumer_offset(&consumer, &stream, &topic, Some(partition))
            .await
            .map_err(|e| Error::engine(format!("iggy get_consumer_offset: {e}")))?;
        Ok(info.map(|i| i.stored_offset))
    }

    /// Store `offset` as the group's last consumed offset for `partition`;
    /// without `allow_replay`, an offset below the stored one is left alone.
    async fn store_offset(&self, partition: u32, offset: u64) -> Result<()> {
        if !self.options.allow_replay
            && let Some(stored) = self.stored_offset(partition).await?
            && offset <= stored
        {
            return Ok(());
        }
        let (stream, topic) = self.ids()?;
        let consumer = self.consumer()?;
        self.client
            .store_consumer_offset(&consumer, &stream, &topic, Some(partition), offset)
            .await
            .map_err(|e| Error::engine(format!("iggy store_consumer_offset: {e}")))
    }

    async fn poll_once(&self, partition: u32, offset: u64) -> Result<PolledMessages> {
        let (stream, topic) = self.ids()?;
        let consumer = self.consumer()?;
        self.client
            .poll_messages(
                &stream,
                &topic,
                Some(partition),
                &consumer,
                &PollingStrategy::offset(offset),
                self.options.batch_length,
                false,
            )
            .await
            .map_err(|e| Error::engine(format!("iggy poll_messages: {e}")))
    }

    /// [`poll_once`](Self::poll_once), retried after `polling_retry_interval`
    /// on a failure when one is set.
    async fn poll(&self, partition: u32, offset: u64) -> Result<PolledMessages> {
        loop {
            match self.poll_once(partition, offset).await {
                Ok(polled) => return Ok(polled),
                Err(e) => match self.options.polling_retry_interval {
                    Some(interval) => {
                        tracing::warn!("iggy poll failed, retrying in {interval:?}: {e}");
                        tokio::time::sleep(interval).await;
                    }
                    None => return Err(e),
                },
            }
        }
    }

    /// Read `partitions` from offset 0 to their current end, passing each
    /// `(partition, message)` to `f`, and record where each ended for `watch`.
    async fn scan(&self, partitions: &[u32], mut f: impl FnMut(u32, &IggyMessage)) -> Result<()> {
        let mut ends: BTreeMap<u32, u64> = BTreeMap::new();
        for &pid in partitions {
            let mut offset = 0u64;
            loop {
                let polled = self.poll(pid, offset).await?;
                if polled.messages.is_empty() {
                    break;
                }
                let hwm = polled.current_offset;
                for m in &polled.messages {
                    offset = m.header.offset + 1;
                    f(pid, m);
                }
                if offset > hwm {
                    break;
                }
            }
            ends.insert(pid, offset);
        }
        *self.watch_start.lock().await = ends;
        Ok(())
    }

    /// Resume from the group's stored offsets when every partition has one,
    /// skipping the catch-up scan; `watch` then replays the messages since.
    async fn skip_initial_scan(&self, partitions: &[u32]) -> Result<bool> {
        self.ensure_group().await?;
        let mut starts: BTreeMap<u32, u64> = BTreeMap::new();
        for &pid in partitions {
            let Some(stored) = self.stored_offset(pid).await? else {
                return Ok(false);
            };
            starts.insert(pid, stored + 1);
        }
        *self.watch_start.lock().await = starts;
        Ok(true)
    }

    /// Tail `partitions` from `watch_start`, delivering each message through
    /// `deliver`; after each delivered batch the partition's last offset is
    /// stored for the consumer group.
    async fn watch(
        &self,
        partitions: &[u32],
        deliver: &dyn Deliver,
        subscriber: &LiveMapSubscriber<String, Vec<u8>>,
    ) -> Result<()> {
        let _watch_token = self.watch_guard.enter()?;
        self.ensure_group().await?;
        let mut offsets = self.watch_start.lock().await.clone();
        for &pid in partitions {
            let next = *offsets.entry(pid).or_insert(0);
            // The scan's end is the position the next run resumes from if
            // nothing new arrives.
            if next > 0 {
                self.store_offset(pid, next - 1).await?;
            }
        }
        loop {
            let mut got_any = false;
            for &pid in partitions {
                let offset = *offsets.get(&pid).unwrap_or(&0);
                let polled = self.poll(pid, offset).await?;
                if polled.messages.is_empty() {
                    continue;
                }
                got_any = true;
                let mut last = offset;
                for m in &polled.messages {
                    last = m.header.offset;
                    deliver.deliver(pid, m, subscriber).await?;
                }
                offsets.insert(pid, last + 1);
                self.store_offset(pid, last).await?;
            }
            if !got_any {
                tokio::time::sleep(self.options.poll_interval).await;
            }
        }
    }
}

/// How a source delivers one polled message to its subscriber.
#[crate::async_trait]
trait Deliver: Send + Sync {
    async fn deliver(
        &self,
        partition: u32,
        message: &IggyMessage,
        subscriber: &LiveMapSubscriber<String, Vec<u8>>,
    ) -> Result<()>;
}

/// A keyed change feed over an Iggy topic partition: the latest payload per key
/// (as derived by the key function), with [`IggySourceOptions::is_deletion`]
/// removing keys. The Rust analogue of Python's `topic_as_map`.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads the partition log up
/// to its current offset (compacted to the latest payload per key) and
/// [`watch`](LiveMapFeed::watch) tails new messages from there. The consumer
/// group's offset is stored on the server once a polled batch's children have
/// synced, and a later live run resumes from it instead of scanning. Feed it to
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). Single-partition only.
pub struct IggyTopicMap {
    poller: TopicPoller,
    partition: u32,
    key_fn: IggyKeyFn,
    is_deletion: Option<IggyIsDeletionFn>,
}

/// Build an [`IggyTopicMap`] over partition 0 of `stream`/`topic`, keyed by
/// `key_fn`, consuming as `consumer_group`.
pub fn topic_as_map(
    consumer: &IggyConsumer,
    consumer_group: impl Into<String>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    key_fn: IggyKeyFn,
) -> IggyTopicMap {
    topic_as_map_with_options(
        consumer,
        consumer_group,
        stream,
        topic,
        key_fn,
        IggySourceOptions::default(),
    )
}

/// [`topic_as_map`] with explicit [`IggySourceOptions`] (partition, polling,
/// deletion).
pub fn topic_as_map_with_options(
    consumer: &IggyConsumer,
    consumer_group: impl Into<String>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    key_fn: IggyKeyFn,
    options: IggySourceOptions,
) -> IggyTopicMap {
    IggyTopicMap {
        poller: TopicPoller::new(
            consumer,
            consumer_group.into(),
            stream.into(),
            topic.into(),
            options.poll,
            "IggyTopicMap",
        ),
        partition: options.partition,
        key_fn,
        is_deletion: options.is_deletion,
    }
}

impl IggyTopicMap {
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
}

#[crate::async_trait]
impl Deliver for IggyTopicMap {
    async fn deliver(
        &self,
        _partition: u32,
        message: &IggyMessage,
        subscriber: &LiveMapSubscriber<String, Vec<u8>>,
    ) -> Result<()> {
        let Some((key, value)) = self.classify(message.payload.as_ref()) else {
            return Ok(());
        };
        match value {
            Some(v) => subscriber.update(key, v).await,
            None => subscriber.delete(key).await,
        }
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for IggyTopicMap {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        // The partition must exist; `topic_details` also applies the init retries.
        self.poller.topic_details().await?;
        let mut latest: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        self.poller
            .scan(&[self.partition], |_pid, m| {
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
            })
            .await?;
        Ok(latest.into_iter().collect())
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for IggyTopicMap {
    async fn skip_initial_scan(&self, _operator: &LiveComponentOperator) -> Result<bool> {
        self.poller.topic_details().await?;
        self.poller.skip_initial_scan(&[self.partition]).await
    }

    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        self.poller
            .watch(&[self.partition], self, &subscriber)
            .await
    }
}

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
/// [`watch`](LiveMapFeed::watch) round-robins new messages across partitions,
/// storing each partition's offset for the consumer group as it goes. Re-running
/// is idempotent: the `partition:offset` keys are stable.
pub struct IggyTopicStream {
    poller: TopicPoller,
}

/// Build an [`IggyTopicStream`] over all partitions of `stream`/`topic`,
/// consuming as `consumer_group`.
pub fn topic_as_stream(
    consumer: &IggyConsumer,
    consumer_group: impl Into<String>,
    stream: impl Into<String>,
    topic: impl Into<String>,
) -> IggyTopicStream {
    topic_as_stream_with_options(
        consumer,
        consumer_group,
        stream,
        topic,
        IggyPollOptions::default(),
    )
}

/// [`topic_as_stream`] with explicit [`IggyPollOptions`].
pub fn topic_as_stream_with_options(
    consumer: &IggyConsumer,
    consumer_group: impl Into<String>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyPollOptions,
) -> IggyTopicStream {
    IggyTopicStream {
        poller: TopicPoller::new(
            consumer,
            consumer_group.into(),
            stream.into(),
            topic.into(),
            options,
            "IggyTopicStream",
        ),
    }
}

/// The stream's child key for a message: its position.
fn position_key(partition: u32, message: &IggyMessage) -> String {
    format!("{partition}:{}", message.header.offset)
}

#[crate::async_trait]
impl Deliver for IggyTopicStream {
    async fn deliver(
        &self,
        partition: u32,
        message: &IggyMessage,
        subscriber: &LiveMapSubscriber<String, Vec<u8>>,
    ) -> Result<()> {
        subscriber
            .update(position_key(partition, message), message.payload.to_vec())
            .await
    }
}

#[crate::async_trait]
impl LiveMapView<String, Vec<u8>> for IggyTopicStream {
    async fn scan(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let ids = self.poller.partition_ids().await?;
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        self.poller
            .scan(&ids, |pid, m| {
                out.push((position_key(pid, m), m.payload.to_vec()));
            })
            .await?;
        Ok(out)
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, Vec<u8>> for IggyTopicStream {
    async fn skip_initial_scan(&self, _operator: &LiveComponentOperator) -> Result<bool> {
        let ids = self.poller.partition_ids().await?;
        self.poller.skip_initial_scan(&ids).await
    }

    async fn watch(&self, subscriber: LiveMapSubscriber<String, Vec<u8>>) -> Result<()> {
        let ids = self.poller.partition_ids().await?;
        self.poller.watch(&ids, self, &subscriber).await
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
