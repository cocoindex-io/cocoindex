//! Apache Iggy connector: a topic target and topic sources.
//!
//! # Target
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
//! [`iggy_topic_target`] builds the topic's [`TargetState`] for `mount_target` /
//! `declare_target_state_with_child`; [`declare_iggy_topic_target`] /
//! [`mount_iggy_topic_target`] wrap those and return a handle for declaring
//! messages.
//!
//! # Sources
//!
//! [`topic_as_stream`] reads a topic as a [`LiveStream`] of every message
//! ([`IggyTopicStream`]; [`payloads`](IggyTopicStream::payloads) is the
//! `LiveStream<Vec<u8>>` view) and [`topic_as_map`] reads one partition as a
//! keyed map, the key and deletions derived from each [`IggyReceivedMessage`]
//! by the caller's functions, as Iggy messages have no key or tombstone. Both
//! are also [`LiveMapView`]s for [`Ctx::mount_each_live`](crate::Ctx::mount_each_live).
//! The consumer group's offsets are stored on the server once a polled batch
//! has been processed, and a later live run resumes from them.
//!
//! This is the Rust analogue of Python's `cocoindex.connectors.iggy`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
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

use crate::connectors::partition_tail::{PartitionSource, PartitionTail, watch_partitions};
use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::live_component::{
    LiveComponentOperator, LiveMapFeed, LiveMapSubscriber, LiveMapView, LiveStream,
    LiveStreamSubscriber, ReadyHandle, SingleWatcherGuard,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider,
    declare_target_state, declare_target_state_with_child, mount_target,
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

    /// Wrap a connected `IggyClient` the caller built, as the Python target
    /// takes the caller's client. `state_id` is the stable identity of the
    /// server (its address), never a credential.
    pub fn from_client(client: IggyClient, state_id: impl Into<String>) -> Self {
        Self {
            client: Arc::new(client),
            state_id: Arc::from(state_id.into()),
        }
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

/// Callback producing the deletion value for a message key (a `&str` key is
/// its UTF-8 bytes). Iggy has no tombstone, so this is required to delete a
/// previously-declared message.
pub type DeletionValueFn = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// Options for the Iggy topic target.
#[derive(Clone, Default)]
pub struct IggyTopicOptions {
    /// Partition to send messages to (default `0`).
    pub partition: u32,
    /// Value to send when a previously-declared message is removed. Iggy has no
    /// tombstone, so deleting a declared message without this is an error.
    ///
    /// The callback lives in this process: a run that declares the topic target
    /// (or drops the app from a process that did) applies it; `App::drop_state`
    /// from a process that never declared the topic target cannot delete.
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

/// Build the composable [`TargetState`] of an Iggy topic partition (the spec
/// constructor), for `mount_target` / `declare_target_state_with_child`.
/// `producer` names the [`IggyProducer`] provided in the environment; it is
/// resolved when the messages are sent.
pub fn iggy_topic_target(
    ctx: &Ctx,
    producer: &ContextKey<IggyProducer>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<TargetState<TopicSpec>> {
    let stream = stream.into();
    let topic = topic.into();
    if stream.is_empty() || topic.is_empty() {
        return Err(Error::engine("iggy stream and topic must be non-empty"));
    }
    let key = TopicKey {
        producer_key: producer.name().to_string(),
        stream,
        topic,
        partition: options.partition,
    };
    set_deletion_value_fn(&key, options.deletion_value_fn);
    let provider = TOPIC_PROVIDER.get(ctx)?;
    Ok(provider.target_state(key.stable_key(), TopicSpec {}))
}

/// Declare an Iggy topic target in the current component and return a handle
/// for declaring messages (the message provider resolves at this component's
/// commit).
pub fn declare_iggy_topic_target(
    ctx: &Ctx,
    producer: &ContextKey<IggyProducer>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    let stream = stream.into();
    let topic = topic.into();
    let ts = iggy_topic_target(ctx, producer, stream.clone(), topic.clone(), options)?;
    let messages = declare_target_state_with_child::<TopicSpec, Vec<u8>>(ctx, ts)?;
    Ok(IggyTopicTarget {
        messages,
        stream: Arc::from(stream),
        topic: Arc::from(topic),
    })
}

/// Mount an Iggy topic target **foreground** (messages can be declared at once)
/// and return a handle for declaring messages.
pub async fn mount_iggy_topic_target(
    ctx: &Ctx,
    producer: &ContextKey<IggyProducer>,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<IggyTopicTarget> {
    let stream = stream.into();
    let topic = topic.into();
    let ts = iggy_topic_target(ctx, producer, stream.clone(), topic.clone(), options)?;
    let messages = mount_target::<TopicSpec, Vec<u8>>(ctx, ts).await?;
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

    /// [`declare_message`](Self::declare_message) with a binary key. A `&str`
    /// key and its UTF-8 bytes are distinct target states.
    pub fn declare_message_bytes(
        &self,
        ctx: &Ctx,
        key: &[u8],
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        if key.is_empty() {
            return Err(Error::engine(
                "iggy declare_message_bytes: key must be non-empty",
            ));
        }
        declare_target_state(
            ctx,
            self.messages
                .target_state(StableKey::Bytes(Arc::from(key)), value.as_ref().to_vec()),
        )
    }
}

// ---------------------------------------------------------------------------
// Topic handler (root, static) and its message child handler
// ---------------------------------------------------------------------------

/// Spec of an Iggy topic target (the declared container value). Streams and
/// topics are user-managed, so the spec carries nothing: the target's identity
/// is in its key.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSpec {}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TOPIC_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TopicSpec, TopicHandler>("cocoindex/iggy/topic");

/// A topic target state's key: the producer's `ContextKey` name, the stream,
/// the topic and the partition. Mirrors Python's `_TopicKey`.
#[derive(Clone, PartialEq, Eq, Hash)]
struct TopicKey {
    producer_key: String,
    stream: String,
    topic: String,
    partition: u32,
}

impl TopicKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.producer_key)),
            StableKey::Str(Arc::from(self.stream)),
            StableKey::Str(Arc::from(self.topic)),
            StableKey::Int(i64::from(self.partition)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(producer_key),
                StableKey::Str(stream),
                StableKey::Str(topic),
                StableKey::Int(partition),
            ] = &parts[..]
            && let Ok(partition) = u32::try_from(*partition)
        {
            return Ok(Self {
                producer_key: producer_key.to_string(),
                stream: stream.to_string(),
                topic: topic.to_string(),
                partition,
            });
        }
        Err(Error::engine(format!("unexpected iggy topic key: {key:?}")))
    }
}

/// The deletion-value callbacks of the topic targets this process declared,
/// looked up when a topic's message handler is built (the spec is persisted,
/// a closure cannot be).
static DELETION_VALUE_FNS: Mutex<Option<std::collections::HashMap<TopicKey, DeletionValueFn>>> =
    Mutex::new(None);

fn set_deletion_value_fn(key: &TopicKey, f: Option<DeletionValueFn>) {
    let mut guard = DELETION_VALUE_FNS.lock().unwrap_or_else(|e| e.into_inner());
    let fns = guard.get_or_insert_with(Default::default);
    match f {
        Some(f) => {
            fns.insert(key.clone(), f);
        }
        None => {
            fns.remove(key);
        }
    }
}

fn deletion_value_fn(key: &TopicKey) -> Option<DeletionValueFn> {
    DELETION_VALUE_FNS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|fns| fns.get(key).cloned())
}

/// The topic-level action: nothing to apply (topics are user-managed); it
/// carries the key the message child handler is built from.
#[derive(Serialize, Deserialize)]
struct TopicAction {
    producer_key: String,
    stream: String,
    topic: String,
    partition: u32,
}

#[derive(Default)]
struct TopicHandler {
    sink: SinkCache<TopicAction>,
}

impl TargetHandler<TopicSpec> for TopicHandler {
    type TrackingRecord = ();
    type Action = TopicAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TopicSpec>,
        _prev: Vec<()>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TopicAction, ()>>> {
        let topic_key = TopicKey::from_stable_key(&key)?;
        let action = TopicAction {
            producer_key: topic_key.producer_key,
            stream: topic_key.stream,
            topic: topic_key.topic,
            partition: topic_key.partition,
        };
        // Always emit when declared, so the sink builds the message child.
        Ok(Some(TargetReconcileOutput {
            action: match desired {
                Some(_) => TargetAction::Update(action),
                None => TargetAction::Delete(action),
            },
            sink: self.sink.get(topic_sink),
            tracking_record: desired.map(|_| ()),
            child_invalidation: None,
        }))
    }
}

fn topic_sink() -> TargetActionSink<TopicAction> {
    TargetActionSink::from_async_fn_with_child_handler(
        // Topics are not created or deleted, so there is nothing to apply.
        |_actions| async { Ok(()) },
        |action: &TopicAction| {
            let key = TopicKey {
                producer_key: action.producer_key.clone(),
                stream: action.stream.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            };
            let deletion_value_fn = deletion_value_fn(&key);
            Ok(ChildTargetDef::new::<Vec<u8>, _>(MessageHandler::new(
                key.producer_key,
                key.stream,
                key.topic,
                key.partition,
                deletion_value_fn,
            )))
        },
    )
}

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

/// The bytes of a message target state's key: a `Str` key is its UTF-8 bytes.
fn message_key_bytes(key: &StableKey) -> Result<Vec<u8>> {
    match key {
        StableKey::Str(s) => Ok(s.as_bytes().to_vec()),
        StableKey::Bytes(b) => Ok(b.to_vec()),
        other => Err(Error::engine(format!(
            "unexpected iggy message key: {other:?}"
        ))),
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
        let key = message_key_bytes(&key)?;
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
                    let (stream_id, topic_id) = topic_ids(&stream, &topic)?;
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
    key: &[u8],
    desired_value: Option<&[u8]>,
    prev_fps: &[Fingerprint],
    prev_may_be_missing: bool,
    deletion_value_fn: Option<&DeletionValueFn>,
) -> Result<Option<MessageDecision>> {
    // Upsert path.
    if let Some(value) = desired_value {
        let fp = Fingerprint::from_bytes(value);
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
            "iggy: cannot delete message {:?} — Iggy has no tombstone; \
             set IggyTopicOptions::deletion_value_fn or encode deletes in the payload",
            String::from_utf8_lossy(key)
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

    /// Wrap a connected `IggyClient` the caller built, as the Python sources
    /// take the caller's client. `state_id` is the stable identity of the
    /// server (its address), never a credential.
    pub fn from_client(client: IggyClient, state_id: impl Into<String>) -> Self {
        Self {
            client: Arc::new(client),
            state_id: Arc::from(state_id.into()),
        }
    }

    /// Stable identity (the server address, credentials stripped).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

/// A polled Iggy message, as the sources yield it: the Rust analogue of the
/// Python SDK's `ReceiveMessage`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IggyReceivedMessage {
    pub partition: u32,
    pub offset: u64,
    /// The message id the producer set (`0` when none).
    pub id: u128,
    /// The server's receive time, in microseconds since the Unix epoch.
    pub timestamp: u64,
    pub checksum: u64,
    pub payload: Vec<u8>,
}

impl IggyReceivedMessage {
    fn from_message(partition: u32, message: &IggyMessage) -> Self {
        Self {
            partition,
            offset: message.header.offset,
            id: message.header.id,
            timestamp: message.header.timestamp,
            checksum: message.header.checksum,
            payload: message.payload.to_vec(),
        }
    }

    /// The payload as text (lossy on invalid UTF-8).
    pub fn payload_str(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    }
}

/// Extracts a map key from a message. Iggy messages have no native key, so a
/// key function is required (returning `None` skips the message).
pub type IggyKeyFn = Arc<dyn Fn(&IggyReceivedMessage) -> Option<String> + Send + Sync>;

/// Decides whether a message represents a *deletion* of its key (defaults to
/// "never" — Iggy has no tombstone).
pub type IggyIsDeletionFn = Arc<dyn Fn(&IggyReceivedMessage) -> bool + Send + Sync>;

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
    /// Custom deletion predicate over the message (default: never delete).
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

    /// Each partition's end (the offset its next message gets), from the topic
    /// details: the initial high watermark readiness is measured against.
    async fn high_watermarks(&self, partitions: &[u32]) -> Result<BTreeMap<u32, u64>> {
        let details = self.topic_details().await?;
        Ok(partitions
            .iter()
            .map(|&pid| {
                let end = details
                    .partitions
                    .iter()
                    .find(|p| p.id == pid)
                    .map(|p| {
                        if p.messages_count == 0 {
                            0
                        } else {
                            p.current_offset + 1
                        }
                    })
                    .unwrap_or(0);
                (pid, end)
            })
            .collect())
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
    /// message to `f`, and record where each ended for `watch`.
    async fn scan(&self, partitions: &[u32], mut f: impl FnMut(IggyReceivedMessage)) -> Result<()> {
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
                    f(IggyReceivedMessage::from_message(pid, m));
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

    /// Tail `partitions` from `watch_start`, delivering each message to
    /// `subscriber`; after each polled batch has been processed the
    /// partition's last offset is stored for the consumer group.
    async fn watch(
        &self,
        partitions: &[u32],
        subscriber: &dyn LiveStreamSubscriber<IggyReceivedMessage>,
    ) -> Result<()> {
        let _watch_token = self.watch_guard.enter()?;
        self.ensure_group().await?;
        let starts = self.watch_start.lock().await.clone();
        let high_watermarks = self.high_watermarks(partitions).await?;
        let mut tails = Vec::new();
        for &pid in partitions {
            let start = starts.get(&pid).copied().unwrap_or(0);
            let source = IggyPartition {
                poller: self,
                partition: pid,
            };
            // The scan's end is the position the next run resumes from if
            // nothing new arrives.
            source.commit(start as i64).await?;
            tails.push(PartitionTail {
                source,
                start: start as i64,
                high_watermark: high_watermarks.get(&pid).copied().unwrap_or(0) as i64,
            });
        }
        watch_partitions(tails, subscriber).await
    }
}

/// One partition of a watch, polled through its poller; the consumer group's
/// offset is stored on the server.
struct IggyPartition<'a> {
    poller: &'a TopicPoller,
    partition: u32,
}

#[crate::async_trait]
impl PartitionSource<IggyReceivedMessage> for IggyPartition<'_> {
    async fn fetch(&self, offset: i64) -> Result<Vec<(i64, IggyReceivedMessage)>> {
        let polled = self.poller.poll(self.partition, offset as u64).await?;
        if polled.messages.is_empty() {
            // Iggy answers an empty poll at once; pace the loop as Python's
            // `poll_interval` does.
            tokio::time::sleep(self.poller.options.poll_interval).await;
        }
        Ok(polled
            .messages
            .iter()
            .map(|m| {
                (
                    m.header.offset as i64,
                    IggyReceivedMessage::from_message(self.partition, m),
                )
            })
            .collect())
    }

    async fn commit(&self, next: i64) -> Result<()> {
        // Iggy stores the last consumed offset, not the next one to read.
        if next > 0 {
            self.poller
                .store_offset(self.partition, (next - 1) as u64)
                .await?;
        }
        Ok(())
    }
}

/// Delivers a stream's messages to a [`LiveMapSubscriber`]: each message is
/// keyed by `key_of`, mounted as a child (`Some(value)`) or deleted (`None`),
/// and skipped when `key_of` yields nothing.
struct StreamToMap<'a, F> {
    map: &'a LiveMapSubscriber<String, IggyReceivedMessage>,
    key_of: F,
}

#[crate::async_trait]
impl<F> LiveStreamSubscriber<IggyReceivedMessage> for StreamToMap<'_, F>
where
    F: Fn(IggyReceivedMessage) -> Option<(String, Option<IggyReceivedMessage>)> + Send + Sync,
{
    async fn send(&self, message: IggyReceivedMessage) -> Result<ReadyHandle> {
        match (self.key_of)(message) {
            None => Ok(ReadyHandle::immediate()),
            Some((key, Some(value))) => self.map.start_update(key, value).await,
            Some((key, None)) => self.map.start_delete(key).await,
        }
    }

    async fn mark_ready(&self) {
        self.map.mark_ready().await;
    }

    async fn read_committed_state(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.map.read_committed_state(key).await
    }

    async fn write_committed_state(&self, key: &str, value: &[u8]) -> Result<()> {
        self.map.write_committed_state(key, &value.to_vec()).await
    }
}

/// A keyed change feed over an Iggy topic partition: the latest message per key
/// (as derived by the key function), with [`IggySourceOptions::is_deletion`]
/// removing keys. The Rust analogue of Python's `topic_as_map`.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads the partition log up
/// to its current offset (compacted to the latest message per key) and
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

/// Classify a message into `(key, Some(message))` upsert or `(key, None)`
/// delete; `None` when `key_fn` yields no key (message skipped).
fn classify_message(
    key_fn: &IggyKeyFn,
    is_deletion: Option<&IggyIsDeletionFn>,
    message: IggyReceivedMessage,
) -> Option<(String, Option<IggyReceivedMessage>)> {
    let key = key_fn(&message)?;
    let is_delete = is_deletion.is_some_and(|f| f(&message));
    Some((key, if is_delete { None } else { Some(message) }))
}

impl IggyTopicMap {
    fn classify(
        &self,
        message: IggyReceivedMessage,
    ) -> Option<(String, Option<IggyReceivedMessage>)> {
        classify_message(&self.key_fn, self.is_deletion.as_ref(), message)
    }
}

#[crate::async_trait]
impl LiveMapView<String, IggyReceivedMessage> for IggyTopicMap {
    async fn scan(&self) -> Result<Vec<(String, IggyReceivedMessage)>> {
        // The partition must exist; `topic_details` also applies the init retries.
        self.poller.topic_details().await?;
        let mut latest: BTreeMap<String, IggyReceivedMessage> = BTreeMap::new();
        self.poller
            .scan(&[self.partition], |m| {
                if let Some((key, value)) = self.classify(m) {
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
impl LiveMapFeed<String, IggyReceivedMessage> for IggyTopicMap {
    async fn skip_initial_scan(&self, _operator: &LiveComponentOperator) -> Result<bool> {
        self.poller.topic_details().await?;
        self.poller.skip_initial_scan(&[self.partition]).await
    }

    async fn watch(
        &self,
        subscriber: LiveMapSubscriber<String, IggyReceivedMessage>,
    ) -> Result<()> {
        self.poller
            .watch(
                &[self.partition],
                &StreamToMap {
                    map: &subscriber,
                    key_of: |message| self.classify(message),
                },
            )
            .await
    }
}

/// A **keyless, append-only** [`LiveStream`] of every message of an Iggy
/// topic, across **all partitions** — the Rust analogue of Python's
/// `topic_as_stream`, extended to read every partition instead of one.
/// [`payloads`](Self::payloads) is the `LiveStream<Vec<u8>>` view.
///
/// [`watch`](LiveStream::watch) delivers each message to the subscriber,
/// stores a partition's offset for the consumer group once the messages up to
/// it are processed, and marks the subscriber ready once every partition has
/// caught up to the end it had when the watch started.
///
/// It is also a [`LiveMapView`] for
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live): each message becomes
/// a distinct child keyed by its `"{partition}:{offset}"` position;
/// [`scan`](LiveMapView::scan) reads every partition up to its current offset
/// and the feed's `watch` tails new messages from there. Re-running is
/// idempotent: the position keys are stable.
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

impl IggyTopicStream {
    /// The `LiveStream<Vec<u8>>` view of this stream: each message's payload.
    /// Python's `TopicStream.payloads()`.
    pub fn payloads(self) -> IggyPayloadStream {
        IggyPayloadStream { stream: self }
    }
}

/// The stream's child key for a message: its position.
fn position_key(message: &IggyReceivedMessage) -> String {
    format!("{}:{}", message.partition, message.offset)
}

#[crate::async_trait]
impl LiveStream<IggyReceivedMessage> for IggyTopicStream {
    async fn watch(
        &self,
        subscriber: &dyn LiveStreamSubscriber<IggyReceivedMessage>,
    ) -> Result<()> {
        let ids = self.poller.partition_ids().await?;
        self.poller.watch(&ids, subscriber).await
    }
}

#[crate::async_trait]
impl LiveMapView<String, IggyReceivedMessage> for IggyTopicStream {
    async fn scan(&self) -> Result<Vec<(String, IggyReceivedMessage)>> {
        let ids = self.poller.partition_ids().await?;
        let mut out: Vec<(String, IggyReceivedMessage)> = Vec::new();
        self.poller
            .scan(&ids, |m| out.push((position_key(&m), m)))
            .await?;
        Ok(out)
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, IggyReceivedMessage> for IggyTopicStream {
    async fn skip_initial_scan(&self, _operator: &LiveComponentOperator) -> Result<bool> {
        let ids = self.poller.partition_ids().await?;
        self.poller.skip_initial_scan(&ids).await
    }

    async fn watch(
        &self,
        subscriber: LiveMapSubscriber<String, IggyReceivedMessage>,
    ) -> Result<()> {
        let ids = self.poller.partition_ids().await?;
        self.poller
            .watch(
                &ids,
                &StreamToMap {
                    map: &subscriber,
                    key_of: |message| Some((position_key(&message), Some(message))),
                },
            )
            .await
    }
}

/// The `LiveStream<Vec<u8>>` view of an [`IggyTopicStream`]: each message's
/// payload. From [`IggyTopicStream::payloads`].
pub struct IggyPayloadStream {
    stream: IggyTopicStream,
}

/// Adapts a payload subscriber to the message-level stream.
struct PayloadsAdapter<'a>(&'a dyn LiveStreamSubscriber<Vec<u8>>);

#[crate::async_trait]
impl LiveStreamSubscriber<IggyReceivedMessage> for PayloadsAdapter<'_> {
    async fn send(&self, message: IggyReceivedMessage) -> Result<ReadyHandle> {
        self.0.send(message.payload).await
    }

    async fn mark_ready(&self) {
        self.0.mark_ready().await;
    }

    async fn read_committed_state(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.read_committed_state(key).await
    }

    async fn write_committed_state(&self, key: &str, value: &[u8]) -> Result<()> {
        self.0.write_committed_state(key, value).await
    }
}

#[crate::async_trait]
impl LiveStream<Vec<u8>> for IggyPayloadStream {
    async fn watch(&self, subscriber: &dyn LiveStreamSubscriber<Vec<u8>>) -> Result<()> {
        let ids = self.stream.poller.partition_ids().await?;
        self.stream
            .poller
            .watch(&ids, &PayloadsAdapter(subscriber))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(bytes: &[u8]) -> Fingerprint {
        Fingerprint::from_bytes(bytes)
    }

    // Upsert new state (no previous record, prev_may_be_missing).
    #[test]
    fn upsert_new_state() {
        let d = decide_message(b"k1", Some(b"v1"), &[], true, None)
            .unwrap()
            .expect("should send");
        assert_eq!(d.value, b"v1");
        assert_eq!(d.tracking, Some(fp(b"v1")));
    }

    // Unchanged value with a matching previous fingerprint → skip.
    #[test]
    fn upsert_unchanged_skips() {
        let d = decide_message(b"k1", Some(b"v1"), &[fp(b"v1")], false, None).unwrap();
        assert_eq!(d, None);
    }

    // Changed value → send the new value.
    #[test]
    fn upsert_changed_value() {
        let d = decide_message(b"k1", Some(b"v2"), &[fp(b"v1")], false, None)
            .unwrap()
            .expect("should send");
        assert_eq!(d.value, b"v2");
        assert_eq!(d.tracking, Some(fp(b"v2")));
    }

    // prev_may_be_missing forces a send even when the fingerprint matches.
    #[test]
    fn upsert_prev_may_be_missing_forces_send() {
        let d = decide_message(b"k1", Some(b"v1"), &[fp(b"v1")], true, None).unwrap();
        assert!(d.is_some());
    }

    // Delete without a deletion_value_fn → error (Iggy has no tombstone).
    #[test]
    fn delete_without_callback_errors() {
        let err = decide_message(b"k1", None, &[fp(b"v1")], false, None).unwrap_err();
        assert!(err.to_string().contains("tombstone"));
    }

    // Delete with a deletion_value_fn → send that value, no tracking record.
    #[test]
    fn delete_with_callback_uses_value() {
        let f: DeletionValueFn =
            Arc::new(|k: &[u8]| format!("deleted:{}", String::from_utf8_lossy(k)).into_bytes());
        let d = decide_message(b"k1", None, &[fp(b"v1")], false, Some(&f))
            .unwrap()
            .expect("should send deletion value");
        assert_eq!(d.value, b"deleted:k1");
        assert_eq!(d.tracking, None);
    }

    // Delete with no previous record and prev known-present → skip.
    #[test]
    fn delete_no_prev_skips() {
        let d = decide_message(b"k1", None, &[], false, None).unwrap();
        assert_eq!(d, None);
    }

    // The tracking record is the raw-bytes fingerprint, as Python's
    // `fingerprint_bytes` computes it, not a serde encoding of the bytes.
    #[test]
    fn tracking_record_is_the_raw_bytes_fingerprint() {
        let d = decide_message(b"k", Some(b"v1"), &[], true, None)
            .unwrap()
            .unwrap();
        assert_ne!(
            d.tracking,
            Some(Fingerprint::from(&b"v1".to_vec()).unwrap())
        );
    }

    #[test]
    fn topic_key_round_trips_through_the_stable_key() {
        let key = TopicKey {
            producer_key: "p".into(),
            stream: "s".into(),
            topic: "t".into(),
            partition: 3,
        };
        let back = TopicKey::from_stable_key(&key.clone().stable_key()).unwrap();
        assert!(back == key);
        assert!(TopicKey::from_stable_key(&StableKey::Int(1)).is_err());
    }

    fn message(offset: u64, payload: &str) -> IggyReceivedMessage {
        IggyReceivedMessage {
            partition: 2,
            offset,
            id: 0,
            timestamp: 0,
            checksum: 0,
            payload: payload.as_bytes().to_vec(),
        }
    }

    // The key and deletion functions see the whole message, not only its
    // payload; a message without a key is skipped.
    #[test]
    fn map_keys_and_deletes_from_the_whole_message() {
        let key_fn: IggyKeyFn = Arc::new(|m| {
            m.payload_str()
                .split_once('=')
                .map(|(k, _)| format!("{}:{k}", m.partition))
        });
        let is_deletion: IggyIsDeletionFn = Arc::new(|m| m.payload_str().ends_with("=DEL"));
        assert_eq!(
            classify_message(&key_fn, Some(&is_deletion), message(1, "k=DEL")),
            Some(("2:k".into(), None))
        );
        let kept = classify_message(&key_fn, Some(&is_deletion), message(1, "k=v")).unwrap();
        assert_eq!(kept.0, "2:k");
        assert_eq!(kept.1.unwrap().payload_str(), "k=v");
        assert_eq!(classify_message(&key_fn, None, message(1, "nokey")), None);
    }

    #[test]
    fn stream_position_key_names_partition_and_offset() {
        assert_eq!(position_key(&message(7, "")), "2:7");
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
