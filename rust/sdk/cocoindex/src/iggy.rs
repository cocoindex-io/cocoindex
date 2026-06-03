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
//! Use [`iggy_topic_target`] to build a composable target state, or
//! [`declare_iggy_topic_target`] / [`mount_iggy_topic_target`] to get a handle
//! for declaring messages.
//!
//! This is the Rust analogue of Python's `cocoindex.connectors.iggy` **target**.
//! The Iggy *source* (`topic_as_stream` / `topic_as_map`) depends on the live
//! source/map runtime, which the Rust SDK does not expose yet.

use std::sync::Arc;

use bytes::Bytes;
use cocoindex_utils::fingerprint::Fingerprint;
use iggy::prelude::{Client, Identifier, IggyClient, IggyMessage, MessageClient, Partitioning};
use serde::{Deserialize, Serialize};

/// Re-export of the upstream [`iggy`] crate prelude. Streams and topics are
/// user-managed, so callers use this to create/manage them and to poll messages
/// back — without having to depend on the `iggy` crate directly. e.g.
/// `cocoindex::iggy::prelude::{StreamClient, TopicClient}`.
pub use ::iggy::prelude;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    register_root_target_states_provider,
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

/// Build a composable [`TargetState`] for an Iggy stream/topic. Pass it to
/// [`declare_iggy_topic_target`]/[`mount_iggy_topic_target`], or to the generic
/// [`crate::target_state::declare_target_state_with_child`]/`mount_target`.
pub fn iggy_topic_target(
    ctx: &Ctx,
    producer: &IggyProducer,
    stream: impl Into<String>,
    topic: impl Into<String>,
    options: IggyTopicOptions,
) -> Result<TargetState<TopicSpec>> {
    let stream = stream.into();
    let topic = topic.into();
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/iggy/topic_spec/{}/{}/{}/{}",
            producer.state_id(),
            stream,
            topic,
            options.partition
        ),
        TopicHandler::new(producer.client.clone(), options.clone()),
    )?;
    Ok(provider.target_state(
        "default",
        TopicSpec {
            stream,
            topic,
            partition: options.partition,
        },
    ))
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
// Topic container handler (root)
// ---------------------------------------------------------------------------

/// The topic container spec (stream/topic/partition). Tracking record + spec are
/// the same: the stream/topic are user-managed, so this never creates/drops.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicSpec {
    stream: String,
    topic: String,
    partition: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TopicAction {
    stream: String,
    topic: String,
    partition: u32,
}

struct TopicHandler {
    client: Arc<IggyClient>,
    deletion_value_fn: Option<DeletionValueFn>,
}

impl TopicHandler {
    fn new(client: Arc<IggyClient>, options: IggyTopicOptions) -> Self {
        Self {
            client,
            deletion_value_fn: options.deletion_value_fn,
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
        // the message child provider. The stream/topic are user-managed (no
        // create/drop), so there is nothing to do on un-declare.
        let Some(spec) = desired else {
            return Ok(None);
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(TopicAction {
                stream: spec.stream.clone(),
                topic: spec.topic.clone(),
                partition: spec.partition,
            }),
            sink: self.topic_sink(),
            tracking_record: Some(spec),
            child_invalidation: None,
        }))
    }
}

impl TopicHandler {
    /// Container sink: fulfills each declared topic with a fresh message child
    /// handler bound to that stream/topic/partition.
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
                                        a.stream,
                                        a.topic,
                                        a.partition,
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
