//! Kafka connector: a topic target and topic sources.
//!
//! # Target
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
//! The producer is provided under a [`ContextKey`] (as every connection in the
//! Rust SDK is) and resolved when the target's actions apply. Use
//! [`declare_kafka_topic_target`] / [`mount_kafka_topic_target`] to get a
//! handle for declaring messages. [`KafkaProducer::ensure_topic`] is an
//! explicit, idempotent topic-creation helper outside reconciliation.
//!
//! # Sources
//!
//! [`topic_as_map`] reads one topic as a keyed map (the latest value per message
//! key; a tombstone removes the key) and [`topic_as_stream`] reads one or more
//! topics as an append-only stream of every record. Both yield the full
//! [`KafkaMessage`] (key, value, headers, partition, offset, timestamp), and both
//! read every partition.
//!
//! A [`KafkaConsumer`] built with a
//! [`consumer_group`](KafkaConsumerOptions::consumer_group) keeps its position:
//! the offsets of the records a live run has processed are committed, per
//! partition, to the app's state database under that group name once their
//! child components have synced, and the next live run resumes from them instead
//! of re-reading the topic from the beginning. A consumer without a group
//! re-reads the topic on every run.
//!
//! # Client
//!
//! Uses [`rskafka`], a pure-Rust async Kafka client with no `librdkafka`/C
//! dependency. SASL (PLAIN, SCRAM) and TLS are configured with
//! [`KafkaConnectOptions`]. `rskafka` does not speak the consumer-group
//! protocol, so a consumer group here is CocoIndex's own durable cursor, not a
//! broker-side group: the broker does not see its lag, and two processes with
//! the same group name do not share partitions between them.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::{Record, RecordAndOffset};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

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
// Connection options (shared by the producer and the consumer)
// ---------------------------------------------------------------------------

/// SASL authentication for a Kafka connection.
#[derive(Clone)]
pub enum KafkaSasl {
    /// `SASL/PLAIN` (RFC 4616). Use it over TLS only.
    Plain { username: String, password: String },
    /// `SASL/SCRAM-SHA-256` (RFC 7677).
    ScramSha256 { username: String, password: String },
    /// `SASL/SCRAM-SHA-512`.
    ScramSha512 { username: String, password: String },
}

impl std::fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (mechanism, username) = match self {
            Self::Plain { username, .. } => ("PLAIN", username),
            Self::ScramSha256 { username, .. } => ("SCRAM-SHA-256", username),
            Self::ScramSha512 { username, .. } => ("SCRAM-SHA-512", username),
        };
        f.debug_struct("KafkaSasl")
            .field("mechanism", &mechanism)
            .field("username", username)
            .finish_non_exhaustive()
    }
}

impl KafkaSasl {
    fn into_rskafka(self) -> SaslConfig {
        match self {
            Self::Plain { username, password } => {
                SaslConfig::Plain(Credentials::new(username, password))
            }
            Self::ScramSha256 { username, password } => {
                SaslConfig::ScramSha256(Credentials::new(username, password))
            }
            Self::ScramSha512 { username, password } => {
                SaslConfig::ScramSha512(Credentials::new(username, password))
            }
        }
    }
}

/// TLS for a Kafka connection. `KafkaTls::default()` trusts the Mozilla root
/// certificates (as `webpki-roots` ships them); the fields add a private CA
/// and a client certificate for mutual TLS.
#[derive(Clone, Debug, Default)]
pub struct KafkaTls {
    /// A PEM file of CA certificates to trust in addition to the public roots.
    pub ca_cert_pem: Option<PathBuf>,
    /// A PEM file with the client certificate chain, for mutual TLS. Requires
    /// `client_key_pem`.
    pub client_cert_pem: Option<PathBuf>,
    /// A PEM file with the client's private key (PKCS#8, PKCS#1 or SEC1).
    pub client_key_pem: Option<PathBuf>,
}

impl KafkaTls {
    fn into_rustls(self) -> Result<Arc<rustls::ClientConfig>> {
        use rustls_pki_types::pem::PemObject;
        use rustls_pki_types::{CertificateDer, PrivateKeyDer};

        let mut roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        if let Some(path) = &self.ca_cert_pem {
            for cert in CertificateDer::pem_file_iter(path)
                .map_err(|e| Error::engine(format!("kafka tls: read {}: {e}", path.display())))?
            {
                let cert = cert.map_err(|e| {
                    Error::engine(format!("kafka tls: parse {}: {e}", path.display()))
                })?;
                roots.add(cert).map_err(|e| {
                    Error::engine(format!(
                        "kafka tls: CA certificate in {}: {e}",
                        path.display()
                    ))
                })?;
            }
        }
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::engine(format!("kafka tls: {e}")))?
        .with_root_certificates(roots);
        let config = match (&self.client_cert_pem, &self.client_key_pem) {
            (None, None) => builder.with_no_client_auth(),
            (Some(cert_path), Some(key_path)) => {
                let certs = CertificateDer::pem_file_iter(cert_path)
                    .map_err(|e| {
                        Error::engine(format!("kafka tls: read {}: {e}", cert_path.display()))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        Error::engine(format!("kafka tls: parse {}: {e}", cert_path.display()))
                    })?;
                let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| {
                    Error::engine(format!("kafka tls: read {}: {e}", key_path.display()))
                })?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| Error::engine(format!("kafka tls: client certificate: {e}")))?
            }
            _ => {
                return Err(Error::engine(
                    "kafka tls: client_cert_pem and client_key_pem must be set together",
                ));
            }
        };
        Ok(Arc::new(config))
    }
}

/// How to reach and authenticate with the cluster. The default is a plaintext
/// connection without authentication, as for a local development broker.
#[derive(Clone, Debug, Default)]
pub struct KafkaConnectOptions {
    /// SASL authentication, or `None` for an unauthenticated connection.
    pub sasl: Option<KafkaSasl>,
    /// TLS, or `None` for a plaintext connection.
    pub tls: Option<KafkaTls>,
}

/// Build an `rskafka` client for `bootstrap_servers`; returns it with the
/// stable identity (the bootstrap-server list) handles use in state keys.
async fn build_client(
    bootstrap_servers: &[&str],
    options: KafkaConnectOptions,
) -> Result<(Arc<Client>, Arc<str>)> {
    let boot: Vec<String> = bootstrap_servers.iter().map(|s| s.to_string()).collect();
    if boot.is_empty() {
        return Err(Error::engine(
            "kafka: at least one bootstrap server is required",
        ));
    }
    let state_id = boot.join(",");
    let mut builder = ClientBuilder::new(boot);
    if let Some(sasl) = options.sasl {
        builder = builder.sasl_config(sasl.into_rskafka());
    }
    if let Some(tls) = options.tls {
        builder = builder.tls_config(tls.into_rustls()?);
    }
    let client = builder
        .build()
        .await
        .map_err(|e| Error::engine(format!("kafka connect: {e}")))?;
    Ok((Arc::new(client), Arc::from(state_id)))
}

// ---------------------------------------------------------------------------
// KafkaProducer — connection handle
// ---------------------------------------------------------------------------

/// A Kafka connection handle for the topic target. Clone-cheap (the underlying
/// client is shared). Provide it under a [`ContextKey`] with
/// `EnvironmentBuilder::provide_key`; the target names the key.
#[derive(Clone)]
pub struct KafkaProducer {
    client: Arc<Client>,
    state_id: Arc<str>,
}

impl KafkaProducer {
    /// Connect to a Kafka (or Redpanda) cluster given its bootstrap servers,
    /// e.g. `["localhost:9092"]`, in plaintext without authentication.
    pub async fn connect(bootstrap_servers: &[&str]) -> Result<Self> {
        Self::connect_with_options(bootstrap_servers, KafkaConnectOptions::default()).await
    }

    /// [`connect`](Self::connect) with SASL and/or TLS.
    pub async fn connect_with_options(
        bootstrap_servers: &[&str],
        options: KafkaConnectOptions,
    ) -> Result<Self> {
        let (client, state_id) = build_client(bootstrap_servers, options).await?;
        Ok(Self { client, state_id })
    }

    /// Stable identity (the bootstrap-server list).
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
    producer: &ContextKey<KafkaProducer>,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    mount_kafka_topic_target(ctx, producer, topic, options)
}

/// Declare a Kafka topic target in the current component and return a handle
/// for declaring messages. `producer` names the [`KafkaProducer`] provided in
/// the environment; it is resolved when the messages are produced.
pub fn mount_kafka_topic_target(
    ctx: &Ctx,
    producer: &ContextKey<KafkaProducer>,
    topic: impl Into<String>,
    options: KafkaTopicOptions,
) -> Result<KafkaTopicTarget> {
    let topic = topic.into();
    let messages = register_root_target_states_provider(
        ctx,
        // Stable connection identity = the ContextKey's name; the client is
        // resolved at apply time from the host context, as the other targets do.
        format!("cocoindex/kafka/topic/{}/{}", producer.name(), topic),
        MessageHandler::new(
            producer.name().to_string(),
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
    /// The `ContextKey` name the producer was provided under.
    producer_key: String,
    topic: String,
    deletion_value_fn: Option<DeletionValueFn>,
    sink: SinkCache<MessageAction>,
}

impl MessageHandler {
    fn new(
        producer_key: String,
        topic: String,
        deletion_value_fn: Option<DeletionValueFn>,
    ) -> Self {
        Self {
            producer_key,
            topic,
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
            sink: self.sink.get(|| self.message_sink()),
            tracking_record: decision.tracking,
            child_invalidation: None,
        }))
    }
}

/// Resolve the live [`KafkaProducer`] from the host context by the
/// `ContextKey` name it was provided under.
fn resolve_producer(
    host_ctx: &Arc<ContextStore>,
    producer_key: &str,
) -> Result<Arc<KafkaProducer>> {
    host_ctx
        .resolve::<KafkaProducer>(producer_key)
        .ok_or_else(|| {
            Error::engine(format!(
                "kafka target: producer `{producer_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, producer))"
            ))
        })
}

impl MessageHandler {
    fn message_sink(&self) -> TargetActionSink<MessageAction> {
        let producer_key = self.producer_key.clone();
        let topic = self.topic.clone();
        // The topic's partitions, loaded on the first apply from the resolved
        // producer and kept for the handler's lifetime.
        let partitions: Arc<OnceCell<Arc<TopicPartitions>>> = Arc::new(OnceCell::new());
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<MessageAction>>| {
                let producer_key = producer_key.clone();
                let topic = topic.clone();
                let partitions = partitions.clone();
                async move {
                    let partitions = partitions
                        .get_or_try_init(|| async {
                            let producer = resolve_producer(&host_ctx, &producer_key)?;
                            Ok::<_, Error>(Arc::new(TopicPartitions::new(
                                producer.client.clone(),
                                topic,
                            )))
                        })
                        .await?;
                    let timestamp = current_timestamp();
                    let mut by_partition: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
                    for action in actions {
                        let msg = match action {
                            TargetAction::Create(m)
                            | TargetAction::Update(m)
                            | TargetAction::Delete(m) => m,
                        };
                        let partition = partitions.partition_for_key(msg.key.as_bytes()).await?;
                        by_partition.entry(partition).or_default().push(Record {
                            key: Some(msg.key.into_bytes()),
                            value: msg.value,
                            headers: BTreeMap::new(),
                            timestamp,
                        });
                    }
                    for (partition, records) in by_partition {
                        partitions
                            .client(partition)
                            .await?
                            .produce(records, Compression::NoCompression)
                            .await
                            .map_err(|e| Error::engine(format!("kafka produce: {e}")))?;
                    }
                    Ok(())
                }
            },
        )
    }
}

/// A topic's partitions and their clients, loaded lazily on first produce.
struct TopicPartitions {
    client: Arc<Client>,
    topic: String,
    /// Partition ids, sorted.
    ids: OnceCell<Vec<i32>>,
    clients: tokio::sync::Mutex<BTreeMap<i32, Arc<PartitionClient>>>,
}

impl TopicPartitions {
    fn new(client: Arc<Client>, topic: String) -> Self {
        Self {
            client,
            topic,
            ids: OnceCell::new(),
            clients: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    async fn partition_for_key(&self, key: &[u8]) -> Result<i32> {
        let ids = self
            .ids
            .get_or_try_init(|| async { topic_partition_ids(&self.client, &self.topic).await })
            .await?;
        Ok(ids[partition_index(key, ids.len())])
    }

    async fn client(&self, partition: i32) -> Result<Arc<PartitionClient>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&partition) {
            return Ok(client.clone());
        }
        let client = Arc::new(
            self.client
                .partition_client(self.topic.clone(), partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| Error::engine(format!("kafka partition_client: {e}")))?,
        );
        clients.insert(partition, client.clone());
        Ok(client)
    }
}

/// A topic's partition ids (sorted) from cluster metadata; an error when the
/// topic does not exist or has no partitions.
async fn topic_partition_ids(client: &Client, topic: &str) -> Result<Vec<i32>> {
    let topics = client
        .list_topics()
        .await
        .map_err(|e| Error::engine(format!("kafka list_topics: {e}")))?;
    let mut ids: Vec<i32> = topics
        .into_iter()
        .find(|t| t.name == topic)
        .ok_or_else(|| Error::engine(format!("kafka topic {topic:?} not found")))?
        .partitions
        .into_iter()
        .collect();
    if ids.is_empty() {
        return Err(Error::engine(format!(
            "kafka topic {topic:?} has no partitions"
        )));
    }
    ids.sort_unstable();
    Ok(ids)
}

/// The offset of a partition's earliest retained record.
async fn earliest_offset(pc: &PartitionClient) -> Result<i64> {
    pc.get_offset(OffsetAt::Earliest)
        .await
        .map_err(|e| Error::engine(format!("kafka get_offset: {e}")))
}

/// The partition index for a record key: librdkafka's default
/// `consistent_random` partitioner (CRC32 of the key modulo the partition
/// count), so the Rust and Python connectors place a key in the same
/// partition and log compaction sees one history per key.
fn partition_index(key: &[u8], num_partitions: usize) -> usize {
    crc32fast::hash(key) as usize % num_partitions
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
// Kafka sources
// ---------------------------------------------------------------------------

/// A consumed Kafka record, as the sources yield it: the Rust analogue of
/// `confluent_kafka.Message`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// The message key; `None` for a keyless record.
    pub key: Option<Vec<u8>>,
    /// The message value; `None` for a tombstone.
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    /// The record timestamp the broker or producer stamped it with.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl KafkaMessage {
    fn from_record(topic: &str, partition: i32, record: RecordAndOffset) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            offset: record.offset,
            key: record.record.key,
            value: record.record.value,
            headers: record.record.headers,
            timestamp: record.record.timestamp,
        }
    }

    /// The key as text (lossy on invalid UTF-8); `None` for a keyless record.
    pub fn key_str(&self) -> Option<String> {
        self.key
            .as_deref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
    }

    /// The value as text (lossy on invalid UTF-8); `None` for a tombstone.
    pub fn value_str(&self) -> Option<String> {
        self.value
            .as_deref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
    }
}

/// Options for [`KafkaConsumer::connect_with_options`].
#[derive(Clone, Debug, Default)]
pub struct KafkaConsumerOptions {
    /// The consumer group: the name the sources commit their per-partition
    /// offsets under, in the app's state database, and resume from on the next
    /// live run. `None` re-reads every topic from the beginning on each run.
    pub consumer_group: Option<String>,
    pub connect: KafkaConnectOptions,
}

/// A Kafka consumer handle. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct KafkaConsumer {
    client: Arc<Client>,
    state_id: Arc<str>,
    consumer_group: Option<Arc<str>>,
}

impl KafkaConsumer {
    /// Connect to a Kafka (or Redpanda) cluster given its bootstrap servers, in
    /// plaintext without authentication and without a consumer group.
    pub async fn connect(bootstrap_servers: &[&str]) -> Result<Self> {
        Self::connect_with_options(bootstrap_servers, KafkaConsumerOptions::default()).await
    }

    /// [`connect`](Self::connect) with a consumer group, SASL and/or TLS.
    pub async fn connect_with_options(
        bootstrap_servers: &[&str],
        options: KafkaConsumerOptions,
    ) -> Result<Self> {
        if options.consumer_group.as_deref() == Some("") {
            return Err(Error::engine("kafka: consumer_group must be non-empty"));
        }
        let (client, state_id) = build_client(bootstrap_servers, options.connect).await?;
        Ok(Self {
            client,
            state_id,
            consumer_group: options.consumer_group.map(Arc::from),
        })
    }

    /// Stable identity (the bootstrap-server list).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// The consumer group, if one was configured.
    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }
}

/// Decides whether a consumed message is a *deletion* for its key. The default
/// treats a tombstone (`None` value) as a deletion.
pub type IsDeletionFn = Arc<dyn Fn(&KafkaMessage) -> bool + Send + Sync>;

/// Options for [`topic_as_map_with_options`].
#[derive(Clone, Default)]
pub struct KafkaSourceOptions {
    /// Custom deletion predicate; defaults to "value is a tombstone (`None`)".
    /// A tombstone is a deletion regardless of this predicate.
    pub is_deletion: Option<IsDeletionFn>,
}

/// A partition's position in the reader's committed-offset state: the key the
/// next offset to read is committed under.
fn offset_state_key(group: &str, topic: &str, partition: i32) -> String {
    format!("cocoindex/kafka/offset/{group}/{topic}/{partition}")
}

/// How a source delivers one consumed message to its subscriber.
#[crate::async_trait]
trait Deliver: Send + Sync {
    async fn deliver(
        &self,
        message: KafkaMessage,
        subscriber: &LiveMapSubscriber<String, KafkaMessage>,
    ) -> Result<()>;
}

/// The partition reading shared by [`KafkaTopicMap`] and [`KafkaTopicStream`]:
/// a catch-up scan of every partition of every topic up to its high-watermark,
/// and a tail from where the scan (or the committed cursor) ended.
struct TopicReader {
    client: Arc<Client>,
    topics: Vec<String>,
    consumer_group: Option<Arc<str>>,
    /// Per-partition offset `watch` resumes from: where the most recent `scan`
    /// ended, or the committed cursor when the scan was skipped, so the tail
    /// neither gaps nor double-reads.
    watch_start: tokio::sync::Mutex<BTreeMap<(String, i32), i64>>,
    watch_guard: SingleWatcherGuard,
}

impl TopicReader {
    fn new(consumer: &KafkaConsumer, topics: Vec<String>, label: &str) -> Self {
        Self {
            client: consumer.client.clone(),
            topics,
            consumer_group: consumer.consumer_group.clone(),
            watch_start: tokio::sync::Mutex::new(BTreeMap::new()),
            watch_guard: SingleWatcherGuard::new(label),
        }
    }

    /// Every `(topic, partition)` of the configured topics.
    async fn partitions(&self) -> Result<Vec<(String, i32)>> {
        let mut out = Vec::new();
        for topic in &self.topics {
            for id in topic_partition_ids(&self.client, topic).await? {
                out.push((topic.clone(), id));
            }
        }
        Ok(out)
    }

    async fn partition_client(&self, topic: &str, id: i32) -> Result<PartitionClient> {
        self.client
            .partition_client(topic.to_string(), id, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| Error::engine(format!("kafka partition_client {topic}/{id}: {e}")))
    }

    /// Read every partition from its earliest retained record to its
    /// high-watermark, passing each record to `f`, and record where each
    /// partition ended for `watch`.
    async fn scan(&self, mut f: impl FnMut(KafkaMessage)) -> Result<()> {
        let mut ends: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for (topic, id) in self.partitions().await? {
            let pc = self.partition_client(&topic, id).await?;
            let mut offset = earliest_offset(&pc).await?;
            loop {
                let (records, hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 500)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                let got_any = !records.is_empty();
                for r in records {
                    offset = r.offset + 1;
                    f(KafkaMessage::from_record(&topic, id, r));
                }
                if !got_any || offset >= hwm {
                    break;
                }
            }
            ends.insert((topic, id), offset);
        }
        *self.watch_start.lock().await = ends;
        Ok(())
    }

    /// With a consumer group: resume from the committed cursor when every
    /// partition has one, skipping the catch-up scan. `watch` then replays the
    /// records since the cursor.
    async fn skip_initial_scan(&self, operator: &LiveComponentOperator) -> Result<bool> {
        let Some(group) = &self.consumer_group else {
            return Ok(false);
        };
        let mut starts: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for (topic, id) in self.partitions().await? {
            let committed: Option<i64> = operator
                .read_committed_state(offset_state_key(group, &topic, id))
                .await?;
            let Some(offset) = committed else {
                // A partition without a cursor (first run, or a partition added
                // since) needs the scan.
                return Ok(false);
            };
            starts.insert((topic, id), offset);
        }
        *self.watch_start.lock().await = starts;
        Ok(true)
    }

    /// Tail every partition from `watch_start`, delivering each record through
    /// `deliver`. With a consumer group, each partition's next offset is
    /// committed after every fetched batch has been delivered (and so its child
    /// components synced).
    async fn watch(
        &self,
        deliver: &dyn Deliver,
        subscriber: &LiveMapSubscriber<String, KafkaMessage>,
    ) -> Result<()> {
        let _watch_token = self.watch_guard.enter()?;
        let mut offsets = self.watch_start.lock().await.clone();
        let mut clients = Vec::new();
        for (topic, id) in self.partitions().await? {
            let pc = self.partition_client(&topic, id).await?;
            // A partition without a scan snapshot or cursor starts at its
            // earliest retained record, not offset 0, which retention may have
            // removed.
            if !offsets.contains_key(&(topic.clone(), id)) {
                offsets.insert((topic.clone(), id), earliest_offset(&pc).await?);
            }
            clients.push((topic, id, pc));
        }
        if let Some(group) = &self.consumer_group {
            // The scan's end (or the cursor read back) is the position the next
            // run resumes from if nothing new arrives.
            for ((topic, id), offset) in &offsets {
                subscriber
                    .write_committed_state(offset_state_key(group, topic, *id), offset)
                    .await?;
            }
        }
        // Round-robin all partitions on one loop (one subscriber, used
        // sequentially). Each partition fetch uses a short max-wait so the
        // cycle stays responsive; an empty full cycle backs off briefly.
        loop {
            let mut got_any = false;
            for (topic, id, pc) in &clients {
                let key = (topic.clone(), *id);
                let Some(&offset) = offsets.get(&key) else {
                    continue;
                };
                let (records, _hwm) = pc
                    .fetch_records(offset, 1..1_000_000, 250)
                    .await
                    .map_err(|e| Error::engine(format!("kafka fetch_records: {e}")))?;
                if records.is_empty() {
                    continue;
                }
                got_any = true;
                let mut next = offset;
                for r in records {
                    next = r.offset + 1;
                    deliver
                        .deliver(KafkaMessage::from_record(topic, *id, r), subscriber)
                        .await?;
                }
                offsets.insert(key, next);
                if let Some(group) = &self.consumer_group {
                    subscriber
                        .write_committed_state(offset_state_key(group, topic, *id), &next)
                        .await?;
                }
            }
            if !got_any {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
}

/// A keyed change feed over **all partitions** of a Kafka topic: the latest
/// message per key, with tombstones (or [`KafkaSourceOptions::is_deletion`])
/// removing keys. The Rust analogue of Python's `topic_as_map`. Records with no
/// key are skipped.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads every partition up to
/// its high-watermark (the catch-up snapshot, compacted per key) and
/// [`watch`](LiveMapFeed::watch) tails new records across partitions from there.
/// With a consumer group (see [`KafkaConsumerOptions`]) a live run resumes from
/// the committed offsets instead of scanning. Feed it to
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live).
pub struct KafkaTopicMap {
    reader: TopicReader,
    is_deletion: Option<IsDeletionFn>,
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
        reader: TopicReader::new(consumer, vec![topic.into()], "KafkaTopicMap"),
        is_deletion: options.is_deletion,
    }
}

/// Classify a message into `(key, Some(message))` upsert or `(key, None)`
/// delete. A tombstone is always a delete; `is_deletion` may turn a valued
/// record into one. Returns `None` for a record with no key (skipped, as in
/// Python).
fn classify_message(
    is_deletion: Option<&IsDeletionFn>,
    message: KafkaMessage,
) -> Option<(String, Option<KafkaMessage>)> {
    let key = message.key_str()?;
    let is_delete = message.value.is_none() || is_deletion.is_some_and(|f| f(&message));
    Some((key, if is_delete { None } else { Some(message) }))
}

impl KafkaTopicMap {
    fn classify(&self, message: KafkaMessage) -> Option<(String, Option<KafkaMessage>)> {
        classify_message(self.is_deletion.as_ref(), message)
    }
}

#[crate::async_trait]
impl Deliver for KafkaTopicMap {
    async fn deliver(
        &self,
        message: KafkaMessage,
        subscriber: &LiveMapSubscriber<String, KafkaMessage>,
    ) -> Result<()> {
        let Some((key, value)) = self.classify(message) else {
            return Ok(());
        };
        match value {
            Some(v) => subscriber.update(key, v).await,
            None => subscriber.delete(key).await,
        }
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, KafkaMessage> for KafkaTopicMap {
    async fn skip_initial_scan(&self, operator: &LiveComponentOperator) -> Result<bool> {
        self.reader.skip_initial_scan(operator).await
    }

    async fn watch(&self, subscriber: LiveMapSubscriber<String, KafkaMessage>) -> Result<()> {
        self.reader.watch(self, &subscriber).await
    }
}

#[crate::async_trait]
impl LiveMapView<String, KafkaMessage> for KafkaTopicMap {
    async fn scan(&self) -> Result<Vec<(String, KafkaMessage)>> {
        let mut latest: BTreeMap<String, KafkaMessage> = BTreeMap::new();
        self.reader
            .scan(|message| {
                if let Some((key, value)) = self.classify(message) {
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

/// A **keyless, append-only** feed over **all partitions** of one or more Kafka
/// topics — the Rust analogue of Python's `topic_as_stream`.
///
/// Unlike [`KafkaTopicMap`], nothing is compacted or deleted: every record is
/// delivered exactly once, keyed by its `"{topic}:{partition}:{offset}"`
/// position, so each message becomes a distinct child under
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). Tombstones (records
/// with a `None` value) are delivered too, as Python's message-level stream
/// does; filter on [`KafkaMessage::value`] for the payloads view.
///
/// As a [`LiveMapView`], [`scan`](LiveMapView::scan) reads every partition up to
/// its high-watermark and [`watch`](LiveMapFeed::watch) tails new records from
/// there, or from the committed offsets of the consumer group. Re-running is
/// idempotent: the position keys are stable, so a replayed catch-up reconciles
/// to no-ops.
pub struct KafkaTopicStream {
    reader: TopicReader,
}

/// Build a [`KafkaTopicStream`] over `topics`.
pub fn topic_as_stream(
    consumer: &KafkaConsumer,
    topics: impl IntoIterator<Item = impl Into<String>>,
) -> KafkaTopicStream {
    KafkaTopicStream {
        reader: TopicReader::new(
            consumer,
            topics.into_iter().map(Into::into).collect(),
            "KafkaTopicStream",
        ),
    }
}

/// The stream's child key for a message: its position.
fn position_key(message: &KafkaMessage) -> String {
    format!("{}:{}:{}", message.topic, message.partition, message.offset)
}

#[crate::async_trait]
impl Deliver for KafkaTopicStream {
    async fn deliver(
        &self,
        message: KafkaMessage,
        subscriber: &LiveMapSubscriber<String, KafkaMessage>,
    ) -> Result<()> {
        subscriber.update(position_key(&message), message).await
    }
}

#[crate::async_trait]
impl LiveMapView<String, KafkaMessage> for KafkaTopicStream {
    async fn scan(&self) -> Result<Vec<(String, KafkaMessage)>> {
        let mut out = Vec::new();
        self.reader
            .scan(|message| out.push((position_key(&message), message)))
            .await?;
        Ok(out)
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, KafkaMessage> for KafkaTopicStream {
    async fn skip_initial_scan(&self, operator: &LiveComponentOperator) -> Result<bool> {
        self.reader.skip_initial_scan(operator).await
    }

    async fn watch(&self, subscriber: LiveMapSubscriber<String, KafkaMessage>) -> Result<()> {
        self.reader.watch(self, &subscriber).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_index_matches_librdkafka_consistent_partitioner() {
        // CRC32 (IEEE) check value of "123456789" is 0xCBF43926.
        assert_eq!(partition_index(b"123456789", usize::MAX), 0xCBF4_3926);
        assert_eq!(partition_index(b"123456789", 7), 0xCBF4_3926 % 7);
        // A single-partition topic always maps to partition index 0.
        assert_eq!(partition_index(b"any key", 1), 0);
    }

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

    fn message(key: Option<&[u8]>, value: Option<&[u8]>) -> KafkaMessage {
        KafkaMessage {
            topic: "t".into(),
            partition: 0,
            offset: 7,
            key: key.map(<[u8]>::to_vec),
            value: value.map(<[u8]>::to_vec),
            headers: BTreeMap::new(),
            timestamp: chrono::DateTime::default(),
        }
    }

    // A tombstone deletes its key even with a custom predicate that says no;
    // a keyless record is skipped; the predicate turns a value into a delete.
    #[test]
    fn map_classifies_tombstones_keyless_and_custom_deletes() {
        let never: IsDeletionFn = Arc::new(|_| false);
        assert_eq!(
            classify_message(Some(&never), message(Some(b"k"), None)),
            Some(("k".into(), None))
        );
        assert_eq!(classify_message(None, message(None, Some(b"v"))), None);
        let on_marker: IsDeletionFn = Arc::new(|m| m.value.as_deref() == Some(b"DEL"));
        assert_eq!(
            classify_message(Some(&on_marker), message(Some(b"k"), Some(b"DEL"))),
            Some(("k".into(), None))
        );
        let kept = classify_message(Some(&on_marker), message(Some(b"k"), Some(b"v"))).unwrap();
        assert_eq!(kept.0, "k");
        assert_eq!(kept.1.unwrap().value_str().as_deref(), Some("v"));
    }

    #[test]
    fn stream_position_key_names_topic_partition_offset() {
        assert_eq!(position_key(&message(None, None)), "t:0:7");
    }

    #[test]
    fn tls_without_files_builds_and_half_client_auth_is_an_error() {
        assert!(KafkaTls::default().into_rustls().is_ok());
        let half = KafkaTls {
            client_cert_pem: Some("/nonexistent/cert.pem".into()),
            ..KafkaTls::default()
        };
        assert!(half.into_rustls().is_err());
        let missing_ca = KafkaTls {
            ca_cert_pem: Some("/nonexistent/ca.pem".into()),
            ..KafkaTls::default()
        };
        assert!(missing_ca.into_rustls().is_err());
    }

    #[test]
    fn sasl_debug_hides_the_password() {
        let s = format!(
            "{:?}",
            KafkaSasl::Plain {
                username: "u".into(),
                password: "hunter2".into(),
            }
        );
        assert!(s.contains("PLAIN") && s.contains("\"u\"") && !s.contains("hunter2"));
    }
}
