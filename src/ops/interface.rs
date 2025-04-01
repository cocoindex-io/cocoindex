use std::time::SystemTime;

use crate::base::{
    schema::*,
    spec::{IndexOptions, VectorSimilarityMetric},
    value::*,
};
use crate::prelude::*;
use crate::setup;
use chrono::TimeZone;
use serde::Serialize;

pub struct FlowInstanceContext {
    pub flow_instance_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ordinal(pub i64);

impl Into<i64> for Ordinal {
    fn into(self) -> i64 {
        self.0
    }
}

impl TryFrom<SystemTime> for Ordinal {
    type Error = anyhow::Error;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let duration = time.duration_since(std::time::UNIX_EPOCH)?;
        Ok(duration.as_micros().try_into().map(Ordinal)?)
    }
}

impl<TZ: TimeZone> TryFrom<chrono::DateTime<TZ>> for Ordinal {
    type Error = anyhow::Error;

    fn try_from(time: chrono::DateTime<TZ>) -> Result<Self, Self::Error> {
        Ok(Ordinal(time.timestamp_micros()))
    }
}

pub struct SourceRowMetadata {
    pub key: KeyValue,
    /// None means the ordinal is unavailable.
    pub ordinal: Option<Ordinal>,
}

pub struct SourceChange<'a> {
    /// Last update/deletion ordinal. None means unavailable.
    pub ordinal: Option<Ordinal>,
    pub key: KeyValue,
    /// None means a deletion. None within the `BoxFuture` means the item is gone when polling.
    pub value: Option<BoxFuture<'a, Result<Option<FieldValues>>>>,
}

#[derive(Debug, Default)]
pub struct SourceExecutorListOptions {
    pub include_ordinal: bool,
}

#[async_trait]
pub trait SourceExecutor: Send + Sync {
    /// Get the list of keys for the source.
    fn list<'a>(
        &'a self,
        options: SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<SourceRowMetadata>>>;

    // Get the value for the given key.
    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>>;

    fn change_stream<'a>(&'a self) -> Option<BoxStream<'a, SourceChange<'a>>> {
        None
    }
}

pub trait SourceFactory {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SourceExecutor>>>,
    )>;
}

#[async_trait]
pub trait SimpleFunctionExecutor: Send + Sync {
    /// Evaluate the operation.
    async fn evaluate(&self, args: Vec<Value>) -> Result<Value>;

    fn enable_cache(&self) -> bool {
        false
    }

    /// Must be Some if `enable_cache` is true.
    /// If it changes, the cache will be invalidated.
    fn behavior_version(&self) -> Option<u32> {
        None
    }
}

pub trait SimpleFunctionFactory {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        input_schema: Vec<OpArgSchema>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SimpleFunctionExecutor>>>,
    )>;
}

#[derive(Debug)]
pub struct ExportTargetUpsertEntry {
    pub key: KeyValue,
    pub value: FieldValues,
}

#[derive(Debug, Default)]
pub struct ExportTargetMutation {
    pub upserts: Vec<ExportTargetUpsertEntry>,
    pub delete_keys: Vec<KeyValue>,
}

impl ExportTargetMutation {
    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.delete_keys.is_empty()
    }
}

#[async_trait]
pub trait ExportTargetExecutor: Send + Sync {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupStateCompatibility {
    /// The resource is fully compatible with the desired state.
    /// This means the resource can be updated to the desired state without any loss of data.
    Compatible,
    /// The resource is partially compatible with the desired state.
    /// This means some existing data will be lost after applying the setup change.
    PartialCompatible,
    /// The resource needs to be rebuilt
    NotCompatible,
}

pub trait ExportTargetFactory {
    // The first field of the `input_schema` is the primary key field.
    // If it has struct type, it should be converted to composite primary key.
    fn build(
        self: Arc<Self>,
        name: String,
        spec: serde_json::Value,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        (serde_json::Value, serde_json::Value),
        BoxFuture<'static, Result<(Arc<dyn ExportTargetExecutor>, Option<Arc<dyn QueryTarget>>)>>,
    )>;

    fn check_setup_status(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
    ) -> Result<
        Box<
            dyn setup::ResourceSetupStatusCheck<Key = serde_json::Value, State = serde_json::Value>
                + Send
                + Sync,
        >,
    >;

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility>;
}

#[derive(Clone)]
pub enum ExecutorFactory {
    Source(Arc<dyn SourceFactory + Send + Sync>),
    SimpleFunction(Arc<dyn SimpleFunctionFactory + Send + Sync>),
    ExportTarget(Arc<dyn ExportTargetFactory + Send + Sync>),
}

pub struct VectorMatchQuery {
    pub vector_field_name: String,
    pub vector: Vec<f32>,
    pub similarity_metric: VectorSimilarityMetric,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub data: Vec<Value>,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResults {
    pub fields: Vec<FieldSchema>,
    pub results: Vec<QueryResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResponse {
    pub results: QueryResults,
    pub info: serde_json::Value,
}

#[async_trait]
pub trait QueryTarget: Send + Sync {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults>;
}
