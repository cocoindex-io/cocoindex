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
    pub auth_registry: Arc<AuthRegistry>,
    pub py_exec_ctx: Option<Arc<crate::py::PythonExecutionContext>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ordinal(pub i64);

impl From<Ordinal> for i64 {
    fn from(val: Ordinal) -> Self {
        val.0
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

pub enum SourceValueChange {
    /// None means value unavailable in this change - needs a separate poll by get_value() API.
    Upsert(Option<FieldValues>),
    Delete,
}

pub struct SourceChange {
    /// Last update/deletion ordinal. None means unavailable.
    pub ordinal: Option<Ordinal>,
    pub key: KeyValue,
    pub value: SourceValueChange,
}

#[derive(Debug, Default)]
pub struct SourceExecutorListOptions {
    pub include_ordinal: bool,
}

#[async_trait]
pub trait SourceExecutor: Send + Sync {
    /// Get the list of keys for the source.
    fn list(
        &self,
        options: SourceExecutorListOptions,
    ) -> BoxStream<'_, Result<Vec<SourceRowMetadata>>>;

    // Get the value for the given key.
    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>>;

    async fn change_stream(&self) -> Result<Option<BoxStream<'async_trait, SourceChange>>> {
        Ok(None)
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

#[derive(Debug)]
pub struct ExportTargetMutationWithContext<'ctx, T: ?Sized + Send + Sync> {
    pub mutation: ExportTargetMutation,
    pub export_context: &'ctx T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupStateCompatibility {
    /// The resource is fully compatible with the desired state.
    /// This means the resource can be updated to the desired state without any loss of data.
    Compatible,
    /// The resource is partially compatible with the desired state.
    /// This means data from some existing fields will be lost after applying the setup change.
    /// But at least their key fields of all rows are still preserved.
    PartialCompatible,
    /// The resource needs to be rebuilt. After applying the setup change, all data will be gone.
    NotCompatible,
}

pub struct ExportTargetExecutors {
    pub export_context: Arc<dyn Any + Send + Sync>,
    pub query_target: Option<Arc<dyn QueryTarget>>,
}
pub struct ExportDataCollectionBuildOutput {
    pub executors: BoxFuture<'static, Result<ExportTargetExecutors>>,
    pub setup_key: serde_json::Value,
    pub desired_setup_state: serde_json::Value,
}

pub struct ExportDataCollectionSpec {
    pub name: String,
    pub spec: serde_json::Value,
    pub key_fields_schema: Vec<FieldSchema>,
    pub value_fields_schema: Vec<FieldSchema>,
    pub index_options: IndexOptions,
}

#[async_trait]
pub trait ExportTargetFactory: Send + Sync {
    fn build(
        self: Arc<Self>,
        data_collections: Vec<ExportDataCollectionSpec>,
        declarations: Vec<serde_json::Value>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<ExportDataCollectionBuildOutput>,
        Vec<(serde_json::Value, serde_json::Value)>,
    )>;

    /// Will not be called if it's setup by user.
    /// It returns an error if the target only supports setup by user.
    async fn check_setup_status(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Box<dyn setup::ResourceSetupStatus>>;

    /// Normalize the key. e.g. the JSON format may change (after code change, e.g. new optional field or field ordering), even if the underlying value is not changed.
    /// This should always return the canonical serialized form.
    fn normalize_setup_key(&self, key: &serde_json::Value) -> Result<serde_json::Value>;

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility>;

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String>;

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, dyn Any + Send + Sync>>,
    ) -> Result<()>;

    async fn apply_setup_changes(
        &self,
        setup_status: Vec<&'async_trait dyn setup::ResourceSetupStatus>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub enum ExecutorFactory {
    Source(Arc<dyn SourceFactory + Send + Sync>),
    SimpleFunction(Arc<dyn SimpleFunctionFactory + Send + Sync>),
    ExportTarget(Arc<dyn ExportTargetFactory + Send + Sync>),
}

#[derive(Debug)]
pub struct VectorMatchQuery {
    pub vector_field_name: String,
    pub vector: Vec<f32>,
    pub similarity_metric: VectorSimilarityMetric,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult<Row = Vec<Value>> {
    pub data: Row,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResults<Row = Vec<Value>> {
    pub fields: Vec<FieldSchema>,
    pub results: Vec<QueryResult<Row>>,
}

impl TryFrom<QueryResults<Vec<Value>>> for QueryResults<serde_json::Value> {
    type Error = anyhow::Error;

    fn try_from(values: QueryResults<Vec<Value>>) -> Result<Self, Self::Error> {
        let results = values
            .results
            .into_iter()
            .map(|r| {
                let data = serde_json::to_value(TypedFieldsValue {
                    schema: &values.fields,
                    values_iter: r.data.iter(),
                })?;
                Ok(QueryResult {
                    data,
                    score: r.score,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResults {
            fields: values.fields,
            results,
        })
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct QueryResponse {
    pub results: QueryResults<serde_json::Value>,
    pub info: serde_json::Value,
}

#[async_trait]
pub trait QueryTarget: Send + Sync {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults>;
}
