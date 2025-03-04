use crate::base::{
    schema::*,
    spec::{IndexOptions, VectorSimilarityMetric},
    value::*,
};
use crate::setup;
use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use std::{fmt::Debug, future::Future, pin::Pin, sync::Arc};

pub struct FlowInstanceContext {
    pub flow_instance_name: String,
}

pub type ExecutorFuture<'a, E> = Pin<Box<dyn Future<Output = Result<E>> + Send + 'a>>;

#[async_trait]
pub trait SourceExecutor: Send + Sync {
    /// Get the list of keys for the source.
    async fn list_keys(&self) -> Result<Vec<KeyValue>>;

    // Get the value for the given key.
    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>>;
}

pub trait SourceFactory {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        ExecutorFuture<'static, Box<dyn SourceExecutor>>,
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
        ExecutorFuture<'static, Box<dyn SimpleFunctionExecutor>>,
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

pub trait ExportTargetFactory {
    // The first field of the `input_schema` is the primary key field.
    // If it has struct type, it should be converted to composite primary key.
    fn build(
        self: Arc<Self>,
        name: String,
        target_id: i32,
        spec: serde_json::Value,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        (serde_json::Value, serde_json::Value),
        ExecutorFuture<'static, (Arc<dyn ExportTargetExecutor>, Option<Arc<dyn QueryTarget>>)>,
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

    fn will_keep_all_existing_data(
        &self,
        name: &str,
        target_id: i32,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<bool>;
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
