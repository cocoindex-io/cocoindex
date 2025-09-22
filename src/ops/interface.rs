use std::time::SystemTime;

use crate::base::{schema::*, spec::IndexOptions, value::*};
use crate::prelude::*;
use crate::setup;
use chrono::TimeZone;
use serde::Serialize;

pub struct FlowInstanceContext {
    pub flow_instance_name: String,
    pub auth_registry: Arc<AuthRegistry>,
    pub py_exec_ctx: Option<Arc<crate::py::PythonExecutionContext>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct Ordinal(pub Option<i64>);

impl Ordinal {
    pub fn unavailable() -> Self {
        Self(None)
    }

    pub fn is_available(&self) -> bool {
        self.0.is_some()
    }
}

impl From<Ordinal> for Option<i64> {
    fn from(val: Ordinal) -> Self {
        val.0
    }
}

impl TryFrom<SystemTime> for Ordinal {
    type Error = anyhow::Error;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let duration = time.duration_since(std::time::UNIX_EPOCH)?;
        Ok(Ordinal(Some(duration.as_micros().try_into()?)))
    }
}

impl<TZ: TimeZone> TryFrom<chrono::DateTime<TZ>> for Ordinal {
    type Error = anyhow::Error;

    fn try_from(time: chrono::DateTime<TZ>) -> Result<Self, Self::Error> {
        Ok(Ordinal(Some(time.timestamp_micros())))
    }
}

#[derive(Debug)]
pub enum SourceValue {
    Existence(FieldValues),
    NonExistence,
}

#[derive(Debug, Default)]
pub struct PartialSourceRowData {
    pub ordinal: Option<Ordinal>,

    /// A content version fingerprint can be anything that changes when the content of the row changes.
    /// Note that it's acceptable if sometimes the fingerprint differs even though the content is the same,
    /// which will lead to less optimization opportunities but won't break correctness.
    ///
    /// It's optional. The source shouldn't use generic way to compute it, e.g. computing a hash of the content.
    /// The framework will do so. If there's no fast way to get it from the source, leave it as `None`.
    pub content_version_fp: Option<Vec<u8>>,

    pub value: Option<SourceValue>,
}

pub struct PartialSourceRow {
    pub key: KeyValue,
    /// Auxiliary information for the source row, to be used when reading the content.
    /// e.g. it can be used to uniquely identify version of the row.
    /// Use serde_json::Value::Null to represent no auxiliary information.
    pub key_aux_info: serde_json::Value,

    pub data: PartialSourceRowData,
}

impl SourceValue {
    pub fn is_existent(&self) -> bool {
        matches!(self, Self::Existence(_))
    }

    pub fn as_optional(&self) -> Option<&FieldValues> {
        match self {
            Self::Existence(value) => Some(value),
            Self::NonExistence => None,
        }
    }

    pub fn into_optional(self) -> Option<FieldValues> {
        match self {
            Self::Existence(value) => Some(value),
            Self::NonExistence => None,
        }
    }
}

pub struct SourceChange {
    pub key: KeyValue,
    /// Auxiliary information for the source row, to be used when reading the content.
    /// e.g. it can be used to uniquely identify version of the row.
    pub key_aux_info: serde_json::Value,

    /// If None, the engine will poll to get the latest existence state and value.
    pub data: PartialSourceRowData,
}

pub struct SourceChangeMessage {
    pub changes: Vec<SourceChange>,
    pub ack_fn: Option<Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync>>,
}

#[derive(Debug, Default)]
pub struct SourceExecutorReadOptions {
    /// When set to true, the implementation must return a non-None `ordinal`.
    pub include_ordinal: bool,

    /// When set to true, the implementation has the discretion to decide whether or not to return a non-None `content_version_fp`.
    /// The guideline is to return it only if it's very efficient to get it.
    /// If it's returned in `list()`, it must be returned in `get_value()`.
    pub include_content_version_fp: bool,

    /// For get calls, when set to true, the implementation must return a non-None `value`.
    ///
    /// For list calls, when set to true, the implementation has the discretion to decide whether or not to include it.
    /// The guideline is to only include it if a single "list() with content" call is significantly more efficient than "list() without content + series of get_value()" calls.
    ///
    /// Even if `list()` already returns `value` when it's true, `get_value()` must still return `value` when it's true.
    pub include_value: bool,
}

#[async_trait]
pub trait SourceExecutor: Send + Sync {
    /// Get the list of keys for the source.
    async fn list(
        &self,
        options: &SourceExecutorReadOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRow>>>>;

    // Get the value for the given key.
    async fn get_value(
        &self,
        key: &KeyValue,
        key_aux_info: &serde_json::Value,
        options: &SourceExecutorReadOptions,
    ) -> Result<PartialSourceRowData>;

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
        Ok(None)
    }

    fn provides_ordinal(&self) -> bool;
}

#[async_trait]
pub trait SourceFactory {
    async fn build(
        self: Arc<Self>,
        source_name: &str,
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

#[async_trait]
pub trait SimpleFunctionFactory {
    async fn build(
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
    pub additional_key: serde_json::Value,
    pub value: FieldValues,
}

#[derive(Debug)]
pub struct ExportTargetDeleteEntry {
    pub key: KeyValue,
    pub additional_key: serde_json::Value,
}

#[derive(Debug, Default)]
pub struct ExportTargetMutation {
    pub upserts: Vec<ExportTargetUpsertEntry>,
    pub deletes: Vec<ExportTargetDeleteEntry>,
}

impl ExportTargetMutation {
    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

#[derive(Debug)]
pub struct ExportTargetMutationWithContext<'ctx, T: ?Sized + Send + Sync> {
    pub mutation: ExportTargetMutation,
    pub export_context: &'ctx T,
}

pub struct ResourceSetupChangeItem<'a> {
    pub key: &'a serde_json::Value,
    pub setup_change: &'a dyn setup::ResourceSetupChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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

pub struct ExportDataCollectionBuildOutput {
    pub export_context: BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>>>,
    pub setup_key: serde_json::Value,
    pub desired_setup_state: serde_json::Value,
}

pub struct ExportDataCollectionSpec {
    pub name: String,
    pub spec: serde_json::Value,
    pub key_fields_schema: Box<[FieldSchema]>,
    pub value_fields_schema: Vec<FieldSchema>,
    pub index_options: IndexOptions,
}

#[async_trait]
pub trait TargetFactory: Send + Sync {
    async fn build(
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
    async fn diff_setup_states(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        context: Arc<interface::FlowInstanceContext>,
    ) -> Result<Box<dyn setup::ResourceSetupChange>>;

    /// Normalize the key. e.g. the JSON format may change (after code change, e.g. new optional field or field ordering), even if the underlying value is not changed.
    /// This should always return the canonical serialized form.
    fn normalize_setup_key(&self, key: &serde_json::Value) -> Result<serde_json::Value>;

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility>;

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String>;

    fn extract_additional_key(
        &self,
        key: &KeyValue,
        value: &FieldValues,
        export_context: &(dyn Any + Send + Sync),
    ) -> Result<serde_json::Value>;

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, dyn Any + Send + Sync>>,
    ) -> Result<()>;

    async fn apply_setup_changes(
        &self,
        setup_change: Vec<ResourceSetupChangeItem<'async_trait>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub enum ExecutorFactory {
    Source(Arc<dyn SourceFactory + Send + Sync>),
    SimpleFunction(Arc<dyn SimpleFunctionFactory + Send + Sync>),
    ExportTarget(Arc<dyn TargetFactory + Send + Sync>),
}
