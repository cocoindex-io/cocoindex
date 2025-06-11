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

pub struct PartialSourceRowMetadata {
    pub key: KeyValue,
    pub ordinal: Option<Ordinal>,
}

#[derive(Debug)]
pub enum SourceValue {
    Existence(FieldValues),
    NonExistence,
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

pub struct SourceData {
    pub value: SourceValue,
    pub ordinal: Ordinal,
}

pub struct SourceChange {
    pub key: KeyValue,

    /// If None, the engine will poll to get the latest existence state and value.
    pub data: Option<SourceData>,
}

pub struct SourceChangeMessage {
    pub changes: Vec<SourceChange>,
    pub ack_fn: Option<Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync>>,
}

#[derive(Debug, Default)]
pub struct SourceExecutorListOptions {
    pub include_ordinal: bool,
}

#[derive(Debug, Default)]
pub struct SourceExecutorGetOptions {
    pub include_ordinal: bool,
    pub include_value: bool,
}

#[derive(Debug)]
pub struct PartialSourceRowData {
    pub value: Option<SourceValue>,
    pub ordinal: Option<Ordinal>,
}

impl TryFrom<PartialSourceRowData> for SourceData {
    type Error = anyhow::Error;

    fn try_from(data: PartialSourceRowData) -> Result<Self, Self::Error> {
        Ok(Self {
            value: data
                .value
                .ok_or_else(|| anyhow::anyhow!("value is missing"))?,
            ordinal: data
                .ordinal
                .ok_or_else(|| anyhow::anyhow!("ordinal is missing"))?,
        })
    }
}
#[async_trait]
pub trait SourceExecutor: Send + Sync {
    /// Get the list of keys for the source.
    fn list<'a>(
        &'a self,
        options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<PartialSourceRowMetadata>>>;

    // Get the value for the given key.
    async fn get_value(
        &self,
        key: &KeyValue,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData>;

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
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
    pub setup_status: &'a dyn setup::ResourceSetupStatus,
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

pub struct ExportDataCollectionBuildOutput {
    pub export_context: BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>>>,
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

    fn extract_additional_key<'ctx>(
        &self,
        key: &KeyValue,
        value: &FieldValues,
        export_context: &'ctx (dyn Any + Send + Sync),
    ) -> Result<serde_json::Value>;

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, dyn Any + Send + Sync>>,
    ) -> Result<()>;

    async fn apply_setup_changes(
        &self,
        setup_status: Vec<ResourceSetupChangeItem<'async_trait>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub enum ExecutorFactory {
    Source(Arc<dyn SourceFactory + Send + Sync>),
    SimpleFunction(Arc<dyn SimpleFunctionFactory + Send + Sync>),
    ExportTarget(Arc<dyn ExportTargetFactory + Send + Sync>),
}
