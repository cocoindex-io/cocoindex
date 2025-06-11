use crate::prelude::*;
use crate::setup::ResourceSetupStatus;
use std::fmt::Debug;
use std::hash::Hash;

use super::interface::*;
use super::registry::*;
use crate::api_bail;
use crate::api_error;
use crate::base::schema::*;
use crate::base::spec::*;
use crate::builder::plan::AnalyzedValueMapping;
use crate::setup;
// SourceFactoryBase
pub struct ResolvedOpArg {
    pub name: String,
    pub typ: EnrichedValueType,
    pub idx: usize,
}

pub trait ResolvedOpArgExt: Sized {
    fn expect_type(self, expected_type: &ValueType) -> Result<Self>;
    fn value<'a>(&self, args: &'a [value::Value]) -> Result<&'a value::Value>;
    fn take_value(&self, args: &mut [value::Value]) -> Result<value::Value>;
}

impl ResolvedOpArgExt for ResolvedOpArg {
    fn expect_type(self, expected_type: &ValueType) -> Result<Self> {
        if &self.typ.typ != expected_type {
            api_bail!(
                "Expected argument `{}` to be of type `{}`, got `{}`",
                self.name,
                expected_type,
                self.typ.typ
            );
        }
        Ok(self)
    }

    fn value<'a>(&self, args: &'a [value::Value]) -> Result<&'a value::Value> {
        if self.idx >= args.len() {
            api_bail!(
                "Two few arguments, {} provided, expected at least {} for `{}`",
                args.len(),
                self.idx + 1,
                self.name
            );
        }
        Ok(&args[self.idx])
    }

    fn take_value(&self, args: &mut [value::Value]) -> Result<value::Value> {
        if self.idx >= args.len() {
            api_bail!(
                "Two few arguments, {} provided, expected at least {} for `{}`",
                args.len(),
                self.idx + 1,
                self.name
            );
        }
        Ok(std::mem::take(&mut args[self.idx]))
    }
}

impl ResolvedOpArgExt for Option<ResolvedOpArg> {
    fn expect_type(self, expected_type: &ValueType) -> Result<Self> {
        self.map(|arg| arg.expect_type(expected_type)).transpose()
    }

    fn value<'a>(&self, args: &'a [value::Value]) -> Result<&'a value::Value> {
        Ok(self
            .as_ref()
            .map(|arg| arg.value(args))
            .transpose()?
            .unwrap_or(&value::Value::Null))
    }

    fn take_value(&self, args: &mut [value::Value]) -> Result<value::Value> {
        Ok(self
            .as_ref()
            .map(|arg| arg.take_value(args))
            .transpose()?
            .unwrap_or(value::Value::Null))
    }
}

pub struct OpArgsResolver<'a> {
    args: &'a [OpArgSchema],
    num_positional_args: usize,
    next_positional_idx: usize,
    remaining_kwargs: HashMap<&'a str, usize>,
}

impl<'a> OpArgsResolver<'a> {
    pub fn new(args: &'a [OpArgSchema]) -> Result<Self> {
        let mut num_positional_args = 0;
        let mut kwargs = HashMap::new();
        for (idx, arg) in args.iter().enumerate() {
            if let Some(name) = &arg.name.0 {
                kwargs.insert(name.as_str(), idx);
            } else {
                if !kwargs.is_empty() {
                    api_bail!("Positional arguments must be provided before keyword arguments");
                }
                num_positional_args += 1;
            }
        }
        Ok(Self {
            args,
            num_positional_args,
            next_positional_idx: 0,
            remaining_kwargs: kwargs,
        })
    }

    pub fn next_optional_arg(&mut self, name: &str) -> Result<Option<ResolvedOpArg>> {
        let idx = if let Some(idx) = self.remaining_kwargs.remove(name) {
            if self.next_positional_idx < self.num_positional_args {
                api_bail!("`{name}` is provided as both positional and keyword arguments");
            } else {
                Some(idx)
            }
        } else if self.next_positional_idx < self.num_positional_args {
            let idx = self.next_positional_idx;
            self.next_positional_idx += 1;
            Some(idx)
        } else {
            None
        };
        Ok(idx.map(|idx| ResolvedOpArg {
            name: name.to_string(),
            typ: self.args[idx].value_type.clone(),
            idx,
        }))
    }

    pub fn next_arg(&mut self, name: &str) -> Result<ResolvedOpArg> {
        Ok(self
            .next_optional_arg(name)?
            .ok_or_else(|| api_error!("Required argument `{name}` is missing",))?)
    }

    pub fn done(self) -> Result<()> {
        if self.next_positional_idx < self.num_positional_args {
            api_bail!(
                "Expected {} positional arguments, got {}",
                self.next_positional_idx,
                self.num_positional_args
            );
        }
        if !self.remaining_kwargs.is_empty() {
            api_bail!(
                "Unexpected keyword arguments: {}",
                self.remaining_kwargs
                    .keys()
                    .map(|k| format!("`{k}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Ok(())
    }

    pub fn get_analyze_value(&self, resolved_arg: &ResolvedOpArg) -> &AnalyzedValueMapping {
        &self.args[resolved_arg.idx].analyzed_value
    }
}

#[async_trait]
pub trait SourceFactoryBase: SourceFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;

    fn name(&self) -> &str;

    fn get_output_schema(
        &self,
        spec: &Self::Spec,
        context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType>;

    async fn build_executor(
        self: Arc<Self>,
        spec: Self::Spec,
        context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>>;

    fn register(self, registry: &mut ExecutorFactoryRegistry) -> Result<()>
    where
        Self: Sized,
    {
        registry.register(
            self.name().to_string(),
            ExecutorFactory::Source(Arc::new(self)),
        )
    }
}

impl<T: SourceFactoryBase> SourceFactory for T {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SourceExecutor>>>,
    )> {
        let spec: T::Spec = serde_json::from_value(spec)?;
        let output_schema = self.get_output_schema(&spec, &context)?;
        let executor = self.build_executor(spec, context);
        Ok((output_schema, executor))
    }
}

// SimpleFunctionFactoryBase

#[async_trait]
pub trait SimpleFunctionFactoryBase: SimpleFunctionFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;
    type ResolvedArgs: Send + Sync;

    fn name(&self) -> &str;

    fn resolve_schema<'a>(
        &'a self,
        spec: &'a Self::Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        context: &FlowInstanceContext,
    ) -> Result<(Self::ResolvedArgs, EnrichedValueType)>;

    async fn build_executor(
        self: Arc<Self>,
        spec: Self::Spec,
        resolved_input_schema: Self::ResolvedArgs,
        context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>>;

    fn register(self, registry: &mut ExecutorFactoryRegistry) -> Result<()>
    where
        Self: Sized,
    {
        registry.register(
            self.name().to_string(),
            ExecutorFactory::SimpleFunction(Arc::new(self)),
        )
    }
}

impl<T: SimpleFunctionFactoryBase> SimpleFunctionFactory for T {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        input_schema: Vec<OpArgSchema>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SimpleFunctionExecutor>>>,
    )> {
        let spec: T::Spec = serde_json::from_value(spec)?;
        let mut args_resolver = OpArgsResolver::new(&input_schema)?;
        let (resolved_input_schema, output_schema) =
            self.resolve_schema(&spec, &mut args_resolver, &context)?;
        args_resolver.done()?;
        let executor = self.build_executor(spec, resolved_input_schema, context);
        Ok((output_schema, executor))
    }
}

pub struct TypedExportDataCollectionBuildOutput<F: StorageFactoryBase + ?Sized> {
    pub export_context: BoxFuture<'static, Result<Arc<F::ExportContext>>>,
    pub setup_key: F::Key,
    pub desired_setup_state: F::SetupState,
}
pub struct TypedExportDataCollectionSpec<F: StorageFactoryBase + ?Sized> {
    pub name: String,
    pub spec: F::Spec,
    pub key_fields_schema: Vec<FieldSchema>,
    pub value_fields_schema: Vec<FieldSchema>,
    pub index_options: IndexOptions,
}

pub struct TypedResourceSetupChangeItem<'a, F: StorageFactoryBase + ?Sized> {
    pub key: F::Key,
    pub setup_status: &'a F::SetupStatus,
}

#[async_trait]
pub trait StorageFactoryBase: ExportTargetFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;
    type DeclarationSpec: DeserializeOwned + Send + Sync;
    type Key: Debug + Clone + Serialize + DeserializeOwned + Eq + Hash + Send + Sync;
    type SetupState: Debug + Clone + Serialize + DeserializeOwned + Send + Sync;
    type SetupStatus: ResourceSetupStatus;
    type ExportContext: Send + Sync + 'static;

    fn name(&self) -> &str;

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Self::DeclarationSpec>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(Self::Key, Self::SetupState)>,
    )>;

    /// Deserialize the setup key from a JSON value.
    /// You can override this method to provide a custom deserialization logic, e.g. to perform backward compatible deserialization.
    fn deserialize_setup_key(key: serde_json::Value) -> Result<Self::Key> {
        Ok(serde_json::from_value(key)?)
    }

    /// Will not be called if it's setup by user.
    /// It returns an error if the target only supports setup by user.
    async fn check_setup_status(
        &self,
        key: Self::Key,
        desired_state: Option<Self::SetupState>,
        existing_states: setup::CombinedState<Self::SetupState>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Self::SetupStatus>;

    fn check_state_compatibility(
        &self,
        desired_state: &Self::SetupState,
        existing_state: &Self::SetupState,
    ) -> Result<SetupStateCompatibility>;

    fn describe_resource(&self, key: &Self::Key) -> Result<String>;

    fn extract_additional_key<'ctx>(
        &self,
        _key: &value::KeyValue,
        _value: &value::FieldValues,
        _export_context: &'ctx Self::ExportContext,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }

    fn register(self, registry: &mut ExecutorFactoryRegistry) -> Result<()>
    where
        Self: Sized,
    {
        registry.register(
            self.name().to_string(),
            ExecutorFactory::ExportTarget(Arc::new(self)),
        )
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, Self::ExportContext>>,
    ) -> Result<()>;

    async fn apply_setup_changes(
        &self,
        setup_status: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()>;
}

#[async_trait]
impl<T: StorageFactoryBase> ExportTargetFactory for T {
    fn build(
        self: Arc<Self>,
        data_collections: Vec<interface::ExportDataCollectionSpec>,
        declarations: Vec<serde_json::Value>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<interface::ExportDataCollectionBuildOutput>,
        Vec<(serde_json::Value, serde_json::Value)>,
    )> {
        let (data_coll_output, decl_output) = StorageFactoryBase::build(
            self,
            data_collections
                .into_iter()
                .map(|d| {
                    anyhow::Ok(TypedExportDataCollectionSpec {
                        name: d.name,
                        spec: serde_json::from_value(d.spec)?,
                        key_fields_schema: d.key_fields_schema,
                        value_fields_schema: d.value_fields_schema,
                        index_options: d.index_options,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            declarations
                .into_iter()
                .map(|d| anyhow::Ok(serde_json::from_value(d)?))
                .collect::<Result<Vec<_>>>()?,
            context,
        )?;

        let data_coll_output = data_coll_output
            .into_iter()
            .map(|d| {
                Ok(interface::ExportDataCollectionBuildOutput {
                    export_context: async move {
                        Ok(d.export_context.await? as Arc<dyn Any + Send + Sync>)
                    }
                    .boxed(),
                    setup_key: serde_json::to_value(d.setup_key)?,
                    desired_setup_state: serde_json::to_value(d.desired_setup_state)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let decl_output = decl_output
            .into_iter()
            .map(|(key, state)| Ok((serde_json::to_value(key)?, serde_json::to_value(state)?)))
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, decl_output))
    }

    async fn check_setup_status(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Box<dyn setup::ResourceSetupStatus>> {
        let key: T::Key = Self::deserialize_setup_key(key.clone())?;
        let desired_state: Option<T::SetupState> = desired_state
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        let existing_states = from_json_combined_state(existing_states)?;
        let setup_status = StorageFactoryBase::check_setup_status(
            self,
            key,
            desired_state,
            existing_states,
            auth_registry,
        )
        .await?;
        Ok(Box::new(setup_status))
    }

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String> {
        let key: T::Key = Self::deserialize_setup_key(key.clone())?;
        StorageFactoryBase::describe_resource(self, &key)
    }

    fn normalize_setup_key(&self, key: &serde_json::Value) -> Result<serde_json::Value> {
        let key: T::Key = Self::deserialize_setup_key(key.clone())?;
        Ok(serde_json::to_value(key)?)
    }

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility> {
        let result = StorageFactoryBase::check_state_compatibility(
            self,
            &serde_json::from_value(desired_state.clone())?,
            &serde_json::from_value(existing_state.clone())?,
        )?;
        Ok(result)
    }

    fn extract_additional_key<'ctx>(
        &self,
        key: &value::KeyValue,
        value: &value::FieldValues,
        export_context: &'ctx (dyn Any + Send + Sync),
    ) -> Result<serde_json::Value> {
        StorageFactoryBase::extract_additional_key(
            self,
            key,
            value,
            export_context
                .downcast_ref::<T::ExportContext>()
                .ok_or_else(invariance_violation)?,
        )
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, dyn Any + Send + Sync>>,
    ) -> Result<()> {
        let mutations = mutations
            .into_iter()
            .map(|m| {
                anyhow::Ok(ExportTargetMutationWithContext {
                    mutation: m.mutation,
                    export_context: m
                        .export_context
                        .downcast_ref::<T::ExportContext>()
                        .ok_or_else(invariance_violation)?,
                })
            })
            .collect::<Result<_>>()?;
        StorageFactoryBase::apply_mutation(self, mutations).await
    }

    async fn apply_setup_changes(
        &self,
        setup_status: Vec<ResourceSetupChangeItem<'async_trait>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()> {
        StorageFactoryBase::apply_setup_changes(
            self,
            setup_status
                .into_iter()
                .map(|item| -> anyhow::Result<_> {
                    Ok(TypedResourceSetupChangeItem {
                        key: serde_json::from_value(item.key.clone())?,
                        setup_status: (item.setup_status as &dyn Any)
                            .downcast_ref::<T::SetupStatus>()
                            .ok_or_else(invariance_violation)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            auth_registry,
        )
        .await
    }
}
fn from_json_combined_state<T: Debug + Clone + Serialize + DeserializeOwned>(
    existing_states: setup::CombinedState<serde_json::Value>,
) -> Result<setup::CombinedState<T>> {
    Ok(setup::CombinedState {
        current: existing_states
            .current
            .map(|v| serde_json::from_value(v))
            .transpose()?,
        staging: existing_states
            .staging
            .into_iter()
            .map(|v| {
                anyhow::Ok(match v {
                    setup::StateChange::Upsert(v) => {
                        setup::StateChange::Upsert(serde_json::from_value(v)?)
                    }
                    setup::StateChange::Delete => setup::StateChange::Delete,
                })
            })
            .collect::<Result<_>>()?,
        legacy_state_key: existing_states.legacy_state_key,
    })
}
