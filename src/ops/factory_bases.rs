use crate::prelude::*;
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

pub struct TypedExportTargetExecutors<F: StorageFactoryBase + ?Sized> {
    pub export_context: Arc<F::ExportContext>,
    pub query_target: Option<Arc<dyn QueryTarget>>,
}

pub struct TypedExportTargetBuildOutput<F: StorageFactoryBase + ?Sized> {
    pub executors: BoxFuture<'static, Result<TypedExportTargetExecutors<F>>>,
    pub setup_key: F::Key,
    pub desired_setup_state: F::SetupState,
}

#[async_trait]
pub trait StorageFactoryBase: ExportTargetFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;
    type Key: Debug + Clone + Serialize + DeserializeOwned + Eq + Hash + Send + Sync;
    type SetupState: Debug + Clone + Serialize + DeserializeOwned + Send + Sync;
    type ExportContext: Send + Sync + 'static;

    fn name(&self) -> &str;

    fn build(
        self: Arc<Self>,
        name: String,
        spec: Self::Spec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<TypedExportTargetBuildOutput<Self>>;

    /// Will not be called if it's setup by user.
    /// It returns an error if the target only supports setup by user.
    fn check_setup_status(
        &self,
        key: Self::Key,
        desired_state: Option<Self::SetupState>,
        existing_states: setup::CombinedState<Self::SetupState>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<impl setup::ResourceSetupStatusCheck + 'static>;

    fn check_state_compatibility(
        &self,
        desired_state: &Self::SetupState,
        existing_state: &Self::SetupState,
    ) -> Result<SetupStateCompatibility>;

    fn describe_resource(&self, key: &Self::Key) -> Result<String>;

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
}

#[async_trait]
impl<T: StorageFactoryBase> ExportTargetFactory for T {
    fn build(
        self: Arc<Self>,
        name: String,
        spec: serde_json::Value,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<interface::ExportTargetBuildOutput> {
        let spec: T::Spec = serde_json::from_value(spec)?;
        let build_output = StorageFactoryBase::build(
            self,
            name,
            spec,
            key_fields_schema,
            value_fields_schema,
            storage_options,
            context,
        )?;
        let executors = async move {
            let executors = build_output.executors.await?;
            Ok(interface::ExportTargetExecutors {
                export_context: executors.export_context,
                query_target: executors.query_target,
            })
        };
        Ok(interface::ExportTargetBuildOutput {
            setup_key: serde_json::to_value(build_output.setup_key)?,
            desired_setup_state: serde_json::to_value(build_output.desired_setup_state)?,
            executors: executors.boxed(),
        })
    }

    fn check_setup_status(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Box<dyn setup::ResourceSetupStatusCheck>> {
        let key: T::Key = serde_json::from_value(key.clone())?;
        let desired_state: Option<T::SetupState> = desired_state
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        let existing_states = from_json_combined_state(existing_states)?;
        let status_check = StorageFactoryBase::check_setup_status(
            self,
            key,
            desired_state,
            existing_states,
            auth_registry,
        )?;
        Ok(Box::new(status_check))
    }

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String> {
        let key: T::Key = serde_json::from_value(key.clone())?;
        StorageFactoryBase::describe_resource(self, &key)
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
                        .ok_or_else(|| anyhow!("Unexpected export context type"))?,
                })
            })
            .collect::<Result<_>>()?;
        StorageFactoryBase::apply_mutation(self, mutations).await
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
    })
}
