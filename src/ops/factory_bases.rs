use crate::prelude::*;
use crate::setup::ResourceSetupChange;
use std::fmt::Debug;
use std::hash::Hash;

use super::interface::*;
use super::registry::*;
use crate::api_bail;
use crate::base::schema::*;
use crate::base::spec::*;
use crate::builder::plan::AnalyzedValueMapping;
use crate::setup;
// SourceFactoryBase
pub struct OpArgResolver<'arg> {
    name: String,
    resolved_op_arg: Option<(usize, EnrichedValueType)>,
    nonnull_args_idx: &'arg mut Vec<usize>,
    may_nullify_output: &'arg mut bool,
}

impl<'arg> OpArgResolver<'arg> {
    pub fn expect_nullable_type(self, expected_type: &ValueType) -> Result<Self> {
        let Some((_, typ)) = &self.resolved_op_arg else {
            return Ok(self);
        };
        if &typ.typ != expected_type {
            api_bail!(
                "Expected argument `{}` to be of type `{}`, got `{}`",
                self.name,
                expected_type,
                typ.typ
            );
        }
        Ok(self)
    }
    pub fn expect_type(self, expected_type: &ValueType) -> Result<Self> {
        let resolver = self.expect_nullable_type(expected_type)?;
        resolver.resolved_op_arg.as_ref().map(|(idx, typ)| {
            resolver.nonnull_args_idx.push(*idx);
            if typ.nullable {
                *resolver.may_nullify_output = true;
            }
        });
        Ok(resolver)
    }

    pub fn optional(self) -> Option<ResolvedOpArg> {
        return self.resolved_op_arg.map(|(idx, typ)| ResolvedOpArg {
            name: self.name,
            typ,
            idx,
        });
    }

    pub fn required(self) -> Result<ResolvedOpArg> {
        let Some((idx, typ)) = self.resolved_op_arg else {
            api_bail!("Required argument `{}` is missing", self.name);
        };
        Ok(ResolvedOpArg {
            name: self.name,
            typ,
            idx,
        })
    }
}

pub struct ResolvedOpArg {
    pub name: String,
    pub typ: EnrichedValueType,
    pub idx: usize,
}

pub trait ResolvedOpArgExt: Sized {
    fn value<'a>(&self, args: &'a [value::Value]) -> Result<&'a value::Value>;
    fn take_value(&self, args: &mut [value::Value]) -> Result<value::Value>;
}

impl ResolvedOpArgExt for ResolvedOpArg {
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
    nonnull_args_idx: &'a mut Vec<usize>,
    may_nullify_output: &'a mut bool,
}

impl<'a> OpArgsResolver<'a> {
    pub fn new(
        args: &'a [OpArgSchema],
        nonnull_args_idx: &'a mut Vec<usize>,
        may_nullify_output: &'a mut bool,
    ) -> Result<Self> {
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
            nonnull_args_idx,
            may_nullify_output,
        })
    }

    pub fn next_arg<'arg>(&'arg mut self, name: &str) -> Result<OpArgResolver<'arg>> {
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
        Ok(OpArgResolver {
            name: name.to_string(),
            resolved_op_arg: idx.map(|idx| (idx, self.args[idx].value_type.clone())),
            nonnull_args_idx: self.nonnull_args_idx,
            may_nullify_output: self.may_nullify_output,
        })
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

    async fn get_output_schema(
        &self,
        spec: &Self::Spec,
        context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType>;

    async fn build_executor(
        self: Arc<Self>,
        source_name: &str,
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

#[async_trait]
impl<T: SourceFactoryBase> SourceFactory for T {
    async fn build(
        self: Arc<Self>,
        source_name: &str,
        spec: serde_json::Value,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SourceExecutor>>>,
    )> {
        let spec: T::Spec = utils::deser::from_json_value(spec)
            .with_context(|| format!("Failed in parsing spec for source `{source_name}`"))?;
        let output_schema = self.get_output_schema(&spec, &context).await?;
        let source_name = source_name.to_string();
        let executor = async move { self.build_executor(&source_name, spec, context).await };
        Ok((output_schema, Box::pin(executor)))
    }
}

// SimpleFunctionFactoryBase

#[async_trait]
pub trait SimpleFunctionFactoryBase: SimpleFunctionFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;
    type ResolvedArgs: Send + Sync;

    fn name(&self) -> &str;

    async fn resolve_schema<'a>(
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
    ) -> Result<impl SimpleFunctionExecutor>;

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

struct FunctionExecutorWrapper<E: SimpleFunctionExecutor> {
    executor: E,
    nonnull_args_idx: Vec<usize>,
}

#[async_trait]
impl<E: SimpleFunctionExecutor> SimpleFunctionExecutor for FunctionExecutorWrapper<E> {
    async fn evaluate(&self, args: Vec<value::Value>) -> Result<value::Value> {
        for idx in &self.nonnull_args_idx {
            if args[*idx].is_null() {
                return Ok(value::Value::Null);
            }
        }
        self.executor.evaluate(args).await
    }

    fn enable_cache(&self) -> bool {
        self.executor.enable_cache()
    }

    fn behavior_version(&self) -> Option<u32> {
        self.executor.behavior_version()
    }
}

#[async_trait]
impl<T: SimpleFunctionFactoryBase> SimpleFunctionFactory for T {
    async fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        input_schema: Vec<OpArgSchema>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SimpleFunctionExecutor>>>,
    )> {
        let spec: T::Spec = utils::deser::from_json_value(spec)
            .with_context(|| format!("Failed in parsing spec for function `{}`", self.name()))?;
        let mut nonnull_args_idx = vec![];
        let mut may_nullify_output = false;
        let mut args_resolver = OpArgsResolver::new(
            &input_schema,
            &mut nonnull_args_idx,
            &mut may_nullify_output,
        )?;
        let (resolved_input_schema, mut output_schema) = self
            .resolve_schema(&spec, &mut args_resolver, &context)
            .await?;
        args_resolver.done()?;

        // If any required argument is nullable, the output schema should be nullable.
        if may_nullify_output {
            output_schema.nullable = true;
        }

        let executor = async move {
            Ok(Box::new(FunctionExecutorWrapper {
                executor: self
                    .build_executor(spec, resolved_input_schema, context)
                    .await?,
                nonnull_args_idx,
            }) as Box<dyn SimpleFunctionExecutor>)
        };
        Ok((output_schema, Box::pin(executor)))
    }
}

pub struct TypedExportDataCollectionBuildOutput<F: TargetFactoryBase + ?Sized> {
    pub export_context: BoxFuture<'static, Result<Arc<F::ExportContext>>>,
    pub setup_key: F::SetupKey,
    pub desired_setup_state: F::SetupState,
}
pub struct TypedExportDataCollectionSpec<F: TargetFactoryBase + ?Sized> {
    pub name: String,
    pub spec: F::Spec,
    pub key_fields_schema: Box<[FieldSchema]>,
    pub value_fields_schema: Vec<FieldSchema>,
    pub index_options: IndexOptions,
}

pub struct TypedResourceSetupChangeItem<'a, F: TargetFactoryBase + ?Sized> {
    pub key: F::SetupKey,
    pub setup_change: &'a F::SetupChange,
}

#[async_trait]
pub trait TargetFactoryBase: TargetFactory + Send + Sync + 'static {
    type Spec: DeserializeOwned + Send + Sync;
    type DeclarationSpec: DeserializeOwned + Send + Sync;

    type SetupKey: Debug + Clone + Serialize + DeserializeOwned + Eq + Hash + Send + Sync;
    type SetupState: Debug + Clone + Serialize + DeserializeOwned + Send + Sync;
    type SetupChange: ResourceSetupChange;

    type ExportContext: Send + Sync + 'static;

    fn name(&self) -> &str;

    async fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Self::DeclarationSpec>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(Self::SetupKey, Self::SetupState)>,
    )>;

    /// Deserialize the setup key from a JSON value.
    /// You can override this method to provide a custom deserialization logic, e.g. to perform backward compatible deserialization.
    fn deserialize_setup_key(key: serde_json::Value) -> Result<Self::SetupKey> {
        Ok(utils::deser::from_json_value(key)?)
    }

    /// Will not be called if it's setup by user.
    /// It returns an error if the target only supports setup by user.
    async fn diff_setup_states(
        &self,
        key: Self::SetupKey,
        desired_state: Option<Self::SetupState>,
        existing_states: setup::CombinedState<Self::SetupState>,
        flow_instance_ctx: Arc<FlowInstanceContext>,
    ) -> Result<Self::SetupChange>;

    fn check_state_compatibility(
        &self,
        desired_state: &Self::SetupState,
        existing_state: &Self::SetupState,
    ) -> Result<SetupStateCompatibility>;

    fn describe_resource(&self, key: &Self::SetupKey) -> Result<String>;

    fn extract_additional_key(
        &self,
        _key: &value::KeyValue,
        _value: &value::FieldValues,
        _export_context: &Self::ExportContext,
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
        setup_change: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()>;
}

#[async_trait]
impl<T: TargetFactoryBase> TargetFactory for T {
    async fn build(
        self: Arc<Self>,
        data_collections: Vec<interface::ExportDataCollectionSpec>,
        declarations: Vec<serde_json::Value>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<interface::ExportDataCollectionBuildOutput>,
        Vec<(serde_json::Value, serde_json::Value)>,
    )> {
        let (data_coll_output, decl_output) = TargetFactoryBase::build(
            self,
            data_collections
                .into_iter()
                .map(|d| {
                    anyhow::Ok(TypedExportDataCollectionSpec {
                        spec: utils::deser::from_json_value(d.spec).with_context(|| {
                            format!("Failed in parsing spec for target `{}`", d.name)
                        })?,
                        name: d.name,
                        key_fields_schema: d.key_fields_schema,
                        value_fields_schema: d.value_fields_schema,
                        index_options: d.index_options,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            declarations
                .into_iter()
                .map(|d| anyhow::Ok(utils::deser::from_json_value(d)?))
                .collect::<Result<Vec<_>>>()?,
            context,
        )
        .await?;

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

    async fn diff_setup_states(
        &self,
        key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        flow_instance_ctx: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn setup::ResourceSetupChange>> {
        let key: T::SetupKey = Self::deserialize_setup_key(key.clone())?;
        let desired_state: Option<T::SetupState> = desired_state
            .map(|v| utils::deser::from_json_value(v.clone()))
            .transpose()?;
        let existing_states = from_json_combined_state(existing_states)?;
        let setup_change = TargetFactoryBase::diff_setup_states(
            self,
            key,
            desired_state,
            existing_states,
            flow_instance_ctx,
        )
        .await?;
        Ok(Box::new(setup_change))
    }

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String> {
        let key: T::SetupKey = Self::deserialize_setup_key(key.clone())?;
        TargetFactoryBase::describe_resource(self, &key)
    }

    fn normalize_setup_key(&self, key: &serde_json::Value) -> Result<serde_json::Value> {
        let key: T::SetupKey = Self::deserialize_setup_key(key.clone())?;
        Ok(serde_json::to_value(key)?)
    }

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility> {
        let result = TargetFactoryBase::check_state_compatibility(
            self,
            &utils::deser::from_json_value(desired_state.clone())?,
            &utils::deser::from_json_value(existing_state.clone())?,
        )?;
        Ok(result)
    }

    /// Extract additional keys that are passed through as part of the mutation to `apply_mutation()`.
    /// This is useful for targets that need to use additional parts as key for the target (which is not considered as part of the key for cocoindex).
    fn extract_additional_key(
        &self,
        key: &value::KeyValue,
        value: &value::FieldValues,
        export_context: &(dyn Any + Send + Sync),
    ) -> Result<serde_json::Value> {
        TargetFactoryBase::extract_additional_key(
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
        TargetFactoryBase::apply_mutation(self, mutations).await
    }

    async fn apply_setup_changes(
        &self,
        setup_change: Vec<ResourceSetupChangeItem<'async_trait>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()> {
        TargetFactoryBase::apply_setup_changes(
            self,
            setup_change
                .into_iter()
                .map(|item| -> anyhow::Result<_> {
                    Ok(TypedResourceSetupChangeItem {
                        key: utils::deser::from_json_value(item.key.clone())?,
                        setup_change: (item.setup_change as &dyn Any)
                            .downcast_ref::<T::SetupChange>()
                            .ok_or_else(invariance_violation)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            context,
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
            .map(|v| utils::deser::from_json_value(v))
            .transpose()?,
        staging: existing_states
            .staging
            .into_iter()
            .map(|v| {
                anyhow::Ok(match v {
                    setup::StateChange::Upsert(v) => {
                        setup::StateChange::Upsert(utils::deser::from_json_value(v)?)
                    }
                    setup::StateChange::Delete => setup::StateChange::Delete,
                })
            })
            .collect::<Result<_>>()?,
        legacy_state_key: existing_states.legacy_state_key,
    })
}
