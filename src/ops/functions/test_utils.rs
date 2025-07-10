use crate::builder::plan::{
    AnalyzedFieldReference, AnalyzedLocalFieldReference, AnalyzedValueMapping,
};
use crate::ops::sdk::{
    AuthRegistry, BasicValueType, EnrichedValueType, FlowInstanceContext, OpArgSchema,
    OpArgsResolver, SimpleFunctionExecutor, SimpleFunctionFactoryBase, Value, make_output_type,
};
use anyhow::Result;
use serde::de::DeserializeOwned;
use std::sync::Arc;

// This function builds an argument schema for a flow function.
pub fn build_arg_schema(
    name: &str,
    value_type: BasicValueType,
) -> (Option<&str>, EnrichedValueType) {
    (Some(name), make_output_type(value_type))
}

// This function tests a flow function by providing a spec, input argument schemas, and values.
pub async fn test_flow_function<S, R, F>(
    factory: Arc<F>,
    spec: S,
    input_arg_schemas: Vec<(Option<&str>, EnrichedValueType)>,
    input_arg_values: Vec<Value>,
) -> Result<Value>
where
    S: DeserializeOwned + Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: SimpleFunctionFactoryBase<Spec = S, ResolvedArgs = R> + ?Sized,
{
    // 1. Construct OpArgSchema
    let op_arg_schemas: Vec<OpArgSchema> = input_arg_schemas
        .into_iter()
        .enumerate()
        .map(|(idx, (name, value_type))| OpArgSchema {
            name: name.map_or(crate::base::spec::OpArgName(None), |n| {
                crate::base::spec::OpArgName(Some(n.to_string()))
            }),
            value_type,
            analyzed_value: AnalyzedValueMapping::Field(AnalyzedFieldReference {
                local: AnalyzedLocalFieldReference {
                    fields_idx: vec![idx as u32],
                },
                scope_up_level: 0,
            }),
        })
        .collect();

    // 2. Resolve Schema & Args
    let mut args_resolver = OpArgsResolver::new(&op_arg_schemas)?;
    let context = Arc::new(FlowInstanceContext {
        flow_instance_name: "test_flow_function".to_string(),
        auth_registry: Arc::new(AuthRegistry::default()),
        py_exec_ctx: None,
    });

    let (resolved_args_from_schema, _output_schema): (R, EnrichedValueType) = factory
        .resolve_schema(&spec, &mut args_resolver, &context)
        .await?;

    args_resolver.done()?;

    // 3. Build Executor
    let executor: Box<dyn SimpleFunctionExecutor> = factory
        .build_executor(spec, resolved_args_from_schema, Arc::clone(&context))
        .await?;

    // 4. Evaluate
    let result = executor.evaluate(input_arg_values).await?;

    Ok(result)
}
