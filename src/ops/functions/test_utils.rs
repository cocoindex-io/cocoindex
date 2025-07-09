use crate::builder::plan::AnalyzedValueMapping;
use crate::ops::sdk::{
    BasicValueType, EnrichedValueType, FlowInstanceContext, OpArgSchema, OpArgsResolver,
    SimpleFunctionExecutor, SimpleFunctionFactoryBase, Value, make_output_type,
};
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn new_literal_op_arg_schema(
    name: Option<&str>,
    value: Value,
    value_type: EnrichedValueType,
) -> OpArgSchema {
    OpArgSchema {
        name: name.map_or(crate::base::spec::OpArgName(None), |n| {
            crate::base::spec::OpArgName(Some(n.to_string()))
        }),
        value_type,
        analyzed_value: AnalyzedValueMapping::Constant { value },
    }
}

// This function provides a helper to create OpArgSchema for literal values.
pub fn build_arg_schema(name: &str, value: Value, value_type: BasicValueType) -> OpArgSchema {
    new_literal_op_arg_schema(Some(name), value, make_output_type(value_type))
}

// This function tests a flow function by providing a spec, input argument schemas, and values.
pub async fn test_flow_function<S, R, F>(
    factory: Arc<F>,
    spec_json: JsonValue,
    input_arg_schemas: Vec<OpArgSchema>,
    input_arg_values: Vec<Value>,
    context: Arc<FlowInstanceContext>,
) -> Result<Value>
where
    S: DeserializeOwned + Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: SimpleFunctionFactoryBase<Spec = S, ResolvedArgs = R> + ?Sized,
{
    // 1. Deserialize Spec
    let spec: S = serde_json::from_value(spec_json)?;

    // 2. Resolve Schema & Args
    // The caller of test_flow_function will be responsible for creating these schemas.
    let mut args_resolver = OpArgsResolver::new(&input_arg_schemas)?;

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
