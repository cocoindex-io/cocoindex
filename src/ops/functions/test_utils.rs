use crate::builder::plan::{
    AnalyzedFieldReference, AnalyzedLocalFieldReference, AnalyzedValueMapping,
};
use crate::ops::sdk::{
    AuthRegistry, BasicValueType, EnrichedValueType, FlowInstanceContext, OpArgSchema,
    SimpleFunctionFactory, Value, make_output_type,
};
use crate::prelude::*;
use anyhow::Result;
use std::sync::Arc;

// This function builds an argument schema for a flow function.
pub fn build_arg_schema(
    name: &str,
    value_type: BasicValueType,
) -> (Option<&str>, EnrichedValueType) {
    (Some(name), make_output_type(value_type))
}

// This function tests a flow function by providing a spec, input argument schemas, and values.
pub async fn test_flow_function(
    factory: &Arc<impl SimpleFunctionFactory>,
    spec: &impl Serialize,
    input_arg_schemas: &[(Option<&str>, EnrichedValueType)],
    input_arg_values: Vec<Value>,
) -> Result<Value> {
    // 1. Construct OpArgSchema
    let op_arg_schemas: Vec<OpArgSchema> = input_arg_schemas
        .iter()
        .enumerate()
        .map(|(idx, (name, value_type))| OpArgSchema {
            name: name.map_or(crate::base::spec::OpArgName(None), |n| {
                crate::base::spec::OpArgName(Some(n.to_string()))
            }),
            value_type: value_type.clone(),
            analyzed_value: AnalyzedValueMapping::Field(AnalyzedFieldReference {
                local: AnalyzedLocalFieldReference {
                    fields_idx: vec![idx as u32],
                },
                scope_up_level: 0,
            }),
        })
        .collect();

    // 2. Build Executor
    let context = Arc::new(FlowInstanceContext {
        flow_instance_name: "test_flow_function".to_string(),
        auth_registry: Arc::new(AuthRegistry::default()),
        py_exec_ctx: None,
    });
    let (_, exec_fut) = factory
        .clone()
        .build(serde_json::to_value(spec)?, op_arg_schemas, context)
        .await?;
    let executor = exec_fut.await?;

    // 3. Evaluate
    let result = executor.evaluate(input_arg_values).await?;

    Ok(result)
}
