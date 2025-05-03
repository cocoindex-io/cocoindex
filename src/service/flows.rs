use crate::prelude::*;

use crate::lib_context::LibContext;
use crate::{base::schema::FlowSchema, ops::interface::SourceExecutorListOptions};
use crate::{
    execution::memoization,
    execution::{row_indexer, stats},
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use axum_extra::extract::Query;

pub async fn list_flows(
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<Vec<String>>, ApiError> {
    Ok(Json(
        lib_context.flows.lock().unwrap().keys().cloned().collect(),
    ))
}

pub async fn get_flow_spec(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<spec::FlowInstanceSpec>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    Ok(Json(flow_ctx.flow.flow_instance.clone()))
}

pub async fn get_flow_schema(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<FlowSchema>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    Ok(Json(flow_ctx.flow.data_schema.clone()))
}

#[derive(Deserialize)]
pub struct GetKeysParam {
    field: String,
}

#[derive(Serialize)]
pub struct GetKeysResponse {
    key_type: schema::EnrichedValueType,
    keys: Vec<value::KeyValue>,
}

pub async fn get_keys(
    Path(flow_name): Path<String>,
    Query(query): Query<GetKeysParam>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<GetKeysResponse>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let schema = &flow_ctx.flow.data_schema;

    let field_idx = schema
        .fields
        .iter()
        .position(|f| f.name == query.field)
        .ok_or_else(|| {
            ApiError::new(
                &format!("field not found: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;
    let key_type = schema.fields[field_idx]
        .value_type
        .typ
        .key_type()
        .ok_or_else(|| {
            ApiError::new(
                &format!("field has no key: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;

    let execution_plan = flow_ctx.flow.get_execution_plan().await?;
    let import_op = execution_plan
        .import_ops
        .iter()
        .find(|op| op.output.field_idx == field_idx as u32)
        .ok_or_else(|| {
            ApiError::new(
                &format!("field is not a source: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;

    let mut rows_stream = import_op.executor.list(SourceExecutorListOptions {
        include_ordinal: false,
    });
    let mut keys = Vec::new();
    while let Some(rows) = rows_stream.next().await {
        keys.extend(rows?.into_iter().map(|row| row.key));
    }
    Ok(Json(GetKeysResponse {
        key_type: key_type.clone(),
        keys,
    }))
}

#[derive(Deserialize)]
pub struct EvaluateDataParams {
    field: String,
    key: Vec<String>,
}

#[derive(Serialize)]
pub struct EvaluateDataResponse {
    schema: FlowSchema,
    data: value::ScopeValue,
}

pub async fn evaluate_data(
    Path(flow_name): Path<String>,
    Query(query): Query<EvaluateDataParams>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<EvaluateDataResponse>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let schema = &flow_ctx.flow.data_schema;

    let import_op_idx = flow_ctx
        .flow
        .flow_instance
        .import_ops
        .iter()
        .position(|op| op.name == query.field)
        .ok_or_else(|| {
            ApiError::new(
                &format!("source field not found: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;
    let plan = flow_ctx.flow.get_execution_plan().await?;
    let import_op = &plan.import_ops[import_op_idx];
    let field_schema = &schema.fields[import_op.output.field_idx as usize];
    let table_schema = match &field_schema.value_type.typ {
        schema::ValueType::Table(table) => table,
        _ => api_bail!("field is not a table: {}", query.field),
    };
    let key_field = table_schema
        .key_field()
        .ok_or_else(|| api_error!("field {} does not have a key", query.field))?;
    let key = value::KeyValue::from_strs(query.key, &key_field.value_type.typ)?;

    let evaluate_output = row_indexer::evaluate_source_entry_with_memory(
        &plan,
        import_op,
        schema,
        &key,
        memoization::EvaluationMemoryOptions {
            enable_cache: true,
            evaluation_only: true,
        },
        &lib_context.builtin_db_pool,
    )
    .await?
    .ok_or_else(|| api_error!("value not found for source at the specified key: {key:?}"))?;

    Ok(Json(EvaluateDataResponse {
        schema: schema.clone(),
        data: evaluate_output.data_scope.into(),
    }))
}

pub async fn update(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<stats::IndexUpdateInfo>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let mut live_updater = execution::FlowLiveUpdater::start(
        flow_ctx.clone(),
        &lib_context.builtin_db_pool,
        execution::FlowLiveUpdaterOptions {
            live_mode: false,
            ..Default::default()
        },
    )
    .await?;
    live_updater.wait().await?;
    Ok(Json(live_updater.index_update_info()))
}
