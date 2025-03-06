use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};

use super::error::ApiError;
use crate::{
    api_bail, api_error,
    base::{schema, spec},
    execution::{evaluator, indexer},
};
use crate::{execution::indexer::IndexUpdateInfo, lib_context::LibContext};

use crate::base::{schema::DataSchema, value};

pub async fn list_flows(
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<Vec<String>>, ApiError> {
    Ok(Json(
        lib_context.flows.read().unwrap().keys().cloned().collect(),
    ))
}

pub async fn get_flow_spec(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<spec::FlowInstanceSpec>, ApiError> {
    let fl = &lib_context.with_flow_context(&flow_name, |ctx| ctx.flow.clone())?;
    Ok(Json(fl.flow_instance.clone()))
}

pub async fn get_flow_schema(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<DataSchema>, ApiError> {
    let fl = &lib_context.with_flow_context(&flow_name, |ctx| ctx.flow.clone())?;
    Ok(Json(fl.data_schema.clone()))
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
    let fl = &lib_context.with_flow_context(&flow_name, |ctx| ctx.flow.clone())?;
    let schema = &fl.data_schema;

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

    let execution_plan = fl.get_execution_plan().await?;
    let source_op = execution_plan
        .source_ops
        .iter()
        .find(|op| op.output.field_idx == field_idx as u32)
        .ok_or_else(|| {
            ApiError::new(
                &format!("field is not a source: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;

    let keys = source_op.executor.list_keys().await?;
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
    schema: DataSchema,
    data: value::ScopeValue,
}

pub async fn evaluate_data(
    Path(flow_name): Path<String>,
    Query(query): Query<EvaluateDataParams>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<EvaluateDataResponse>, ApiError> {
    let fl = &lib_context.with_flow_context(&flow_name, |ctx| ctx.flow.clone())?;
    let schema = &fl.data_schema;

    let source_op_idx = fl
        .flow_instance
        .source_ops
        .iter()
        .position(|source_op| source_op.name == query.field)
        .ok_or_else(|| {
            ApiError::new(
                &format!("source field not found: {}", query.field),
                StatusCode::BAD_REQUEST,
            )
        })?;
    let execution_plan = fl.get_execution_plan().await?;
    let field_schema =
        &schema.fields[execution_plan.source_ops[source_op_idx].output.field_idx as usize];
    let collection_schema = match &field_schema.value_type.typ {
        schema::ValueType::Collection(collection) => collection,
        _ => api_bail!("field is not a table: {}", query.field),
    };
    let key_field = collection_schema
        .key_field()
        .ok_or_else(|| api_error!("field {} does not have a key", query.field))?;
    let key = value::KeyValue::from_strs(query.key, &key_field.value_type.typ)?;

    let evaluation_cache = indexer::evaluation_cache_on_existing_data(
        &execution_plan,
        source_op_idx,
        &key,
        &lib_context.pool,
    )
    .await?;
    let data_builder = evaluator::evaluate_source_entry(
        &execution_plan,
        source_op_idx,
        &schema,
        &key,
        Some(&evaluation_cache),
    )
    .await?
    .ok_or_else(|| api_error!("value not found for source at the specified key: {key:?}"))?;

    Ok(Json(EvaluateDataResponse {
        schema: schema.clone(),
        data: data_builder.into(),
    }))
}

pub async fn update(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<IndexUpdateInfo>, ApiError> {
    let fl = &lib_context.with_flow_context(&flow_name, |ctx| ctx.flow.clone())?;
    let execution_plan = fl.get_execution_plan().await?;
    let update_info = indexer::update(
        &fl.flow_instance,
        &execution_plan,
        &fl.data_schema,
        &lib_context.pool,
    )
    .await?;
    Ok(Json(update_info))
}
