use crate::prelude::*;

use crate::execution::{evaluator, indexing_status, memoization, row_indexer, stats};
use crate::lib_context::LibContext;
use crate::service::query_handler::{QueryHandlerSpec, QueryInput, QueryOutput};
use crate::{base::schema::FlowSchema, ops::interface::SourceExecutorReadOptions};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use axum_extra::extract::Query;

pub async fn list_flows(
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<Vec<String>>, ApiError> {
    Ok(Json(
        lib_context.flows.lock().unwrap().keys().cloned().collect(),
    ))
}

pub async fn get_flow_schema(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<FlowSchema>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    Ok(Json(flow_ctx.flow.data_schema.clone()))
}

#[derive(Serialize)]
pub struct GetFlowResponseData {
    flow_spec: spec::FlowInstanceSpec,
    data_schema: FlowSchema,
    query_handlers_spec: HashMap<String, Arc<QueryHandlerSpec>>,
}

#[derive(Serialize)]
pub struct GetFlowResponse {
    #[serde(flatten)]
    data: GetFlowResponseData,
    fingerprint: utils::fingerprint::Fingerprint,
}

pub async fn get_flow(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<GetFlowResponse>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let flow_spec = flow_ctx.flow.flow_instance.clone();
    let data_schema = flow_ctx.flow.data_schema.clone();
    let query_handlers_spec: HashMap<_, _> = {
        let query_handlers = flow_ctx.query_handlers.read().unwrap();
        query_handlers
            .iter()
            .map(|(name, handler)| (name.clone(), handler.info.clone()))
            .collect()
    };
    let data = GetFlowResponseData {
        flow_spec,
        data_schema,
        query_handlers_spec,
    };
    let fingerprint = utils::fingerprint::Fingerprinter::default()
        .with(&data)
        .map_err(|e| api_error!("failed to fingerprint flow response: {e}"))?
        .into_fingerprint();
    Ok(Json(GetFlowResponse { data, fingerprint }))
}

#[derive(Deserialize)]
pub struct GetKeysParam {
    field: String,
}

#[derive(Serialize)]
pub struct GetKeysResponse {
    key_schema: Vec<schema::FieldSchema>,
    keys: Vec<(value::KeyValue, serde_json::Value)>,
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
    let pk_schema = schema.fields[field_idx].value_type.typ.key_schema();
    if pk_schema.is_empty() {
        api_bail!("field has no key: {}", query.field);
    }

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

    let mut rows_stream = import_op
        .executor
        .list(&SourceExecutorReadOptions {
            include_ordinal: false,
            include_content_version_fp: false,
            include_value: false,
        })
        .await?;
    let mut keys = Vec::new();
    while let Some(rows) = rows_stream.next().await {
        keys.extend(rows?.into_iter().map(|row| (row.key, row.key_aux_info)));
    }
    Ok(Json(GetKeysResponse {
        key_schema: pk_schema.to_vec(),
        keys,
    }))
}

#[derive(Deserialize)]
pub struct SourceRowKeyParams {
    field: String,
    key: Vec<String>,
    key_aux: Option<String>,
}

#[derive(Serialize)]
pub struct EvaluateDataResponse {
    schema: FlowSchema,
    data: value::ScopeValue,
}

struct SourceRowKeyContextHolder<'a> {
    plan: Arc<plan::ExecutionPlan>,
    import_op_idx: usize,
    schema: &'a FlowSchema,
    key: value::KeyValue,
    key_aux_info: serde_json::Value,
}

impl<'a> SourceRowKeyContextHolder<'a> {
    async fn create(flow_ctx: &'a FlowContext, source_row_key: SourceRowKeyParams) -> Result<Self> {
        let schema = &flow_ctx.flow.data_schema;
        let import_op_idx = flow_ctx
            .flow
            .flow_instance
            .import_ops
            .iter()
            .position(|op| op.name == source_row_key.field)
            .ok_or_else(|| {
                ApiError::new(
                    &format!("source field not found: {}", source_row_key.field),
                    StatusCode::BAD_REQUEST,
                )
            })?;
        let plan = flow_ctx.flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[import_op_idx];
        let field_schema = &schema.fields[import_op.output.field_idx as usize];
        let table_schema = match &field_schema.value_type.typ {
            schema::ValueType::Table(table) => table,
            _ => api_bail!("field is not a table: {}", source_row_key.field),
        };
        let key_schema = table_schema.key_schema();
        let key = value::KeyValue::decode_from_strs(source_row_key.key, key_schema)?;
        let key_aux_info = source_row_key
            .key_aux
            .map(|s| utils::deser::from_json_str(&s))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            plan,
            import_op_idx,
            schema,
            key,
            key_aux_info,
        })
    }

    fn as_context<'b>(&'b self) -> evaluator::SourceRowEvaluationContext<'b> {
        evaluator::SourceRowEvaluationContext {
            plan: &self.plan,
            import_op: &self.plan.import_ops[self.import_op_idx],
            schema: self.schema,
            key: &self.key,
            import_op_idx: self.import_op_idx,
        }
    }
}

pub async fn evaluate_data(
    Path(flow_name): Path<String>,
    Query(query): Query<SourceRowKeyParams>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<EvaluateDataResponse>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let source_row_key_ctx = SourceRowKeyContextHolder::create(&flow_ctx, query).await?;
    let execution_ctx = flow_ctx.use_execution_ctx().await?;
    let evaluate_output = row_indexer::evaluate_source_entry_with_memory(
        &source_row_key_ctx.as_context(),
        &source_row_key_ctx.key_aux_info,
        &execution_ctx.setup_execution_context,
        memoization::EvaluationMemoryOptions {
            enable_cache: true,
            evaluation_only: true,
        },
        lib_context.require_builtin_db_pool()?,
    )
    .await?
    .ok_or_else(|| {
        api_error!(
            "value not found for source at the specified key: {key:?}",
            key = source_row_key_ctx.key
        )
    })?;

    Ok(Json(EvaluateDataResponse {
        schema: flow_ctx.flow.data_schema.clone(),
        data: evaluate_output.data_scope.into(),
    }))
}

pub async fn update(
    Path(flow_name): Path<String>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<stats::IndexUpdateInfo>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let live_updater = execution::FlowLiveUpdater::start(
        flow_ctx.clone(),
        lib_context.require_builtin_db_pool()?,
        execution::FlowLiveUpdaterOptions {
            live_mode: false,
            ..Default::default()
        },
    )
    .await?;
    live_updater.wait().await?;
    Ok(Json(live_updater.index_update_info()))
}

pub async fn get_row_indexing_status(
    Path(flow_name): Path<String>,
    Query(query): Query<SourceRowKeyParams>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<indexing_status::SourceRowIndexingStatus>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let source_row_key_ctx = SourceRowKeyContextHolder::create(&flow_ctx, query).await?;

    let execution_ctx = flow_ctx.use_execution_ctx().await?;
    let indexing_status = indexing_status::get_source_row_indexing_status(
        &source_row_key_ctx.as_context(),
        &source_row_key_ctx.key_aux_info,
        &execution_ctx.setup_execution_context,
        lib_context.require_builtin_db_pool()?,
    )
    .await?;
    Ok(Json(indexing_status))
}

pub async fn query(
    Path((flow_name, query_handler_name)): Path<(String, String)>,
    Query(query): Query<QueryInput>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<QueryOutput>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let query_handler = {
        let query_handlers = flow_ctx.query_handlers.read().unwrap();
        query_handlers
            .get(&query_handler_name)
            .ok_or_else(|| {
                ApiError::new(
                    &format!("query handler not found: {query_handler_name}"),
                    StatusCode::BAD_REQUEST,
                )
            })?
            .handler
            .clone()
    };
    let query_output = query_handler
        .query(query.into(), &flow_ctx.flow.flow_instance_ctx)
        .await?;
    Ok(Json(query_output))
}
