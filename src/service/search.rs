use crate::prelude::*;

use axum::extract::Path;
use axum::http::StatusCode;

use crate::lib_context::LibContext;
use crate::ops::interface::QueryResponse;
use axum::{extract::State, Json};
use axum_extra::extract::Query;

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    handler: Option<String>,
    field: Option<String>,
    query: String,
    limit: u32,
    metric: Option<spec::VectorSimilarityMetric>,
}

pub async fn search(
    Path(flow_name): Path<String>,
    Query(query): Query<SearchParams>,
    State(lib_context): State<Arc<LibContext>>,
) -> Result<Json<QueryResponse>, ApiError> {
    let flow_ctx = lib_context.get_flow_context(&flow_name)?;
    let query_handler = match &query.handler {
        Some(handler) => flow_ctx.get_query_handler(handler)?,
        None => {
            let query_handlers = flow_ctx.query_handlers.lock().unwrap();
            if query_handlers.is_empty() {
                return Err(ApiError::new(
                    &format!("No query handler found for flow: {flow_name}"),
                    StatusCode::NOT_FOUND,
                ));
            } else if query_handlers.len() == 1 {
                query_handlers.values().next().unwrap().clone()
            } else {
                return Err(ApiError::new(
                    "Found multiple query handlers for flow {}",
                    StatusCode::BAD_REQUEST,
                ));
            }
        }
    };
    let (results, info) = query_handler
        .search(query.query, query.limit, query.field, query.metric)
        .await?;
    let response = QueryResponse {
        results: results.try_into()?,
        info: serde_json::to_value(info).map_err(|e| {
            ApiError::new(
                &format!("Failed to serialize query info: {e}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?,
    };
    Ok(Json(response))
}
