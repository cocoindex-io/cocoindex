use crate::prelude::*;

#[derive(Serialize)]
pub struct QueryHandlerInfo {}

#[derive(Serialize, Deserialize)]
pub struct QueryInput {
    pub query: String,
}

#[derive(Serialize, Deserialize, Default)]
pub struct QueryInfo {
    pub embedding: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryOutput {
    pub results: Vec<IndexMap<String, serde_json::Value>>,
    pub query_info: QueryInfo,
}

#[async_trait]
pub trait QueryHandler: Send + Sync {
    async fn query(
        &self,
        input: QueryInput,
        flow_ctx: &interface::FlowInstanceContext,
    ) -> Result<QueryOutput>;
}
