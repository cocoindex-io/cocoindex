use crate::prelude::*;

#[derive(Serialize, Deserialize, Default)]
pub struct QueryHandlerResultFields {
    embedding: Vec<String>,
    score: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct QueryHandlerInfo {
    #[serde(default)]
    result_fields: QueryHandlerResultFields,
}

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
    pub results: Vec<HashMap<String, serde_json::Value>>,
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
