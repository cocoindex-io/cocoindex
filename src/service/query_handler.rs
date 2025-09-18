use crate::{
    base::spec::{FieldName, VectorSimilarityMetric},
    prelude::*,
};

#[derive(Serialize, Deserialize, Default)]
pub struct QueryHandlerResultFields {
    embedding: Vec<String>,
    score: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct QueryHandlerSpec {
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
    pub similarity_metric: Option<VectorSimilarityMetric>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryOutput {
    pub results: Vec<Vec<(FieldName, serde_json::Value)>>,
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
