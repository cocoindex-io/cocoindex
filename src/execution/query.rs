use std::{sync::Arc, vec};

use anyhow::{bail, Result};
use serde::Serialize;

use super::evaluator::evaluate_transient_flow;
use crate::{
    api_error,
    base::{spec::VectorSimilarityMetric, value},
    builder::{AnalyzedFlow, AnalyzedTransientFlow},
    ops::interface::{QueryResults, QueryTarget, VectorMatchQuery},
};

pub struct SimpleSemanticsQueryHandler {
    pub flow_name: String,
    pub query_target: Arc<dyn QueryTarget>,
    pub query_transform_flow: Arc<AnalyzedTransientFlow>,
    pub default_similarity_metric: VectorSimilarityMetric,
    pub default_vector_field_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SimpleSemanticsQueryInfo {
    pub similarity_metric: VectorSimilarityMetric,
    pub query_vector: Vec<f32>,
    pub vector_field_name: String,
}

impl SimpleSemanticsQueryHandler {
    pub async fn new(
        flow: Arc<AnalyzedFlow>,
        target_name: &str,
        query_transform_flow: Arc<AnalyzedTransientFlow>,
        default_similarity_metric: VectorSimilarityMetric,
    ) -> Result<Self> {
        let export_op_idx = flow
            .flow_instance
            .export_ops
            .iter()
            .position(|export_op| export_op.name == target_name)
            .unwrap();
        let export_op = &flow.flow_instance.export_ops[export_op_idx];
        let vector_index_defs = &export_op.spec.index_options.vector_index_defs;
        let execution_plan = flow.get_execution_plan().await?;
        let analyzed_export_op = &execution_plan.export_ops[export_op_idx];
        Ok(Self {
            flow_name: flow.flow_instance.name.clone(),
            query_target: if let Some(query_target) = &analyzed_export_op.query_target {
                query_target.clone()
            } else {
                bail!(
                    "Query target is not supported by export op: {}",
                    target_name
                );
            },
            query_transform_flow,
            default_similarity_metric,
            default_vector_field_name: if vector_index_defs.len() == 1 {
                Some(vector_index_defs[0].field_name.clone())
            } else {
                None
            },
        })
    }

    pub async fn search(
        &self,
        query: String,
        limit: u32,
        vector_field_name: Option<String>,
        similarity_matric: Option<VectorSimilarityMetric>,
    ) -> Result<(QueryResults, SimpleSemanticsQueryInfo)> {
        let query_results = evaluate_transient_flow(
            &self.query_transform_flow,
            &vec![value::BasicValue::Str(Arc::from(query)).into()],
        )
        .await?;
        let vector = match query_results {
            value::Value::Basic(value::BasicValue::Vector(v)) => v
                .iter()
                .map(|f| {
                    Ok(match f {
                        value::BasicValue::Int64(i) => *i as f32,
                        value::BasicValue::Float32(f) => *f,
                        value::BasicValue::Float64(f) => *f as f32,
                        value::BasicValue::Bytes(_)
                        | value::BasicValue::Str(_)
                        | value::BasicValue::Bool(_)
                        | value::BasicValue::Range(_)
                        | value::BasicValue::Json(_)
                        | value::BasicValue::Vector(_) => {
                            bail!("Query results is not a vector of number")
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            _ => bail!("Query results is not a vector"),
        };

        let vector_field_name = vector_field_name
            .or(self.default_vector_field_name.clone())
            .ok_or_else(|| api_error!("vector field name must be provided"))?;

        let similarity_metric = similarity_matric.unwrap_or(self.default_similarity_metric);
        let info = SimpleSemanticsQueryInfo {
            similarity_metric,
            query_vector: vector.clone(),
            vector_field_name: vector_field_name.clone(),
        };
        let query = VectorMatchQuery {
            vector_field_name,
            vector,
            similarity_metric,
            limit,
        };
        Ok((self.query_target.search(query).await?, info))
    }
}
