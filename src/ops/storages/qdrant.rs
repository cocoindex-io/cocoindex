use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::Arc;

use crate::ops::sdk::*;
use crate::setup;
use anyhow::{bail, Result};
use futures::FutureExt;
use qdrant_client::qdrant::vectors_output::VectorsOptions;
use qdrant_client::qdrant::{
    DeletePointsBuilder, NamedVectors, PointId, PointStruct, PointsIdsList, UpsertPointsBuilder,
    Value as QdrantValue,
};
use qdrant_client::qdrant::{Query, QueryPointsBuilder, ScoredPoint};
use qdrant_client::Qdrant;
use serde_json::json;

#[derive(Debug, Deserialize, Clone)]
pub struct Spec {
    collection_name: String,
    grpc_url: String,
    api_key: Option<String>,
}

pub struct ExportContext {
    client: Qdrant,
    collection_name: String,
    value_fields_schema: Vec<FieldSchema>,
    all_fields: Vec<FieldSchema>,
}

impl ExportContext {
    fn new(
        url: String,
        collection_name: String,
        api_key: Option<String>,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
    ) -> Result<Self> {
        let all_fields = key_fields_schema
            .iter()
            .chain(value_fields_schema.iter())
            .cloned()
            .collect::<Vec<_>>();

        // Hotfix to resolve
        // `no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point`
        // when using HTTPS URLs.
        let _ = rustls::crypto::ring::default_provider().install_default();

        Ok(Self {
            client: Qdrant::from_url(&url)
                .api_key(api_key)
                .skip_compatibility_check()
                .build()?,
            value_fields_schema,
            all_fields,
            collection_name,
        })
    }

    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let mut points: Vec<PointStruct> = Vec::with_capacity(mutation.upserts.len());
        for upsert in mutation.upserts.iter() {
            let point_id = key_to_point_id(&upsert.key)?;
            let (payload, vectors) =
                values_to_payload(&upsert.value.fields, &self.value_fields_schema)?;

            points.push(PointStruct::new(point_id, vectors, payload));
        }

        if !points.is_empty() {
            self.client
                .upsert_points(UpsertPointsBuilder::new(&self.collection_name, points).wait(true))
                .await?;
        }

        let ids = mutation
            .delete_keys
            .iter()
            .map(key_to_point_id)
            .collect::<Result<Vec<_>>>()?;

        if !ids.is_empty() {
            self.client
                .delete_points(
                    DeletePointsBuilder::new(&self.collection_name)
                        .points(PointsIdsList { ids })
                        .wait(true),
                )
                .await?;
        }

        Ok(())
    }
}
fn key_to_point_id(key_value: &KeyValue) -> Result<PointId> {
    let point_id = match key_value {
        KeyValue::Str(v) => PointId::from(v.to_string()),
        KeyValue::Int64(v) => PointId::from(*v as u64),
        KeyValue::Uuid(v) => PointId::from(v.to_string()),
        e => bail!("Invalid Qdrant point ID: {e}"),
    };

    Ok(point_id)
}

fn values_to_payload(
    value_fields: &[Value],
    schema: &[FieldSchema],
) -> Result<(HashMap<String, QdrantValue>, NamedVectors)> {
    let mut payload = HashMap::with_capacity(value_fields.len());
    let mut vectors = NamedVectors::default();

    for (value, field_schema) in value_fields.iter().zip(schema.iter()) {
        let field_name = &field_schema.name;

        match value {
            Value::Basic(basic_value) => {
                let json_value: serde_json::Value = match basic_value {
                    BasicValue::Bytes(v) => String::from_utf8_lossy(v).into(),
                    BasicValue::Str(v) => v.clone().to_string().into(),
                    BasicValue::Bool(v) => (*v).into(),
                    BasicValue::Int64(v) => (*v).into(),
                    BasicValue::Float32(v) => (*v as f64).into(),
                    BasicValue::Float64(v) => (*v).into(),
                    BasicValue::Range(v) => json!({ "start": v.start, "end": v.end }),
                    BasicValue::Uuid(v) => v.to_string().into(),
                    BasicValue::Date(v) => v.to_string().into(),
                    BasicValue::LocalDateTime(v) => v.to_string().into(),
                    BasicValue::Time(v) => v.to_string().into(),
                    BasicValue::OffsetDateTime(v) => v.to_string().into(),
                    BasicValue::Json(v) => (**v).clone(),
                    BasicValue::Vector(v) => {
                        let vector = convert_to_vector(v.to_vec());
                        vectors = vectors.add_vector(field_name, vector);
                        continue;
                    }
                };
                payload.insert(field_name.clone(), json_value.into());
            }
            Value::Null => {
                payload.insert(field_name.clone(), QdrantValue { kind: None });
            }
            _ => bail!("Unsupported Value variant: {:?}", value),
        }
    }

    Ok((payload, vectors))
}

fn convert_to_vector(v: Vec<BasicValue>) -> Vec<f32> {
    v.iter()
        .filter_map(|elem| match elem {
            BasicValue::Float32(f) => Some(*f),
            BasicValue::Float64(f) => Some(*f as f32),
            BasicValue::Int64(i) => Some(*i as f32),
            _ => None,
        })
        .collect()
}

fn into_value(point: &ScoredPoint, schema: &FieldSchema) -> Result<Value> {
    let field_name = &schema.name;
    let typ = schema.value_type.typ.clone();
    let value = match typ {
        ValueType::Basic(basic_type) => {
            let basic_value = match basic_type {
                BasicValueType::Str => point.payload.get(field_name).and_then(|v| {
                    v.as_str()
                        .map(|s| BasicValue::Str(Arc::from(s.to_string())))
                }),
                BasicValueType::Bool => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_bool().map(BasicValue::Bool)),

                BasicValueType::Int64 => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_integer().map(BasicValue::Int64)),

                BasicValueType::Float32 => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_double().map(|f| BasicValue::Float32(f as f32))),

                BasicValueType::Float64 => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_double().map(BasicValue::Float64)),

                BasicValueType::Json => point
                    .payload
                    .get(field_name)
                    .map(|v| BasicValue::Json(Arc::from(v.clone().into_json()))),

                BasicValueType::Vector(_) => point
                    .vectors
                    .as_ref()
                    .and_then(|v| v.vectors_options.as_ref())
                    .and_then(|vectors_options| match vectors_options {
                        VectorsOptions::Vector(vector) => {
                            let values = vector
                                .data
                                .iter()
                                .map(|f| BasicValue::Float32(*f))
                                .collect::<Vec<_>>();
                            Some(BasicValue::Vector(Arc::from(values)))
                        }
                        VectorsOptions::Vectors(vectors) => {
                            vectors.vectors.get(field_name).map(|vector| {
                                let values = vector
                                    .data
                                    .iter()
                                    .map(|f| BasicValue::Float32(*f))
                                    .collect::<Vec<_>>();
                                BasicValue::Vector(Arc::from(values))
                            })
                        }
                    }),

                BasicValueType::Uuid => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_str()?.parse().ok().map(BasicValue::Uuid)),

                BasicValueType::Date => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_str()?.parse().ok().map(BasicValue::Date)),

                BasicValueType::Time => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_str()?.parse().ok().map(BasicValue::Time)),

                BasicValueType::LocalDateTime => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_str()?.parse().ok().map(BasicValue::LocalDateTime)),

                BasicValueType::OffsetDateTime => point
                    .payload
                    .get(field_name)
                    .and_then(|v| v.as_str()?.parse().ok().map(BasicValue::OffsetDateTime)),

                BasicValueType::Range => point.payload.get(field_name).and_then(|v| {
                    v.as_struct().and_then(|s| {
                        let start = s.fields.get("start").and_then(|f| f.as_integer());
                        let end = s.fields.get("end").and_then(|f| f.as_integer());

                        match (start, end) {
                            (Some(start), Some(end)) => Some(BasicValue::Range(RangeValue {
                                start: start as usize,
                                end: end as usize,
                            })),
                            _ => None,
                        }
                    })
                }),
                _ => {
                    anyhow::bail!("Unsupported value type")
                }
            };
            basic_value.map(Value::Basic)
        }
        _ => point
            .payload
            .get(field_name)
            .map(|v| Value::from_json(v.clone().into_json(), &typ))
            .transpose()?,
    };

    let final_value = if let Some(v) = value { v } else { Value::Null };
    Ok(final_value)
}

#[async_trait]
impl QueryTarget for ExportContext {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults> {
        let points = self
            .client
            .query(
                QueryPointsBuilder::new(&self.collection_name)
                    .query(Query::new_nearest(query.vector))
                    .limit(query.limit as u64)
                    .using(query.vector_field_name)
                    .with_payload(true)
                    .with_vectors(true),
            )
            .await?
            .result;

        let results = points
            .iter()
            .map(|point| {
                let score = point.score as f64;
                let data = self
                    .all_fields
                    .iter()
                    .map(|schema| into_value(point, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(QueryResult { data, score })
            })
            .collect::<Result<Vec<QueryResult>>>()?;
        Ok(QueryResults {
            fields: self.all_fields.clone(),
            results,
        })
    }
}

#[derive(Default)]
pub struct Factory {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CollectionId {
    collection_name: String,
}

impl Display for CollectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.collection_name)?;
        Ok(())
    }
}

#[async_trait]
impl StorageFactoryBase for Arc<Factory> {
    type Spec = Spec;
    type DeclarationSpec = ();
    type SetupState = ();
    type SetupStatus = Infallible;
    type Key = String;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "Qdrant"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        _declarations: Vec<()>,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(String, ())>,
    )> {
        let data_coll_output = data_collections
            .into_iter()
            .map(|d| {
                if d.key_fields_schema.len() != 1 {
                    api_bail!(
                        "Expected one primary key field for the point ID. Got {}.",
                        d.key_fields_schema.len()
                    )
                }

                let collection_name = d.spec.collection_name.clone();

                let export_context = Arc::new(ExportContext::new(
                    d.spec.grpc_url,
                    d.spec.collection_name.clone(),
                    d.spec.api_key,
                    d.key_fields_schema,
                    d.value_fields_schema,
                )?);
                let query_target = export_context.clone();
                let executors = async move {
                    Ok(TypedExportTargetExecutors {
                        export_context,
                        query_target: Some(query_target as Arc<dyn QueryTarget>),
                    })
                };
                Ok(TypedExportDataCollectionBuildOutput {
                    executors: executors.boxed(),
                    setup_key: collection_name,
                    desired_setup_state: (),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, vec![]))
    }

    async fn check_setup_status(
        &self,
        _key: String,
        _desired: Option<()>,
        _existing: setup::CombinedState<()>,
        _auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Self::SetupStatus> {
        Err(anyhow!("Set `setup_by_user` to `true` to export to Qdrant")) as Result<Infallible, _>
    }

    fn check_state_compatibility(
        &self,
        _desired: &(),
        _existing: &(),
    ) -> Result<SetupStateCompatibility> {
        Ok(SetupStateCompatibility::Compatible)
    }

    fn describe_resource(&self, key: &String) -> Result<String> {
        Ok(format!("Qdrant collection {}", key))
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, ExportContext>>,
    ) -> Result<()> {
        for mutation_w_ctx in mutations.into_iter() {
            mutation_w_ctx
                .export_context
                .apply_mutation(mutation_w_ctx.mutation)
                .await?;
        }
        Ok(())
    }

    async fn apply_setup_changes(
        &self,
        _setup_status: Vec<&'async_trait Self::SetupStatus>,
    ) -> Result<()> {
        Err(anyhow!("Qdrant does not support setup changes"))
    }
}
