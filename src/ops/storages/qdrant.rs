use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::Arc;

use crate::base::spec::*;
use crate::ops::sdk::*;
use crate::setup;
use anyhow::{bail, Result};
use futures::FutureExt;
use qdrant_client::qdrant::vectors_output::VectorsOptions;
use qdrant_client::qdrant::{
    NamedVectors, PointId, PointStruct, UpsertPointsBuilder, Value as QdrantValue,
};
use qdrant_client::qdrant::{Query, QueryPointsBuilder, ScoredPoint};
use qdrant_client::Qdrant;
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

fn key_value_fields_iter<'a>(
    key_fields_schema: &[FieldSchema],
    key_value: &'a KeyValue,
) -> Result<&'a [KeyValue]> {
    let slice = if key_fields_schema.len() == 1 {
        std::slice::from_ref(key_value)
    } else {
        match key_value {
            KeyValue::Struct(fields) => fields,
            _ => anyhow::bail!("expect struct key value"),
        }
    };
    Ok(slice)
}

#[derive(Debug, Deserialize, Clone)]
pub struct Spec {
    collection_name: String,
}

pub struct Executor {
    client: Qdrant,
    collection_name: String,
    key_fields_schema: Vec<FieldSchema>,
    value_fields_schema: Vec<FieldSchema>,
    all_fields: Vec<FieldSchema>,
}

impl Executor {
    fn new(
        url: String,
        collection_name: String,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
    ) -> Result<Self> {
        let all_fields = key_fields_schema
            .iter()
            .chain(value_fields_schema.iter())
            .cloned()
            .collect::<Vec<_>>();
        Ok(Self {
            client: Qdrant::from_url(&url).build()?,
            key_fields_schema,
            value_fields_schema,
            all_fields,
            collection_name,
        })
    }
}

#[async_trait]
impl ExportTargetExecutor for Executor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let mut points: Vec<PointStruct> = Vec::with_capacity(mutation.upserts.len());
        for upsert in mutation.upserts.iter() {
            let key_fields = key_value_fields_iter(&self.key_fields_schema, &upsert.key)?;
            let point_id = key_to_point_id(key_fields)?;
            let (payload, vectors) =
                values_to_payload(&upsert.value.fields, &self.value_fields_schema)?;

            points.push(PointStruct::new(point_id, vectors, payload));
        }

        self.client
            .upsert_points(UpsertPointsBuilder::new(&self.collection_name, points))
            .await?;
        Ok(())
    }
}
fn key_to_point_id(key_values: &[KeyValue]) -> Result<PointId> {
    let point_id = if let Some(key_value) = key_values.first() {
        match key_value {
            KeyValue::Str(v) => PointId::from(v.to_string()),
            KeyValue::Int64(v) => PointId::from(*v as u64),
            KeyValue::Uuid(v) => PointId::from(v.to_string()),
            _ => bail!("Unsupported Qdrant Point ID key type"),
        }
    } else {
        let uuid = Uuid::new_v4().to_string();
        PointId::from(uuid)
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
                let json_value = match basic_value {
                    BasicValue::Bytes(v) => String::from_utf8_lossy(v).into(),
                    BasicValue::Str(v) => v.clone().to_string().into(),
                    BasicValue::Bool(v) => (*v).into(),
                    BasicValue::Int64(v) => (*v).into(),
                    BasicValue::Float32(v) => (*v as f64).into(),
                    BasicValue::Float64(v) => (*v).into(),
                    BasicValue::Range(v) => json!({ "start": v.start, "end": v.end }),
                    BasicValue::Vector(v) => {
                        let vector = convert_to_vector(v.to_vec());
                        vectors = vectors.add_vector(field_name, vector);
                        continue;
                    }
                    _ => bail!("Unsupported BasicValue type in Value::Basic"),
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

                BasicValueType::Vector(_) => {
                    let vectors_options = point.vectors.clone().unwrap().vectors_options.unwrap();

                    match vectors_options {
                        VectorsOptions::Vector(vector) => {
                            let x = vector
                                .data
                                .into_iter()
                                .map(BasicValue::Float32)
                                .collect::<Vec<_>>();
                            Some(BasicValue::Vector(Arc::from(x)))
                        }
                        VectorsOptions::Vectors(vectors) => {
                            let vector = vectors.vectors[field_name].clone();
                            let x = vector
                                .data
                                .into_iter()
                                .map(BasicValue::Float32)
                                .collect::<Vec<_>>();
                            Some(BasicValue::Vector(Arc::from(x)))
                        }
                    }
                }
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
impl QueryTarget for Executor {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults> {
        let points = self
            .client
            .query(
                QueryPointsBuilder::new(&self.collection_name)
                    .query(Query::new_nearest(query.vector))
                    .limit(query.limit as u64)
                    .using(query.vector_field_name)
                    .with_payload(true),
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

impl StorageFactoryBase for Arc<Factory> {
    type Spec = Spec;
    type SetupState = ();
    type Key = String;

    fn name(&self) -> &str {
        "Qdrant"
    }

    fn build(
        self: Arc<Self>,
        _name: String,
        spec: Spec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        _storage_options: IndexOptions,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<ExportTargetBuildOutput<Self>> {
        // TODO(Anush008): Add as a field to the Spec
        let url = "http://localhost:6334/";
        let collection_name = spec.collection_name.clone();

        let executors = async move {
            let executor = Arc::new(Executor::new(
                url.to_string(),
                spec.collection_name.clone(),
                key_fields_schema,
                value_fields_schema,
            )?);
            let query_target = executor.clone();
            Ok((
                executor as Arc<dyn ExportTargetExecutor>,
                Some(query_target as Arc<dyn QueryTarget>),
            ))
        };
        Ok(ExportTargetBuildOutput {
            executor: executors.boxed(),
            setup_key: collection_name,
            desired_setup_state: (),
        })
    }

    fn check_setup_status(
        &self,
        _key: String,
        _desired: Option<()>,
        _existing: setup::CombinedState<()>,
    ) -> Result<impl setup::ResourceSetupStatusCheck<String, ()> + 'static> {
        Err(anyhow!(
            "Set `setup_by_user` to `true` to use Qdrant storage"
        )) as Result<Infallible, _>
    }

    fn check_state_compatibility(
        &self,
        _desired: &(),
        _existing: &(),
    ) -> Result<SetupStateCompatibility> {
        Ok(SetupStateCompatibility::Compatible)
    }
}
