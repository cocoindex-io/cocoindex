use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use crate::base::spec::*;
use crate::ops::sdk::*;
use crate::setup;
use anyhow::{bail, Result};
use derivative::Derivative;
use futures::FutureExt;
use qdrant_client::qdrant::value::Kind;
use qdrant_client::qdrant::vectors_output::VectorsOptions;
use qdrant_client::qdrant::{NamedVectors, PointStruct, UpsertPointsBuilder, Value as QdrantValue};
use qdrant_client::qdrant::{Query, QueryPointsBuilder, ScoredPoint};
use qdrant_client::Qdrant;
use serde::Serialize;

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
        url: &str,
        collection_name: &str,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
    ) -> Result<Self> {
        let all_fields = key_fields_schema
            .iter()
            .chain(value_fields_schema.iter())
            .cloned()
            .collect::<Vec<_>>();
        Ok(Self {
            client: Qdrant::from_url(url).build()?,
            key_fields_schema,
            value_fields_schema,
            all_fields,
            collection_name: collection_name.to_string(),
        })
    }
}

#[async_trait]
impl ExportTargetExecutor for Executor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let mut points: Vec<PointStruct> = Vec::with_capacity(mutation.upserts.len());
        for upsert in mutation.upserts.iter() {
            let key_fields = key_value_fields_iter(&self.key_fields_schema, &upsert.key)?
                .iter()
                .collect();
            let key_fields = parse_key_fields(&key_fields, &self.key_fields_schema)?;
            let (mut payload, vectors) =
                parse_value_fields(&upsert.value.fields, &self.value_fields_schema)?;
            payload.extend(key_fields);

            points.push(PointStruct::new(1, vectors, payload));
        }

        self.client
            .upsert_points(UpsertPointsBuilder::new(&self.collection_name, points))
            .await?;
        Ok(())
    }
}

fn parse_key_fields(
    key_fields: &Vec<&KeyValue>,
    schema: &Vec<FieldSchema>,
) -> Result<HashMap<String, QdrantValue>> {
    let mut payload = HashMap::with_capacity(key_fields.len());

    for (key_value, field_schema) in key_fields.iter().zip(schema.iter()) {
        let value = match key_value {
            KeyValue::Bytes(v) => QdrantValue {
                kind: Some(Kind::StringValue(String::from_utf8_lossy(v).into_owned())),
            },
            KeyValue::Str(v) => QdrantValue {
                kind: Some(Kind::StringValue(v.clone().to_string())),
            },
            KeyValue::Bool(v) => QdrantValue {
                kind: Some(Kind::BoolValue(*v)),
            },
            KeyValue::Int64(v) => QdrantValue {
                kind: Some(Kind::IntegerValue(*v)),
            },
            e => anyhow::bail!("Unsupported key value type {}", e),
        };

        payload.insert(field_schema.name.clone(), value);
    }

    Ok(payload)
}

fn parse_value_fields(
    value_fields: &Vec<Value>,
    schema: &Vec<FieldSchema>,
) -> Result<(HashMap<String, QdrantValue>, NamedVectors)> {
    let mut payload = HashMap::with_capacity(value_fields.len());
    let mut vectors = NamedVectors::default();

    for (value, field_schema) in value_fields.iter().zip(schema.iter()) {
        let field_name = &field_schema.name;
        match value {
            Value::Basic(basic_value) => match basic_value {
                BasicValue::Bytes(v) => insert_qdrant_value(
                    &mut payload,
                    field_name,
                    Kind::StringValue(String::from_utf8_lossy(v).into_owned()),
                ),
                BasicValue::Str(v) => insert_qdrant_value(
                    &mut payload,
                    field_name,
                    Kind::StringValue(v.clone().to_string()),
                ),
                BasicValue::Bool(v) => {
                    insert_qdrant_value(&mut payload, field_name, Kind::BoolValue(*v))
                }
                BasicValue::Int64(v) => {
                    insert_qdrant_value(&mut payload, field_name, Kind::IntegerValue(*v))
                }
                BasicValue::Float32(v) => {
                    insert_qdrant_value(&mut payload, field_name, Kind::DoubleValue(*v as f64))
                }
                BasicValue::Float64(v) => {
                    insert_qdrant_value(&mut payload, field_name, Kind::DoubleValue(*v))
                }
                BasicValue::Range(v) => insert_qdrant_value(
                    &mut payload,
                    field_name,
                    Kind::StringValue(format!("[{}, {})", v.start, v.end)),
                ),
                BasicValue::Vector(v) => {
                    let vector = convert_to_vector(v.to_vec());
                    vectors = vectors.add_vector(field_name, vector);
                }
                _ => {
                    bail!("Unsupported BasicValue type in Value::Basic");
                }
            },
            Value::Null => {
                payload.insert(field_schema.name.clone(), QdrantValue { kind: None });
            }
            _ => {
                bail!("Unsupported Value variant: {:?}", value);
            }
        }
    }

    Ok((payload, vectors))
}

fn insert_qdrant_value(payload: &mut HashMap<String, QdrantValue>, field_name: &str, kind: Kind) {
    payload.insert(field_name.to_string(), QdrantValue { kind: Some(kind) });
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupState {}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetupStatusCheck {
    #[derivative(Debug = "ignore")]
    table_id: CollectionId,

    desired_state: Option<SetupState>,
}

impl SetupStatusCheck {
    fn new(table_id: CollectionId, desired_state: Option<SetupState>) -> Self {
        Self {
            table_id,
            desired_state,
        }
    }
}

#[async_trait]
impl setup::ResourceSetupStatusCheck for SetupStatusCheck {
    type Key = CollectionId;
    type State = SetupState;

    fn describe_resource(&self) -> String {
        format!("Qdrant collection {}", self.table_id)
    }

    fn key(&self) -> &Self::Key {
        &self.table_id
    }

    fn desired_state(&self) -> Option<&Self::State> {
        self.desired_state.as_ref()
    }

    fn describe_changes(&self) -> Vec<String> {
        vec![]
    }

    fn change_type(&self) -> setup::SetupChangeType {
        setup::SetupChangeType::NoChange
    }

    async fn apply_change(&self) -> Result<()> {
        Ok(())
    }
}

impl StorageFactoryBase for Arc<Factory> {
    type Spec = Spec;
    type SetupState = SetupState;
    type Key = CollectionId;

    fn name(&self) -> &str {
        "Qdrant"
    }

    fn build(
        self: Arc<Self>,
        _name: String,
        _target_id: i32,
        spec: Spec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        _storage_options: IndexOptions,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<(
        (CollectionId, SetupState),
        ExecutorFuture<'static, (Arc<dyn ExportTargetExecutor>, Option<Arc<dyn QueryTarget>>)>,
    )> {
        let url = "http://localhost:6334/";
        let collection_name = spec.collection_name;
        let table_id = CollectionId {
            collection_name: collection_name.to_owned(),
        };
        let setup_state = SetupState {};
        let executors = async move {
            let executor = Arc::new(Executor::new(
                &url,
                &collection_name,
                key_fields_schema,
                value_fields_schema,
            )?);
            let query_target = executor.clone();
            Ok((
                executor as Arc<dyn ExportTargetExecutor>,
                Some(query_target as Arc<dyn QueryTarget>),
            ))
        };
        Ok(((table_id, setup_state), executors.boxed()))
    }

    fn check_setup_status(
        &self,
        key: CollectionId,
        desired: Option<SetupState>,
        _existing: setup::CombinedState<SetupState>,
    ) -> Result<
        impl setup::ResourceSetupStatusCheck<Key = CollectionId, State = SetupState> + 'static,
    > {
        Ok(SetupStatusCheck::new(key, desired))
    }

    fn will_keep_all_existing_data(
        &self,
        _name: &str,
        _target_id: i32,
        _desired: &SetupState,
        _existing: &SetupState,
    ) -> Result<bool> {
        Ok(true)
    }
}
