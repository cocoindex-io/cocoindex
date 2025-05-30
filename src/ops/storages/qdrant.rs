use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::Arc;

use crate::ops::registry::ExecutorFactoryRegistry;
use crate::ops::sdk::*;
use crate::setup;
use anyhow::{Result, bail};
use futures::FutureExt;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    DeletePointsBuilder, NamedVectors, PointId, PointStruct, PointsIdsList, UpsertPointsBuilder,
    Value as QdrantValue,
};
use serde_json::json;

#[derive(Debug, Deserialize, Clone)]
struct Spec {
    collection_name: String,
    grpc_url: String,
    api_key: Option<String>,
}

struct ExportContext {
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
            .deletes
            .iter()
            .map(|deletion| key_to_point_id(&deletion.key))
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
                    BasicValue::Time(v) => v.to_string().into(),
                    BasicValue::LocalDateTime(v) => v.to_string().into(),
                    BasicValue::OffsetDateTime(v) => v.to_string().into(),
                    BasicValue::TimeDelta(v) => v.to_string().into(),
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

#[derive(Default)]
struct Factory {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct CollectionId {
    collection_name: String,
}

impl Display for CollectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.collection_name)?;
        Ok(())
    }
}

#[async_trait]
impl StorageFactoryBase for Factory {
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
                let executors = async move {
                    Ok(TypedExportTargetExecutors {
                        export_context,
                        query_target: None,
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
        _setup_status: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        _auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()> {
        Err(anyhow!("Qdrant does not support setup changes"))
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory {}.register(registry)
}
