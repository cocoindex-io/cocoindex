use crate::ops::sdk::*;
use crate::prelude::*;

use std::fmt::Display;

use crate::ops::registry::ExecutorFactoryRegistry;
use crate::setup;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, DeletePointsBuilder, Distance, NamedVectors, PointId, PointStruct,
    PointsIdsList, UpsertPointsBuilder, Value as QdrantValue, VectorParamsBuilder,
    VectorsConfigBuilder,
};

const DEFAULT_VECTOR_SIMILARITY_METRIC: spec::VectorSimilarityMetric =
    spec::VectorSimilarityMetric::CosineSimilarity;
const DEFAULT_URL: &str = "http://localhost:6334/";

////////////////////////////////////////////////////////////
// Public Types
////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    grpc_url: String,
    api_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Spec {
    connection: Option<spec::AuthEntryReference<ConnectionSpec>>,
    collection_name: String,
}

////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////

struct FieldInfo {
    field_schema: schema::FieldSchema,
    is_qdrant_vector: bool,
}

fn parse_supported_vector_size(typ: &schema::ValueType) -> Option<usize> {
    match typ {
        schema::ValueType::Basic(schema::BasicValueType::Vector(vector_schema)) => {
            match &*vector_schema.element_type {
                schema::BasicValueType::Float32
                | schema::BasicValueType::Float64
                | schema::BasicValueType::Int64 => vector_schema.dimension,
                _ => None,
            }
        }
        _ => None,
    }
}

fn encode_vector(v: &[BasicValue]) -> Result<Vec<f32>> {
    v.iter()
        .map(|elem| {
            Ok(match elem {
                BasicValue::Float32(f) => *f,
                BasicValue::Float64(f) => *f as f32,
                BasicValue::Int64(i) => *i as f32,
                _ => bail!("Unsupported vector type: {:?}", elem.kind()),
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn embedding_metric_to_qdrant(metric: spec::VectorSimilarityMetric) -> Result<Distance> {
    Ok(match metric {
        spec::VectorSimilarityMetric::CosineSimilarity => Distance::Cosine,
        spec::VectorSimilarityMetric::L2Distance => Distance::Euclid,
        spec::VectorSimilarityMetric::InnerProduct => Distance::Dot,
    })
}

////////////////////////////////////////////////////////////
// Setup
////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct CollectionKey {
    connection: Option<spec::AuthEntryReference<ConnectionSpec>>,
    collection_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct VectorDef {
    vector_size: usize,
    metric: spec::VectorSimilarityMetric,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetupState {
    #[serde(default)]
    vectors: BTreeMap<String, VectorDef>,
}

#[derive(Debug)]
struct SetupStatus {
    delete_collection: bool,
    add_collection: Option<SetupState>,
}

impl setup::ResourceSetupStatus for SetupStatus {
    fn describe_changes(&self) -> Vec<String> {
        let mut result = vec![];
        if self.delete_collection {
            result.push("Delete collection".to_string());
        }
        if let Some(add_collection) = &self.add_collection {
            let vector_descriptions = add_collection
                .vectors
                .iter()
                .map(|(name, vector_def)| {
                    format!(
                        "{}[{}], {}",
                        name, vector_def.vector_size, vector_def.metric
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            result.push(format!(
                "Create collection{}",
                if vector_descriptions.is_empty() {
                    "".to_string()
                } else {
                    format!(" with vectors: {vector_descriptions}")
                }
            ));
        }
        result
    }

    fn change_type(&self) -> setup::SetupChangeType {
        match (self.delete_collection, self.add_collection.is_some()) {
            (false, false) => setup::SetupChangeType::NoChange,
            (false, true) => setup::SetupChangeType::Create,
            (true, false) => setup::SetupChangeType::Delete,
            (true, true) => setup::SetupChangeType::Update,
        }
    }
}

impl SetupStatus {
    async fn apply_delete(&self, collection_name: &String, qdrant_client: &Qdrant) -> Result<()> {
        if self.delete_collection {
            qdrant_client.delete_collection(collection_name).await?;
        }
        Ok(())
    }

    async fn apply_create(&self, collection_name: &String, qdrant_client: &Qdrant) -> Result<()> {
        if let Some(add_collection) = &self.add_collection {
            let mut builder = CreateCollectionBuilder::new(collection_name);
            if !add_collection.vectors.is_empty() {
                let mut vectors_config = VectorsConfigBuilder::default();
                for (name, vector_def) in add_collection.vectors.iter() {
                    let params = VectorParamsBuilder::new(
                        vector_def.vector_size as u64,
                        embedding_metric_to_qdrant(vector_def.metric)?,
                    );
                    vectors_config.add_named_vector_params(name, params);
                }
                builder = builder.vectors_config(vectors_config);
            }
            qdrant_client.create_collection(builder).await?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////
// Deal with mutations
////////////////////////////////////////////////////////////

struct ExportContext {
    qdrant_client: Arc<Qdrant>,
    collection_name: String,
    fields_info: Vec<FieldInfo>,
}

impl ExportContext {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let mut points: Vec<PointStruct> = Vec::with_capacity(mutation.upserts.len());
        for upsert in mutation.upserts.iter() {
            let point_id = key_to_point_id(&upsert.key)?;
            let (payload, vectors) = values_to_payload(&upsert.value.fields, &self.fields_info)?;

            points.push(PointStruct::new(point_id, vectors, payload));
        }

        if !points.is_empty() {
            self.qdrant_client
                .upsert_points(UpsertPointsBuilder::new(&self.collection_name, points).wait(true))
                .await?;
        }

        let ids = mutation
            .deletes
            .iter()
            .map(|deletion| key_to_point_id(&deletion.key))
            .collect::<Result<Vec<_>>>()?;

        if !ids.is_empty() {
            self.qdrant_client
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
    fields_info: &[FieldInfo],
) -> Result<(HashMap<String, QdrantValue>, NamedVectors)> {
    let mut payload = HashMap::with_capacity(value_fields.len());
    let mut vectors = NamedVectors::default();

    for (value, field_info) in value_fields.iter().zip(fields_info.iter()) {
        let field_name = &field_info.field_schema.name;
        match value {
            Value::Basic(BasicValue::Vector(v)) if field_info.is_qdrant_vector => {
                let vector = encode_vector(v.as_ref())?;
                vectors = vectors.add_vector(field_name, vector);
            }
            v => {
                let json_value = serde_json::to_value(TypedValue {
                    t: &field_info.field_schema.value_type.typ,
                    v,
                })?;
                payload.insert(field_name.clone(), json_value.into());
            }
        }
    }

    Ok((payload, vectors))
}

////////////////////////////////////////////////////////////
// Factory implementation
////////////////////////////////////////////////////////////

#[derive(Default)]
struct Factory {
    qdrant_clients: Mutex<HashMap<Option<spec::AuthEntryReference<ConnectionSpec>>, Arc<Qdrant>>>,
}

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
    type SetupState = SetupState;
    type SetupStatus = SetupStatus;
    type Key = CollectionKey;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "Qdrant"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        _declarations: Vec<()>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(CollectionKey, SetupState)>,
    )> {
        // Hotfix to resolve
        // `no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point`
        // when using HTTPS URLs.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let data_coll_output = data_collections
            .into_iter()
            .map(|d| {
                if d.key_fields_schema.len() != 1 {
                    api_bail!(
                        "Expected one primary key field for the point ID. Got {}.",
                        d.key_fields_schema.len()
                    )
                }

                let mut fields_info = Vec::<FieldInfo>::new();
                let mut vector_def = BTreeMap::<String, VectorDef>::new();

                for field in d.value_fields_schema.iter() {
                    let vector_size = parse_supported_vector_size(&field.value_type.typ);
                    fields_info.push(FieldInfo {
                        field_schema: field.clone(),
                        is_qdrant_vector: vector_size.is_some(),
                    });
                    if let Some(vector_size) = vector_size {
                        vector_def.insert(
                            field.name.clone(),
                            VectorDef {
                                vector_size,
                                metric: DEFAULT_VECTOR_SIMILARITY_METRIC,
                            },
                        );
                    }
                }

                let mut specified_vector_fields = HashSet::new();
                for vector_index in d.index_options.vector_indexes {
                    match vector_def.get_mut(&vector_index.field_name) {
                        Some(vector_def) => {
                            if specified_vector_fields.insert(vector_index.field_name.clone()) {
                                // Validate the metric is supported by Qdrant
                                embedding_metric_to_qdrant(vector_index.metric)
                                    .with_context(||
                                        format!("Parsing vector index metric {} for field `{}`", vector_index.metric, vector_index.field_name))?;
                                vector_def.metric = vector_index.metric;
                            } else {
                                api_bail!("Field `{}` specified more than once in vector index definition", vector_index.field_name);
                            }
                        }
                        None => {
                            if let Some(field) = d.value_fields_schema.iter().find(|f| f.name == vector_index.field_name) {
                                api_bail!(
                                    "Field `{}` specified in vector index is expected to be a number vector with fixed size, actual type: {}",
                                    vector_index.field_name, field.value_type.typ
                                );
                            } else {
                                api_bail!("Field `{}` specified in vector index is not found", vector_index.field_name);
                            }
                        }
                    }
                }

                let export_context = Arc::new(ExportContext {
                    qdrant_client: self
                        .get_qdrant_client(&d.spec.connection, &context.auth_registry)?,
                    collection_name: d.spec.collection_name.clone(),
                    fields_info,
                });
                Ok(TypedExportDataCollectionBuildOutput {
                    export_context: Box::pin(async move { Ok(export_context) }),
                    setup_key: CollectionKey {
                        connection: d.spec.connection,
                        collection_name: d.spec.collection_name,
                    },
                    desired_setup_state: SetupState {
                        vectors: vector_def,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, vec![]))
    }

    fn deserialize_setup_key(key: serde_json::Value) -> Result<CollectionKey> {
        Ok(match key {
            serde_json::Value::String(s) => {
                // For backward compatibility.
                CollectionKey {
                    collection_name: s,
                    connection: None,
                }
            }
            _ => serde_json::from_value(key)?,
        })
    }

    async fn check_setup_status(
        &self,
        _key: CollectionKey,
        desired: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
        _auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Self::SetupStatus> {
        let desired_exists = desired.is_some();
        let add_collection = desired
            .filter(|state| {
                !existing.always_exists()
                    || existing
                        .possible_versions()
                        .any(|v| v.vectors != state.vectors)
            })
            .map(|state| SetupState {
                vectors: state.vectors,
            });
        let delete_collection = existing.possible_versions().next().is_some()
            && (!desired_exists || add_collection.is_some());
        Ok(SetupStatus {
            delete_collection,
            add_collection,
        })
    }

    fn check_state_compatibility(
        &self,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<SetupStateCompatibility> {
        Ok(if desired.vectors == existing.vectors {
            SetupStateCompatibility::Compatible
        } else {
            SetupStateCompatibility::NotCompatible
        })
    }

    fn describe_resource(&self, key: &CollectionKey) -> Result<String> {
        Ok(format!(
            "Qdrant collection {}{}",
            key.collection_name,
            key.connection
                .as_ref()
                .map_or_else(|| "".to_string(), |auth_entry| format!(" @ {}", auth_entry))
        ))
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
        setup_status: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()> {
        for setup_change in setup_status.iter() {
            let qdrant_client =
                self.get_qdrant_client(&setup_change.key.connection, auth_registry)?;
            setup_change
                .setup_status
                .apply_delete(&setup_change.key.collection_name, &qdrant_client)
                .await?;
        }
        for setup_change in setup_status.iter() {
            let qdrant_client =
                self.get_qdrant_client(&setup_change.key.connection, auth_registry)?;
            setup_change
                .setup_status
                .apply_create(&setup_change.key.collection_name, &qdrant_client)
                .await?;
        }
        Ok(())
    }
}

impl Factory {
    fn new() -> Self {
        Self {
            qdrant_clients: Mutex::new(HashMap::new()),
        }
    }

    fn get_qdrant_client(
        &self,
        auth_entry: &Option<spec::AuthEntryReference<ConnectionSpec>>,
        auth_registry: &AuthRegistry,
    ) -> Result<Arc<Qdrant>> {
        let mut clients = self.qdrant_clients.lock().unwrap();
        if let Some(client) = clients.get(auth_entry) {
            return Ok(client.clone());
        }

        let spec = auth_entry.as_ref().map_or_else(
            || {
                Ok(ConnectionSpec {
                    grpc_url: DEFAULT_URL.to_string(),
                    api_key: None,
                })
            },
            |auth_entry| auth_registry.get(auth_entry),
        )?;
        let client = Arc::new(
            Qdrant::from_url(&spec.grpc_url)
                .api_key(spec.api_key)
                .skip_compatibility_check()
                .build()?,
        );
        clients.insert(auth_entry.clone(), client.clone());
        Ok(client)
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory::new().register(registry)
}
