use crate::ops::sdk::*;
use crate::prelude::*;

use super::shared::table_columns::{
    TableColumnsSchema, TableMainSetupAction, TableUpsertionAction, check_table_compatibility,
};
use crate::ops::registry::ExecutorFactoryRegistry;
use crate::setup;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use lancedb::connect as lancedb_connect;
use std::sync::Arc as StdArc;

use arrow_array::{
    Array as ArrowArray, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

////////////////////////////////////////////////////////////
// Public Types
////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    /// LanceDB connection URI, e.g. file:///tmp/lancedb or db://...
    uri: String,
}

#[derive(Debug, Deserialize, Clone)]
struct Spec {
    connection: Option<spec::AuthEntryReference<ConnectionSpec>>,
    /// Table/collection name within the database
    table_name: String,
}

const DEFAULT_URI: &str = "file:./lancedb";

////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////

struct FieldInfo {
    field_schema: schema::FieldSchema,
    vector_shape: Option<VectorShape>,
}

enum VectorShape {
    Vector(usize),
}

impl VectorShape {
    fn vector_size(&self) -> usize {
        match self {
            VectorShape::Vector(size) => *size,
        }
    }
}

fn parse_vector_schema_shape(vector_schema: &schema::VectorTypeSchema) -> Option<VectorShape> {
    match &*vector_schema.element_type {
        schema::BasicValueType::Float32
        | schema::BasicValueType::Float64
        | schema::BasicValueType::Int64 => vector_schema.dimension.map(VectorShape::Vector),

        // Nested vectors (multi-vector) are not supported in LanceDB target for now.
        schema::BasicValueType::Vector(_nested_vector_schema) => None,
        _ => None,
    }
}

fn parse_vector_shape(typ: &schema::ValueType) -> Option<VectorShape> {
    match typ {
        schema::ValueType::Basic(schema::BasicValueType::Vector(vector_schema)) => {
            parse_vector_schema_shape(vector_schema)
        }
        _ => None,
    }
}

////////////////////////////////////////////////////////////
// Setup
////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct TableKey {
    connection: Option<spec::AuthEntryReference<ConnectionSpec>>,
    table_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct VectorDef {
    vector_size: usize,
    /// Stored for visibility; LanceDB will choose metric at query time typically
    metric: spec::VectorSimilarityMetric,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetupState {
    #[serde(flatten)]
    columns: TableColumnsSchema<ValueType>,

    #[serde(default)]
    vectors: BTreeMap<String, VectorDef>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    unsupported_vector_fields: Vec<(String, ValueType)>,
}

#[derive(Debug)]
struct SetupChange {
    actions: TableMainSetupAction<String>,
    desired_schema: Option<TableColumnsSchema<ValueType>>,
}

impl setup::ResourceSetupChange for SetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
        self.actions.describe_changes()
    }

    fn change_type(&self) -> setup::SetupChangeType {
        self.actions.change_type(false)
    }
}

impl SetupState {
    fn new(key_fields_schema: &[FieldSchema], value_fields_schema: &[FieldSchema]) -> Self {
        Self {
            columns: TableColumnsSchema {
                key_columns: key_fields_schema
                    .iter()
                    .map(|f| (f.name.clone(), f.value_type.typ.without_attrs()))
                    .collect(),
                value_columns: value_fields_schema
                    .iter()
                    .map(|f| (f.name.clone(), f.value_type.typ.without_attrs()))
                    .collect(),
            },
            vectors: Default::default(),
            unsupported_vector_fields: Default::default(),
        }
    }
}

fn to_column_type_str(column_type: &ValueType) -> String {
    match column_type {
        ValueType::Basic(basic_type) => match basic_type {
            BasicValueType::Bytes => "Binary".into(),
            BasicValueType::Str => "Utf8".into(),
            BasicValueType::Bool => "Boolean".into(),
            BasicValueType::Int64 => "Int64".into(),
            BasicValueType::Float32 => "Float32".into(),
            BasicValueType::Float64 => "Float64".into(),
            BasicValueType::Range => "Utf8".into(),
            BasicValueType::Uuid => "Utf8".into(),
            BasicValueType::Date => "Utf8".into(),
            BasicValueType::Time => "Utf8".into(),
            BasicValueType::LocalDateTime => "Utf8".into(),
            BasicValueType::OffsetDateTime => "Utf8".into(),
            BasicValueType::TimeDelta => "Utf8".into(),
            BasicValueType::Json => "Utf8".into(),
            BasicValueType::Vector(vec_schema) => {
                if let Some(VectorShape::Vector(dim)) = parse_vector_schema_shape(vec_schema) {
                    format!("FixedSizeList(Float32,{})", dim)
                } else {
                    "Utf8".into()
                }
            }
            BasicValueType::Union(_) => "Utf8".into(),
        },
        _ => "Utf8".into(),
    }
}

impl<'a> From<&'a SetupState> for Cow<'a, TableColumnsSchema<String>> {
    fn from(val: &'a SetupState) -> Self {
        Cow::Owned(TableColumnsSchema {
            key_columns: val
                .columns
                .key_columns
                .iter()
                .map(|(k, v)| (k.clone(), to_column_type_str(v)))
                .collect(),
            value_columns: val
                .columns
                .value_columns
                .iter()
                .map(|(k, v)| (k.clone(), to_column_type_str(v)))
                .collect(),
        })
    }
}

impl SetupChange {
    fn new(desired: Option<SetupState>, existing: setup::CombinedState<SetupState>) -> Self {
        let actions = TableMainSetupAction::from_states(desired.as_ref(), &existing, false);
        let desired_schema = desired.as_ref().map(|s| s.columns.clone());
        Self {
            actions,
            desired_schema,
        }
    }
}

////////////////////////////////////////////////////////////
// Export Context & Mutation Handling
////////////////////////////////////////////////////////////

struct ExportContext {
    connection_uri: String,
    table_name: String,
    key_fields_schema: Vec<FieldSchema>,
    fields_info: Vec<FieldInfo>,
}

impl ExportContext {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        if mutation.upserts.is_empty() && mutation.deletes.is_empty() {
            return Ok(());
        }

        let db = lancedb_connect(&self.connection_uri).execute().await?;

        // Build Arrow schema from known fields (all key columns + value columns)
        let mut arrow_fields =
            Vec::with_capacity(self.key_fields_schema.len() + self.fields_info.len());
        for key_field in self.key_fields_schema.iter() {
            arrow_fields.push(ArrowField::new(
                key_field.name.clone(),
                arrow_type_for_key_field(key_field),
                false,
            ));
        }
        for info in self.fields_info.iter() {
            let (dt, nullable) = arrow_type_for_value_field(&info.field_schema, &info.vector_shape);
            arrow_fields.push(ArrowField::new(
                info.field_schema.name.clone(),
                dt,
                nullable,
            ));
        }
        let arrow_schema = StdArc::new(ArrowSchema::new(arrow_fields));

        // Ensure table exists
        let table = match db.open_table(&self.table_name).execute().await {
            Ok(t) => t,
            Err(_) => {
                // Create empty table with the schema by inserting zero rows
                let empty_columns: Vec<StdArc<dyn ArrowArray>> = build_empty_columns(
                    &arrow_schema,
                    0,
                    &self.key_fields_schema,
                    &self.fields_info,
                )?;
                let empty_batch = RecordBatch::try_new(arrow_schema.clone(), empty_columns)?;
                let batches = RecordBatchIterator::new(
                    vec![empty_batch].into_iter().map(Ok),
                    arrow_schema.clone(),
                );
                db.create_table(&self.table_name, Box::new(batches))
                    .execute()
                    .await?
            }
        };

        // Apply deletions first
        if !mutation.deletes.is_empty() {
            let predicate = build_delete_predicate(&self.key_fields_schema, &mutation.deletes)?;
            if !predicate.is_empty() {
                table.delete(&predicate).await?;
            }
        }

        // Apply upserts as delete-then-insert for provided keys
        if !mutation.upserts.is_empty() {
            let delete_for_upserts =
                build_delete_predicate_from_upserts(&self.key_fields_schema, &mutation.upserts)?;
            if !delete_for_upserts.is_empty() {
                table.delete(&delete_for_upserts).await?;
            }

            let (schema, columns) = build_batch_for_upserts(
                arrow_schema.clone(),
                &self.key_fields_schema,
                &self.fields_info,
                &mutation.upserts,
            )?;
            let batch = RecordBatch::try_new(schema, columns)?;
            // LanceDB uses add/insert to append rows. We pass an iterator of batches.
            let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), arrow_schema);
            table.add(Box::new(reader)).execute().await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////
// Factory
////////////////////////////////////////////////////////////

#[derive(Default)]
struct Factory {}

#[async_trait]
impl TargetFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = ();
    type SetupState = SetupState;
    type SetupChange = SetupChange;
    type SetupKey = TableKey;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "LanceDB"
    }

    async fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        _declarations: Vec<()>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(Self::SetupKey, Self::SetupState)>,
    )> {
        let data_coll_output = data_collections
            .into_iter()
            .map(|d| {
                // Support one or more key fields

                let mut fields_info = Vec::<FieldInfo>::new();
                let mut vector_def = BTreeMap::<String, VectorDef>::new();
                let mut unsupported_vector_fields = Vec::<(String, ValueType)>::new();

                for field in d.value_fields_schema.iter() {
                    let vector_shape = parse_vector_shape(&field.value_type.typ);
                    if let Some(vector_shape) = &vector_shape {
                        vector_def.insert(
                            field.name.clone(),
                            VectorDef {
                                vector_size: vector_shape.vector_size(),
                                metric: spec::VectorSimilarityMetric::CosineSimilarity,
                            },
                        );
                    } else if matches!(
                        &field.value_type.typ,
                        schema::ValueType::Basic(schema::BasicValueType::Vector(_))
                    ) {
                        // This is a vector field but not supported by current LanceDB target shape
                        unsupported_vector_fields.push((field.name.clone(), field.value_type.typ.clone()));
                    }
                    fields_info.push(FieldInfo {
                        field_schema: field.clone(),
                        vector_shape,
                    });
                }

                for vector_index in d.index_options.vector_indexes {
                    if let Some(def) = vector_def.get_mut(&vector_index.field_name) {
                        def.metric = vector_index.metric;
                    } else if let Some(field) = d.value_fields_schema.iter().find(|f| f.name == vector_index.field_name) {
                        api_bail!(
                            "Field `{}` specified in vector index is expected to be a number vector with fixed size, actual type: {}",
                            vector_index.field_name, field.value_type.typ
                        );
                    } else {
                        api_bail!("Field `{}` specified in vector index is not found", vector_index.field_name);
                    }
                }

                let connection_uri = match d.spec.connection.as_ref() {
                    Some(auth_entry) => context.auth_registry.get(auth_entry)?.uri,
                    None => DEFAULT_URI.to_string(),
                };
                let export_context = Arc::new(ExportContext {
                    connection_uri,
                    table_name: d.spec.table_name.clone(),
                    key_fields_schema: d.key_fields_schema.clone(),
                    fields_info,
                });

                let desired_setup_state = SetupState::new(&d.key_fields_schema, &d.value_fields_schema);

                Ok(TypedExportDataCollectionBuildOutput {
                    export_context: Box::pin(async move { Ok(export_context) }),
                    setup_key: TableKey {
                        connection: d.spec.connection,
                        table_name: d.spec.table_name,
                    },
                    desired_setup_state,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, vec![]))
    }

    async fn diff_setup_states(
        &self,
        _key: TableKey,
        desired: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
        _flow_instance_ctx: Arc<FlowInstanceContext>,
    ) -> Result<Self::SetupChange> {
        Ok(SetupChange::new(desired, existing))
    }

    fn check_state_compatibility(
        &self,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<SetupStateCompatibility> {
        Ok(check_table_compatibility(
            &desired.columns,
            &existing.columns,
        ))
    }

    fn describe_resource(&self, key: &TableKey) -> Result<String> {
        Ok(format!(
            "LanceDB table {}{}",
            key.table_name,
            key.connection
                .as_ref()
                .map_or_else(|| "".to_string(), |auth| format!(" @ {}", auth))
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
        changes: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()> {
        for change in changes.iter() {
            let conn_spec = change
                .key
                .connection
                .as_ref()
                .map(|r| context.auth_registry.get(r))
                .transpose()?;
            let uri = conn_spec
                .map(|c| c.uri)
                .unwrap_or_else(|| DEFAULT_URI.to_string());
            let db = lancedb_connect(&uri).execute().await?;

            // Drop existing table if needed
            if change.setup_change.actions.drop_existing {
                // Best-effort drop; ignore not found
                let _ = db.drop_table(&change.key.table_name, &[]).await;
            }

            // Create or update columns using desired schema (LanceDB lacks ALTER, so drop+create)
            if let Some(desired) = &change.setup_change.desired_schema {
                let mut fields: Vec<ArrowField> = Vec::new();
                for (name, typ) in desired.key_columns.iter() {
                    fields.push(ArrowField::new(
                        name.clone(),
                        arrow_type_for_key_field(&FieldSchema::new(
                            name.clone(),
                            EnrichedValueType {
                                typ: typ.clone(),
                                nullable: false,
                                attrs: Default::default(),
                            },
                        )),
                        false,
                    ));
                }
                for (name, typ) in desired.value_columns.iter() {
                    let (dt, nullable) = arrow_type_for_value_field(
                        &FieldSchema::new(
                            name.clone(),
                            EnrichedValueType {
                                typ: typ.clone(),
                                nullable: true,
                                attrs: Default::default(),
                            },
                        ),
                        &parse_vector_shape(typ),
                    );
                    fields.push(ArrowField::new(name.clone(), dt, nullable));
                }
                let schema = StdArc::new(ArrowSchema::new(fields));
                let empty_columns: Vec<StdArc<dyn ArrowArray>> =
                    build_empty_columns(&schema, 0, &[], &[])?;
                let empty_batch = RecordBatch::try_new(schema.clone(), empty_columns)?;
                let batches =
                    RecordBatchIterator::new(vec![empty_batch].into_iter().map(Ok), schema.clone());
                db.create_table(&change.key.table_name, Box::new(batches))
                    .execute()
                    .await?;
            }
        }
        Ok(())
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory::default().register(registry)
}

////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////

fn arrow_type_for_key_field(field: &schema::FieldSchema) -> ArrowDataType {
    match &field.value_type.typ {
        schema::ValueType::Basic(b) => match b {
            schema::BasicValueType::Str => ArrowDataType::Utf8,
            schema::BasicValueType::Int64 => ArrowDataType::Int64,
            schema::BasicValueType::Uuid => ArrowDataType::Utf8,
            _ => ArrowDataType::Utf8,
        },
        _ => ArrowDataType::Utf8,
    }
}

fn arrow_type_for_value_field(
    field: &schema::FieldSchema,
    vector_shape: &Option<VectorShape>,
) -> (ArrowDataType, bool) {
    if let Some(VectorShape::Vector(dim)) = vector_shape {
        let item = StdArc::new(ArrowField::new("item", ArrowDataType::Float32, true));
        return (
            ArrowDataType::FixedSizeList(item, *dim as i32),
            field.value_type.nullable,
        );
    }
    match &field.value_type.typ {
        schema::ValueType::Basic(b) => {
            let dt = match b {
                schema::BasicValueType::Bytes => ArrowDataType::Binary,
                schema::BasicValueType::Str => ArrowDataType::Utf8,
                schema::BasicValueType::Bool => ArrowDataType::Boolean,
                schema::BasicValueType::Int64 => ArrowDataType::Int64,
                schema::BasicValueType::Float32 => ArrowDataType::Float32,
                schema::BasicValueType::Float64 => ArrowDataType::Float64,
                schema::BasicValueType::Range => ArrowDataType::Utf8,
                schema::BasicValueType::Uuid => ArrowDataType::Utf8,
                schema::BasicValueType::Date => ArrowDataType::Utf8,
                schema::BasicValueType::Time => ArrowDataType::Utf8,
                schema::BasicValueType::LocalDateTime => ArrowDataType::Utf8,
                schema::BasicValueType::OffsetDateTime => ArrowDataType::Utf8,
                schema::BasicValueType::TimeDelta => ArrowDataType::Utf8,
                schema::BasicValueType::Json => ArrowDataType::Utf8,
                schema::BasicValueType::Vector(_) => ArrowDataType::Utf8,
                schema::BasicValueType::Union(_) => ArrowDataType::Utf8,
            };
            (dt, field.value_type.nullable)
        }
        _ => (ArrowDataType::Utf8, true),
    }
}

fn build_delete_predicate(
    key_schema: &[schema::FieldSchema],
    deletions: &[interface::ExportTargetDeleteEntry],
) -> Result<String> {
    if deletions.is_empty() {
        return Ok(String::new());
    }
    let mut disjuncts = Vec::with_capacity(deletions.len());
    for d in deletions.iter() {
        let conjuncts = key_schema
            .iter()
            .zip(d.key.iter())
            .map(|(field, part)| {
                Ok(format!(
                    "{} = {}",
                    field.name,
                    build_scalar_predicate_value(part)?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        disjuncts.push(format!("({})", conjuncts.join(" AND ")));
    }
    Ok(disjuncts.join(" OR "))
}

fn build_delete_predicate_from_upserts(
    key_schema: &[schema::FieldSchema],
    upserts: &[interface::ExportTargetUpsertEntry],
) -> Result<String> {
    if upserts.is_empty() {
        return Ok(String::new());
    }
    let mut disjuncts = Vec::with_capacity(upserts.len());
    for u in upserts.iter() {
        let conjuncts = key_schema
            .iter()
            .zip(u.key.iter())
            .map(|(field, part)| {
                Ok(format!(
                    "{} = {}",
                    field.name,
                    build_scalar_predicate_value(part)?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        disjuncts.push(format!("({})", conjuncts.join(" AND ")));
    }
    Ok(disjuncts.join(" OR "))
}

fn build_scalar_predicate_value(key_part: &KeyPart) -> Result<String> {
    let s = match key_part {
        KeyPart::Str(s) => format!("'{}'", s.replace("'", "''")),
        KeyPart::Int64(i) => i.to_string(),
        KeyPart::Uuid(u) => format!("'{}'", u),
        KeyPart::Bytes(b) => format!("'{}'", B64.encode(b)),
        KeyPart::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        KeyPart::Range(r) => format!("'[{}, {})'", r.start, r.end),
        KeyPart::Date(d) => format!("'{}'", d),
        KeyPart::Struct(_) => anyhow::bail!("composite key not supported for LanceDB"),
    };
    Ok(s)
}

fn build_empty_columns(
    arrow_schema: &StdArc<ArrowSchema>,
    num_rows: usize,
    key_schema: &[schema::FieldSchema],
    fields_info: &[FieldInfo],
) -> Result<Vec<StdArc<dyn ArrowArray>>> {
    let mut arrays: Vec<StdArc<dyn ArrowArray>> =
        Vec::with_capacity(key_schema.len() + fields_info.len());
    for idx in 0..key_schema.len() {
        arrays.push(build_empty_array(
            &arrow_schema.fields()[idx].data_type().clone(),
            num_rows,
        ));
    }
    for (idx, _info) in fields_info.iter().enumerate() {
        let dt = arrow_schema.fields()[key_schema.len() + idx]
            .data_type()
            .clone();
        arrays.push(build_empty_array(&dt, num_rows));
    }
    Ok(arrays)
}

fn build_empty_array(dt: &ArrowDataType, len: usize) -> StdArc<dyn ArrowArray> {
    match dt {
        ArrowDataType::Utf8 => StdArc::new(StringArray::from_iter(
            std::iter::repeat(None::<&str>).take(len),
        )),
        ArrowDataType::Boolean => StdArc::new(BooleanArray::from_iter(
            std::iter::repeat(None::<bool>).take(len),
        )),
        ArrowDataType::Int64 => StdArc::new(Int64Array::from_iter(
            std::iter::repeat(None::<i64>).take(len),
        )),
        ArrowDataType::Binary => StdArc::new(BinaryArray::from_iter(
            std::iter::repeat(None::<&[u8]>).take(len),
        )),
        ArrowDataType::Float32 => StdArc::new(Float32Array::from_iter(
            std::iter::repeat(None::<f32>).take(len),
        )),
        ArrowDataType::Float64 => StdArc::new(Float64Array::from_iter(
            std::iter::repeat(None::<f64>).take(len),
        )),
        ArrowDataType::FixedSizeList(item, dim) => {
            let dim_i32 = *dim;
            let dim_usize = dim_i32 as usize;
            // Build inner values array matching the expected child field type
            let inner: StdArc<dyn ArrowArray> = match item.data_type() {
                ArrowDataType::Float32 => StdArc::new(Float32Array::from_iter(
                    std::iter::repeat(None::<f32>).take(len * dim_usize),
                )),
                ArrowDataType::Float64 => StdArc::new(Float64Array::from_iter(
                    std::iter::repeat(None::<f64>).take(len * dim_usize),
                )),
                _ => StdArc::new(Float32Array::from_iter(
                    std::iter::repeat(None::<f32>).take(len * dim_usize),
                )),
            };
            let list = FixedSizeListArray::try_new(item.clone(), dim_i32, inner, None).unwrap();
            StdArc::new(list)
        }
        _ => StdArc::new(StringArray::from_iter(
            std::iter::repeat(None::<&str>).take(len),
        )),
    }
}

fn build_batch_for_upserts(
    schema: StdArc<ArrowSchema>,
    key_schema: &[schema::FieldSchema],
    fields_info: &[FieldInfo],
    upserts: &[interface::ExportTargetUpsertEntry],
) -> Result<(StdArc<ArrowSchema>, Vec<StdArc<dyn ArrowArray>>)> {
    let _num_rows = upserts.len();

    // Key columns
    let mut columns: Vec<StdArc<dyn ArrowArray>> =
        Vec::with_capacity(key_schema.len() + fields_info.len());
    for (key_idx, kf) in key_schema.iter().enumerate() {
        let _dt = schema.fields()[key_idx].data_type().clone();
        let key_array: StdArc<dyn ArrowArray> = match &kf.value_type.typ {
            schema::ValueType::Basic(schema::BasicValueType::Str) => {
                let it = upserts.iter().map(|u| match &u.key[key_idx] {
                    KeyPart::Str(s) => Some(s.as_ref().to_string()),
                    other => Some(other.to_string()),
                });
                StdArc::new(StringArray::from_iter(it))
            }
            schema::ValueType::Basic(schema::BasicValueType::Int64) => {
                let it = upserts.iter().map(|u| match &u.key[key_idx] {
                    KeyPart::Int64(i) => Some(*i),
                    _ => None,
                });
                StdArc::new(Int64Array::from_iter(it))
            }
            _ => {
                let it = upserts.iter().map(|u| Some(u.key[key_idx].to_string()));
                StdArc::new(StringArray::from_iter(it))
            }
        };
        columns.push(key_array);
    }

    // Value columns
    for (field_idx, field_info) in fields_info.iter().enumerate() {
        let arr = build_value_array(
            &schema.fields()[key_schema.len() + field_idx]
                .data_type()
                .clone(),
            field_idx,
            field_info,
            upserts,
        )?;
        columns.push(arr);
    }

    Ok((schema, columns))
}

fn build_value_array(
    dt: &ArrowDataType,
    value_index: usize,
    field_info: &FieldInfo,
    upserts: &[interface::ExportTargetUpsertEntry],
) -> Result<StdArc<dyn ArrowArray>> {
    match &field_info.vector_shape {
        Some(VectorShape::Vector(dim)) => {
            let dim = *dim;
            let mut flat: Vec<Option<f32>> = Vec::with_capacity(upserts.len() * dim);
            for u in upserts.iter() {
                let v = &u.value.fields[value_index];
                if let Value::Basic(BasicValue::Vector(vec)) = v {
                    let mut count = 0;
                    for elem in vec.iter() {
                        let val = match elem {
                            BasicValue::Float32(f) => Some(*f),
                            BasicValue::Float64(f) => Some(*f as f32),
                            BasicValue::Int64(i) => Some(*i as f32),
                            _ => None,
                        };
                        flat.push(val);
                        count += 1;
                    }
                    while count < dim {
                        flat.push(None);
                        count += 1;
                    }
                } else {
                    for _ in 0..dim {
                        flat.push(None);
                    }
                }
            }
            let inner = StdArc::new(Float32Array::from(flat)) as StdArc<dyn ArrowArray>;
            let field = StdArc::new(ArrowField::new("item", ArrowDataType::Float32, true));
            let list = FixedSizeListArray::try_new(field, dim as i32, inner, None)?;
            Ok(StdArc::new(list))
        }
        None => match dt {
            ArrowDataType::Utf8 => {
                let it = upserts.iter().map(|u| {
                    let v = &u.value.fields[value_index];
                    Some(
                        serde_json::to_string(&TypedValue {
                            t: &field_info.field_schema.value_type.typ,
                            v,
                        })
                        .unwrap_or_default(),
                    )
                });
                Ok(StdArc::new(StringArray::from_iter(it)))
            }
            ArrowDataType::Boolean => {
                let it = upserts.iter().map(|u| match &u.value.fields[value_index] {
                    Value::Basic(BasicValue::Bool(b)) => Some(*b),
                    _ => None,
                });
                Ok(StdArc::new(BooleanArray::from_iter(it)))
            }
            ArrowDataType::Int64 => {
                let it = upserts.iter().map(|u| match &u.value.fields[value_index] {
                    Value::Basic(BasicValue::Int64(i)) => Some(*i),
                    _ => None,
                });
                Ok(StdArc::new(Int64Array::from_iter(it)))
            }
            ArrowDataType::Binary => {
                let it = upserts.iter().map(|u| match &u.value.fields[value_index] {
                    Value::Basic(BasicValue::Bytes(b)) => Some(&b[..]),
                    _ => None,
                });
                Ok(StdArc::new(BinaryArray::from_iter(it)))
            }
            ArrowDataType::FixedSizeList(item, dim) => {
                let dim_i32 = *dim;
                let dim_usize = dim_i32 as usize;
                let inner: StdArc<dyn ArrowArray> = match item.data_type() {
                    ArrowDataType::Float32 => StdArc::new(Float32Array::from_iter(
                        std::iter::repeat(None::<f32>).take(upserts.len() * dim_usize),
                    )),
                    ArrowDataType::Float64 => StdArc::new(Float64Array::from_iter(
                        std::iter::repeat(None::<f64>).take(upserts.len() * dim_usize),
                    )),
                    _ => StdArc::new(Float32Array::from_iter(
                        std::iter::repeat(None::<f32>).take(upserts.len() * dim_usize),
                    )),
                };
                let list = FixedSizeListArray::try_new(item.clone(), dim_i32, inner, None)?;
                Ok(StdArc::new(list))
            }
            ArrowDataType::Float32 => {
                let it = upserts.iter().map(|u| match &u.value.fields[value_index] {
                    Value::Basic(BasicValue::Float32(f)) => Some(*f),
                    _ => None,
                });
                Ok(StdArc::new(Float32Array::from_iter(it)))
            }
            ArrowDataType::Float64 => {
                let it = upserts.iter().map(|u| match &u.value.fields[value_index] {
                    Value::Basic(BasicValue::Float64(f)) => Some(*f),
                    _ => None,
                });
                Ok(StdArc::new(Float64Array::from_iter(it)))
            }
            _ => {
                let it = upserts.iter().map(|_| None::<&str>);
                Ok(StdArc::new(StringArray::from_iter(it)))
            }
        },
    }
}
