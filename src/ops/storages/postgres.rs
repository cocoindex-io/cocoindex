use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::base::spec::{self, *};
use crate::ops::sdk::*;
use crate::service::error::{shared_ok, SharedError, SharedResultExt};
use crate::utils::db::ValidIdentifier;
use crate::{get_lib_context, setup};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::future::Shared;
use futures::FutureExt;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use sqlx::postgres::types::PgRange;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

#[derive(Debug, Deserialize)]
pub struct Spec {
    database_url: Option<String>,
    table_name: Option<String>,
}
const BIND_LIMIT: usize = 65535;

fn key_value_fields_iter<'a>(
    key_fields_schema: &[FieldSchema],
    key_value: &'a KeyValue,
) -> Result<&'a [KeyValue]> {
    let slice = if key_fields_schema.len() == 1 {
        std::slice::from_ref(key_value)
    } else {
        match key_value {
            KeyValue::Struct(fields) => fields,
            _ => bail!("expect struct key value"),
        }
    };
    Ok(slice)
}

fn convertible_to_pgvector(vec_schema: &VectorTypeSchema) -> bool {
    if vec_schema.dimension.is_some() {
        match &*vec_schema.element_type {
            BasicValueType::Float32 => true,
            BasicValueType::Float64 => true,
            BasicValueType::Int64 => true,
            _ => false,
        }
    } else {
        false
    }
}

fn bind_key_field<'arg>(
    builder: &mut sqlx::QueryBuilder<'arg, sqlx::Postgres>,
    key_value: &'arg KeyValue,
) -> Result<()> {
    match key_value {
        KeyValue::Bytes(v) => {
            builder.push_bind(&**v);
        }
        KeyValue::Str(v) => {
            builder.push_bind(&**v);
        }
        KeyValue::Bool(v) => {
            builder.push_bind(v);
        }
        KeyValue::Int64(v) => {
            builder.push_bind(v);
        }
        KeyValue::Range(v) => {
            builder.push_bind(PgRange {
                start: Bound::Included(v.start as i64),
                end: Bound::Excluded(v.end as i64),
            });
        }
        KeyValue::Struct(fields) => {
            builder.push_bind(sqlx::types::Json(fields));
        }
    }
    Ok(())
}

fn bind_value_field<'arg>(
    builder: &mut sqlx::QueryBuilder<'arg, sqlx::Postgres>,
    field_schema: &FieldSchema,
    value: &'arg Value,
) -> Result<()> {
    match &value {
        Value::Basic(v) => match v {
            BasicValue::Bytes(v) => {
                builder.push_bind(&**v);
            }
            BasicValue::Str(v) => {
                builder.push_bind(&**v);
            }
            BasicValue::Bool(v) => {
                builder.push_bind(v);
            }
            BasicValue::Int64(v) => {
                builder.push_bind(v);
            }
            BasicValue::Float32(v) => {
                builder.push_bind(v);
            }
            BasicValue::Float64(v) => {
                builder.push_bind(v);
            }
            BasicValue::Json(v) => {
                builder.push_bind(sqlx::types::Json(&**v));
            }
            BasicValue::Range(v) => {
                builder.push_bind(PgRange {
                    start: Bound::Included(v.start as i64),
                    end: Bound::Excluded(v.end as i64),
                });
            }
            BasicValue::Vector(v) => match &field_schema.value_type.typ {
                ValueType::Basic(BasicValueType::Vector(vs)) if convertible_to_pgvector(vs) => {
                    let vec = v
                        .iter()
                        .map(|v| {
                            Ok(match v {
                                BasicValue::Float32(v) => *v,
                                BasicValue::Float64(v) => *v as f32,
                                BasicValue::Int64(v) => *v as f32,
                                v => bail!("unexpected vector element type: {}", v.kind()),
                            })
                        })
                        .collect::<Result<Vec<f32>>>()?;
                    builder.push_bind(pgvector::Vector::from(vec));
                }
                _ => {
                    builder.push_bind(sqlx::types::Json(v));
                }
            },
        },
        Value::Null => {
            builder.push("NULL");
        }
        v => {
            builder.push_bind(sqlx::types::Json(*v));
        }
    };
    Ok(())
}

fn from_pg_value(row: &PgRow, field_idx: usize, typ: &ValueType) -> Result<Value> {
    let value = match typ {
        ValueType::Basic(basic_type) => {
            let basic_value = match basic_type {
                BasicValueType::Bytes => row
                    .try_get::<Option<Vec<u8>>, _>(field_idx)?
                    .map(|v| BasicValue::Bytes(Arc::from(v))),
                BasicValueType::Str => row
                    .try_get::<Option<String>, _>(field_idx)?
                    .map(|v| BasicValue::Str(Arc::from(v))),
                BasicValueType::Bool => row
                    .try_get::<Option<bool>, _>(field_idx)?
                    .map(|v| BasicValue::Bool(v)),
                BasicValueType::Int64 => row
                    .try_get::<Option<i64>, _>(field_idx)?
                    .map(|v| BasicValue::Int64(v)),
                BasicValueType::Float32 => row
                    .try_get::<Option<f32>, _>(field_idx)?
                    .map(|v| BasicValue::Float32(v)),
                BasicValueType::Float64 => row
                    .try_get::<Option<f64>, _>(field_idx)?
                    .map(|v| BasicValue::Float64(v)),
                BasicValueType::Range => row
                    .try_get::<Option<PgRange<i64>>, _>(field_idx)?
                    .map(|v| match (v.start, v.end) {
                        (Bound::Included(start), Bound::Excluded(end)) => {
                            Ok(BasicValue::Range(RangeValue {
                                start: start as usize,
                                end: end as usize,
                            }))
                        }
                        _ => anyhow::bail!("invalid range value"),
                    })
                    .transpose()?,
                BasicValueType::Json => row
                    .try_get::<Option<serde_json::Value>, _>(field_idx)?
                    .map(|v| BasicValue::Json(Arc::from(v))),
                BasicValueType::Vector(vs) => {
                    if convertible_to_pgvector(vs) {
                        row.try_get::<Option<pgvector::Vector>, _>(field_idx)?
                            .map(|v| -> Result<_> {
                                Ok(BasicValue::Vector(Arc::from(
                                    v.as_slice()
                                        .iter()
                                        .map(|e| {
                                            Ok(match &*vs.element_type {
                                                &BasicValueType::Float32 => BasicValue::Float32(*e),
                                                &BasicValueType::Float64 => {
                                                    BasicValue::Float64(*e as f64)
                                                }
                                                &BasicValueType::Int64 => {
                                                    BasicValue::Int64(*e as i64)
                                                }
                                                _ => anyhow::bail!("invalid vector element type"),
                                            })
                                        })
                                        .collect::<Result<Vec<_>>>()?,
                                )))
                            })
                            .transpose()?
                    } else {
                        row.try_get::<Option<serde_json::Value>, _>(field_idx)?
                            .map(|v| BasicValue::from_json(v, basic_type))
                            .transpose()?
                    }
                }
            };
            basic_value.map(|bv| Value::Basic(bv))
        }
        _ => row
            .try_get::<Option<serde_json::Value>, _>(field_idx)?
            .map(|v| Value::from_json(v, typ))
            .transpose()?,
    };
    let final_value = if let Some(v) = value { v } else { Value::Null };
    Ok(final_value)
}

pub struct Executor {
    db_pool: PgPool,
    table_name: ValidIdentifier,
    key_fields_schema: Vec<FieldSchema>,
    value_fields_schema: Vec<FieldSchema>,
    all_fields: Vec<FieldSchema>,
    all_fields_comma_separated: String,
    upsert_sql_prefix: String,
    upsert_sql_suffix: String,
    delete_sql_prefix: String,
}

impl Executor {
    fn new(
        db_pool: PgPool,
        table_name: String,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
    ) -> Result<Self> {
        let key_fields = key_fields_schema
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let value_fields = value_fields_schema
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let set_value_fields = value_fields_schema
            .iter()
            .map(|f| format!("{} = EXCLUDED.{}", f.name, f.name))
            .collect::<Vec<_>>()
            .join(", ");

        let all_fields = key_fields_schema
            .iter()
            .chain(value_fields_schema.iter())
            .cloned()
            .collect::<Vec<_>>();
        let table_name = ValidIdentifier::try_from(table_name)?;
        Ok(Self {
            db_pool,
            key_fields_schema,
            value_fields_schema,
            all_fields_comma_separated: all_fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            all_fields,
            upsert_sql_prefix: format!(
                "INSERT INTO {table_name} ({key_fields}, {value_fields}) VALUES "
            ),
            upsert_sql_suffix: format!(
                " ON CONFLICT ({key_fields}) DO UPDATE SET {set_value_fields};"
            ),
            delete_sql_prefix: format!("DELETE FROM {table_name} WHERE "),
            table_name,
        })
    }
}

#[async_trait]
impl ExportTargetExecutor for Executor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let num_parameters = self.key_fields_schema.len() + self.value_fields_schema.len();
        let mut txn = self.db_pool.begin().await?;

        for upsert_chunk in mutation.upserts.chunks(BIND_LIMIT / num_parameters) {
            let mut query_builder = sqlx::QueryBuilder::new(&self.upsert_sql_prefix);
            for (i, upsert) in upsert_chunk.iter().enumerate() {
                if i > 0 {
                    query_builder.push(",");
                }
                query_builder.push(" (");
                for (j, key_value) in key_value_fields_iter(&self.key_fields_schema, &upsert.key)?
                    .iter()
                    .enumerate()
                {
                    if j > 0 {
                        query_builder.push(", ");
                    }
                    bind_key_field(&mut query_builder, key_value)?;
                }
                if self.value_fields_schema.len() != upsert.value.fields.len() {
                    bail!(
                        "unmatched value length: {} vs {}",
                        self.value_fields_schema.len(),
                        upsert.value.fields.len()
                    );
                }
                for (schema, value) in self
                    .value_fields_schema
                    .iter()
                    .zip(upsert.value.fields.iter())
                {
                    query_builder.push(", ");
                    bind_value_field(&mut query_builder, schema, value)?;
                }
                query_builder.push(")");
            }
            query_builder.push(&self.upsert_sql_suffix);
            query_builder.build().execute(&mut *txn).await?;
        }

        // TODO: Find a way to batch delete.
        for delete_key in mutation.delete_keys.iter() {
            let mut query_builder = sqlx::QueryBuilder::new("");
            query_builder.push(&self.delete_sql_prefix);
            for (i, (schema, value)) in self
                .key_fields_schema
                .iter()
                .zip(key_value_fields_iter(&self.key_fields_schema, delete_key)?.iter())
                .enumerate()
            {
                if i > 0 {
                    query_builder.push(" AND ");
                }
                query_builder.push(schema.name.as_str());
                query_builder.push("=");
                bind_key_field(&mut query_builder, value)?;
            }
            query_builder.build().execute(&mut *txn).await?;
        }

        txn.commit().await?;

        Ok(())
    }
}

static SCORE_FIELD_NAME: &str = "__score";

#[async_trait]
impl QueryTarget for Executor {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults> {
        let query_str = format!(
            "SELECT {} {} $1 AS {SCORE_FIELD_NAME}, {} FROM {} ORDER BY {SCORE_FIELD_NAME} LIMIT $2",
            ValidIdentifier::try_from(query.vector_field_name)?,
            to_distance_operator(query.similarity_metric),
            self.all_fields_comma_separated,
            self.table_name,
        );
        let results = sqlx::query(&query_str)
            .bind(pgvector::Vector::from(query.vector))
            .bind(query.limit as i64)
            .fetch_all(&self.db_pool)
            .await?
            .into_iter()
            .map(|r| -> Result<QueryResult> {
                let score: f64 = distance_to_similarity(query.similarity_metric, r.try_get(0)?);
                let data = self
                    .key_fields_schema
                    .iter()
                    .chain(self.value_fields_schema.iter())
                    .enumerate()
                    .map(|(idx, schema)| from_pg_value(&r, idx + 1, &schema.value_type.typ))
                    .collect::<Result<Vec<_>>>()?;
                let result = QueryResult { data, score };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryResults {
            fields: self.all_fields.clone(),
            results,
        })
    }
}

fn to_distance_operator(metric: VectorSimilarityMetric) -> &'static str {
    match metric {
        VectorSimilarityMetric::CosineSimilarity => "<=>",
        VectorSimilarityMetric::L2Distance => "<->",
        VectorSimilarityMetric::InnerProduct => "<#>",
    }
}

fn distance_to_similarity(metric: VectorSimilarityMetric, distance: f64) -> f64 {
    match metric {
        // cosine distance => cosine similarity
        VectorSimilarityMetric::CosineSimilarity => 1.0 - distance,
        VectorSimilarityMetric::L2Distance => distance,
        // negative inner product => inner product
        VectorSimilarityMetric::InnerProduct => -distance,
    }
}

pub struct Factory {
    db_pools: Mutex<
        HashMap<
            Option<String>,
            Shared<Pin<Box<dyn Future<Output = Result<PgPool, SharedError>> + Send>>>,
        >,
    >,
}

impl Default for Factory {
    fn default() -> Self {
        Self {
            db_pools: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableId {
    database_url: Option<String>,
    table_name: String,
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table_name)?;
        if let Some(database_url) = &self.database_url {
            write!(f, " (database: {})", database_url)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupState {
    #[serde(with = "indexmap::map::serde_seq")]
    key_fields_schema: IndexMap<String, ValueType>,

    #[serde(with = "indexmap::map::serde_seq")]
    value_fields_schema: IndexMap<String, ValueType>,

    vector_indexes: BTreeMap<String, VectorIndexDef>,
}

impl SetupState {
    fn new(
        table_id: &TableId,
        key_fields_schema: &Vec<FieldSchema>,
        value_fields_schema: &Vec<FieldSchema>,
        index_options: &IndexOptions,
    ) -> Self {
        Self {
            key_fields_schema: key_fields_schema
                .iter()
                .map(|f| (f.name.clone(), f.value_type.typ.without_attrs()))
                .collect(),
            value_fields_schema: value_fields_schema
                .iter()
                .map(|f| (f.name.clone(), f.value_type.typ.without_attrs()))
                .collect(),
            vector_indexes: index_options
                .vector_index_defs
                .iter()
                .map(|v| (to_vector_index_name(&table_id.table_name, v), v.clone()))
                .collect(),
        }
    }

    fn is_compatible(&self, other: &Self) -> bool {
        self.key_fields_schema == other.key_fields_schema
    }

    fn uses_pgvector(&self) -> bool {
        self.value_fields_schema
            .iter()
            .any(|(_, value)| match &value {
                ValueType::Basic(BasicValueType::Vector(vec_schema)) => {
                    convertible_to_pgvector(vec_schema)
                }
                _ => false,
            })
    }
}

#[derive(Debug)]
pub enum TableUpsertionAction {
    Create {
        keys: IndexMap<String, ValueType>,
        values: IndexMap<String, ValueType>,
    },
    Update {
        columns_to_delete: IndexSet<String>,
        columns_to_upsert: IndexMap<String, ValueType>,
    },
}

impl TableUpsertionAction {
    fn is_empty(&self) -> bool {
        match self {
            TableUpsertionAction::Create { .. } => false,
            TableUpsertionAction::Update {
                columns_to_delete,
                columns_to_upsert,
            } => columns_to_delete.is_empty() && columns_to_upsert.is_empty(),
        }
    }
}

#[derive(Debug)]
pub struct TableSetupAction {
    table_upsertion: TableUpsertionAction,
    indexes_to_delete: IndexSet<String>,
    indexes_to_create: IndexMap<String, VectorIndexDef>,
}

impl TableSetupAction {
    fn is_empty(&self) -> bool {
        self.table_upsertion.is_empty()
            && self.indexes_to_delete.is_empty()
            && self.indexes_to_create.is_empty()
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetupStatusCheck {
    #[derivative(Debug = "ignore")]
    factory: Arc<Factory>,
    table_id: TableId,

    desired_state: Option<SetupState>,
    drop_existing: bool,
    create_pgvector_extension: bool,
    desired_table_setup: Option<TableSetupAction>,
}

impl SetupStatusCheck {
    fn new(
        factory: Arc<Factory>,
        table_id: TableId,
        desired_state: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
    ) -> Self {
        let desired_table_setup = desired_state
            .as_ref()
            .map(|desired| {
                let table_upsertion = if existing.always_exists()
                    && existing
                        .possible_versions()
                        .all(|v| v.is_compatible(&desired))
                {
                    TableUpsertionAction::Update {
                        columns_to_delete: existing
                            .possible_versions()
                            .map(|v| v.value_fields_schema.keys())
                            .flatten()
                            .filter(|column_name| {
                                !desired.value_fields_schema.contains_key(*column_name)
                            })
                            .cloned()
                            .collect(),
                        columns_to_upsert: desired
                            .value_fields_schema
                            .iter()
                            .filter(|(field_name, schema)| {
                                existing
                                    .possible_versions()
                                    .any(|v| v.value_fields_schema.get(*field_name) != Some(schema))
                            })
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    }
                } else {
                    TableUpsertionAction::Create {
                        keys: desired.key_fields_schema.clone(),
                        values: desired.value_fields_schema.clone(),
                    }
                };
                TableSetupAction {
                    table_upsertion,
                    indexes_to_delete: existing
                        .possible_versions()
                        .map(|v| v.vector_indexes.keys())
                        .flatten()
                        .filter(|index_name| !desired.vector_indexes.contains_key(*index_name))
                        .cloned()
                        .collect(),
                    indexes_to_create: desired
                        .vector_indexes
                        .iter()
                        .filter(|(name, def)| {
                            existing
                                .possible_versions()
                                .any(|v| v.vector_indexes.get(*name) != Some(def))
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                }
            })
            .filter(|action| !action.is_empty());
        let drop_existing = desired_state
            .as_ref()
            .map(|state| {
                existing
                    .possible_versions()
                    .any(|v| !v.is_compatible(&state))
            })
            .unwrap_or(true);
        let create_pgvector_extension = desired_state
            .as_ref()
            .map(|s| s.uses_pgvector())
            .unwrap_or(false)
            && !existing.current.map(|s| s.uses_pgvector()).unwrap_or(false);

        Self {
            factory,
            table_id,
            desired_state,
            drop_existing,
            create_pgvector_extension,
            desired_table_setup,
        }
    }
}

fn to_column_type_sql(column_type: &ValueType) -> Cow<'static, str> {
    match column_type {
        ValueType::Basic(basic_type) => match basic_type {
            BasicValueType::Bytes => "bytea".into(),
            BasicValueType::Str => "text".into(),
            BasicValueType::Bool => "boolean".into(),
            BasicValueType::Int64 => "bigint".into(),
            BasicValueType::Float32 => "real".into(),
            BasicValueType::Float64 => "double precision".into(),
            BasicValueType::Range => "int8range".into(),
            BasicValueType::Json => "jsonb".into(),
            BasicValueType::Vector(vec_schema) => {
                if convertible_to_pgvector(vec_schema) {
                    format!("vector({})", vec_schema.dimension.unwrap_or(0)).into()
                } else {
                    "jsonb".into()
                }
            }
        },
        _ => "jsonb".into(),
    }
}

fn to_vector_similarity_metric_sql(metric: VectorSimilarityMetric) -> &'static str {
    match metric {
        VectorSimilarityMetric::CosineSimilarity => "vector_cosine_ops",
        VectorSimilarityMetric::L2Distance => "vector_l2_ops",
        VectorSimilarityMetric::InnerProduct => "vector_ip_ops",
    }
}

fn to_index_spec_sql(index_spec: &VectorIndexDef) -> Cow<'static, str> {
    format!(
        "USING hnsw ({} {})",
        index_spec.field_name,
        to_vector_similarity_metric_sql(index_spec.metric)
    )
    .into()
}

fn to_vector_index_name(table_name: &str, vector_index_def: &spec::VectorIndexDef) -> String {
    format!(
        "{}__{}__{}",
        table_name,
        vector_index_def.field_name,
        to_vector_similarity_metric_sql(vector_index_def.metric)
    )
}

fn describe_field_schema(field_name: &str, value_type: &ValueType) -> String {
    format!("{} {}", field_name, to_column_type_sql(value_type))
}

fn describe_index_spec(index_name: &str, index_spec: &VectorIndexDef) -> String {
    format!("{} {}", index_name, to_index_spec_sql(index_spec))
}

#[async_trait]
impl setup::ResourceSetupStatusCheck for SetupStatusCheck {
    type Key = TableId;
    type State = SetupState;

    fn describe_resource(&self) -> String {
        format!("Postgres table {}", self.table_id)
    }

    fn key(&self) -> &Self::Key {
        &self.table_id
    }

    fn desired_state(&self) -> Option<&Self::State> {
        self.desired_state.as_ref()
    }

    fn describe_changes(&self) -> Vec<String> {
        let mut descriptions = vec![];
        if self.drop_existing {
            descriptions.push("Drop table".to_string());
        }
        if self.create_pgvector_extension {
            descriptions.push("Create pg_vector extension (if not exists)".to_string());
        }
        if let Some(desired_table_setup) = &self.desired_table_setup {
            match &desired_table_setup.table_upsertion {
                TableUpsertionAction::Create { keys, values } => {
                    descriptions.push(format!(
                        "Create table:\n  key columns: {}\n  value columns: {}\n",
                        keys.iter()
                            .map(|(k, v)| describe_field_schema(k, v))
                            .join(",  "),
                        values
                            .iter()
                            .map(|(k, v)| describe_field_schema(k, v))
                            .join(",  "),
                    ));
                }
                TableUpsertionAction::Update {
                    columns_to_delete,
                    columns_to_upsert,
                } => {
                    if !columns_to_delete.is_empty() {
                        descriptions.push(format!(
                            "Delete column from table: {}",
                            columns_to_delete.iter().join(",  "),
                        ));
                    }
                    if !columns_to_upsert.is_empty() {
                        descriptions.push(format!(
                            "Add / update columns in table: {}",
                            columns_to_upsert
                                .iter()
                                .map(|(k, v)| describe_field_schema(k, v))
                                .join(",  "),
                        ));
                    }
                }
            }
            if !desired_table_setup.indexes_to_delete.is_empty() {
                descriptions.push(format!(
                    "Delete indexes from table: {}",
                    desired_table_setup.indexes_to_delete.iter().join(",  "),
                ));
            }
            if !desired_table_setup.indexes_to_create.is_empty() {
                descriptions.push(format!(
                    "Create indexes in table: {}",
                    desired_table_setup
                        .indexes_to_create
                        .iter()
                        .map(|(index_name, index_spec)| describe_index_spec(index_name, index_spec))
                        .join(",  "),
                ));
            }
        }
        descriptions
    }

    fn change_type(&self) -> setup::SetupChangeType {
        if self.drop_existing {
            if self.desired_state.is_none() {
                setup::SetupChangeType::Delete
            } else {
                setup::SetupChangeType::Update
            }
        } else {
            match &self.desired_table_setup {
                Some(setup) => match setup.table_upsertion {
                    TableUpsertionAction::Create { .. } => setup::SetupChangeType::Create,
                    TableUpsertionAction::Update { .. } => setup::SetupChangeType::Update,
                },
                None => setup::SetupChangeType::NoChange,
            }
        }
    }

    async fn apply_change(&self) -> Result<()> {
        let db_pool = self
            .factory
            .get_db_pool(self.table_id.database_url.clone())
            .await?;
        let table_name = &self.table_id.table_name;
        if self.drop_existing {
            sqlx::query(&format!("DROP TABLE IF EXISTS {table_name}"))
                .execute(&db_pool)
                .await?;
        }
        if self.create_pgvector_extension {
            sqlx::query(&format!("CREATE EXTENSION IF NOT EXISTS vector;"))
                .execute(&db_pool)
                .await?;
        }
        if let Some(desired_table_setup) = &self.desired_table_setup {
            for index_name in desired_table_setup.indexes_to_delete.iter() {
                let sql = format!("DROP INDEX IF EXISTS {}", index_name);
                sqlx::query(&sql).execute(&db_pool).await?;
            }
            match &desired_table_setup.table_upsertion {
                TableUpsertionAction::Create { keys, values } => {
                    let mut fields = (keys
                        .iter()
                        .map(|(k, v)| format!("{} {} NOT NULL", k, to_column_type_sql(v))))
                    .chain(
                        values
                            .iter()
                            .map(|(k, v)| format!("{} {}", k, to_column_type_sql(v))),
                    );
                    let sql = format!(
                        "CREATE TABLE IF NOT EXISTS {table_name} ({}, PRIMARY KEY ({}))",
                        fields.join(", "),
                        keys.keys().join(", ")
                    );
                    sqlx::query(&sql).execute(&db_pool).await?;
                }
                TableUpsertionAction::Update {
                    columns_to_delete,
                    columns_to_upsert,
                } => {
                    for column_name in columns_to_delete.iter() {
                        let sql = format!(
                            "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}",
                        );
                        sqlx::query(&sql).execute(&db_pool).await?;
                    }
                    for (column_name, column_type) in columns_to_upsert.iter() {
                        let sql = format!(
                            "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}, ADD COLUMN {column_name} {}",
                            to_column_type_sql(column_type)
                        );
                        sqlx::query(&sql).execute(&db_pool).await?;
                    }
                }
            }
            for (index_name, index_spec) in desired_table_setup.indexes_to_create.iter() {
                let sql = format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} {}",
                    index_name,
                    index_spec.field_name,
                    to_index_spec_sql(index_spec)
                );
                sqlx::query(&sql).execute(&db_pool).await?;
            }
        }
        Ok(())
    }
}

impl StorageFactoryBase for Arc<Factory> {
    type Spec = Spec;
    type SetupState = SetupState;
    type Key = TableId;

    fn name(&self) -> &str {
        "Postgres"
    }

    fn build(
        self: Arc<Self>,
        name: String,
        target_id: i32,
        spec: Spec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        (TableId, SetupState),
        ExecutorFuture<'static, (Arc<dyn ExportTargetExecutor>, Option<Arc<dyn QueryTarget>>)>,
    )> {
        let table_id = TableId {
            database_url: spec.database_url.clone(),
            table_name: spec.table_name.unwrap_or_else(|| {
                format!("{}__{}__{}", context.flow_instance_name, name, target_id)
            }),
        };
        let setup_state = SetupState::new(
            &table_id,
            &key_fields_schema,
            &value_fields_schema,
            &storage_options,
        );
        let table_name = table_id.table_name.clone();
        let executors = async move {
            let executor = Arc::new(Executor::new(
                self.get_db_pool(spec.database_url).await?,
                table_name,
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
        key: TableId,
        desired: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
    ) -> Result<
        impl setup::ResourceSetupStatusCheck<Key = TableId, State = SetupState> + Send + Sync + 'static,
    > {
        Ok(SetupStatusCheck::new(self.clone(), key, desired, existing))
    }

    fn will_keep_all_existing_data(
        &self,
        _name: &str,
        _target_id: i32,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<bool> {
        let result = existing
            .key_fields_schema
            .iter()
            .all(|(k, v)| desired.key_fields_schema.get(k) == Some(v))
            && existing
                .value_fields_schema
                .iter()
                .any(|(k, v)| desired.value_fields_schema.get(k) != Some(v));
        Ok(result)
    }
}

impl Factory {
    async fn get_db_pool(&self, database_url: Option<String>) -> Result<PgPool> {
        let pool_fut = {
            let mut db_pools = self.db_pools.lock().unwrap();
            match db_pools.entry(database_url) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let database_url = entry.key().clone();
                    let pool_fut = async {
                        shared_ok(if let Some(database_url) = database_url {
                            PgPool::connect(&database_url).await?
                        } else {
                            get_lib_context()
                                .ok_or_else(|| {
                                    SharedError::new(anyhow!("Cocoindex is not initialized"))
                                })?
                                .pool
                                .clone()
                        })
                    };
                    let shared_fut = pool_fut.boxed().shared();
                    entry.insert(shared_fut.clone());
                    shared_fut
                }
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            }
        };
        Ok(pool_fut.await.std_result()?)
    }
}
