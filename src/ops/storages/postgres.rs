use crate::prelude::*;

use super::shared::table_columns::{
    TableColumnsSchema, TableMainSetupAction, TableUpsertionAction, check_table_compatibility,
};
use crate::base::spec::{self, *};
use crate::ops::sdk::*;
use crate::settings::DatabaseConnectionSpec;
use crate::utils::db::ValidIdentifier;
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::postgres::types::PgRange;
use sqlx::{PgPool, Row};
use std::ops::Bound;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct Spec {
    database: Option<spec::AuthEntryReference<DatabaseConnectionSpec>>,
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
        matches!(
            *vec_schema.element_type,
            BasicValueType::Float32 | BasicValueType::Float64 | BasicValueType::Int64
        )
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
        KeyValue::Uuid(v) => {
            builder.push_bind(v);
        }
        KeyValue::Date(v) => {
            builder.push_bind(v);
        }
        KeyValue::Struct(fields) => {
            builder.push_bind(sqlx::types::Json(fields));
        }
    }
    Ok(())
}

fn bind_value_field<'arg>(
    builder: &mut sqlx::QueryBuilder<'arg, sqlx::Postgres>,
    field_schema: &'arg FieldSchema,
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
            BasicValue::Range(v) => {
                builder.push_bind(PgRange {
                    start: Bound::Included(v.start as i64),
                    end: Bound::Excluded(v.end as i64),
                });
            }
            BasicValue::Uuid(v) => {
                builder.push_bind(v);
            }
            BasicValue::Date(v) => {
                builder.push_bind(v);
            }
            BasicValue::Time(v) => {
                builder.push_bind(v);
            }
            BasicValue::LocalDateTime(v) => {
                builder.push_bind(v);
            }
            BasicValue::OffsetDateTime(v) => {
                builder.push_bind(v);
            }
            BasicValue::TimeDelta(v) => {
                builder.push_bind(v);
            }
            BasicValue::Json(v) => {
                builder.push_bind(sqlx::types::Json(&**v));
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
            builder.push_bind(sqlx::types::Json(TypedValue {
                t: &field_schema.value_type.typ,
                v,
            }));
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
                    .map(|v| BasicValue::Bytes(Bytes::from(v))),
                BasicValueType::Str => row
                    .try_get::<Option<String>, _>(field_idx)?
                    .map(|v| BasicValue::Str(Arc::from(v))),
                BasicValueType::Bool => row
                    .try_get::<Option<bool>, _>(field_idx)?
                    .map(BasicValue::Bool),
                BasicValueType::Int64 => row
                    .try_get::<Option<i64>, _>(field_idx)?
                    .map(BasicValue::Int64),
                BasicValueType::Float32 => row
                    .try_get::<Option<f32>, _>(field_idx)?
                    .map(BasicValue::Float32),
                BasicValueType::Float64 => row
                    .try_get::<Option<f64>, _>(field_idx)?
                    .map(BasicValue::Float64),
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
                BasicValueType::Uuid => row
                    .try_get::<Option<Uuid>, _>(field_idx)?
                    .map(BasicValue::Uuid),
                BasicValueType::Date => row
                    .try_get::<Option<chrono::NaiveDate>, _>(field_idx)?
                    .map(BasicValue::Date),
                BasicValueType::Time => row
                    .try_get::<Option<chrono::NaiveTime>, _>(field_idx)?
                    .map(BasicValue::Time),
                BasicValueType::LocalDateTime => row
                    .try_get::<Option<chrono::NaiveDateTime>, _>(field_idx)?
                    .map(BasicValue::LocalDateTime),
                BasicValueType::OffsetDateTime => row
                    .try_get::<Option<chrono::DateTime<chrono::FixedOffset>>, _>(field_idx)?
                    .map(BasicValue::OffsetDateTime),
                BasicValueType::TimeDelta => row
                    .try_get::<Option<sqlx::postgres::types::PgInterval>, _>(field_idx)?
                    .map(|pg_interval| {
                        let duration = chrono::Duration::microseconds(pg_interval.microseconds)
                            + chrono::Duration::days(pg_interval.days as i64)
                            + chrono::Duration::days((pg_interval.months as i64) * 30);
                        BasicValue::TimeDelta(duration)
                    }),
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
                                            Ok(match *vs.element_type {
                                                BasicValueType::Float32 => BasicValue::Float32(*e),
                                                BasicValueType::Float64 => {
                                                    BasicValue::Float64(*e as f64)
                                                }
                                                BasicValueType::Int64 => {
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
            basic_value.map(Value::Basic)
        }
        _ => row
            .try_get::<Option<serde_json::Value>, _>(field_idx)?
            .map(|v| Value::from_json(v, typ))
            .transpose()?,
    };
    let final_value = if let Some(v) = value { v } else { Value::Null };
    Ok(final_value)
}

pub struct ExportContext {
    db_ref: Option<spec::AuthEntryReference<DatabaseConnectionSpec>>,
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

impl ExportContext {
    fn new(
        db_ref: Option<spec::AuthEntryReference<DatabaseConnectionSpec>>,
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
            db_ref,
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

impl ExportContext {
    async fn upsert(
        &self,
        upserts: &[interface::ExportTargetUpsertEntry],
        txn: &mut sqlx::PgTransaction<'_>,
    ) -> Result<()> {
        let num_parameters = self.key_fields_schema.len() + self.value_fields_schema.len();
        for upsert_chunk in upserts.chunks(BIND_LIMIT / num_parameters) {
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
            query_builder.build().execute(&mut **txn).await?;
        }
        Ok(())
    }

    async fn delete(
        &self,
        deletions: &[interface::ExportTargetDeleteEntry],
        txn: &mut sqlx::PgTransaction<'_>,
    ) -> Result<()> {
        // TODO: Find a way to batch delete.
        for deletion in deletions.iter() {
            let mut query_builder = sqlx::QueryBuilder::new("");
            query_builder.push(&self.delete_sql_prefix);
            for (i, (schema, value)) in self
                .key_fields_schema
                .iter()
                .zip(key_value_fields_iter(&self.key_fields_schema, &deletion.key)?.iter())
                .enumerate()
            {
                if i > 0 {
                    query_builder.push(" AND ");
                }
                query_builder.push(schema.name.as_str());
                query_builder.push("=");
                bind_key_field(&mut query_builder, value)?;
            }
            query_builder.build().execute(&mut **txn).await?;
        }
        Ok(())
    }
}

static SCORE_FIELD_NAME: &str = "__score";

struct PostgresQueryTarget {
    db_pool: PgPool,
    context: Arc<ExportContext>,
}

#[async_trait]
impl QueryTarget for PostgresQueryTarget {
    async fn search(&self, query: VectorMatchQuery) -> Result<QueryResults> {
        let query_str = format!(
            "SELECT {} {} $1 AS {SCORE_FIELD_NAME}, {} FROM {} ORDER BY {SCORE_FIELD_NAME} LIMIT $2",
            ValidIdentifier::try_from(query.vector_field_name)?,
            to_distance_operator(query.similarity_metric),
            self.context.all_fields_comma_separated,
            self.context.table_name,
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
                    .context
                    .key_fields_schema
                    .iter()
                    .chain(self.context.value_fields_schema.iter())
                    .enumerate()
                    .map(|(idx, schema)| from_pg_value(&r, idx + 1, &schema.value_type.typ))
                    .collect::<Result<Vec<_>>>()?;
                let result = QueryResult { data, score };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryResults {
            fields: self.context.all_fields.clone(),
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

#[derive(Default)]
pub struct Factory {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableId {
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<spec::AuthEntryReference<DatabaseConnectionSpec>>,
    table_name: String,
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table_name)?;
        if let Some(database) = &self.database {
            write!(f, " (database: {database})")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupState {
    #[serde(flatten)]
    columns: TableColumnsSchema<ValueType>,

    vector_indexes: BTreeMap<String, VectorIndexDef>,
}

impl SetupState {
    fn new(
        table_id: &TableId,
        key_fields_schema: &[FieldSchema],
        value_fields_schema: &[FieldSchema],
        index_options: &IndexOptions,
    ) -> Self {
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
            vector_indexes: index_options
                .vector_indexes
                .iter()
                .map(|v| (to_vector_index_name(&table_id.table_name, v), v.clone()))
                .collect(),
        }
    }

    fn uses_pgvector(&self) -> bool {
        self.columns
            .value_columns
            .iter()
            .any(|(_, value)| match &value {
                ValueType::Basic(BasicValueType::Vector(vec_schema)) => {
                    convertible_to_pgvector(vec_schema)
                }
                _ => false,
            })
    }
}

fn to_column_type_sql(column_type: &ValueType) -> String {
    match column_type {
        ValueType::Basic(basic_type) => match basic_type {
            BasicValueType::Bytes => "bytea".into(),
            BasicValueType::Str => "text".into(),
            BasicValueType::Bool => "boolean".into(),
            BasicValueType::Int64 => "bigint".into(),
            BasicValueType::Float32 => "real".into(),
            BasicValueType::Float64 => "double precision".into(),
            BasicValueType::Range => "int8range".into(),
            BasicValueType::Uuid => "uuid".into(),
            BasicValueType::Date => "date".into(),
            BasicValueType::Time => "time".into(),
            BasicValueType::LocalDateTime => "timestamp".into(),
            BasicValueType::OffsetDateTime => "timestamp with time zone".into(),
            BasicValueType::TimeDelta => "interval".into(),
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

impl<'a> Into<Cow<'a, TableColumnsSchema<String>>> for &'a SetupState {
    fn into(self) -> Cow<'a, TableColumnsSchema<String>> {
        Cow::Owned(TableColumnsSchema {
            key_columns: self
                .columns
                .key_columns
                .iter()
                .map(|(k, v)| (k.clone(), to_column_type_sql(v)))
                .collect(),
            value_columns: self
                .columns
                .value_columns
                .iter()
                .map(|(k, v)| (k.clone(), to_column_type_sql(v)))
                .collect(),
        })
    }
}

#[derive(Debug)]
pub struct TableSetupAction {
    table_action: TableMainSetupAction<String>,
    indexes_to_delete: IndexSet<String>,
    indexes_to_create: IndexMap<String, VectorIndexDef>,
}

#[derive(Debug)]
pub struct SetupStatus {
    create_pgvector_extension: bool,
    actions: TableSetupAction,
}

impl SetupStatus {
    fn new(desired_state: Option<SetupState>, existing: setup::CombinedState<SetupState>) -> Self {
        let table_action =
            TableMainSetupAction::from_states(desired_state.as_ref(), &existing, false);
        let (indexes_to_delete, indexes_to_create) = desired_state
            .as_ref()
            .map(|desired| {
                (
                    existing
                        .possible_versions()
                        .flat_map(|v| v.vector_indexes.keys())
                        .filter(|index_name| !desired.vector_indexes.contains_key(*index_name))
                        .cloned()
                        .collect::<IndexSet<_>>(),
                    desired
                        .vector_indexes
                        .iter()
                        .filter(|(name, def)| {
                            !existing.always_exists()
                                || existing
                                    .possible_versions()
                                    .any(|v| v.vector_indexes.get(*name) != Some(def))
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<IndexMap<_, _>>(),
                )
            })
            .unwrap_or_default();
        let create_pgvector_extension = desired_state
            .as_ref()
            .map(|s| s.uses_pgvector())
            .unwrap_or(false)
            && !existing.current.map(|s| s.uses_pgvector()).unwrap_or(false);

        Self {
            create_pgvector_extension,
            actions: TableSetupAction {
                table_action,
                indexes_to_delete,
                indexes_to_create,
            },
        }
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

fn describe_index_spec(index_name: &str, index_spec: &VectorIndexDef) -> String {
    format!("{} {}", index_name, to_index_spec_sql(index_spec))
}

impl setup::ResourceSetupStatus for SetupStatus {
    fn describe_changes(&self) -> Vec<String> {
        let mut descriptions = self.actions.table_action.describe_changes();
        if self.create_pgvector_extension {
            descriptions.push("Create pg_vector extension (if not exists)".to_string());
        }
        if !self.actions.indexes_to_delete.is_empty() {
            descriptions.push(format!(
                "Delete indexes from table: {}",
                self.actions.indexes_to_delete.iter().join(",  "),
            ));
        }
        if !self.actions.indexes_to_create.is_empty() {
            descriptions.push(format!(
                "Create indexes in table: {}",
                self.actions
                    .indexes_to_create
                    .iter()
                    .map(|(index_name, index_spec)| describe_index_spec(index_name, index_spec))
                    .join(",  "),
            ));
        }
        descriptions
    }

    fn change_type(&self) -> setup::SetupChangeType {
        let has_other_update = !self.actions.indexes_to_create.is_empty()
            || !self.actions.indexes_to_delete.is_empty();
        self.actions.table_action.change_type(has_other_update)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl SetupStatus {
    async fn apply_change(&self, db_pool: &PgPool, table_name: &str) -> Result<()> {
        if self.actions.table_action.drop_existing {
            sqlx::query(&format!("DROP TABLE IF EXISTS {table_name}"))
                .execute(db_pool)
                .await?;
        }
        if self.create_pgvector_extension {
            sqlx::query("CREATE EXTENSION IF NOT EXISTS vector;")
                .execute(db_pool)
                .await?;
        }
        for index_name in self.actions.indexes_to_delete.iter() {
            let sql = format!("DROP INDEX IF EXISTS {}", index_name);
            sqlx::query(&sql).execute(db_pool).await?;
        }
        if let Some(table_upsertion) = &self.actions.table_action.table_upsertion {
            match table_upsertion {
                TableUpsertionAction::Create { keys, values } => {
                    let mut fields = (keys.iter().map(|(k, v)| format!("{k} {v} NOT NULL")))
                        .chain(values.iter().map(|(k, v)| format!("{k} {v}")));
                    let sql = format!(
                        "CREATE TABLE IF NOT EXISTS {table_name} ({}, PRIMARY KEY ({}))",
                        fields.join(", "),
                        keys.keys().join(", ")
                    );
                    sqlx::query(&sql).execute(db_pool).await?;
                }
                TableUpsertionAction::Update {
                    columns_to_delete,
                    columns_to_upsert,
                } => {
                    for column_name in columns_to_delete.iter() {
                        let sql = format!(
                            "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}",
                        );
                        sqlx::query(&sql).execute(db_pool).await?;
                    }
                    for (column_name, column_type) in columns_to_upsert.iter() {
                        let sql = format!(
                            "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}, ADD COLUMN {column_name} {column_type}"
                        );
                        sqlx::query(&sql).execute(db_pool).await?;
                    }
                }
            }
        }
        for (index_name, index_spec) in self.actions.indexes_to_create.iter() {
            let sql = format!(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} {}",
                to_index_spec_sql(index_spec)
            );
            sqlx::query(&sql).execute(db_pool).await?;
        }
        Ok(())
    }
}

async fn get_db_pool(
    db_ref: Option<&spec::AuthEntryReference<DatabaseConnectionSpec>>,
    auth_registry: &AuthRegistry,
) -> Result<PgPool> {
    let lib_context = get_lib_context()?;
    let db_conn_spec = db_ref
        .as_ref()
        .map(|db_ref| auth_registry.get(db_ref))
        .transpose()?;
    let db_pool = match db_conn_spec {
        Some(db_conn_spec) => lib_context.db_pools.get_pool(&db_conn_spec).await?,
        None => lib_context.builtin_db_pool.clone(),
    };
    Ok(db_pool)
}

#[async_trait]
impl StorageFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = ();
    type SetupState = SetupState;
    type SetupStatus = SetupStatus;
    type Key = TableId;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "Postgres"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        _declarations: Vec<()>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(TableId, SetupState)>,
    )> {
        let data_coll_output = data_collections
            .into_iter()
            .map(|d| {
                let table_id = TableId {
                    database: d.spec.database.clone(),
                    table_name: d.spec.table_name.unwrap_or_else(|| {
                        utils::db::sanitize_identifier(&format!(
                            "{}__{}",
                            context.flow_instance_name, d.name
                        ))
                    }),
                };
                let setup_state = SetupState::new(
                    &table_id,
                    &d.key_fields_schema,
                    &d.value_fields_schema,
                    &d.index_options,
                );
                let table_name = table_id.table_name.clone();
                let db_ref = d.spec.database;
                let auth_registry = context.auth_registry.clone();
                let executors = async move {
                    let db_pool = get_db_pool(db_ref.as_ref(), &auth_registry).await?;
                    let export_context = Arc::new(ExportContext::new(
                        db_ref,
                        db_pool.clone(),
                        table_name,
                        d.key_fields_schema,
                        d.value_fields_schema,
                    )?);
                    let query_target = Arc::new(PostgresQueryTarget {
                        db_pool,
                        context: export_context.clone(),
                    });
                    Ok(TypedExportTargetExecutors {
                        export_context: export_context.clone(),
                        query_target: Some(query_target as Arc<dyn QueryTarget>),
                    })
                };
                Ok(TypedExportDataCollectionBuildOutput {
                    setup_key: table_id,
                    desired_setup_state: setup_state,
                    executors: executors.boxed(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, vec![]))
    }

    async fn check_setup_status(
        &self,
        _key: TableId,
        desired: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
        _auth_registry: &Arc<AuthRegistry>,
    ) -> Result<SetupStatus> {
        Ok(SetupStatus::new(desired, existing))
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

    fn describe_resource(&self, key: &TableId) -> Result<String> {
        Ok(format!("Postgres table {}", key.table_name))
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, ExportContext>>,
    ) -> Result<()> {
        let mut mut_groups_by_db_ref = HashMap::new();
        for mutation in mutations.iter() {
            mut_groups_by_db_ref
                .entry(mutation.export_context.db_ref.clone())
                .or_insert_with(Vec::new)
                .push(mutation);
        }
        for mut_groups in mut_groups_by_db_ref.values() {
            let db_pool = &mut_groups
                .first()
                .ok_or_else(|| anyhow!("empty group"))?
                .export_context
                .db_pool;
            let mut txn = db_pool.begin().await?;
            for mut_group in mut_groups.iter() {
                mut_group
                    .export_context
                    .upsert(&mut_group.mutation.upserts, &mut txn)
                    .await?;
            }
            for mut_group in mut_groups.iter() {
                mut_group
                    .export_context
                    .delete(&mut_group.mutation.deletes, &mut txn)
                    .await?;
            }
            txn.commit().await?;
        }
        Ok(())
    }

    async fn apply_setup_changes(
        &self,
        changes: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()> {
        for change in changes.iter() {
            let db_pool = get_db_pool(change.key.database.as_ref(), &auth_registry).await?;
            change
                .setup_status
                .apply_change(&db_pool, &change.key.table_name)
                .await?;
        }
        Ok(())
    }
}
