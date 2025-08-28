use crate::ops::sdk::*;

use crate::ops::shared::postgres::{bind_key_field, get_db_pool};
use crate::settings::DatabaseConnectionSpec;
use sqlx::postgres::types::PgInterval;
use sqlx::{PgPool, Row};

type PgValueDecoder = fn(&sqlx::postgres::PgRow, usize) -> Result<Value>;

#[derive(Clone)]
struct FieldSchemaInfo {
    schema: FieldSchema,
    decoder: PgValueDecoder,
}

#[derive(Debug, Deserialize)]
pub struct Spec {
    /// Table name to read from (required)
    table_name: String,
    /// Database connection specification (optional)
    database: Option<spec::AuthEntryReference<DatabaseConnectionSpec>>,
    /// Optional: columns to include (if None, includes all columns)
    included_columns: Option<Vec<String>>,
    /// Optional: ordinal column for tracking changes
    ordinal_column: Option<String>,
}

#[derive(Clone)]
struct PostgresTableSchema {
    primary_key_columns: Vec<FieldSchemaInfo>,
    value_columns: Vec<FieldSchemaInfo>,
    ordinal_field_idx: Option<usize>,
    ordinal_field_schema: Option<FieldSchemaInfo>,
}

struct Executor {
    db_pool: PgPool,
    table_name: String,
    table_schema: PostgresTableSchema,
}

/// Map PostgreSQL data types to CocoIndex BasicValueType and a decoder function
fn map_postgres_type_to_cocoindex_and_decoder(pg_type: &str) -> (BasicValueType, PgValueDecoder) {
    match pg_type {
        "bytea" => (
            BasicValueType::Bytes,
            (|row, idx| Ok(Value::from(row.try_get::<Option<Vec<u8>>, _>(idx)?))) as PgValueDecoder,
        ),
        "text" | "varchar" | "char" | "character" | "character varying" => (
            BasicValueType::Str,
            (|row, idx| Ok(Value::from(row.try_get::<Option<String>, _>(idx)?))) as PgValueDecoder,
        ),
        "boolean" | "bool" => (
            BasicValueType::Bool,
            (|row, idx| Ok(Value::from(row.try_get::<Option<bool>, _>(idx)?))) as PgValueDecoder,
        ),
        // Integers: decode with actual PG width, convert to i64 Value
        "bigint" | "int8" => (
            BasicValueType::Int64,
            (|row, idx| Ok(Value::from(row.try_get::<Option<i64>, _>(idx)?))) as PgValueDecoder,
        ),
        "integer" | "int4" => (
            BasicValueType::Int64,
            (|row, idx| {
                let opt_v = row.try_get::<Option<i32>, _>(idx)?;
                Ok(Value::from(opt_v.map(|v| v as i64)))
            }) as PgValueDecoder,
        ),
        "smallint" | "int2" => (
            BasicValueType::Int64,
            (|row, idx| {
                let opt_v = row.try_get::<Option<i16>, _>(idx)?;
                Ok(Value::from(opt_v.map(|v| v as i64)))
            }) as PgValueDecoder,
        ),
        "real" | "float4" => (
            BasicValueType::Float32,
            (|row, idx| Ok(Value::from(row.try_get::<Option<f32>, _>(idx)?))) as PgValueDecoder,
        ),
        "double precision" | "float8" => (
            BasicValueType::Float64,
            (|row, idx| Ok(Value::from(row.try_get::<Option<f64>, _>(idx)?))) as PgValueDecoder,
        ),
        "uuid" => (
            BasicValueType::Uuid,
            (|row, idx| Ok(Value::from(row.try_get::<Option<uuid::Uuid>, _>(idx)?)))
                as PgValueDecoder,
        ),
        "date" => (
            BasicValueType::Date,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<chrono::NaiveDate>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
        "time" | "time without time zone" => (
            BasicValueType::Time,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<chrono::NaiveTime>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
        "timestamp" | "timestamp without time zone" => (
            BasicValueType::LocalDateTime,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<chrono::NaiveDateTime>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
        "timestamp with time zone" | "timestamptz" => (
            BasicValueType::OffsetDateTime,
            (|row, idx| {
                Ok(Value::from(row.try_get::<Option<
                    chrono::DateTime<chrono::FixedOffset>,
                >, _>(idx)?))
            }) as PgValueDecoder,
        ),
        "interval" => (
            BasicValueType::TimeDelta,
            (|row, idx| {
                let opt_iv = row.try_get::<Option<PgInterval>, _>(idx)?;
                let opt_dur = opt_iv.map(|iv| {
                    let approx_days = iv.days as i64 + (iv.months as i64) * 30;
                    chrono::Duration::microseconds(iv.microseconds)
                        + chrono::Duration::days(approx_days)
                });
                Ok(Value::from(opt_dur))
            }) as PgValueDecoder,
        ),
        "jsonb" | "json" => (
            BasicValueType::Json,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<serde_json::Value>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
        // Vector types (if supported) -> fallback to JSON
        t if t.starts_with("vector(") => (
            BasicValueType::Json,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<serde_json::Value>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
        // Others fallback to JSON
        _ => (
            BasicValueType::Json,
            (|row, idx| {
                Ok(Value::from(
                    row.try_get::<Option<serde_json::Value>, _>(idx)?,
                ))
            }) as PgValueDecoder,
        ),
    }
}

/// Fetch table schema information from PostgreSQL
async fn fetch_table_schema(
    pool: &PgPool,
    table_name: &str,
    included_columns: &Option<Vec<String>>,
    ordinal_column: &Option<String>,
) -> Result<PostgresTableSchema> {
    // Query to get column information including primary key status
    let query = r#"
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            (pk.column_name IS NOT NULL) as is_primary_key
        FROM
            information_schema.columns c
        LEFT JOIN (
            SELECT
                kcu.column_name
            FROM
                information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
            WHERE
                tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = $1
        ) pk ON c.column_name = pk.column_name
        WHERE
            c.table_name = $1
        ORDER BY c.ordinal_position
    "#;

    let rows = sqlx::query(query).bind(table_name).fetch_all(pool).await?;

    let mut primary_key_columns: Vec<FieldSchemaInfo> = Vec::new();
    let mut value_columns: Vec<FieldSchemaInfo> = Vec::new();
    let mut ordinal_field_schema: Option<FieldSchemaInfo> = None;

    for row in rows {
        let col_name: String = row.try_get::<String, _>("column_name")?;
        let pg_type_str: String = row.try_get::<String, _>("data_type")?;
        let is_nullable: bool = row.try_get::<String, _>("is_nullable")? == "YES";
        let is_primary_key: bool = row.try_get::<bool, _>("is_primary_key")?;

        let (basic_type, decoder) = map_postgres_type_to_cocoindex_and_decoder(&pg_type_str);
        let field_schema = FieldSchema::new(
            &col_name,
            make_output_type(basic_type).with_nullable(is_nullable),
        );

        let info = FieldSchemaInfo {
            schema: field_schema.clone(),
            decoder: decoder.clone(),
        };

        if let Some(ord_col) = ordinal_column {
            if &col_name == ord_col {
                ordinal_field_schema = Some(info.clone());
                if is_primary_key {
                    api_bail!(
                        "`ordinal_column` cannot be a primary key column. It must be one of the value columns."
                    );
                }
            }
        }

        if is_primary_key {
            primary_key_columns.push(info);
        } else if included_columns
            .as_ref()
            .map_or(true, |cols| cols.contains(&col_name))
        {
            value_columns.push(info.clone());
        }
    }

    if primary_key_columns.is_empty() {
        if value_columns.is_empty() {
            api_bail!("Table `{table_name}` not found");
        }
        api_bail!("Table `{table_name}` has no primary key defined");
    }

    // If ordinal column specified, validate and compute its index within value columns if present
    let ordinal_field_idx = match ordinal_column {
        Some(ord) => {
            let schema = ordinal_field_schema
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("`ordinal_column` `{}` not found in table", ord))?;
            if !is_supported_ordinal_type(&schema.schema.value_type.typ) {
                api_bail!(
                    "Unsupported `ordinal_column` type for `{}`. Supported types: Int64, LocalDateTime, OffsetDateTime",
                    schema.schema.name
                );
            }
            value_columns.iter().position(|c| c.schema.name == *ord)
        }
        None => None,
    };

    Ok(PostgresTableSchema {
        primary_key_columns,
        value_columns,
        ordinal_field_idx,
        ordinal_field_schema,
    })
}

// Per-column decoders are attached to schema; no generic converter needed anymore

/// Convert a CocoIndex `Value` into an `Ordinal` if supported.
/// Supported inputs:
/// - Basic(Int64): interpreted directly as microseconds
/// - Basic(LocalDateTime): converted to UTC micros
/// - Basic(OffsetDateTime): micros since epoch
/// Otherwise returns unavailable.
fn is_supported_ordinal_type(t: &ValueType) -> bool {
    matches!(
        t,
        ValueType::Basic(BasicValueType::Int64)
            | ValueType::Basic(BasicValueType::LocalDateTime)
            | ValueType::Basic(BasicValueType::OffsetDateTime)
    )
}

fn value_to_ordinal(value: &Value) -> Ordinal {
    match value {
        Value::Null => Ordinal::unavailable(),
        Value::Basic(basic) => match basic {
            crate::base::value::BasicValue::Int64(v) => Ordinal(Some(*v)),
            crate::base::value::BasicValue::LocalDateTime(dt) => {
                Ordinal(Some(dt.and_utc().timestamp_micros()))
            }
            crate::base::value::BasicValue::OffsetDateTime(dt) => {
                Ordinal(Some(dt.timestamp_micros()))
            }
            _ => Ordinal::unavailable(),
        },
        _ => Ordinal::unavailable(),
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list(
        &self,
        options: &SourceExecutorListOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRowMetadata>>>> {
        let stream = try_stream! {
            // Build query to select primary key columns
            let pk_columns: Vec<String> = self
                .table_schema
                .primary_key_columns
                .iter()
                .map(|col| format!("\"{}\"", col.schema.name))
                .collect();

            let mut select_parts = pk_columns.clone();
            let mut ordinal_col_index: Option<usize> = None;
            if options.include_ordinal
                && let Some(ord_schema) = &self.table_schema.ordinal_field_schema
            {
                // Only append ordinal column if present.
                select_parts.push(format!("\"{}\"", ord_schema.schema.name));
                ordinal_col_index = Some(select_parts.len() - 1);
            }

            let mut query = format!(
                "SELECT {} FROM \"{}\"",
                select_parts.join(", "),
                self.table_name
            );

            // Add ordering by ordinal column if specified
            if let Some(ord_schema) = &self.table_schema.ordinal_field_schema {
                query.push_str(&format!(" ORDER BY \"{}\"", ord_schema.schema.name));
            }

            let mut rows = sqlx::query(&query).fetch(&self.db_pool);
            while let Some(row) = rows.try_next().await? {
                let parts = self
                    .table_schema
                    .primary_key_columns
                    .iter()
                    .enumerate()
                    .map(|(i, info)| (info.decoder)(&row, i)?.into_key())
                    .collect::<Result<Box<[KeyValue]>>>()?;
                let key = FullKeyValue(parts);

                // Compute ordinal if requested
                let ordinal = if options.include_ordinal {
                    if let (Some(col_idx), Some(_ord_schema)) = (
                        ordinal_col_index,
                        self.table_schema.ordinal_field_schema.as_ref(),
                    ) {
                        let val = match self.table_schema.ordinal_field_idx {
                            Some(idx) => (self.table_schema.value_columns[idx].decoder)(&row, col_idx)?,
                            None => (self.table_schema.ordinal_field_schema.as_ref().unwrap().decoder)(&row, col_idx)?,
                        };
                        Some(value_to_ordinal(&val))
                    } else {
                        Some(Ordinal::unavailable())
                    }
                } else {
                    None
                };

                yield vec![PartialSourceRowMetadata {
                    key,
                    key_aux_info: serde_json::Value::Null,
                    ordinal,
                    content_version_fp: None,
                }];
            }
        };
        Ok(stream.boxed())
    }

    async fn get_value(
        &self,
        key: &FullKeyValue,
        _key_aux_info: &serde_json::Value,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData> {
        let mut qb = sqlx::QueryBuilder::new("SELECT ");
        let mut selected_columns: Vec<String> = Vec::new();

        if options.include_value {
            selected_columns.extend(
                self.table_schema
                    .value_columns
                    .iter()
                    .map(|col| format!("\"{}\"", col.schema.name)),
            );
        }

        if options.include_ordinal {
            if let Some(ord_schema) = &self.table_schema.ordinal_field_schema {
                // Append ordinal column if not already provided by included value columns,
                // or when value columns are not selected at all
                if self.table_schema.ordinal_field_idx.is_none() || !options.include_value {
                    selected_columns.push(format!("\"{}\"", ord_schema.schema.name));
                }
            }
        }

        if selected_columns.is_empty() {
            qb.push("1");
        } else {
            qb.push(selected_columns.join(", "));
        }
        qb.push(" FROM \"");
        qb.push(&self.table_name);
        qb.push("\" WHERE ");

        if key.len() != self.table_schema.primary_key_columns.len() {
            bail!(
                "Composite key has {} values but table has {} primary key columns",
                key.len(),
                self.table_schema.primary_key_columns.len()
            );
        }

        for (i, (pk_col, key_value)) in self
            .table_schema
            .primary_key_columns
            .iter()
            .zip(key.iter())
            .enumerate()
        {
            if i > 0 {
                qb.push(" AND ");
            }
            qb.push("\"");
            qb.push(pk_col.schema.name.as_str());
            qb.push("\" = ");
            bind_key_field(&mut qb, key_value)?;
        }

        let row_opt = qb.build().fetch_optional(&self.db_pool).await?;

        let value = if options.include_value {
            match &row_opt {
                Some(row) => {
                    let mut fields = Vec::with_capacity(self.table_schema.value_columns.len());
                    for (i, info) in self.table_schema.value_columns.iter().enumerate() {
                        let value = (info.decoder)(&row, i)?;
                        fields.push(value);
                    }
                    Some(SourceValue::Existence(FieldValues { fields }))
                }
                None => Some(SourceValue::NonExistence),
            }
        } else {
            None
        };

        let ordinal = if options.include_ordinal {
            match (&row_opt, &self.table_schema.ordinal_field_schema) {
                (Some(row), Some(ord_schema)) => {
                    // Determine index without scanning the row metadata.
                    let col_index = if options.include_value {
                        match self.table_schema.ordinal_field_idx {
                            Some(idx) => idx,
                            None => self.table_schema.value_columns.len(),
                        }
                    } else {
                        // Only ordinal was selected
                        0
                    };
                    let val = (ord_schema.decoder)(&row, col_index)?;
                    Some(value_to_ordinal(&val))
                }
                _ => Some(Ordinal::unavailable()),
            }
        } else {
            None
        };

        Ok(PartialSourceRowData {
            value,
            ordinal,
            content_version_fp: None,
        })
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "Postgres"
    }

    async fn get_output_schema(
        &self,
        spec: &Spec,
        context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType> {
        // Fetch table schema to build dynamic output schema
        let db_pool = get_db_pool(spec.database.as_ref(), &context.auth_registry).await?;
        let table_schema = fetch_table_schema(
            &db_pool,
            &spec.table_name,
            &spec.included_columns,
            &spec.ordinal_column,
        )
        .await?;

        Ok(make_output_type(TableSchema::new(
            TableKind::KTable(KTableInfo {
                num_key_parts: table_schema.primary_key_columns.len(),
            }),
            StructSchema {
                fields: Arc::new(
                    (table_schema.primary_key_columns.into_iter().map(|pk_col| {
                        FieldSchema::new(&pk_col.schema.name, pk_col.schema.value_type)
                    }))
                    .chain(table_schema.value_columns.into_iter().map(|value_col| {
                        FieldSchema::new(&value_col.schema.name, value_col.schema.value_type)
                    }))
                    .collect(),
                ),
                description: None,
            },
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        let db_pool = get_db_pool(spec.database.as_ref(), &context.auth_registry).await?;

        // Fetch table schema for dynamic type handling
        let table_schema = fetch_table_schema(
            &db_pool,
            &spec.table_name,
            &spec.included_columns,
            &spec.ordinal_column,
        )
        .await?;

        let executor = Executor {
            db_pool,
            table_name: spec.table_name.clone(),
            table_schema,
        };

        Ok(Box::new(executor))
    }
}
