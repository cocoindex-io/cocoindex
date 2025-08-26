use crate::ops::sdk::*;

use crate::ops::shared::postgres::{bind_key_field, get_db_pool, key_value_fields_iter};
use crate::settings::DatabaseConnectionSpec;
use sqlx::postgres::types::PgInterval;
use sqlx::{Column, PgPool, Row};

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

#[derive(Debug, Clone)]
struct PostgresTableSchema {
    primary_key_columns: Vec<FieldSchema>,
    value_columns: Vec<FieldSchema>,
}

struct Executor {
    db_pool: PgPool,
    table_name: String,
    ordinal_column: Option<String>,
    table_schema: PostgresTableSchema,
}

/// Map PostgreSQL data types to CocoIndex BasicValueType
fn map_postgres_type_to_cocoindex(pg_type: &str) -> BasicValueType {
    match pg_type {
        "bytea" => BasicValueType::Bytes,
        "text" | "varchar" | "char" | "character" | "character varying" => BasicValueType::Str,
        "boolean" | "bool" => BasicValueType::Bool,
        "bigint" | "int8" | "integer" | "int4" | "smallint" | "int2" => BasicValueType::Int64,
        "real" | "float4" => BasicValueType::Float32,
        "double precision" | "float8" => BasicValueType::Float64,
        "uuid" => BasicValueType::Uuid,
        "date" => BasicValueType::Date,
        "time" | "time without time zone" => BasicValueType::Time,
        "timestamp" | "timestamp without time zone" => BasicValueType::LocalDateTime,
        "timestamp with time zone" | "timestamptz" => BasicValueType::OffsetDateTime,
        "interval" => BasicValueType::TimeDelta,
        "jsonb" | "json" => BasicValueType::Json,
        // Vector types (if supported)
        t if t.starts_with("vector(") => BasicValueType::Json, // fallback to JSON
        // Union types and others fallback to JSON
        _ => BasicValueType::Json,
    }
}

/// Fetch table schema information from PostgreSQL
async fn fetch_table_schema(
    pool: &PgPool,
    table_name: &str,
    included_columns: &Option<Vec<String>>,
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

    let mut primary_key_columns = Vec::new();
    let mut value_columns = Vec::new();

    for row in rows {
        let col_name: String = row.try_get::<String, _>("column_name")?;
        let pg_type_str: String = row.try_get::<String, _>("data_type")?;
        let is_nullable: bool = row.try_get::<String, _>("is_nullable")? == "YES";
        let is_primary_key: bool = row.try_get::<bool, _>("is_primary_key")?;

        let field_schema = FieldSchema::new(
            &col_name,
            make_output_type(map_postgres_type_to_cocoindex(&pg_type_str))
                .with_nullable(is_nullable),
        );

        if is_primary_key {
            primary_key_columns.push(field_schema);
        } else if included_columns
            .as_ref()
            .map_or(true, |cols| cols.contains(&col_name))
        {
            value_columns.push(field_schema);
        }
    }

    if primary_key_columns.is_empty() {
        if value_columns.is_empty() {
            api_bail!("Table `{table_name}` not found");
        }
        api_bail!("Table `{table_name}` has no primary key defined");
    }

    Ok(PostgresTableSchema {
        primary_key_columns,
        value_columns,
    })
}

/// Convert a PostgreSQL row value directly into Value (Basic or Null)
fn convert_pg_value_to_value(
    row: &sqlx::postgres::PgRow,
    column: &FieldSchema,
    col_index: usize,
) -> Result<Value> {
    let basic_type = match &column.value_type.typ {
        ValueType::Basic(t) => t,
        _ => bail!("expect basic value type"),
    };
    let value = match basic_type {
        BasicValueType::Bytes => Value::from(row.try_get::<Option<Vec<u8>>, _>(col_index)?),
        BasicValueType::Str => Value::from(row.try_get::<Option<String>, _>(col_index)?),
        BasicValueType::Bool => Value::from(row.try_get::<Option<bool>, _>(col_index)?),
        BasicValueType::Int64 => Value::from(row.try_get::<Option<i64>, _>(col_index)?),
        BasicValueType::Float32 => Value::from(row.try_get::<Option<f32>, _>(col_index)?),
        BasicValueType::Float64 => Value::from(row.try_get::<Option<f64>, _>(col_index)?),
        BasicValueType::Range => {
            Value::from(row.try_get::<Option<serde_json::Value>, _>(col_index)?)
        }
        BasicValueType::Uuid => Value::from(row.try_get::<Option<uuid::Uuid>, _>(col_index)?),
        BasicValueType::Date => {
            Value::from(row.try_get::<Option<chrono::NaiveDate>, _>(col_index)?)
        }
        BasicValueType::Time => {
            Value::from(row.try_get::<Option<chrono::NaiveTime>, _>(col_index)?)
        }
        BasicValueType::LocalDateTime => {
            Value::from(row.try_get::<Option<chrono::NaiveDateTime>, _>(col_index)?)
        }
        BasicValueType::OffsetDateTime => {
            Value::from(row.try_get::<Option<chrono::DateTime<chrono::FixedOffset>>, _>(col_index)?)
        }
        BasicValueType::TimeDelta => {
            let opt_iv = row.try_get::<Option<PgInterval>, _>(col_index)?;
            let opt_dur = opt_iv.map(|iv| {
                let approx_days = iv.days as i64 + (iv.months as i64) * 30;
                chrono::Duration::microseconds(iv.microseconds)
                    + chrono::Duration::days(approx_days)
            });
            Value::from(opt_dur)
        }
        BasicValueType::Json => {
            Value::from(row.try_get::<Option<serde_json::Value>, _>(col_index)?)
        }
        // Fallback: treat as JSON
        _ => Value::from(row.try_get::<Option<serde_json::Value>, _>(col_index)?),
    };
    Ok(value)
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list(
        &self,
        _options: &SourceExecutorListOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRowMetadata>>>> {
        let stream = try_stream! {
            // Build query to select primary key columns
            let pk_columns: Vec<String> = self.table_schema.primary_key_columns
                .iter()
                .map(|col| format!("\"{}\"", col.name))
                .collect();

            let mut query = format!("SELECT {} FROM \"{}\"", pk_columns.join(", "), self.table_name);

            // Add ordering by ordinal column if specified
            if let Some(ref ordinal_col) = self.ordinal_column {
                query.push_str(&format!(" ORDER BY \"{}\"", ordinal_col));
            }

            let mut rows = sqlx::query(&query).fetch(&self.db_pool);
            while let Some(row) = rows.try_next().await? {
                let parts = self
                    .table_schema
                    .primary_key_columns
                    .iter()
                    .enumerate()
                    .map(|(i, pk_col)| convert_pg_value_to_value(&row, pk_col, i))
                    .collect::<Result<Vec<_>>>()?;
                if parts.iter().any(|v| v.is_null()) {
                    Err(anyhow::anyhow!("Composite primary key contains NULL component"))?;
                }
                let key = KeyValue::from_values(parts.iter())?;

                yield vec![PartialSourceRowMetadata {
                    key,
                    key_aux_info: serde_json::Value::Null,
                    ordinal: Some(Ordinal::unavailable()),
                    content_version_fp: None,
                }];
            }
        };
        Ok(stream.boxed())
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        _key_aux_info: &serde_json::Value,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData> {
        let mut qb = sqlx::QueryBuilder::new("SELECT ");
        if self.table_schema.value_columns.is_empty() {
            qb.push("1");
        } else {
            qb.push(
                self.table_schema
                    .value_columns
                    .iter()
                    .map(|col| format!("\"{}\"", col.name))
                    .collect::<Vec<String>>()
                    .join(", "),
            );
        }
        qb.push(" FROM \"");
        qb.push(&self.table_name);
        qb.push("\" WHERE ");

        let key_values = key_value_fields_iter(&self.table_schema.primary_key_columns, key)?;
        if key_values.len() != self.table_schema.primary_key_columns.len() {
            bail!(
                "Composite key has {} values but table has {} primary key columns",
                key_values.len(),
                self.table_schema.primary_key_columns.len()
            );
        }

        for (i, (pk_col, key_value)) in self
            .table_schema
            .primary_key_columns
            .iter()
            .zip(key_values.iter())
            .enumerate()
        {
            if i > 0 {
                qb.push(" AND ");
            }
            qb.push("\"");
            qb.push(pk_col.name.as_str());
            qb.push("\" = ");
            bind_key_field(&mut qb, key_value)?;
        }

        let row_opt = qb.build().fetch_optional(&self.db_pool).await?;

        let ordinal = if options.include_ordinal {
            Some(Ordinal::unavailable())
        } else {
            None
        };

        let value = if options.include_value {
            match row_opt {
                Some(row) => {
                    let mut fields = Vec::new();
                    for value_col in &self.table_schema.value_columns {
                        // Find the column by name in the result row to get the correct index
                        let col_index = row
                            .columns()
                            .iter()
                            .position(|col| col.name() == value_col.name.as_str())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Column '{}' not found in result row",
                                    value_col.name
                                )
                            })?;

                        let value = convert_pg_value_to_value(&row, value_col, col_index)?;
                        fields.push(value);
                    }
                    Some(SourceValue::Existence(FieldValues { fields }))
                }
                None => Some(SourceValue::NonExistence),
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
        let table_schema =
            fetch_table_schema(&db_pool, &spec.table_name, &spec.included_columns).await?;

        let mut struct_schema = StructSchema::default();
        let mut schema_builder = StructSchemaBuilder::new(&mut struct_schema);

        // For KTable, first field is the key, remaining fields are values
        // This matches the KTable schema: KTable<KeyStruct, ValueStruct>

        if table_schema.primary_key_columns.len() == 1 {
            let pk_col = &table_schema.primary_key_columns[0];
            schema_builder.add_field(FieldSchema::new(&pk_col.name, pk_col.value_type.clone()));
        } else {
            // Composite primary key - first field is _key containing all PK columns
            let mut key_struct_schema = StructSchema::default();
            let mut key_builder = StructSchemaBuilder::new(&mut key_struct_schema);

            for pk_col in &table_schema.primary_key_columns {
                key_builder.add_field(FieldSchema::new(&pk_col.name, pk_col.value_type.clone()));
            }

            // Add _key field containing the composite primary key
            schema_builder.add_field(FieldSchema::new(
                "_key",
                make_output_type(key_struct_schema),
            ));
        }

        for value_col in &table_schema.value_columns {
            schema_builder.add_field(FieldSchema::new(
                &value_col.name,
                value_col.value_type.clone(),
            ));
        }

        // Log schema information for debugging
        if table_schema.primary_key_columns.len() > 1 {
            info!(
                "Composite primary key detected: {} columns",
                table_schema.primary_key_columns.len()
            );
        }

        Ok(make_output_type(TableSchema::new(
            TableKind::KTable,
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        let db_pool = get_db_pool(spec.database.as_ref(), &context.auth_registry).await?;

        // Fetch table schema for dynamic type handling
        let table_schema =
            fetch_table_schema(&db_pool, &spec.table_name, &spec.included_columns).await?;

        let executor = Executor {
            db_pool,
            table_name: spec.table_name.clone(),
            ordinal_column: spec.ordinal_column.clone(),
            table_schema,
        };

        let filter_info = match &spec.included_columns {
            Some(cols) => format!(" (filtered to {} specified columns)", cols.len()),
            None => " (all columns)".to_string(),
        };

        info!(
            "Successfully connected to PostgreSQL table '{}' with {} primary key columns and {} value columns{}",
            spec.table_name,
            executor.table_schema.primary_key_columns.len(),
            executor.table_schema.value_columns.len(),
            filter_info
        );

        Ok(Box::new(executor))
    }
}
