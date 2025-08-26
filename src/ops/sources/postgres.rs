use crate::ops::sdk::*;

use crate::fields_value;
use crate::ops::shared::postgres::get_db_pool;
use crate::settings::DatabaseConnectionSpec;
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
struct ColumnInfo {
    name: String,
    data_type: String,
    is_nullable: bool,
    is_primary_key: bool,
}

#[derive(Debug, Clone)]
struct PostgresTableSchema {
    primary_key_columns: Vec<ColumnInfo>,
    value_columns: Vec<ColumnInfo>,
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
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
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
        let column_info = ColumnInfo {
            name: row.try_get::<String, _>("column_name")?,
            data_type: row.try_get::<String, _>("data_type")?,
            is_nullable: row.try_get::<String, _>("is_nullable")? == "YES",
            is_primary_key: row.try_get::<bool, _>("is_primary_key")?,
        };

        // Always include primary key columns
        if column_info.is_primary_key {
            primary_key_columns.push(column_info.clone());
        } else {
            // For value columns, check if filtering is enabled
            let should_include = match included_columns {
                Some(included_list) => included_list.contains(&column_info.name),
                None => true, // Include all columns if no filter specified
            };

            if should_include {
                value_columns.push(column_info);
            }
        }
    }

    if primary_key_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table '{}' has no primary key defined",
            table_name
        ));
    }

    Ok(PostgresTableSchema {
        primary_key_columns,
        value_columns,
    })
}

/// Convert a PostgreSQL row value to a serde_json::Value based on the column type
fn convert_pg_value_to_json(
    row: &sqlx::postgres::PgRow,
    column: &ColumnInfo,
    col_index: usize,
) -> Result<serde_json::Value> {
    // Check for null values in a type-agnostic way based on the actual column type
    let is_null = match map_postgres_type_to_cocoindex(&column.data_type) {
        BasicValueType::Uuid => row.try_get::<Option<uuid::Uuid>, _>(col_index)?.is_none(),
        BasicValueType::Int64 => row.try_get::<Option<i64>, _>(col_index)?.is_none(),
        BasicValueType::Bool => row.try_get::<Option<bool>, _>(col_index)?.is_none(),
        BasicValueType::Float32 => row.try_get::<Option<f32>, _>(col_index)?.is_none(),
        BasicValueType::Float64 => row.try_get::<Option<f64>, _>(col_index)?.is_none(),
        _ => row.try_get::<Option<String>, _>(col_index)?.is_none(), // Default to string check
    };

    if is_null {
        return Ok(serde_json::Value::Null);
    }

    match map_postgres_type_to_cocoindex(&column.data_type) {
        BasicValueType::Bytes => {
            let bytes: Vec<u8> = row.try_get(col_index)?;
            Ok(serde_json::to_value(bytes)?)
        }
        BasicValueType::Str => {
            let s: String = row.try_get(col_index)?;
            Ok(serde_json::Value::String(s))
        }
        BasicValueType::Bool => {
            let b: bool = row.try_get(col_index)?;
            Ok(serde_json::Value::Bool(b))
        }
        BasicValueType::Int64 => {
            let i: i64 = row.try_get(col_index)?;
            Ok(serde_json::Value::Number(i.into()))
        }
        BasicValueType::Float32 => {
            let f: f32 = row.try_get(col_index)?;
            Ok(serde_json::to_value(f)?)
        }
        BasicValueType::Float64 => {
            let f: f64 = row.try_get(col_index)?;
            Ok(serde_json::to_value(f)?)
        }
        BasicValueType::Uuid => {
            let uuid: uuid::Uuid = row.try_get(col_index)?;
            Ok(serde_json::Value::String(uuid.to_string()))
        }
        BasicValueType::Date => {
            let date: chrono::NaiveDate = row.try_get(col_index)?;
            Ok(serde_json::Value::String(date.to_string()))
        }
        BasicValueType::Time => {
            let time: chrono::NaiveTime = row.try_get(col_index)?;
            Ok(serde_json::Value::String(time.to_string()))
        }
        BasicValueType::LocalDateTime => {
            let dt: chrono::NaiveDateTime = row.try_get(col_index)?;
            Ok(serde_json::Value::String(dt.to_string()))
        }
        BasicValueType::OffsetDateTime => {
            let dt: chrono::DateTime<chrono::Utc> = row.try_get(col_index)?;
            Ok(serde_json::Value::String(dt.to_rfc3339()))
        }
        BasicValueType::TimeDelta => {
            // PostgreSQL interval to string representation
            let interval: String = row.try_get(col_index)?;
            Ok(serde_json::Value::String(interval))
        }
        BasicValueType::Json => {
            let json: serde_json::Value = row.try_get(col_index)?;
            Ok(json)
        }
        _ => {
            // Fallback: convert to string
            let s: String = row.try_get(col_index)?;
            Ok(serde_json::Value::String(s))
        }
    }
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

            info!("Executing query: {}", query);

            let mut rows = sqlx::query(&query).fetch(&self.db_pool);
            let mut batch: Vec<PartialSourceRowMetadata> = Vec::new();
            let batch_size = 1000; // Process in batches

            while let Some(row) = rows.try_next().await? {
                // Handle both single and composite primary keys
                let key = if self.table_schema.primary_key_columns.len() == 1 {
                    // Single primary key - extract directly
                    let pk_col = &self.table_schema.primary_key_columns[0];
                    let json_value = convert_pg_value_to_json(&row, pk_col, 0)?;

                    match map_postgres_type_to_cocoindex(&pk_col.data_type) {
                        BasicValueType::Str => KeyValue::Str(json_value.as_str().unwrap_or("").to_string().into()),
                        BasicValueType::Int64 => KeyValue::Int64(json_value.as_i64().unwrap_or(0)),
                        BasicValueType::Uuid => {
                            let uuid_str = json_value.as_str().unwrap_or("");
                            KeyValue::Uuid(uuid::Uuid::parse_str(uuid_str).unwrap_or_default())
                        },
                        _ => {
                            // For other types, convert to string representation
                            KeyValue::Str(json_value.to_string().into())
                        }
                    }
                } else {
                    // Composite primary key - create a struct with individual KeyValue fields
                    let mut key_values = Vec::new();
                    for (i, pk_col) in self.table_schema.primary_key_columns.iter().enumerate() {
                        let json_value = convert_pg_value_to_json(&row, pk_col, i)?;

                        // Convert each primary key column to appropriate KeyValue type
                        let key_value = match map_postgres_type_to_cocoindex(&pk_col.data_type) {
                            BasicValueType::Str => KeyValue::Str(json_value.as_str().unwrap_or("").to_string().into()),
                            BasicValueType::Int64 => KeyValue::Int64(json_value.as_i64().unwrap_or(0)),
                            BasicValueType::Uuid => {
                                let uuid_str = json_value.as_str().unwrap_or("");
                                KeyValue::Uuid(uuid::Uuid::parse_str(uuid_str).unwrap_or_default())
                            },
                            BasicValueType::Bool => KeyValue::Bool(json_value.as_bool().unwrap_or(false)),
                            BasicValueType::Date => {
                                let date_str = json_value.as_str().unwrap_or("");
                                KeyValue::Date(chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap_or_default())
                            },
                            _ => {
                                // For other types, convert to string representation
                                KeyValue::Str(json_value.to_string().into())
                            }
                        };
                        key_values.push(key_value);
                    }
                    KeyValue::Struct(key_values)
                };

                batch.push(PartialSourceRowMetadata {
                    key,
                    key_aux_info: serde_json::Value::Null,
                    ordinal: Some(Ordinal::unavailable()),
                    content_version_fp: None,
                });

                if batch.len() >= batch_size {
                    yield batch;
                    batch = Vec::new();
                }
            }

            if !batch.is_empty() {
                yield batch;
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
        // Select ONLY value columns (non-primary key columns) for get_value() return
        let value_columns: Vec<String> = self
            .table_schema
            .value_columns
            .iter()
            .map(|col| format!("\"{}\"", col.name))
            .collect();
        let simple_query = if self.table_schema.primary_key_columns.len() == 1 {
            let pk_col = &self.table_schema.primary_key_columns[0];
            let key_condition = match map_postgres_type_to_cocoindex(&pk_col.data_type) {
                BasicValueType::Uuid => {
                    // For UUID keys, extract the UUID value directly
                    let uuid_val = match key {
                        KeyValue::Uuid(uuid) => uuid,
                        _ => return Err(anyhow::anyhow!("Expected UUID key, got {:?}", key)),
                    };
                    format!("\"{}\" = '{}'", pk_col.name, uuid_val)
                }
                BasicValueType::Str => {
                    // For string keys, use string comparison
                    let key_str = key.str_value()?;
                    format!("\"{}\" = '{}'", pk_col.name, key_str.replace("'", "''"))
                }
                BasicValueType::Int64 => {
                    // For integer keys, use numeric comparison
                    let key_int = key.int64_value()?;
                    format!("\"{}\" = {}", pk_col.name, key_int)
                }
                _ => {
                    // For other types, convert to string
                    let key_str = key.to_string();
                    format!("\"{}\" = '{}'", pk_col.name, key_str.replace("'", "''"))
                }
            };
            format!(
                "SELECT {} FROM \"{}\" WHERE {}",
                if value_columns.is_empty() {
                    "1".to_string()
                } else {
                    value_columns.join(", ")
                },
                self.table_name,
                key_condition
            )
        } else {
            // For composite keys, we need to parse and build the query
            let mut conditions = Vec::new();

            match key {
                KeyValue::Struct(key_values) => {
                    // Handle struct keys (composite primary keys)
                    if key_values.len() != self.table_schema.primary_key_columns.len() {
                        return Err(anyhow::anyhow!(
                            "Composite key has {} values but table has {} primary key columns",
                            key_values.len(),
                            self.table_schema.primary_key_columns.len()
                        ));
                    }

                    for (i, (pk_col, key_value)) in self
                        .table_schema
                        .primary_key_columns
                        .iter()
                        .zip(key_values.iter())
                        .enumerate()
                    {
                        let condition = match map_postgres_type_to_cocoindex(&pk_col.data_type) {
                            BasicValueType::Uuid => {
                                let uuid_val = match key_value {
                                    KeyValue::Uuid(uuid) => uuid,
                                    _ => {
                                        return Err(anyhow::anyhow!(
                                            "Expected UUID key value at position {}, got {:?}",
                                            i,
                                            key_value
                                        ));
                                    }
                                };
                                format!("\"{}\" = '{}'", pk_col.name, uuid_val)
                            }
                            BasicValueType::Str => {
                                let key_str = key_value.str_value()?;
                                format!("\"{}\" = '{}'", pk_col.name, key_str.replace("'", "''"))
                            }
                            BasicValueType::Int64 => {
                                let key_int = key_value.int64_value()?;
                                format!("\"{}\" = {}", pk_col.name, key_int)
                            }
                            BasicValueType::Bool => {
                                let key_bool = key_value.bool_value()?;
                                format!("\"{}\" = {}", pk_col.name, key_bool)
                            }
                            BasicValueType::Date => {
                                let key_date = key_value.date_value()?;
                                format!("\"{}\" = '{}'", pk_col.name, key_date)
                            }
                            _ => {
                                // For other types, convert to string
                                let key_str = key_value.to_string();
                                format!("\"{}\" = '{}'", pk_col.name, key_str.replace("'", "''"))
                            }
                        };
                        conditions.push(condition);
                    }
                }
                KeyValue::Str(s) => {
                    // Fallback: try to parse as JSON string for backward compatibility
                    let key_obj: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_str(&s.to_string())?;
                    for pk_col in &self.table_schema.primary_key_columns {
                        if let Some(value) = key_obj.get(&pk_col.name) {
                            let value_str = match value {
                                serde_json::Value::String(s) => s.clone(),
                                _ => value.to_string(),
                            };
                            conditions.push(format!(
                                "\"{}\" = '{}'",
                                pk_col.name,
                                value_str.replace("'", "''")
                            ));
                        }
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected struct or string key for composite primary key, got {:?}",
                        key
                    ));
                }
            }

            format!(
                "SELECT {} FROM \"{}\" WHERE {}",
                if value_columns.is_empty() {
                    "1".to_string()
                } else {
                    value_columns.join(", ")
                },
                self.table_name,
                conditions.join(" AND ")
            )
        };

        let row_opt = sqlx::query(&simple_query)
            .fetch_optional(&self.db_pool)
            .await?;

        let ordinal = if options.include_ordinal {
            Some(Ordinal::unavailable())
        } else {
            None
        };

        let value = if options.include_value {
            match row_opt {
                Some(row) => {
                    // Return value columns as individual fields (like Google Drive)
                    if value_columns.is_empty() {
                        // If no value columns, just indicate existence
                        Some(SourceValue::Existence(fields_value!(true)))
                    } else {
                        // Return each value column as a separate field
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

                            let json_value = convert_pg_value_to_json(&row, value_col, col_index)?;

                            // Convert each column value to a field, similar to Google Drive
                            match json_value {
                                serde_json::Value::String(s) => fields.push(s.into()),
                                serde_json::Value::Number(n) => fields.push(n.to_string().into()),
                                serde_json::Value::Bool(b) => fields.push(b.to_string().into()),
                                serde_json::Value::Null => fields.push("".to_string().into()),
                                _ => fields.push(json_value.to_string().into()),
                            }
                        }
                        Some(SourceValue::Existence(FieldValues { fields }))
                    }
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
            // Single primary key - first field is the key
            let pk_col = &table_schema.primary_key_columns[0];
            let cocoindex_type = map_postgres_type_to_cocoindex(&pk_col.data_type);
            let field_type = if pk_col.is_nullable {
                make_output_type(cocoindex_type).with_nullable(true)
            } else {
                make_output_type(cocoindex_type)
            };

            schema_builder.add_field(FieldSchema::new(&pk_col.name, field_type));
        } else {
            // Composite primary key - first field is _key containing all PK columns
            let mut key_struct_schema = StructSchema::default();
            let mut key_builder = StructSchemaBuilder::new(&mut key_struct_schema);

            for pk_col in &table_schema.primary_key_columns {
                let cocoindex_type = map_postgres_type_to_cocoindex(&pk_col.data_type);
                let field_type = if pk_col.is_nullable {
                    make_output_type(cocoindex_type).with_nullable(true)
                } else {
                    make_output_type(cocoindex_type)
                };

                key_builder.add_field(FieldSchema::new(&pk_col.name, field_type));
            }

            // Add _key field containing the composite primary key
            schema_builder.add_field(FieldSchema::new(
                "_key",
                make_output_type(key_struct_schema),
            ));
        }

        // Add value columns as fields (these match what get_value() returns)
        for value_col in &table_schema.value_columns {
            let cocoindex_type = map_postgres_type_to_cocoindex(&value_col.data_type);
            let field_type = if value_col.is_nullable {
                make_output_type(cocoindex_type).with_nullable(true)
            } else {
                make_output_type(cocoindex_type)
            };

            schema_builder.add_field(FieldSchema::new(&value_col.name, field_type));
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
