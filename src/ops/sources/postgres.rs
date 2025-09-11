use crate::ops::sdk::*;

use crate::ops::shared::postgres::{bind_key_field, get_db_pool};
use crate::settings::DatabaseConnectionSpec;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use indoc::formatdoc;
use sqlx::postgres::types::PgInterval;
use sqlx::postgres::{PgListener, PgNotification};
use sqlx::{PgPool, Row};

type PgValueDecoder = fn(&sqlx::postgres::PgRow, usize) -> Result<Value>;

#[derive(Clone)]
struct FieldSchemaInfo {
    schema: FieldSchema,
    decoder: PgValueDecoder,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotificationSpec {
    channel_name: Option<String>,
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
    /// Optional: notification for change capture
    notification: Option<NotificationSpec>,
}

#[derive(Clone)]
struct PostgresTableSchema {
    primary_key_columns: Vec<FieldSchemaInfo>,
    value_columns: Vec<FieldSchemaInfo>,
    ordinal_field_idx: Option<usize>,
    ordinal_field_schema: Option<FieldSchemaInfo>,
}

struct NotificationContext {
    channel_name: String,
    function_name: String,
    trigger_name: String,
}

struct PostgresSourceExecutor {
    db_pool: PgPool,
    table_name: String,
    table_schema: PostgresTableSchema,
    notification_ctx: Option<NotificationContext>,
}

impl PostgresSourceExecutor {
    /// Append value and ordinal columns to the provided columns vector.
    /// Returns the optional index of the ordinal column in the final selection.
    fn build_selected_columns(
        &self,
        columns: &mut Vec<String>,
        options: &SourceExecutorReadOptions,
    ) -> Option<usize> {
        let base_len = columns.len();
        if options.include_value {
            columns.extend(
                self.table_schema
                    .value_columns
                    .iter()
                    .map(|col| format!("\"{}\"", col.schema.name)),
            );
        }

        if options.include_ordinal {
            if let Some(ord_schema) = &self.table_schema.ordinal_field_schema {
                if options.include_value {
                    if let Some(val_idx) = self.table_schema.ordinal_field_idx {
                        return Some(base_len + val_idx);
                    }
                }
                columns.push(format!("\"{}\"", ord_schema.schema.name));
                return Some(columns.len() - 1);
            }
        }

        None
    }

    /// Decode all value columns from a row, starting at the given index offset.
    fn decode_row_data(
        &self,
        row: &sqlx::postgres::PgRow,
        options: &SourceExecutorReadOptions,
        ordinal_col_index: Option<usize>,
        value_start_idx: usize,
    ) -> Result<PartialSourceRowData> {
        let value = if options.include_value {
            let mut fields = Vec::with_capacity(self.table_schema.value_columns.len());
            for (i, info) in self.table_schema.value_columns.iter().enumerate() {
                let value = (info.decoder)(row, value_start_idx + i)?;
                fields.push(value);
            }
            Some(SourceValue::Existence(FieldValues { fields }))
        } else {
            None
        };

        let ordinal = if options.include_ordinal {
            if let (Some(idx), Some(ord_schema)) = (
                ordinal_col_index,
                self.table_schema.ordinal_field_schema.as_ref(),
            ) {
                let val = (ord_schema.decoder)(row, idx)?;
                Some(value_to_ordinal(&val))
            } else {
                Some(Ordinal::unavailable())
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
impl SourceExecutor for PostgresSourceExecutor {
    async fn list(
        &self,
        options: &SourceExecutorReadOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRow>>>> {
        let stream = try_stream! {
            // Build selection including PKs (for keys), and optionally values and ordinal
            let pk_columns: Vec<String> = self
                .table_schema
                .primary_key_columns
                .iter()
                .map(|col| format!("\"{}\"", col.schema.name))
                .collect();
            let pk_count = pk_columns.len();
            let mut select_parts = pk_columns;
            let ordinal_col_index = self.build_selected_columns(&mut select_parts, options);

            let mut query = format!("SELECT {} FROM \"{}\"", select_parts.join(", "), self.table_name);

            // Add ordering by ordinal column if specified
            if let Some(ord_schema) = &self.table_schema.ordinal_field_schema {
                query.push_str(&format!(" ORDER BY \"{}\"", ord_schema.schema.name));
            }

            let mut rows = sqlx::query(&query).fetch(&self.db_pool);
            while let Some(row) = rows.try_next().await? {
                // Decode key from PKs (selected first)
                let parts = self.table_schema.primary_key_columns
                    .iter()
                    .enumerate()
                    .map(|(i, info)| (info.decoder)(&row, i)?.into_key())
                    .collect::<Result<Box<[KeyPart]>>>()?;
                let key = KeyValue(parts);

                // Decode value and ordinal
                let data = self.decode_row_data(&row, options, ordinal_col_index, pk_count)?;

                yield vec![PartialSourceRow {
                    key,
                    key_aux_info: serde_json::Value::Null,
                    data,
                }];
            }
        };
        Ok(stream.boxed())
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        _key_aux_info: &serde_json::Value,
        options: &SourceExecutorReadOptions,
    ) -> Result<PartialSourceRowData> {
        let mut qb = sqlx::QueryBuilder::new("SELECT ");
        let mut selected_columns: Vec<String> = Vec::new();
        let ordinal_col_index = self.build_selected_columns(&mut selected_columns, options);

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
        let data = match &row_opt {
            Some(row) => self.decode_row_data(&row, options, ordinal_col_index, 0)?,
            None => PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
                content_version_fp: None,
            },
        };

        Ok(data)
    }

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
        let Some(notification_ctx) = &self.notification_ctx else {
            return Ok(None);
        };
        // Create the notification channel
        self.create_notification_function(notification_ctx).await?;

        // Set up listener
        let mut listener = PgListener::connect_with(&self.db_pool).await?;
        listener.listen(&notification_ctx.channel_name).await?;

        let stream = stream! {
            while let Ok(notification) = listener.recv().await {
                let change = self.parse_notification_payload(&notification);
                yield change.map(|change| SourceChangeMessage {
                    changes: vec![change],
                    ack_fn: None,
                });
            }
        };

        Ok(Some(stream.boxed()))
    }

    fn provides_ordinal(&self) -> bool {
        self.table_schema.ordinal_field_schema.is_some()
    }
}

impl PostgresSourceExecutor {
    async fn create_notification_function(
        &self,
        notification_ctx: &NotificationContext,
    ) -> Result<()> {
        let channel_name = &notification_ctx.channel_name;
        let function_name = &notification_ctx.function_name;
        let trigger_name = &notification_ctx.trigger_name;

        let json_object_expr = |var: &str| {
            let mut fields = (self.table_schema.primary_key_columns.iter())
                .chain(self.table_schema.ordinal_field_schema.iter())
                .map(|col| {
                    let field_name = &col.schema.name;
                    if matches!(
                        col.schema.value_type.typ,
                        ValueType::Basic(BasicValueType::Bytes)
                    ) {
                        format!("'{field_name}', encode({var}.\"{field_name}\", 'base64')")
                    } else {
                        format!("'{field_name}', {var}.\"{field_name}\"")
                    }
                });
            format!("jsonb_build_object({})", fields.join(", "))
        };

        let statements = [
            formatdoc! {r#"
            CREATE OR REPLACE FUNCTION {function_name}() RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('{channel_name}', jsonb_build_object(
                    'op', TG_OP,
                    'fields',
                    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN {json_object_expr_new}
                    WHEN TG_OP = 'DELETE' THEN {json_object_expr_old}
                    ELSE NULL END
                )::text);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            "#,
                    function_name = function_name,
                    channel_name = channel_name,
                    json_object_expr_new = json_object_expr("NEW"),
                    json_object_expr_old = json_object_expr("OLD"),
            },
            format!(
                "DROP TRIGGER IF EXISTS {trigger_name} ON \"{table_name}\";",
                trigger_name = trigger_name,
                table_name = self.table_name,
            ),
            formatdoc! {r#"
            CREATE TRIGGER {trigger_name}
                AFTER INSERT OR UPDATE OR DELETE ON "{table_name}"
                FOR EACH ROW EXECUTE FUNCTION {function_name}();
            "#,
                trigger_name = trigger_name,
                table_name = self.table_name,
                function_name = function_name,
            },
        ];

        let mut tx = self.db_pool.begin().await?;
        for stmt in statements {
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    fn parse_notification_payload(&self, notification: &PgNotification) -> Result<SourceChange> {
        let mut payload: serde_json::Value = utils::deser::from_json_str(notification.payload())?;
        let payload = payload
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("'fields' field is not an object"))?;

        let Some(serde_json::Value::String(op)) = payload.get_mut("op") else {
            return Err(anyhow::anyhow!(
                "Missing or invalid 'op' field in notification"
            ));
        };
        let op = std::mem::take(op);

        let mut fields = std::mem::take(
            payload
                .get_mut("fields")
                .ok_or_else(|| anyhow::anyhow!("Missing 'fields' field in notification"))?
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("'fields' field is not an object"))?,
        );

        // Extract primary key values to construct the key
        let mut key_parts = Vec::with_capacity(self.table_schema.primary_key_columns.len());
        for pk_col in &self.table_schema.primary_key_columns {
            let field_value = fields.get_mut(&pk_col.schema.name).ok_or_else(|| {
                anyhow::anyhow!("Missing primary key field: {}", pk_col.schema.name)
            })?;

            let key_part = Self::decode_key_ordinal_value_in_json(
                std::mem::take(field_value),
                &pk_col.schema.value_type.typ,
            )?
            .into_key()?;
            key_parts.push(key_part);
        }

        let key = KeyValue(key_parts.into_boxed_slice());

        // Extract ordinal if available
        let ordinal = if let Some(ord_schema) = &self.table_schema.ordinal_field_schema {
            if let Some(ord_value) = fields.get_mut(&ord_schema.schema.name) {
                let value = Self::decode_key_ordinal_value_in_json(
                    std::mem::take(ord_value),
                    &ord_schema.schema.value_type.typ,
                )?;
                Some(value_to_ordinal(&value))
            } else {
                Some(Ordinal::unavailable())
            }
        } else {
            None
        };

        let data = match op.as_str() {
            "DELETE" => PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal,
                content_version_fp: None,
            },
            "INSERT" | "UPDATE" => {
                // For INSERT/UPDATE, we signal that the row exists but don't include the full value
                // The engine will call get_value() to retrieve the actual data
                PartialSourceRowData {
                    value: None, // Let the engine fetch the value
                    ordinal,
                    content_version_fp: None,
                }
            }
            _ => return Err(anyhow::anyhow!("Unknown operation: {}", op)),
        };

        Ok(SourceChange {
            key,
            key_aux_info: serde_json::Value::Null,
            data,
        })
    }

    fn decode_key_ordinal_value_in_json(
        json_value: serde_json::Value,
        value_type: &ValueType,
    ) -> Result<Value> {
        let result = match (value_type, json_value) {
            (_, serde_json::Value::Null) => Value::Null,
            (ValueType::Basic(BasicValueType::Bool), serde_json::Value::Bool(b)) => {
                BasicValue::Bool(b).into()
            }
            (ValueType::Basic(BasicValueType::Bytes), serde_json::Value::String(s)) => {
                let bytes = BASE64_STANDARD.decode(&s)?;
                BasicValue::Bytes(bytes::Bytes::from(bytes)).into()
            }
            (ValueType::Basic(BasicValueType::Str), serde_json::Value::String(s)) => {
                BasicValue::Str(s.into()).into()
            }
            (ValueType::Basic(BasicValueType::Int64), serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    BasicValue::Int64(i).into()
                } else {
                    bail!("Invalid integer value: {}", n)
                }
            }
            (ValueType::Basic(BasicValueType::Uuid), serde_json::Value::String(s)) => {
                let uuid = s.parse::<uuid::Uuid>()?;
                BasicValue::Uuid(uuid).into()
            }
            (ValueType::Basic(BasicValueType::Date), serde_json::Value::String(s)) => {
                let dt = s.parse::<chrono::NaiveDate>()?;
                BasicValue::Date(dt).into()
            }
            (ValueType::Basic(BasicValueType::LocalDateTime), serde_json::Value::String(s)) => {
                let dt = s.parse::<chrono::NaiveDateTime>()?;
                BasicValue::LocalDateTime(dt).into()
            }
            (ValueType::Basic(BasicValueType::OffsetDateTime), serde_json::Value::String(s)) => {
                let dt = s.parse::<chrono::DateTime<chrono::FixedOffset>>()?;
                BasicValue::OffsetDateTime(dt).into()
            }
            (_, json_value) => {
                bail!(
                    "Got unsupported JSON value for type {value_type}: {}",
                    serde_json::to_string(&json_value)?
                );
            }
        };
        Ok(result)
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
        source_name: &str,
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

        let notification_ctx = spec.notification.map(|spec| {
            let channel_name = spec.channel_name.unwrap_or_else(|| {
                format!("{}__{}__cocoindex", context.flow_instance_name, source_name)
            });
            NotificationContext {
                function_name: format!("{channel_name}_n"),
                trigger_name: format!("{channel_name}_t"),
                channel_name,
            }
        });

        let executor = PostgresSourceExecutor {
            db_pool,
            table_name: spec.table_name.clone(),
            table_schema,
            notification_ctx,
        };

        Ok(Box::new(executor))
    }
}
