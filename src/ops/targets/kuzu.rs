use chrono::TimeDelta;
use serde_json::json;

use std::fmt::Write;

use super::shared::property_graph::GraphElementMapping;
use super::shared::property_graph::*;
use super::shared::table_columns::{
    TableColumnsSchema, TableMainSetupAction, TableUpsertionAction, check_table_compatibility,
};
use crate::ops::registry::ExecutorFactoryRegistry;
use crate::prelude::*;

use crate::setup::SetupChangeType;
use crate::{ops::sdk::*, setup::CombinedState};

const SELF_CONTAINED_TAG_FIELD_NAME: &str = "__self_contained";

////////////////////////////////////////////////////////////
// Public Types
////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    /// The URL of the [Kuzu API server](https://kuzu.com/docs/api/server/overview),
    /// e.g. `http://localhost:8000`.
    api_server_url: String,
}

#[derive(Debug, Deserialize)]
pub struct Spec {
    connection: spec::AuthEntryReference<ConnectionSpec>,
    mapping: GraphElementMapping,
}

#[derive(Debug, Deserialize)]
pub struct Declaration {
    connection: spec::AuthEntryReference<ConnectionSpec>,
    #[serde(flatten)]
    decl: GraphDeclaration,
}

////////////////////////////////////////////////////////////
// Utils to deal with Kuzu
////////////////////////////////////////////////////////////

struct CypherBuilder {
    query: String,
}

impl CypherBuilder {
    fn new() -> Self {
        Self {
            query: String::new(),
        }
    }

    fn query_mut(&mut self) -> &mut String {
        &mut self.query
    }
}

struct KuzuThinClient {
    reqwest_client: reqwest::Client,
    query_url: String,
}

impl KuzuThinClient {
    fn new(conn_spec: &ConnectionSpec, reqwest_client: reqwest::Client) -> Self {
        Self {
            reqwest_client,
            query_url: format!("{}/cypher", conn_spec.api_server_url.trim_end_matches('/')),
        }
    }

    async fn run_cypher(&self, cyper_builder: CypherBuilder) -> Result<()> {
        if cyper_builder.query.is_empty() {
            return Ok(());
        }
        let query = json!({
            "query": cyper_builder.query
        });
        let response = self
            .reqwest_client
            .post(&self.query_url)
            .json(&query)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to run cypher: {}",
                response.text().await?
            ));
        }
        Ok(())
    }
}

fn kuzu_table_type(elem_type: &ElementType) -> &'static str {
    match elem_type {
        ElementType::Node(_) => "NODE",
        ElementType::Relationship(_) => "REL",
    }
}

fn basic_type_to_kuzu(basic_type: &BasicValueType) -> Result<String> {
    Ok(match basic_type {
        BasicValueType::Bytes => "BLOB".to_string(),
        BasicValueType::Str => "STRING".to_string(),
        BasicValueType::Bool => "BOOL".to_string(),
        BasicValueType::Int64 => "INT64".to_string(),
        BasicValueType::Float32 => "FLOAT".to_string(),
        BasicValueType::Float64 => "DOUBLE".to_string(),
        BasicValueType::Range => "UINT64[2]".to_string(),
        BasicValueType::Uuid => "UUID".to_string(),
        BasicValueType::Date => "DATE".to_string(),
        BasicValueType::LocalDateTime => "TIMESTAMP".to_string(),
        BasicValueType::OffsetDateTime => "TIMESTAMP".to_string(),
        BasicValueType::TimeDelta => "INTERVAL".to_string(),
        BasicValueType::Vector(t) => format!(
            "{}[{}]",
            basic_type_to_kuzu(&t.element_type)?,
            t.dimension
                .map_or_else(|| "".to_string(), |d| d.to_string())
        ),
        t @ (BasicValueType::Union(_) | BasicValueType::Time | BasicValueType::Json) => {
            api_bail!("{t} is not supported in Kuzu")
        }
    })
}

fn struct_schema_to_kuzu(struct_schema: &StructSchema) -> Result<String> {
    Ok(format!(
        "STRUCT({})",
        struct_schema
            .fields
            .iter()
            .map(|f| Ok(format!(
                "{} {}",
                f.name,
                value_type_to_kuzu(&f.value_type.typ)?
            )))
            .collect::<Result<Vec<_>>>()?
            .join(", ")
    ))
}

fn value_type_to_kuzu(value_type: &ValueType) -> Result<String> {
    Ok(match value_type {
        ValueType::Basic(basic_type) => basic_type_to_kuzu(basic_type)?,
        ValueType::Struct(struct_type) => struct_schema_to_kuzu(struct_type)?,
        ValueType::Table(table_type) => format!("{}[]", struct_schema_to_kuzu(&table_type.row)?),
    })
}

////////////////////////////////////////////////////////////
// Setup
////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
struct ReferencedNodeTable {
    table_name: String,

    #[serde(with = "indexmap::map::serde_seq")]
    key_columns: IndexMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SetupState {
    schema: TableColumnsSchema<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    referenced_node_tables: Option<(ReferencedNodeTable, ReferencedNodeTable)>,
}

impl<'a> Into<Cow<'a, TableColumnsSchema<String>>> for &'a SetupState {
    fn into(self) -> Cow<'a, TableColumnsSchema<String>> {
        Cow::Borrowed(&self.schema)
    }
}

#[derive(Debug)]
struct GraphElementDataSetupStatus {
    actions: TableMainSetupAction<String>,
    referenced_node_tables: Option<(String, String)>,
    drop_affected_referenced_node_tables: IndexSet<String>,
}

impl setup::ResourceSetupStatus for GraphElementDataSetupStatus {
    fn describe_changes(&self) -> Vec<String> {
        self.actions.describe_changes()
    }

    fn change_type(&self) -> SetupChangeType {
        self.actions.change_type(false)
    }
}

fn append_drop_table(
    cypher: &mut CypherBuilder,
    setup_status: &GraphElementDataSetupStatus,
    elem_type: &ElementType,
) -> Result<()> {
    if !setup_status.actions.drop_existing {
        return Ok(());
    }
    write!(
        cypher.query_mut(),
        "DROP TABLE IF EXISTS {};\n",
        elem_type.label()
    )?;
    Ok(())
}

fn append_delete_orphaned_nodes(cypher: &mut CypherBuilder, node_table: &str) -> Result<()> {
    write!(
        cypher.query_mut(),
        "MATCH (n:{node_table}) WITH n WHERE NOT (n)--() DELETE n;\n"
    )?;
    Ok(())
}

fn append_upsert_table(
    cypher: &mut CypherBuilder,
    setup_status: &GraphElementDataSetupStatus,
    elem_type: &ElementType,
) -> Result<()> {
    let table_upsertion = if let Some(table_upsertion) = &setup_status.actions.table_upsertion {
        table_upsertion
    } else {
        return Ok(());
    };
    match table_upsertion {
        TableUpsertionAction::Create { keys, values } => {
            write!(
                cypher.query_mut(),
                "CREATE {kuzu_table_type} TABLE IF NOT EXISTS {table_name} (",
                kuzu_table_type = kuzu_table_type(elem_type),
                table_name = elem_type.label(),
            )?;
            if let Some((src, tgt)) = &setup_status.referenced_node_tables {
                write!(cypher.query_mut(), "FROM {src} TO {tgt}, ")?;
            }
            cypher.query_mut().push_str(
                keys.iter()
                    .chain(values.iter())
                    .map(|(name, kuzu_type)| format!("{} {}", name, kuzu_type))
                    .join(", ")
                    .as_str(),
            );
            match elem_type {
                ElementType::Node(_) => {
                    write!(
                        cypher.query_mut(),
                        ", {SELF_CONTAINED_TAG_FIELD_NAME} BOOL, PRIMARY KEY ({})",
                        keys.iter().map(|(name, _)| name).join(", ")
                    )?;
                }
                ElementType::Relationship(_) => {}
            }
            write!(cypher.query_mut(), ");\n\n")?;
        }
        TableUpsertionAction::Update {
            columns_to_delete,
            columns_to_upsert,
        } => {
            let table_name = elem_type.label();
            for name in columns_to_delete
                .iter()
                .chain(columns_to_upsert.iter().map(|(name, _)| name))
            {
                write!(
                    cypher.query_mut(),
                    "ALTER TABLE {table_name} DROP IF EXISTS {name};\n"
                )?;
            }
            for (name, kuzu_type) in columns_to_upsert.iter() {
                write!(
                    cypher.query_mut(),
                    "ALTER TABLE {table_name} ADD {name} {kuzu_type};\n",
                )?;
            }
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////
// Utils to convert value to Kuzu literals
////////////////////////////////////////////////////////////

fn append_string_literal(cypher: &mut CypherBuilder, s: &str) -> Result<()> {
    let out = cypher.query_mut();
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            // Control characters (0x00..=0x1F)
            c if (c as u32) < 0x20 => write!(out, "\\u{:04X}", c as u32)?,
            // BMP Unicode
            c if (c as u32) <= 0xFFFF => out.push(c),
            // Non-BMP Unicode: Encode as surrogate pairs for Cypher \uXXXX\uXXXX
            c => {
                let code = c as u32;
                let high = 0xD800 + ((code - 0x10000) >> 10);
                let low = 0xDC00 + ((code - 0x10000) & 0x3FF);
                write!(out, "\\u{:04X}\\u{:04X}", high, low)?;
            }
        }
    }
    out.push('"');
    Ok(())
}

fn append_basic_value(cypher: &mut CypherBuilder, basic_value: &BasicValue) -> Result<()> {
    match basic_value {
        BasicValue::Bytes(bytes) => {
            write!(cypher.query_mut(), "BLOB(")?;
            for byte in bytes {
                write!(cypher.query_mut(), "\\\\x{:02X}", byte)?;
            }
            write!(cypher.query_mut(), ")")?;
        }
        BasicValue::Str(s) => {
            append_string_literal(cypher, s)?;
        }
        BasicValue::Bool(b) => {
            write!(cypher.query_mut(), "{}", b)?;
        }
        BasicValue::Int64(i) => {
            write!(cypher.query_mut(), "{}", i)?;
        }
        BasicValue::Float32(f) => {
            write!(cypher.query_mut(), "{}", f)?;
        }
        BasicValue::Float64(f) => {
            write!(cypher.query_mut(), "{}", f)?;
        }
        BasicValue::Range(r) => {
            write!(cypher.query_mut(), "[{}, {}]", r.start, r.end)?;
        }
        BasicValue::Uuid(u) => {
            write!(cypher.query_mut(), "UUID(\"{}\")", u)?;
        }
        BasicValue::Date(d) => {
            write!(cypher.query_mut(), "DATE(\"{}\")", d)?;
        }
        BasicValue::LocalDateTime(dt) => {
            write!(cypher.query_mut(), "TIMESTAMP(\"{}\")", dt)?;
        }
        BasicValue::OffsetDateTime(dt) => {
            write!(cypher.query_mut(), "TIMESTAMP(\"{}\")", dt)?;
        }
        BasicValue::TimeDelta(td) => {
            let num_days = td.num_days();
            let sub_day_duration = *td - TimeDelta::days(num_days);
            write!(cypher.query_mut(), "INTERVAL(\"")?;
            if num_days != 0 {
                write!(cypher.query_mut(), "{} days ", num_days)?;
            }
            write!(
                cypher.query_mut(),
                "{} microseconds\")",
                sub_day_duration
                    .num_microseconds()
                    .ok_or_else(invariance_violation)?
            )?;
        }
        BasicValue::Vector(v) => {
            write!(cypher.query_mut(), "[")?;
            let mut prefix = "";
            for elem in v.iter() {
                cypher.query_mut().push_str(prefix);
                append_basic_value(cypher, elem)?;
                prefix = ", ";
            }
            write!(cypher.query_mut(), "]")?;
        }
        v @ (BasicValue::UnionVariant { .. } | BasicValue::Time(_) | BasicValue::Json(_)) => {
            bail!("value types are not supported in Kuzu: {}", v.kind());
        }
    }
    Ok(())
}

fn append_struct_fields<'a>(
    cypher: &'a mut CypherBuilder,
    field_schema: &[schema::FieldSchema],
    field_values: impl Iterator<Item = &'a value::Value>,
) -> Result<()> {
    let mut prefix = "";
    for (f, v) in std::iter::zip(field_schema.iter(), field_values) {
        write!(cypher.query_mut(), "{prefix}{}: ", f.name)?;
        append_value(cypher, &f.value_type.typ, v)?;
        prefix = ", ";
    }
    Ok(())
}

fn append_value(
    cypher: &mut CypherBuilder,
    typ: &schema::ValueType,
    value: &value::Value,
) -> Result<()> {
    match value {
        value::Value::Null => {
            write!(cypher.query_mut(), "NULL")?;
        }
        value::Value::Basic(basic_value) => append_basic_value(cypher, basic_value)?,
        value::Value::Struct(struct_value) => {
            let struct_schema = match typ {
                schema::ValueType::Struct(struct_schema) => struct_schema,
                _ => {
                    api_bail!("Expected struct type, got {}", typ);
                }
            };
            cypher.query_mut().push('{');
            append_struct_fields(cypher, &struct_schema.fields, struct_value.fields.iter())?;
            cypher.query_mut().push('}');
        }
        value::Value::KTable(map) => {
            let row_schema = match typ {
                schema::ValueType::Table(table_schema) => &table_schema.row,
                _ => {
                    api_bail!("Expected table type, got {}", typ);
                }
            };
            cypher.query_mut().push('[');
            let mut prefix = "";
            for (k, v) in map.iter() {
                let key_value = value::Value::from(k);
                cypher.query_mut().push_str(prefix);
                cypher.query_mut().push('{');
                append_struct_fields(
                    cypher,
                    &row_schema.fields,
                    std::iter::once(&key_value).chain(v.fields.iter()),
                )?;
                cypher.query_mut().push('}');
                prefix = ", ";
            }
            cypher.query_mut().push(']');
        }
        value::Value::LTable(rows) | value::Value::UTable(rows) => {
            let row_schema = match typ {
                schema::ValueType::Table(table_schema) => &table_schema.row,
                _ => {
                    api_bail!("Expected table type, got {}", typ);
                }
            };
            cypher.query_mut().push('[');
            let mut prefix = "";
            for v in rows.iter() {
                cypher.query_mut().push_str(prefix);
                cypher.query_mut().push('{');
                append_struct_fields(cypher, &row_schema.fields, v.fields.iter())?;
                cypher.query_mut().push('}');
                prefix = ", ";
            }
            cypher.query_mut().push(']');
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////
// Deal with mutations
////////////////////////////////////////////////////////////

struct ExportContext {
    conn_ref: AuthEntryReference<ConnectionSpec>,
    kuzu_client: KuzuThinClient,
    analyzed_data_coll: AnalyzedDataCollection,
}

fn append_key_pattern<'a>(
    cypher: &'a mut CypherBuilder,
    key_fields: &'a [FieldSchema],
    values: impl Iterator<Item = Cow<'a, value::Value>>,
) -> Result<()> {
    write!(cypher.query_mut(), "{{")?;
    let mut prefix = "";
    for (f, v) in std::iter::zip(key_fields.iter(), values) {
        write!(cypher.query_mut(), "{prefix}{}: ", f.name)?;
        append_value(cypher, &f.value_type.typ, v.as_ref())?;
        prefix = ", ";
    }
    write!(cypher.query_mut(), "}}")?;
    Ok(())
}

fn append_set_value_fields(
    cypher: &mut CypherBuilder,
    var_name: &str,
    value_fields: &[FieldSchema],
    value_fields_idx: &[usize],
    upsert_entry: &ExportTargetUpsertEntry,
    set_self_contained_tag: bool,
) -> Result<()> {
    let mut prefix = " SET ";
    if set_self_contained_tag {
        write!(
            cypher.query_mut(),
            "{prefix}{var_name}.{SELF_CONTAINED_TAG_FIELD_NAME} = TRUE"
        )?;
        prefix = ", ";
    }
    for (value_field, value_idx) in std::iter::zip(value_fields.iter(), value_fields_idx.iter()) {
        let field_name = &value_field.name;
        write!(cypher.query_mut(), "{prefix}{var_name}.{field_name}=")?;
        append_value(
            cypher,
            &value_field.value_type.typ,
            &upsert_entry.value.fields[*value_idx],
        )?;
        prefix = ", ";
    }
    Ok(())
}

fn append_upsert_node(
    cypher: &mut CypherBuilder,
    data_coll: &AnalyzedDataCollection,
    upsert_entry: &ExportTargetUpsertEntry,
) -> Result<()> {
    const NODE_VAR_NAME: &str = "n";
    {
        write!(
            cypher.query_mut(),
            "MERGE ({NODE_VAR_NAME}:{label} ",
            label = data_coll.schema.elem_type.label(),
        )?;
        append_key_pattern(
            cypher,
            &data_coll.schema.key_fields,
            upsert_entry
                .key
                .fields_iter(data_coll.schema.key_fields.len())?
                .map(|f| Cow::Owned(value::Value::from(f))),
        )?;
        write!(cypher.query_mut(), ")")?;
    }
    append_set_value_fields(
        cypher,
        NODE_VAR_NAME,
        &data_coll.schema.value_fields,
        &data_coll.value_fields_input_idx,
        &upsert_entry,
        true,
    )?;
    write!(cypher.query_mut(), ";\n")?;
    Ok(())
}

fn append_merge_node_for_rel(
    cypher: &mut CypherBuilder,
    var_name: &str,
    field_mapping: &AnalyzedGraphElementFieldMapping,
    upsert_entry: &ExportTargetUpsertEntry,
) -> Result<()> {
    {
        write!(
            cypher.query_mut(),
            "MERGE ({var_name}:{label} ",
            label = field_mapping.schema.elem_type.label(),
        )?;
        append_key_pattern(
            cypher,
            &field_mapping.schema.key_fields,
            field_mapping
                .fields_input_idx
                .key
                .iter()
                .map(|idx| Cow::Borrowed(&upsert_entry.value.fields[*idx])),
        )?;
        write!(cypher.query_mut(), ")")?;
    }
    append_set_value_fields(
        cypher,
        var_name,
        &field_mapping.schema.value_fields,
        &field_mapping.fields_input_idx.value,
        &upsert_entry,
        false,
    )?;
    write!(cypher.query_mut(), "\n")?;
    Ok(())
}

fn append_upsert_rel(
    cypher: &mut CypherBuilder,
    data_coll: &AnalyzedDataCollection,
    upsert_entry: &ExportTargetUpsertEntry,
) -> Result<()> {
    const REL_VAR_NAME: &str = "r";
    const SRC_NODE_VAR_NAME: &str = "s";
    const TGT_NODE_VAR_NAME: &str = "t";

    let rel_info = if let Some(rel_info) = &data_coll.rel {
        rel_info
    } else {
        return Ok(());
    };
    append_merge_node_for_rel(cypher, SRC_NODE_VAR_NAME, &rel_info.source, &upsert_entry)?;
    append_merge_node_for_rel(cypher, TGT_NODE_VAR_NAME, &rel_info.target, &upsert_entry)?;
    {
        let rel_type = data_coll.schema.elem_type.label();
        write!(
            cypher.query_mut(),
            "MERGE ({SRC_NODE_VAR_NAME})-[{REL_VAR_NAME}:{rel_type} "
        )?;
        append_key_pattern(
            cypher,
            &data_coll.schema.key_fields,
            upsert_entry
                .key
                .fields_iter(data_coll.schema.key_fields.len())?
                .map(|f| Cow::Owned(value::Value::from(f))),
        )?;
        write!(cypher.query_mut(), "]->({TGT_NODE_VAR_NAME})")?;
    }
    append_set_value_fields(
        cypher,
        REL_VAR_NAME,
        &data_coll.schema.value_fields,
        &data_coll.value_fields_input_idx,
        &upsert_entry,
        false,
    )?;
    write!(cypher.query_mut(), ";\n")?;
    Ok(())
}

fn append_delete_node(
    cypher: &mut CypherBuilder,
    data_coll: &AnalyzedDataCollection,
    key: &KeyValue,
) -> Result<()> {
    const NODE_VAR_NAME: &str = "n";
    let node_label = data_coll.schema.elem_type.label();
    write!(cypher.query_mut(), "MATCH ({NODE_VAR_NAME}:{node_label} ")?;
    append_key_pattern(
        cypher,
        &data_coll.schema.key_fields,
        key.fields_iter(data_coll.schema.key_fields.len())?
            .map(|f| Cow::Owned(value::Value::from(f))),
    )?;
    write!(cypher.query_mut(), ")\n")?;
    write!(
        cypher.query_mut(),
        "WITH {NODE_VAR_NAME} SET {NODE_VAR_NAME}.{SELF_CONTAINED_TAG_FIELD_NAME} = NULL\n"
    )?;
    write!(
        cypher.query_mut(),
        "WITH {NODE_VAR_NAME} WHERE NOT ({NODE_VAR_NAME})--() DELETE {NODE_VAR_NAME}\n"
    )?;
    write!(cypher.query_mut(), ";\n")?;
    Ok(())
}

fn append_delete_rel(
    cypher: &mut CypherBuilder,
    data_coll: &AnalyzedDataCollection,
    key: &KeyValue,
    src_node_key: &KeyValue,
    tgt_node_key: &KeyValue,
) -> Result<()> {
    const REL_VAR_NAME: &str = "r";

    let rel = data_coll.rel.as_ref().ok_or_else(invariance_violation)?;
    let rel_type = data_coll.schema.elem_type.label();

    write!(
        cypher.query_mut(),
        "MATCH (:{label} ",
        label = rel.source.schema.elem_type.label()
    )?;
    let src_key_schema = &rel.source.schema.key_fields;
    append_key_pattern(
        cypher,
        src_key_schema,
        src_node_key
            .fields_iter(src_key_schema.len())?
            .map(|k| Cow::Owned(value::Value::from(k))),
    )?;

    write!(cypher.query_mut(), ")-[{REL_VAR_NAME}:{rel_type} ")?;
    let key_schema = &data_coll.schema.key_fields;
    append_key_pattern(
        cypher,
        key_schema,
        key.fields_iter(key_schema.len())?
            .map(|k| Cow::Owned(value::Value::from(k))),
    )?;

    write!(
        cypher.query_mut(),
        "]->(:{label} ",
        label = rel.target.schema.elem_type.label()
    )?;
    let tgt_key_schema = &rel.target.schema.key_fields;
    append_key_pattern(
        cypher,
        tgt_key_schema,
        tgt_node_key
            .fields_iter(tgt_key_schema.len())?
            .map(|k| Cow::Owned(value::Value::from(k))),
    )?;
    write!(cypher.query_mut(), ") DELETE {REL_VAR_NAME}")?;
    write!(cypher.query_mut(), ";\n")?;
    Ok(())
}

fn append_maybe_gc_node(
    cypher: &mut CypherBuilder,
    schema: &GraphElementSchema,
    key: &KeyValue,
) -> Result<()> {
    const NODE_VAR_NAME: &str = "n";
    let node_label = schema.elem_type.label();
    write!(cypher.query_mut(), "MATCH ({NODE_VAR_NAME}:{node_label} ")?;
    append_key_pattern(
        cypher,
        &schema.key_fields,
        key.fields_iter(schema.key_fields.len())?
            .map(|f| Cow::Owned(value::Value::from(f))),
    )?;
    write!(cypher.query_mut(), ")\n")?;
    write!(
        cypher.query_mut(),
        "WITH {NODE_VAR_NAME} WHERE NOT ({NODE_VAR_NAME})--() DELETE {NODE_VAR_NAME}"
    )?;
    write!(cypher.query_mut(), ";\n")?;
    Ok(())
}

////////////////////////////////////////////////////////////
// Factory implementation
////////////////////////////////////////////////////////////

type KuzuGraphElement = GraphElementType<ConnectionSpec>;

struct Factory {
    reqwest_client: reqwest::Client,
}

#[async_trait]
impl StorageFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupStatus = GraphElementDataSetupStatus;

    type Key = KuzuGraphElement;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "Kuzu"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Declaration>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(KuzuGraphElement, SetupState)>,
    )> {
        let (analyzed_data_colls, declared_graph_elements) = analyze_graph_mappings(
            data_collections
                .iter()
                .map(|d| DataCollectionGraphMappingInput {
                    auth_ref: &d.spec.connection,
                    mapping: &d.spec.mapping,
                    index_options: &d.index_options,
                    key_fields_schema: d.key_fields_schema.clone(),
                    value_fields_schema: d.value_fields_schema.clone(),
                }),
            declarations.iter().map(|d| (&d.connection, &d.decl)),
        )?;
        fn to_kuzu_cols(fields: &[FieldSchema]) -> Result<IndexMap<String, String>> {
            fields
                .iter()
                .map(|f| Ok((f.name.clone(), value_type_to_kuzu(&f.value_type.typ)?)))
                .collect::<Result<IndexMap<_, _>>>()
        }
        let data_coll_outputs: Vec<TypedExportDataCollectionBuildOutput<Self>> =
            std::iter::zip(data_collections, analyzed_data_colls.into_iter())
                .map(|(data_coll, analyzed)| {
                    fn to_dep_table(
                        field_mapping: &AnalyzedGraphElementFieldMapping,
                    ) -> Result<ReferencedNodeTable> {
                        Ok(ReferencedNodeTable {
                            table_name: field_mapping.schema.elem_type.label().to_string(),
                            key_columns: to_kuzu_cols(&field_mapping.schema.key_fields)?,
                        })
                    }
                    let setup_key = KuzuGraphElement {
                        connection: data_coll.spec.connection.clone(),
                        typ: analyzed.schema.elem_type.clone(),
                    };
                    let desired_setup_state = SetupState {
                        schema: TableColumnsSchema {
                            key_columns: to_kuzu_cols(&analyzed.schema.key_fields)?,
                            value_columns: to_kuzu_cols(&analyzed.schema.value_fields)?,
                        },
                        referenced_node_tables: (analyzed.rel.as_ref())
                            .map(|rel| {
                                anyhow::Ok((to_dep_table(&rel.source)?, to_dep_table(&rel.target)?))
                            })
                            .transpose()?,
                    };

                    let export_context = ExportContext {
                        conn_ref: data_coll.spec.connection.clone(),
                        kuzu_client: KuzuThinClient::new(
                            &context
                                .auth_registry
                                .get::<ConnectionSpec>(&data_coll.spec.connection)?,
                            self.reqwest_client.clone(),
                        ),
                        analyzed_data_coll: analyzed,
                    };
                    Ok(TypedExportDataCollectionBuildOutput {
                        export_context: async move { Ok(Arc::new(export_context)) }.boxed(),
                        setup_key,
                        desired_setup_state,
                    })
                })
                .collect::<Result<_>>()?;
        let decl_output = std::iter::zip(declarations, declared_graph_elements)
            .map(|(decl, graph_elem_schema)| {
                let setup_state = SetupState {
                    schema: TableColumnsSchema {
                        key_columns: to_kuzu_cols(&graph_elem_schema.key_fields)?,
                        value_columns: to_kuzu_cols(&graph_elem_schema.value_fields)?,
                    },
                    referenced_node_tables: None,
                };
                let setup_key = GraphElementType {
                    connection: decl.connection,
                    typ: graph_elem_schema.elem_type.clone(),
                };
                Ok((setup_key, setup_state))
            })
            .collect::<Result<_>>()?;
        Ok((data_coll_outputs, decl_output))
    }

    async fn check_setup_status(
        &self,
        _key: KuzuGraphElement,
        desired: Option<SetupState>,
        existing: CombinedState<SetupState>,
        _auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Self::SetupStatus> {
        let existing_invalidated = desired.as_ref().map_or(false, |desired| {
            existing
                .possible_versions()
                .any(|v| v.referenced_node_tables != desired.referenced_node_tables)
        });
        let actions =
            TableMainSetupAction::from_states(desired.as_ref(), &existing, existing_invalidated);
        let drop_affected_referenced_node_tables = if actions.drop_existing {
            existing
                .possible_versions()
                .flat_map(|v| &v.referenced_node_tables)
                .flat_map(|(src, tgt)| [src.table_name.clone(), tgt.table_name.clone()].into_iter())
                .collect()
        } else {
            IndexSet::new()
        };
        Ok(GraphElementDataSetupStatus {
            actions,
            referenced_node_tables: desired
                .map(|desired| desired.referenced_node_tables)
                .flatten()
                .map(|(src, tgt)| (src.table_name, tgt.table_name)),
            drop_affected_referenced_node_tables,
        })
    }

    fn check_state_compatibility(
        &self,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<SetupStateCompatibility> {
        Ok(
            if desired.referenced_node_tables != existing.referenced_node_tables {
                SetupStateCompatibility::NotCompatible
            } else {
                check_table_compatibility(&desired.schema, &existing.schema)
            },
        )
    }

    fn describe_resource(&self, key: &KuzuGraphElement) -> Result<String> {
        Ok(format!(
            "Kuzu {} TABLE {}",
            kuzu_table_type(&key.typ),
            key.typ.label()
        ))
    }

    fn extract_additional_key<'ctx>(
        &self,
        _key: &KeyValue,
        value: &FieldValues,
        export_context: &'ctx ExportContext,
    ) -> Result<serde_json::Value> {
        let additional_key = if let Some(rel_info) = &export_context.analyzed_data_coll.rel {
            serde_json::to_value((
                (rel_info.source.fields_input_idx).extract_key(&value.fields)?,
                (rel_info.target.fields_input_idx).extract_key(&value.fields)?,
            ))?
        } else {
            serde_json::Value::Null
        };
        Ok(additional_key)
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, Self::ExportContext>>,
    ) -> Result<()> {
        let mut mutations_by_conn = IndexMap::new();
        for mutation in mutations.into_iter() {
            mutations_by_conn
                .entry(mutation.export_context.conn_ref.clone())
                .or_insert_with(Vec::new)
                .push(mutation);
        }
        for mutations in mutations_by_conn.into_values() {
            let kuzu_client = &mutations[0].export_context.kuzu_client;
            let mut cypher = CypherBuilder::new();
            write!(cypher.query_mut(), "BEGIN TRANSACTION;\n")?;

            let (mut rel_mutations, nodes_mutations): (Vec<_>, Vec<_>) = mutations
                .into_iter()
                .partition(|m| m.export_context.analyzed_data_coll.rel.is_some());

            struct NodeTableGcInfo {
                schema: Arc<GraphElementSchema>,
                keys: IndexSet<KeyValue>,
            }
            fn register_gc_node(
                map: &mut IndexMap<ElementType, NodeTableGcInfo>,
                schema: &Arc<GraphElementSchema>,
                key: KeyValue,
            ) {
                map.entry(schema.elem_type.clone())
                    .or_insert_with(|| NodeTableGcInfo {
                        schema: schema.clone(),
                        keys: IndexSet::new(),
                    })
                    .keys
                    .insert(key);
            }
            fn resolve_gc_node(
                map: &mut IndexMap<ElementType, NodeTableGcInfo>,
                schema: &Arc<GraphElementSchema>,
                key: &KeyValue,
            ) {
                map.get_mut(&schema.elem_type)
                    .map(|info| info.keys.shift_remove(key));
            }
            let mut gc_info = IndexMap::<ElementType, NodeTableGcInfo>::new();

            // Deletes for relationships
            for rel_mutation in rel_mutations.iter_mut() {
                let data_coll = &rel_mutation.export_context.analyzed_data_coll;

                let rel = data_coll.rel.as_ref().ok_or_else(invariance_violation)?;
                for delete in rel_mutation.mutation.deletes.iter_mut() {
                    let mut additional_keys = match delete.additional_key.take() {
                        serde_json::Value::Array(keys) => keys,
                        _ => return Err(invariance_violation()),
                    };
                    if additional_keys.len() != 2 {
                        api_bail!(
                            "Expected additional key with 2 fields, got {}",
                            delete.additional_key
                        );
                    }
                    let src_key = KeyValue::from_json(
                        additional_keys[0].take(),
                        &rel.source.schema.key_fields,
                    )?;
                    let tgt_key = KeyValue::from_json(
                        additional_keys[1].take(),
                        &rel.target.schema.key_fields,
                    )?;
                    append_delete_rel(&mut cypher, data_coll, &delete.key, &src_key, &tgt_key)?;
                    register_gc_node(&mut gc_info, &rel.source.schema, src_key);
                    register_gc_node(&mut gc_info, &rel.target.schema, tgt_key);
                }
            }

            for node_mutation in nodes_mutations.iter() {
                let data_coll = &node_mutation.export_context.analyzed_data_coll;
                // Deletes for nodes
                for delete in node_mutation.mutation.deletes.iter() {
                    append_delete_node(&mut cypher, data_coll, &delete.key)?;
                    resolve_gc_node(&mut gc_info, &data_coll.schema, &delete.key);
                }

                // Upserts for nodes
                for upsert in node_mutation.mutation.upserts.iter() {
                    append_upsert_node(&mut cypher, data_coll, upsert)?;
                    resolve_gc_node(&mut gc_info, &data_coll.schema, &upsert.key);
                }
            }
            // Upserts for relationships
            for rel_mutation in rel_mutations.iter() {
                let data_coll = &rel_mutation.export_context.analyzed_data_coll;
                for upsert in rel_mutation.mutation.upserts.iter() {
                    append_upsert_rel(&mut cypher, data_coll, upsert)?;

                    let rel = data_coll.rel.as_ref().ok_or_else(invariance_violation)?;
                    resolve_gc_node(
                        &mut gc_info,
                        &rel.source.schema,
                        &(rel.source.fields_input_idx).extract_key(&upsert.value.fields)?,
                    );
                    resolve_gc_node(
                        &mut gc_info,
                        &rel.target.schema,
                        &(rel.target.fields_input_idx).extract_key(&upsert.value.fields)?,
                    );
                }
            }

            // GC orphaned nodes
            for info in gc_info.into_values() {
                for key in info.keys {
                    append_maybe_gc_node(&mut cypher, &info.schema, &key)?;
                }
            }

            write!(cypher.query_mut(), "COMMIT;\n")?;
            kuzu_client.run_cypher(cypher).await?;
        }
        Ok(())
    }

    async fn apply_setup_changes(
        &self,
        changes: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<()> {
        let mut changes_by_conn = IndexMap::new();
        for change in changes.into_iter() {
            changes_by_conn
                .entry(change.key.connection.clone())
                .or_insert_with(Vec::new)
                .push(change);
        }
        for (conn, changes) in changes_by_conn.into_iter() {
            let conn_spec = auth_registry.get::<ConnectionSpec>(&conn)?;
            let kuzu_client = KuzuThinClient::new(&conn_spec, self.reqwest_client.clone());

            let (node_changes, rel_changes): (Vec<_>, Vec<_>) =
                changes.into_iter().partition(|c| match &c.key.typ {
                    ElementType::Node(_) => true,
                    ElementType::Relationship(_) => false,
                });

            let mut partial_affected_node_tables = IndexSet::new();
            let mut cypher = CypherBuilder::new();
            // Relationships first when dropping.
            for change in rel_changes.iter().chain(node_changes.iter()) {
                if !change.setup_status.actions.drop_existing {
                    continue;
                }
                append_drop_table(&mut cypher, &change.setup_status, &change.key.typ)?;

                partial_affected_node_tables.extend(
                    change
                        .setup_status
                        .drop_affected_referenced_node_tables
                        .iter(),
                );
                if let ElementType::Node(label) = &change.key.typ {
                    partial_affected_node_tables.swap_remove(label);
                }
            }
            // Nodes first when creating.
            for change in node_changes.iter().chain(rel_changes.iter()) {
                append_upsert_table(&mut cypher, &change.setup_status, &change.key.typ)?;
            }

            for table in partial_affected_node_tables {
                append_delete_orphaned_nodes(&mut cypher, &table)?;
            }

            kuzu_client.run_cypher(cypher).await?;
        }
        Ok(())
    }
}

pub fn register(
    registry: &mut ExecutorFactoryRegistry,
    reqwest_client: reqwest::Client,
) -> Result<()> {
    Factory { reqwest_client }.register(registry)
}
