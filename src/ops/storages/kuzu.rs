use serde_json::json;

use std::fmt::Write;

use super::shared::property_graph::GraphElementMapping;
use super::shared::property_graph::*;
use super::shared::table_columns::{
    TableColumnsSchema, TableMainSetupAction, TableUpsertionAction, check_table_compatibility,
};
use crate::prelude::*;

use crate::setup::{ResourceSetupStatus, SetupChangeType};
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
    params: Vec<(String, serde_json::Value)>,
}

impl CypherBuilder {
    fn new() -> Self {
        Self {
            query: String::new(),
            params: vec![],
        }
    }

    fn query_mut(&mut self) -> &mut String {
        &mut self.query
    }

    fn add_param(&mut self, key: String, value: serde_json::Value) {
        self.params.push((key, value));
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
        debug!("Running cypher:\n{}", cyper_builder.query);
        if cyper_builder.query.is_empty() {
            return Ok(());
        }
        let query = json!({
            "query": cyper_builder.query,
            "params": serde_json::Value::Object(cyper_builder.params.into_iter().collect()),
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
        BasicValueType::Time => api_bail!("Time is not supported in Kuzu"),
        BasicValueType::LocalDateTime => "TIMESTAMP".to_string(),
        BasicValueType::OffsetDateTime => "TIMESTAMP".to_string(),
        BasicValueType::TimeDelta => "INTERVAL".to_string(),
        BasicValueType::Json => "JSON".to_string(),
        BasicValueType::Vector(t) => format!(
            "{}[{}]",
            basic_type_to_kuzu(&t.element_type)?,
            t.dimension
                .map_or_else(|| "".to_string(), |d| d.to_string())
        ),
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
pub struct SetupState {
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
pub struct GraphElementDataSetupStatus {
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GraphElementDataSetupStatus {
    fn add_drop_cypher(
        &self,
        elem_type: &ElementType,
        cypher_builder: &mut CypherBuilder,
    ) -> Result<()> {
        if !self.actions.drop_existing {
            return Ok(());
        }
        write!(
            cypher_builder.query_mut(),
            "DROP TABLE IF EXISTS {};\n",
            elem_type.label()
        )?;
        Ok(())
    }

    fn add_delete_orphaned_nodes_cypher(
        node_table: &str,
        cypher_builder: &mut CypherBuilder,
    ) -> Result<()> {
        write!(
            cypher_builder.query_mut(),
            "MATCH (n:{node_table}) WITH n WHERE NOT (n)--() DELETE n;\n"
        )?;
        Ok(())
    }

    fn add_create_alter_cypher(
        &self,
        elem_type: &ElementType,
        cypher_builder: &mut CypherBuilder,
    ) -> Result<()> {
        let table_upsertion = if let Some(table_upsertion) = &self.actions.table_upsertion {
            table_upsertion
        } else {
            return Ok(());
        };
        match table_upsertion {
            TableUpsertionAction::Create { keys, values } => {
                write!(
                    cypher_builder.query_mut(),
                    "CREATE {kuzu_table_type} TABLE IF NOT EXISTS {table_name} (",
                    kuzu_table_type = kuzu_table_type(elem_type),
                    table_name = elem_type.label(),
                )?;
                if let Some((src, tgt)) = &self.referenced_node_tables {
                    write!(cypher_builder.query_mut(), "FROM {src} TO {tgt}, ")?;
                }
                cypher_builder.query_mut().push_str(
                    keys.iter()
                        .chain(values.iter())
                        .map(|(name, kuzu_type)| format!("{} {}", name, kuzu_type))
                        .join(", ")
                        .as_str(),
                );
                match elem_type {
                    ElementType::Node(_) => {
                        write!(
                            cypher_builder.query_mut(),
                            ", {SELF_CONTAINED_TAG_FIELD_NAME} BOOL, PRIMARY KEY ({})",
                            keys.iter().map(|(name, _)| name).join(", ")
                        )?;
                    }
                    ElementType::Relationship(_) => {}
                }
                write!(cypher_builder.query_mut(), ");\n\n")?;
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
                        cypher_builder.query_mut(),
                        "ALTER TABLE {table_name} DROP IF EXISTS {name};\n"
                    )?;
                }
                for (name, kuzu_type) in columns_to_upsert.iter() {
                    write!(
                        cypher_builder.query_mut(),
                        "ALTER TABLE {table_name} ADD {name} {kuzu_type};\n",
                    )?;
                }
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////
// Factory implementation
////////////////////////////////////////////////////////////

type KuzuGraphElement = GraphElementType<ConnectionSpec>;

pub struct Factory {
    reqwest_client: reqwest::Client,
}

impl Factory {
    pub fn new(reqwest_client: reqwest::Client) -> Self {
        Self { reqwest_client }
    }
}

#[async_trait]
impl StorageFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupStatus = GraphElementDataSetupStatus;

    type Key = KuzuGraphElement;
    type ExportContext = ();

    fn name(&self) -> &str {
        "Kuzu"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Declaration>,
        _context: Arc<FlowInstanceContext>,
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
                    fn to_dep_node_table(
                        field_mapping: &AnalyzedGraphElementFieldMapping,
                    ) -> Result<ReferencedNodeTable> {
                        Ok(ReferencedNodeTable {
                            table_name: field_mapping.schema.elem_type.label().to_string(),
                            key_columns: to_kuzu_cols(&field_mapping.schema.key_fields)?,
                        })
                    }
                    let executors = Box::pin(async {
                        Ok(TypedExportTargetExecutors {
                            export_context: Arc::new(()),
                            query_target: None,
                        })
                    });
                    let setup_key = KuzuGraphElement {
                        connection: data_coll.spec.connection.clone(),
                        typ: analyzed.schema.elem_type.clone(),
                    };
                    let desired_setup_state = SetupState {
                        schema: TableColumnsSchema {
                            key_columns: to_kuzu_cols(&analyzed.schema.key_fields)?,
                            value_columns: to_kuzu_cols(&analyzed.schema.value_fields)?,
                        },
                        referenced_node_tables: match (&analyzed.source, &analyzed.target) {
                            (Some(source), Some(target)) => {
                                Some((to_dep_node_table(source)?, to_dep_node_table(target)?))
                            }
                            _ => None,
                        },
                    };
                    Ok(TypedExportDataCollectionBuildOutput {
                        executors,
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

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, Self::ExportContext>>,
    ) -> Result<()> {
        unimplemented!()
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
            let client = KuzuThinClient::new(&conn_spec, self.reqwest_client.clone());

            let (node_changes, rel_changes): (Vec<_>, Vec<_>) =
                changes.into_iter().partition(|c| match &c.key.typ {
                    ElementType::Node(_) => true,
                    ElementType::Relationship(_) => false,
                });

            let mut partial_affected_node_tables = IndexSet::new();
            let mut cypher_builder = CypherBuilder::new();
            // Relationships first when dropping.
            for change in rel_changes.iter().chain(node_changes.iter()) {
                if !change.setup_status.actions.drop_existing {
                    continue;
                }
                change
                    .setup_status
                    .add_drop_cypher(&change.key.typ, &mut cypher_builder)?;

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
                change
                    .setup_status
                    .add_create_alter_cypher(&change.key.typ, &mut cypher_builder)?;
            }

            debug!(
                "Partial affected node tables: {:?}",
                partial_affected_node_tables
            );
            for table in partial_affected_node_tables {
                GraphElementDataSetupStatus::add_delete_orphaned_nodes_cypher(
                    table,
                    &mut cypher_builder,
                )?;
            }

            client.run_cypher(cypher_builder).await?;
        }
        Ok(())
    }
}
