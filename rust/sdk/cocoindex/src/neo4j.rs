//! Neo4j graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;

use crate::ctx::Ctx;
use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::statediff::ManagedTargetOptions;

pub use cypher_graph::{ColumnDef, TableSchema};

#[derive(Clone)]
pub struct Graph {
    db: Arc<neo4rs::Graph>,
    database: Arc<str>,
    state_id: Arc<str>,
}

fn normalize_uri(uri: &str) -> String {
    uri.strip_prefix("bolt://")
        .or_else(|| uri.strip_prefix("neo4j://"))
        .unwrap_or(uri)
        .to_string()
}

impl Graph {
    pub async fn connect(uri: &str, user: &str, password: &str, database: &str) -> Result<Self> {
        validate_database(database)?;
        let config = neo4rs::ConfigBuilder::default()
            .uri(normalize_uri(uri))
            .user(user)
            .password(password)
            .db(database)
            .build()
            .map_err(|e| Error::engine(format!("neo4j config: {e}")))?;
        let db = neo4rs::Graph::connect(config)
            .await
            .map_err(|e| Error::engine(format!("neo4j connect: {e}")))?;
        Ok(Self {
            db: Arc::new(db),
            database: Arc::from(database.to_string()),
            state_id: Arc::from(format!("{uri}/{database}")),
        })
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

#[async_trait]
impl cypher_graph::CypherExecutor for Graph {
    fn dialect(&self) -> &'static str {
        "neo4j"
    }

    fn state_id(&self) -> &str {
        &self.state_id
    }

    async fn execute(&self, cypher: &str) -> Result<()> {
        self.db
            .run_on(&self.database, neo4rs::query(cypher))
            .await
            .map_err(|e| Error::engine(format!("neo4j: {e}")))
    }
}

#[derive(Clone)]
pub struct TableTarget(cypher_graph::TableTarget);

impl TableTarget {
    pub fn table_name(&self) -> &str {
        self.0.table_name()
    }

    pub fn declare_record<R: serde::Serialize>(
        &self,
        ctx: &Ctx,
        id: impl crate::target_state::IntoStableKey,
        row: &R,
    ) -> Result<()> {
        self.0.declare_record(ctx, id, row)
    }
}

#[derive(Clone)]
pub struct RelationTarget(cypher_graph::RelationTarget);

impl RelationTarget {
    pub fn declare_relation(
        &self,
        ctx: &Ctx,
        from_id: impl crate::target_state::IntoStableKey,
        to_id: impl crate::target_state::IntoStableKey,
    ) -> Result<()> {
        self.0.declare_relation(ctx, from_id, to_id)
    }

    pub fn declare_relation_record<R: serde::Serialize>(
        &self,
        ctx: &Ctx,
        from_id: impl crate::target_state::IntoStableKey,
        to_id: impl crate::target_state::IntoStableKey,
        record: &R,
    ) -> Result<()> {
        self.0.declare_relation_record(ctx, from_id, to_id, record)
    }
}

pub async fn mount_table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    Ok(TableTarget(
        cypher_graph::mount_table_target_with_options(ctx, graph, table_name, schema, options)
            .await?,
    ))
}

pub async fn mount_relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    mount_relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    Ok(RelationTarget(
        cypher_graph::mount_relation_target_with_options(
            ctx,
            graph,
            relation_name,
            &from_table.0,
            &to_table.0,
            options,
        )
        .await?,
    ))
}

fn validate_database(database: &str) -> Result<()> {
    if database.is_empty()
        || database
            .chars()
            .any(|c| !(c == '_' || c == '-' || c == '.' || c.is_ascii_alphanumeric()))
    {
        return Err(Error::engine(format!(
            "Invalid Neo4j database name: {database:?}"
        )));
    }
    Ok(())
}
