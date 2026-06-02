//! FalkorDB graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::ctx::Ctx;
use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::statediff::ManagedTargetOptions;

pub use cypher_graph::{ColumnDef, TableSchema};

#[derive(Clone)]
pub struct Graph {
    conn: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    graph: Arc<str>,
    state_id: Arc<str>,
}

fn normalize_uri(uri: &str) -> String {
    uri.strip_prefix("falkor://")
        .map(|rest| format!("redis://{rest}"))
        .unwrap_or_else(|| uri.to_string())
}

impl Graph {
    pub async fn connect(uri: &str, graph: &str) -> Result<Self> {
        cypher_graph::validate_ident(graph, "graph name")?;
        let client = redis::Client::open(normalize_uri(uri))
            .map_err(|e| Error::engine(format!("falkordb config: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::engine(format!("falkordb connect: {e}")))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            graph: Arc::from(graph.to_string()),
            state_id: Arc::from(format!("{uri}/{graph}")),
        })
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

#[async_trait]
impl cypher_graph::CypherExecutor for Graph {
    fn dialect(&self) -> &'static str {
        "falkordb"
    }

    fn state_id(&self) -> &str {
        &self.state_id
    }

    async fn execute(&self, cypher: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let _: redis::Value = redis::cmd("GRAPH.QUERY")
            .arg(self.graph.as_ref())
            .arg(cypher)
            .query_async(&mut *conn)
            .await
            .map_err(|e| Error::engine(format!("falkordb: {e}; query: {cypher}")))?;
        Ok(())
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
