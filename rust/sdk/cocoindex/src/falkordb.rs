//! FalkorDB graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::ctx::Ctx;
use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::statediff::ManagedTargetOptions;
use crate::target_state::TargetState;

pub use cypher_graph::{ColumnDef, TableSchema, TableSpec};

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

    async fn execute_unique_constraint(
        &self,
        op: &str,
        entity_kind: &str,
        label: &str,
        field: &str,
    ) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let _: redis::Value = redis::cmd("GRAPH.CONSTRAINT")
            .arg(op)
            .arg(self.graph.as_ref())
            .arg("UNIQUE")
            .arg(entity_kind)
            .arg(label)
            .arg("PROPERTIES")
            .arg("1")
            .arg(field)
            .query_async(&mut *conn)
            .await
            .map_err(|e| {
                Error::engine(format!(
                    "falkordb constraint {op} {entity_kind} {label}.{field}: {e}"
                ))
            })?;
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

/// Build a composable [`TargetState`] for a FalkorDB node table. Pass it to the
/// generic [`mount_target`](crate::target_state::mount_target) /
/// [`declare_target_state_with_child`](crate::target_state::declare_target_state_with_child),
/// or use [`declare_table_target`]/[`mount_table_target`].
pub fn table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`table_target`] with explicit [`ManagedTargetOptions`].
pub fn table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    cypher_graph::table_target_state(ctx, graph, table_name, schema, options)
}

/// Build a composable [`TargetState`] for a FalkorDB relation.
pub fn relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<TargetState<TableSpec>> {
    relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
}

/// [`relation_target`] with explicit [`ManagedTargetOptions`].
pub fn relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    cypher_graph::relation_target_state(
        ctx,
        graph,
        relation_name,
        &from_table.0,
        &to_table.0,
        options,
    )
}

/// Declare a FalkorDB node table target in the **current** component and return
/// a pending handle. The record child provider resolves when this component
/// commits; use [`mount_table_target`] when records must be declared
/// immediately.
pub fn declare_table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    Ok(TableTarget(
        cypher_graph::declare_table_target_with_options(ctx, graph, table_name, schema, options)?,
    ))
}

/// Declare a FalkorDB relation target in the **current** component and return a
/// pending handle. Use [`mount_relation_target`] when relation records must be
/// declared immediately.
pub fn declare_relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    declare_relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_relation_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    Ok(RelationTarget(
        cypher_graph::declare_relation_target_with_options(
            ctx,
            graph,
            relation_name,
            &from_table.0,
            &to_table.0,
            options,
        )?,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::Serialize;

    use super::*;
    use crate::{App, ContextKey};

    static GRAPH: LazyLock<ContextKey<Graph>> = LazyLock::new(|| ContextKey::new("falkordb_graph"));

    #[derive(Serialize)]
    struct Person {
        name: String,
    }

    fn nonce() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    async fn try_graph(graph_name: &str) -> Option<Graph> {
        let uri = std::env::var("FALKORDB_URI").ok()?;
        Graph::connect(&uri, graph_name).await.ok()
    }

    fn first_count(value: &redis::Value) -> Option<i64> {
        let redis::Value::Array(top) = value else {
            return None;
        };
        let redis::Value::Array(rows) = top.get(1)? else {
            return None;
        };
        let redis::Value::Array(row) = rows.first()? else {
            return None;
        };
        match row.first()? {
            redis::Value::Int(count) => Some(*count),
            redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
            redis::Value::SimpleString(s) => s.parse().ok(),
            _ => None,
        }
    }

    async fn count(graph: &Graph, cypher: &str) -> Result<i64> {
        let mut conn = graph.conn.lock().await;
        let value: redis::Value = redis::cmd("GRAPH.QUERY")
            .arg(graph.graph.as_ref())
            .arg(cypher)
            .query_async(&mut *conn)
            .await
            .map_err(|e| Error::engine(format!("falkordb count query: {e}")))?;
        first_count(&value)
            .ok_or_else(|| Error::engine(format!("falkordb count decode failed: {value:?}")))
    }

    async fn delete_graph(graph: &Graph) {
        let mut conn = graph.conn.lock().await;
        let _: redis::RedisResult<redis::Value> = redis::cmd("GRAPH.DELETE")
            .arg(graph.graph.as_ref())
            .query_async(&mut *conn)
            .await;
    }

    async fn run_people_graph(
        app: &App,
        person_label: String,
        relation_label: String,
        include_bob: bool,
    ) -> Result<()> {
        app.run(move |ctx| {
            let person_label = person_label.clone();
            let relation_label = relation_label.clone();
            async move {
                let graph = ctx.get_key(&GRAPH)?;
                let schema = TableSchema::new([("name", ColumnDef::new("string"))], "name")?;
                let people = mount_table_target(&ctx, graph, person_label, schema).await?;
                let knows =
                    mount_relation_target(&ctx, graph, relation_label, &people, &people).await?;

                people.declare_record(
                    &ctx,
                    "alice",
                    &Person {
                        name: "alice".to_string(),
                    },
                )?;
                if include_bob {
                    people.declare_record(
                        &ctx,
                        "bob",
                        &Person {
                            name: "bob".to_string(),
                        },
                    )?;
                    knows.declare_relation(&ctx, "alice", "bob")?;
                }
                Ok(())
            }
        })
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn falkordb_target_reconciles_records_and_relations_when_available() -> Result<()> {
        let nonce = nonce();
        let graph_name = format!("cocoindex_rust_falkor_{nonce}");
        let Some(graph) = try_graph(&graph_name).await else {
            eprintln!("skipping live FalkorDB target test; FALKORDB_URI is not set or unavailable");
            return Ok(());
        };
        delete_graph(&graph).await;

        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("FalkorDBTargetE2ETest")
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?;

        run_people_graph(&app, person_label.clone(), relation_label.clone(), true).await?;
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) RETURN count(n)")
            )
            .await?,
            2
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r)"),
            )
            .await?,
            1
        );

        run_people_graph(&app, person_label.clone(), relation_label.clone(), false).await?;
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) RETURN count(n)")
            )
            .await?,
            1
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r)"),
            )
            .await?,
            0
        );

        delete_graph(&graph).await;
        Ok(())
    }
}
