//! FalkorDB graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::cypher_graph;
use crate::error::{Error, Result};

pub use cypher_graph::{ColumnDef, TableSchema, TableSpec, VectorMetric};

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

    async fn execute_with_params(
        &self,
        cypher: &str,
        params: &cypher_graph::CypherParams,
    ) -> Result<()> {
        // FalkorDB binds parameters via a `CYPHER name=value ...` header
        // prefixed to the query, keeping user values out of the query body.
        let full = if params.is_empty() {
            cypher.to_string()
        } else {
            let mut header = String::from("CYPHER ");
            for (name, value) in params {
                header.push_str(name);
                header.push('=');
                header.push_str(&cypher_graph::cypher_literal(value)?);
                header.push(' ');
            }
            format!("{header}{cypher}")
        };
        self.execute(&full).await
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

// The FalkorDB node-table / relation target surface (`TableTarget`,
// `RelationTarget`, and the `mount_*`/`declare_*`/`*_target` functions) is
// generated from the shared macro — it is identical to Neo4j's modulo the
// `Graph` executor type.
cypher_graph::graph_target_api!(Graph);

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
