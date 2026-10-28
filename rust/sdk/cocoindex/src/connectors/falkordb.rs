//! FalkorDB graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::target_state::{STATIC_ROOT_PROVIDERS, StaticRootProvider};

pub use cypher_graph::{ColumnDef, TableSchema, TableSpec, VectorMetric};

#[derive(Clone)]
pub struct Graph {
    conn: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    graph: Arc<str>,
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
        })
    }
}

/// Root provider for every FalkorDB table/relation target; the graph is named by
/// each target state's key and resolved from the environment at apply time.
#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    cypher_graph::table_root_provider::<Graph>("cocoindex/falkordb/table");

#[async_trait]
impl cypher_graph::CypherExecutor for Graph {
    fn dialect(&self) -> &'static str {
        "falkordb"
    }

    fn table_provider() -> &'static StaticRootProvider {
        &TABLE_PROVIDER
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::Serialize;

    use super::*;
    use crate::{App, Environment};

    crate::context_key!(static GRAPH: Graph, key = "falkordb_graph");

    #[derive(Serialize)]
    struct Person {
        name: String,
    }

    #[derive(Serialize)]
    struct PersonById {
        id: String,
        name: String,
    }

    /// The people graph re-keyed by `id`: an identity change for both the
    /// node table and the relation whose endpoints it defines.
    async fn run_people_graph_keyed_by_id(
        app: &App,
        person_label: String,
        relation_label: String,
    ) -> Result<()> {
        app.run(move |ctx| {
            let person_label = person_label.clone();
            let relation_label = relation_label.clone();
            async move {
                let schema = TableSchema::new(
                    [
                        ("id", ColumnDef::new("string")),
                        ("name", ColumnDef::new("string")),
                    ],
                    "id",
                )?;
                let people = mount_table_target(&ctx, &GRAPH, person_label, schema).await?;
                let knows =
                    mount_relation_target(&ctx, &GRAPH, relation_label, &people, &people).await?;
                people.declare_record(
                    &ctx,
                    "a1",
                    &PersonById {
                        id: "a1".to_string(),
                        name: "alice".to_string(),
                    },
                )?;
                people.declare_record(
                    &ctx,
                    "b1",
                    &PersonById {
                        id: "b1".to_string(),
                        name: "bob".to_string(),
                    },
                )?;
                knows.declare_relation(&ctx, "a1", "b1")?;
                Ok(())
            }
        })
        .await
        .map(|_| ())
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
        with_indexes: bool,
    ) -> Result<()> {
        app.run(move |ctx| {
            let person_label = person_label.clone();
            let relation_label = relation_label.clone();
            async move {
                let schema = TableSchema::new([("name", ColumnDef::new("string"))], "name")?;
                let people = mount_table_target(&ctx, &GRAPH, person_label, schema).await?;
                let knows =
                    mount_relation_target(&ctx, &GRAPH, relation_label, &people, &people).await?;
                if with_indexes {
                    people.declare_vector_index(&ctx, "embedding", 3, VectorMetric::Cosine)?;
                    people.declare_node_index(&ctx, &["nickname"])?;
                }

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
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("FalkorDBTargetE2ETest")
            .await?;

        run_people_graph(
            &app,
            person_label.clone(),
            relation_label.clone(),
            true,
            false,
        )
        .await?;
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

        run_people_graph(
            &app,
            person_label.clone(),
            relation_label.clone(),
            false,
            false,
        )
        .await?;
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

    /// Which schema objects owned by the people graph's targets exist: the
    /// table's and the relation's primary-key index and uniqueness constraint,
    /// and the table's vector and node indexes.
    #[derive(Debug, PartialEq)]
    struct SchemaObjects {
        table: bool,
        relation: bool,
        vector_index: bool,
        node_index: bool,
    }

    impl SchemaObjects {
        fn all(present: bool) -> Self {
            Self {
                table: present,
                relation: present,
                vector_index: present,
                node_index: present,
            }
        }

        async fn read(graph: &Graph, person_label: &str, relation_label: &str) -> Result<Self> {
            let indexed = |label: &str, field: &str| {
                let cypher = format!(
                    "CALL db.indexes() YIELD label, properties WHERE label = '{label}' \
                     UNWIND properties AS p WITH p WHERE p = '{field}' RETURN count(p)"
                );
                async move { Ok::<_, Error>(count(graph, &cypher).await? == 1) }
            };
            let constrained = |label: &str| {
                let cypher = format!(
                    "CALL db.constraints() YIELD label WHERE label = '{label}' RETURN count(label)"
                );
                async move { Ok::<_, Error>(count(graph, &cypher).await? == 1) }
            };
            Ok(Self {
                table: indexed(person_label, "name").await? && constrained(person_label).await?,
                relation: indexed(relation_label, "id").await?
                    && constrained(relation_label).await?,
                vector_index: indexed(person_label, "embedding").await?,
                node_index: indexed(person_label, "nickname").await?,
            })
        }
    }

    /// The number of nodes carrying `person_label` and of relationships of
    /// type `relation_label`.
    async fn contents(
        graph: &Graph,
        person_label: &str,
        relation_label: &str,
    ) -> Result<(i64, i64)> {
        let nodes = count(
            graph,
            &format!("MATCH (n:`{person_label}`) RETURN count(n)"),
        )
        .await?;
        let relationships = count(
            graph,
            &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r)"),
        )
        .await?;
        Ok((nodes, relationships))
    }

    #[tokio::test]
    async fn falkordb_undeclared_and_dropped_targets_are_removed_when_available() -> Result<()> {
        let nonce = nonce();
        let graph_name = format!("cocoindex_rust_falkor_removal_{nonce}");
        let Some(graph) = try_graph(&graph_name).await else {
            eprintln!(
                "skipping live FalkorDB target-removal test; FALKORDB_URI is not set or unavailable"
            );
            return Ok(());
        };
        delete_graph(&graph).await;

        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");
        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("FalkorDBTargetRemovalE2ETest")
            .await?;

        run_people_graph(
            &app,
            person_label.clone(),
            relation_label.clone(),
            true,
            true,
        )
        .await?;
        assert_eq!(
            SchemaObjects::read(&graph, &person_label, &relation_label).await?,
            SchemaObjects::all(true)
        );
        assert_eq!(
            contents(&graph, &person_label, &relation_label).await?,
            (2, 1)
        );

        // A run that no longer declares the table and relation removes them,
        // with their nodes/relationships and the table's indexes.
        app.run(|_ctx| async { Ok(()) }).await?;
        assert_eq!(
            SchemaObjects::read(&graph, &person_label, &relation_label).await?,
            SchemaObjects::all(false),
            "a table/relation no longer declared is removed"
        );
        assert_eq!(
            contents(&graph, &person_label, &relation_label).await?,
            (0, 0),
            "a table/relation no longer declared takes its contents along"
        );

        run_people_graph(
            &app,
            person_label.clone(),
            relation_label.clone(),
            true,
            true,
        )
        .await?;
        assert_eq!(
            SchemaObjects::read(&graph, &person_label, &relation_label).await?,
            SchemaObjects::all(true)
        );

        app.drop_state().await?;
        assert_eq!(
            SchemaObjects::read(&graph, &person_label, &relation_label).await?,
            SchemaObjects::all(false),
            "drop_state removes the table/relation"
        );
        assert_eq!(
            contents(&graph, &person_label, &relation_label).await?,
            (0, 0),
            "drop_state removes the table/relation contents"
        );

        delete_graph(&graph).await;
        Ok(())
    }

    #[tokio::test]
    async fn falkordb_primary_key_change_rebuilds_table_when_available() -> Result<()> {
        let nonce = nonce();
        let graph_name = format!("cocoindex_rust_falkor_rebuild_{nonce}");
        let Some(graph) = try_graph(&graph_name).await else {
            eprintln!(
                "skipping live FalkorDB rebuild test; FALKORDB_URI is not set or unavailable"
            );
            return Ok(());
        };
        delete_graph(&graph).await;

        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");
        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("FalkorDBTargetRebuildE2ETest")
            .await?;

        run_people_graph(
            &app,
            person_label.clone(),
            relation_label.clone(),
            true,
            false,
        )
        .await?;
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) RETURN count(n)")
            )
            .await?,
            2
        );

        // Re-keying the table by `id` rebuilds it: the nodes keyed by `name`
        // (with the relationships hanging on them) are deleted before the
        // records are declared again under the new key.
        run_people_graph_keyed_by_id(&app, person_label.clone(), relation_label.clone()).await?;
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
                &format!("MATCH (n:`{person_label}`) WHERE n.id IS NULL RETURN count(n)"),
            )
            .await?,
            0
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r)"),
            )
            .await?,
            1
        );

        delete_graph(&graph).await;
        Ok(())
    }
}
