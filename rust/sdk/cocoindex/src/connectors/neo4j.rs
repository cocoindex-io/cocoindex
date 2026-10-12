//! Neo4j graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::target_state::{STATIC_ROOT_PROVIDERS, StaticRootProvider};

pub use cypher_graph::{ColumnDef, TableSchema, TableSpec, VectorMetric};

#[derive(Clone)]
pub struct Graph {
    db: Arc<neo4rs::Graph>,
    database: Arc<str>,
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
        })
    }
}

/// Root provider for every Neo4j table/relation target; the graph is named by
/// each target state's key and resolved from the environment at apply time.
#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    cypher_graph::table_root_provider::<Graph>("cocoindex/neo4j/table");

#[async_trait]
impl cypher_graph::CypherExecutor for Graph {
    fn dialect(&self) -> &'static str {
        "neo4j"
    }

    fn table_provider() -> &'static StaticRootProvider {
        &TABLE_PROVIDER
    }

    async fn execute(&self, cypher: &str) -> Result<()> {
        self.db
            .run_on(&self.database, neo4rs::query(cypher))
            .await
            .map_err(|e| Error::engine(format!("neo4j: {e}")))
    }

    async fn execute_with_params(
        &self,
        cypher: &str,
        params: &cypher_graph::CypherParams,
    ) -> Result<()> {
        let mut query = neo4rs::query(cypher);
        for (name, value) in params {
            query = query.param(name, json_to_bolt(value)?);
        }
        self.db
            .run_on(&self.database, query)
            .await
            .map_err(|e| Error::engine(format!("neo4j: {e}")))
    }
}

/// Convert a JSON value to a Bolt parameter value (cf. the Python driver's
/// implicit JSON-to-Bolt mapping for query parameters).
fn json_to_bolt(value: &serde_json::Value) -> Result<neo4rs::BoltType> {
    use neo4rs::{
        BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltMap, BoltNull, BoltString, BoltType,
    };
    Ok(match value {
        serde_json::Value::Null => BoltType::Null(BoltNull),
        serde_json::Value::Bool(b) => BoltType::Boolean(BoltBoolean::new(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BoltType::Integer(BoltInteger::new(i))
            } else if let Some(f) = n.as_f64() {
                BoltType::Float(BoltFloat::new(f))
            } else {
                return Err(Error::engine(format!(
                    "neo4j: JSON number cannot be represented as a Bolt parameter: {n}"
                )));
            }
        }
        serde_json::Value::String(s) => BoltType::String(BoltString::new(s)),
        serde_json::Value::Array(items) => BoltType::List(BoltList {
            value: items.iter().map(json_to_bolt).collect::<Result<_>>()?,
        }),
        serde_json::Value::Object(map) => BoltType::Map(BoltMap {
            value: map
                .iter()
                .map(|(k, v)| Ok((BoltString::new(k), json_to_bolt(v)?)))
                .collect::<Result<_>>()?,
        }),
    })
}

// The Neo4j node-table / relation target surface (`TableTarget`,
// `RelationTarget`, and the `mount_*`/`declare_*`/`*_target` functions) is
// generated from the shared macro — it is identical to FalkorDB's modulo the
// `Graph` executor type.
cypher_graph::graph_target_api!(Graph);

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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::Serialize;

    use super::*;
    use crate::{App, Environment};

    crate::context_key!(static GRAPH: Graph, key = "neo4j_graph");

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
                        ("id", ColumnDef::new("STRING")),
                        ("name", ColumnDef::new("STRING")),
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

    #[test]
    fn json_to_bolt_maps_json_values() {
        let value = serde_json::json!({
            "name": "Alice",
            "score": 42,
            "weights": [1.5, null],
        });

        let bolt = json_to_bolt(&value).unwrap();
        let neo4rs::BoltType::Map(map) = bolt else {
            panic!("expected Bolt map");
        };
        let neo4rs::BoltType::String(name) = map.value.get("name").unwrap() else {
            panic!("expected name string");
        };
        assert_eq!(name.value, "Alice");
        let neo4rs::BoltType::Integer(score) = map.value.get("score").unwrap() else {
            panic!("expected score integer");
        };
        assert_eq!(score.value, 42);
        let neo4rs::BoltType::List(weights) = map.value.get("weights").unwrap() else {
            panic!("expected weights list");
        };
        let neo4rs::BoltType::Float(weight) = &weights.value[0] else {
            panic!("expected first weight float");
        };
        assert_eq!(weight.value, 1.5);
        assert!(matches!(weights.value[1], neo4rs::BoltType::Null(_)));
    }

    async fn try_graph() -> Option<Graph> {
        let uri = std::env::var("NEO4J_URI").ok()?;
        let user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
        let password = std::env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "cocoindex".to_string());
        let database = std::env::var("NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string());
        Graph::connect(&uri, &user, &password, &database).await.ok()
    }

    fn nonce() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    async fn count(graph: &Graph, cypher: &str) -> Result<i64> {
        let mut rows = graph
            .db
            .execute_on(&graph.database, neo4rs::query(cypher))
            .await
            .map_err(|e| Error::engine(format!("neo4j count query: {e}")))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Error::engine(format!("neo4j count row: {e}")))?
            .ok_or_else(|| Error::engine("neo4j count query returned no rows"))?;
        row.get("count")
            .map_err(|e| Error::engine(format!("neo4j count decode: {e}")))
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
                let schema = TableSchema::new([("name", ColumnDef::new("STRING"))], "name")?;
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
    async fn neo4j_target_reconciles_records_and_relations_when_available() -> Result<()> {
        let Some(graph) = try_graph().await else {
            eprintln!("skipping live Neo4j target test; NEO4J_URI is not set or unavailable");
            return Ok(());
        };
        let nonce = nonce();
        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");
        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();

        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("Neo4jTargetE2ETest")
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
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
            )
            .await?,
            2
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
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
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
            )
            .await?,
            1
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
            )
            .await?,
            0
        );

        // Revert the constraint and index the targets created.
        app.drop_state().await?;
        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();
        Ok(())
    }

    /// Which schema objects owned by the people graph's targets exist: the
    /// table's uniqueness constraint, the relation's index, and the table's
    /// vector and node indexes.
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
            let exists = |show: &'static str, name: String| async move {
                let cypher = format!(
                    "SHOW {show} YIELD name WHERE name = '{name}' RETURN count(*) AS count"
                );
                Ok::<_, Error>(count(graph, &cypher).await? == 1)
            };
            Ok(Self {
                table: exists("CONSTRAINTS", format!("coco_uniq_{person_label}__name")).await?,
                relation: exists("INDEXES", format!("coco_idx_rel_{relation_label}__id")).await?,
                vector_index: exists(
                    "INDEXES",
                    cypher_graph::vector_index_name(person_label, "embedding"),
                )
                .await?,
                node_index: exists(
                    "INDEXES",
                    cypher_graph::node_index_name(person_label, &["nickname".to_string()]),
                )
                .await?,
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
            &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count"),
        )
        .await?;
        let relationships = count(
            graph,
            &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
        )
        .await?;
        Ok((nodes, relationships))
    }

    #[tokio::test]
    async fn neo4j_undeclared_and_dropped_targets_are_removed_when_available() -> Result<()> {
        let Some(graph) = try_graph().await else {
            eprintln!(
                "skipping live Neo4j target-removal test; NEO4J_URI is not set or unavailable"
            );
            return Ok(());
        };
        let nonce = nonce();
        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");

        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("Neo4jTargetRemovalE2ETest")
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

        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();
        Ok(())
    }

    #[tokio::test]
    async fn neo4j_primary_key_change_rebuilds_table_when_available() -> Result<()> {
        let Some(graph) = try_graph().await else {
            eprintln!("skipping live Neo4j rebuild test; NEO4J_URI is not set or unavailable");
            return Ok(());
        };
        let nonce = nonce();
        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");

        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?
            .app("Neo4jTargetRebuildE2ETest")
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
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
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
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
            )
            .await?,
            2
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) WHERE n.id IS NULL RETURN count(n) AS count"),
            )
            .await?,
            0
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
            )
            .await?,
            1
        );

        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();
        Ok(())
    }
}
