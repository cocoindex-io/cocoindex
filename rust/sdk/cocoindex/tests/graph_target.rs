//! Graph target tests (Neo4j + FalkorDB): derived schemas, schemaless tables,
//! relation schemas and named vector indexes.
//!
//! The schema tests need no server. Each live test skips when its server's URI
//! is unset. Run with:
//!   NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=cocoindex \
//!   FALKORDB_URI=falkor://localhost:6379 \
//!     cargo test -p cocoindex --features neo4j,falkordb --test graph_target
//!
//! The live tests verify that the server accepts what the targets emit; the
//! exact Cypher is asserted by the unit tests in `cypher_graph.rs`.
#![cfg(any(feature = "neo4j", feature = "falkordb"))]

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "falkordb")]
use cocoindex::connectors::falkordb;
#[cfg(feature = "neo4j")]
use cocoindex::connectors::neo4j;
// Both connectors re-export the same schema types.
#[cfg(not(feature = "neo4j"))]
use cocoindex::connectors::falkordb::{ColumnDef, TableSchema};
#[cfg(feature = "neo4j")]
use cocoindex::connectors::neo4j::{ColumnDef, TableSchema};
use cocoindex::{Environment, Result, SchemaFields};
use serde::Serialize;

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[derive(Serialize, SchemaFields)]
struct Person {
    id: String,
    name: Option<String>,
    age: i64,
    #[coco(vector)]
    embedding: Vec<f32>,
}

#[derive(Serialize, SchemaFields)]
struct Knows {
    edge_id: String,
    since: i64,
}

#[test]
fn from_row_matches_explicit_schema() -> Result<()> {
    #[derive(SchemaFields)]
    #[allow(dead_code)]
    struct Row {
        id: String,
        title: Option<String>,
        views: i64,
        rank: i32,
        score: f64,
        active: bool,
        blob: Vec<u8>,
        elapsed: std::time::Duration,
        #[coco(vector = 4)]
        embedding: Vec<f32>,
        #[coco(vector)]
        unsized_embedding: Vec<f32>,
        #[coco(json)]
        tags: Vec<String>,
        #[coco(type = "POINT")]
        location: String,
        #[coco(rename = "label")]
        name: String,
    }

    let explicit = TableSchema::new(
        [
            ("id", ColumnDef::new("STRING")),
            ("title", ColumnDef::new("STRING").nullable()),
            ("views", ColumnDef::new("INTEGER")),
            ("rank", ColumnDef::new("INTEGER")),
            ("score", ColumnDef::new("FLOAT")),
            ("active", ColumnDef::new("BOOLEAN")),
            ("blob", ColumnDef::new("BYTES")),
            ("elapsed", ColumnDef::new("DURATION")),
            // The dimension belongs to the vector index, not the property.
            ("embedding", ColumnDef::new("LIST<FLOAT>")),
            ("unsized_embedding", ColumnDef::new("LIST<FLOAT>")),
            ("tags", ColumnDef::new("MAP")),
            ("location", ColumnDef::new("POINT")),
            ("label", ColumnDef::new("STRING")),
        ],
        "id",
    )?;
    assert_eq!(TableSchema::from_row::<Row>("id")?, explicit);
    Ok(())
}

#[test]
fn from_row_requires_the_primary_key_to_be_a_field() {
    let err = TableSchema::from_row::<Knows>("id").unwrap_err();
    assert!(err.to_string().contains("\"id\" not found"), "{err}");
}

#[test]
fn schemaless_validates_the_primary_key() {
    assert!(TableSchema::schemaless("id").is_ok());
    assert!(TableSchema::schemaless("not valid").is_err());
}

/// Declares, for one graph connector, the graph the live tests run: a `Person`
/// table with a derived schema and a named vector index, a schemaless `Note`
/// table, a `KNOWS` relation keyed by its own `edge_id`, and a schemaless
/// `WROTE` relation.
macro_rules! declare_people_graph {
    ($connector:ident, $ctx:expr, $graph:expr, $suffix:expr, $index_name:expr) => {{
        let ctx = $ctx;
        let suffix: &str = $suffix;
        let people = $connector::mount_table_target(
            ctx,
            $graph,
            format!("Person{suffix}"),
            TableSchema::from_row::<Person>("id")?,
        )
        .await?;
        let notes = $connector::mount_table_target(
            ctx,
            $graph,
            format!("Note{suffix}"),
            TableSchema::schemaless("id")?,
        )
        .await?;
        let knows = $connector::mount_relation_target_with_schema(
            ctx,
            $graph,
            format!("KNOWS{suffix}"),
            &people,
            &people,
            TableSchema::from_row::<Knows>("edge_id")?,
        )
        .await?;
        let wrote = $connector::mount_relation_target(
            ctx,
            $graph,
            format!("WROTE{suffix}"),
            &people,
            &notes,
        )
        .await?;

        for (id, name) in [("a", Some("alice")), ("b", None)] {
            people.declare_record(
                ctx,
                id,
                &Person {
                    id: id.to_string(),
                    name: name.map(str::to_string),
                    age: 30,
                    embedding: vec![0.1, 0.2, 0.3],
                },
            )?;
        }
        people.declare_vector_index_named(
            ctx,
            $index_name,
            "embedding",
            3,
            $connector::VectorMetric::Cosine,
        )?;
        notes.declare_record(
            ctx,
            1_i64,
            &serde_json::json!({ "id": 1, "text": "hello", "pinned": true }),
        )?;
        knows.declare_relation_record(
            ctx,
            "a",
            "b",
            &Knows {
                edge_id: "e1".to_string(),
                since: 2020,
            },
        )?;
        wrote.declare_relation(ctx, "a", 1_i64)?;
        Ok(())
    }};
}

#[cfg(feature = "neo4j")]
#[tokio::test]
async fn neo4j_schemaless_tables_relation_schema_and_named_index_when_available() {
    cocoindex::context_key!(static G: neo4j::Graph, key = "neo4j_graph_target_test");

    let Ok(uri) = std::env::var("NEO4J_URI") else {
        eprintln!("skipping live Neo4j graph target test; NEO4J_URI is not set");
        return;
    };
    let user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
    let password = std::env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "cocoindex".to_string());
    let database = std::env::var("NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string());
    let graph = neo4j::Graph::connect(&uri, &user, &password, &database)
        .await
        .expect("NEO4J_URI is set but the server is unreachable");

    let suffix = nonce().to_string();
    let tmp = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&G, graph)
        .build()
        .await
        .unwrap()
        .app("Neo4jGraphTarget")
        .await
        .unwrap();

    // The second run renames the vector index: the declaration moves to the new
    // name and the database keeps one index on the field.
    for index_name in ["person_embedding", "person_embedding_v2"] {
        let suffix = suffix.clone();
        app.run(move |ctx| {
            let suffix = suffix.clone();
            async move { declare_people_graph!(neo4j, &ctx, &G, &suffix, index_name) }
        })
        .await
        .expect("the Neo4j server should accept the graph");
    }
    app.drop_state()
        .await
        .expect("dropping the Neo4j app state should be accepted");
}

#[cfg(feature = "falkordb")]
#[tokio::test]
async fn falkordb_schemaless_tables_relation_schema_and_named_index_when_available() {
    cocoindex::context_key!(static G: falkordb::Graph, key = "falkordb_graph_target_test");

    let Ok(uri) = std::env::var("FALKORDB_URI") else {
        eprintln!("skipping live FalkorDB graph target test; FALKORDB_URI is not set");
        return;
    };
    let graph = falkordb::Graph::connect(&uri, &format!("graph_target_test_{}", nonce()))
        .await
        .expect("FALKORDB_URI is set but the server is unreachable");

    let tmp = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tmp.path().join("db"))
        .provide_key(&G, graph)
        .build()
        .await
        .unwrap()
        .app("FalkorDbGraphTarget")
        .await
        .unwrap();

    for index_name in ["person_embedding", "person_embedding_v2"] {
        app.run(
            move |ctx| async move { declare_people_graph!(falkordb, &ctx, &G, "", index_name) },
        )
        .await
        .expect("the FalkorDB server should accept the graph");
    }
    app.drop_state()
        .await
        .expect("dropping the FalkorDB app state should be accepted");
}
