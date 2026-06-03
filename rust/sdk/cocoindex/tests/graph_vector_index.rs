//! Live graph vector-index e2e (Neo4j + FalkorDB).
//!
//! Each test skips gracefully when its server is unreachable. Run with:
//!   NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=cocoindex \
//!   FALKORDB_URI=falkor://localhost:6379 \
//!     cargo test -p cocoindex --features neo4j,falkordb --test graph_vector_index
//!
//! Verifies that `declare_vector_index` emits Cypher the real server accepts:
//! a node table + vector index is created on the first run, and the index is
//! dropped on a second run that no longer declares it (the table is kept).
#![cfg(any(feature = "neo4j", feature = "falkordb"))]

#[cfg(feature = "neo4j")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "neo4j")]
fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[cfg(feature = "neo4j")]
#[tokio::test]
async fn neo4j_vector_index_create_then_drop_when_available() {
    use cocoindex::neo4j::{self, ColumnDef, TableSchema, VectorMetric};
    use cocoindex::{App, ContextKey, Result};
    use std::sync::LazyLock;

    static G: LazyLock<ContextKey<neo4j::Graph>> =
        LazyLock::new(|| ContextKey::new("neo4j_vidx_graph"));

    let uri = std::env::var("NEO4J_URI").unwrap_or_else(|_| "bolt://localhost:7687".to_string());
    let user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
    let password = std::env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "cocoindex".to_string());
    let database = std::env::var("NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string());
    let Ok(graph) = neo4j::Graph::connect(&uri, &user, &password, &database).await else {
        eprintln!("skipping Neo4j vector-index test; no reachable server");
        return;
    };

    let label = format!("VecDoc{}", nonce());
    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("Neo4jVectorIndex")
        .db_path(tmp.path().join("db"))
        .provide_key(&G, graph)
        .build()
        .await
        .unwrap();

    fn schema() -> Result<TableSchema> {
        TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("name", ColumnDef::new("STRING")),
                ("embedding", ColumnDef::new("LIST")),
            ],
            "id",
        )
    }

    // Run 1: create the table + a vector index + one node.
    app.run({
        let label = label.clone();
        move |ctx| {
            let label = label.clone();
            async move {
                let g = ctx.get_key(&G)?;
                let table = neo4j::mount_table_target(&ctx, g, label, schema()?).await?;
                table.declare_vector_index(&ctx, "embedding", 3, VectorMetric::Cosine)?;
                table.declare_node_index(&ctx, &["name"])?;
                table.declare_record(
                    &ctx,
                    1_i64,
                    &serde_json::json!({ "id": 1, "name": "doc-1", "embedding": [0.1, 0.2, 0.3] }),
                )?;
                Ok(())
            }
        }
    })
    .await
    .expect("creating a Neo4j vector index should be accepted by the server");

    // Run 2: keep the table + node, stop declaring the index → it is dropped.
    app.run({
        let label = label.clone();
        move |ctx| {
            let label = label.clone();
            async move {
                let g = ctx.get_key(&G)?;
                let table = neo4j::mount_table_target(&ctx, g, label, schema()?).await?;
                table.declare_record(
                    &ctx,
                    1_i64,
                    &serde_json::json!({ "id": 1, "name": "doc-1", "embedding": [0.1, 0.2, 0.3] }),
                )?;
                Ok(())
            }
        }
    })
    .await
    .expect("dropping the orphaned Neo4j vector index should be accepted");
}

#[cfg(feature = "falkordb")]
#[tokio::test]
async fn falkordb_vector_index_create_then_drop_when_available() {
    use cocoindex::falkordb::{self, ColumnDef, TableSchema, VectorMetric};
    use cocoindex::{App, ContextKey, Result};
    use std::sync::LazyLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    static G: LazyLock<ContextKey<falkordb::Graph>> =
        LazyLock::new(|| ContextKey::new("falkordb_vidx_graph"));

    let uri =
        std::env::var("FALKORDB_URI").unwrap_or_else(|_| "falkor://localhost:6379".to_string());
    let graph_name = format!(
        "vidx_test_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let Ok(graph) = falkordb::Graph::connect(&uri, &graph_name).await else {
        eprintln!("skipping FalkorDB vector-index test; no reachable server");
        return;
    };

    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("FalkorDbVectorIndex")
        .db_path(tmp.path().join("db"))
        .provide_key(&G, graph)
        .build()
        .await
        .unwrap();

    fn schema() -> Result<TableSchema> {
        TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("name", ColumnDef::new("STRING")),
                ("embedding", ColumnDef::new("VECTOR")),
            ],
            "id",
        )
    }

    // Run 1: create the table + a vector index + one node.
    app.run(move |ctx| async move {
        let g = ctx.get_key(&G)?;
        let table = falkordb::mount_table_target(&ctx, g, "Doc", schema()?).await?;
        table.declare_vector_index(&ctx, "embedding", 3, VectorMetric::Cosine)?;
        table.declare_node_index(&ctx, &["name"])?;
        table.declare_record(
            &ctx,
            1_i64,
            &serde_json::json!({ "id": 1, "name": "doc-1", "embedding": [0.1, 0.2, 0.3] }),
        )?;
        Ok(())
    })
    .await
    .expect("creating a FalkorDB vector index should be accepted by the server");

    // Run 2: keep the table + node, stop declaring the index → it is dropped.
    app.run(move |ctx| async move {
        let g = ctx.get_key(&G)?;
        let table = falkordb::mount_table_target(&ctx, g, "Doc", schema()?).await?;
        table.declare_record(
            &ctx,
            1_i64,
            &serde_json::json!({ "id": 1, "name": "doc-1", "embedding": [0.1, 0.2, 0.3] }),
        )?;
        Ok(())
    })
    .await
    .expect("dropping the orphaned FalkorDB vector index should be accepted");
}
