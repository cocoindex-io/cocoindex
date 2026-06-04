#![cfg(feature = "surrealdb")]

use std::sync::LazyLock;

use cocoindex::prelude::*;
use cocoindex::surrealdb::{self, ColumnDef, Graph, TableSchema, VectorIndexOptions};

static GRAPH: LazyLock<ContextKey<Graph>> =
    LazyLock::new(|| ContextKey::new("surrealdb_smoke_graph"));

async fn try_graph(db_name: &str) -> Option<Graph> {
    let url = std::env::var("SURREALDB_URL").unwrap_or_else(|_| "127.0.0.1:8787".to_string());
    let ns = std::env::var("SURREALDB_NS").unwrap_or_else(|_| "cocoindex".to_string());
    let user = std::env::var("SURREALDB_USER").unwrap_or_else(|_| "root".to_string());
    let pass = std::env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".to_string());
    Graph::connect(&url, &ns, db_name, &user, &pass).await.ok()
}

#[tokio::test]
async fn surrealdb_targets_reconcile_records_and_relations_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_smoke_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB smoke test; no local SurrealDB connection");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("surrealdb_target_smoke")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap();

    run_graph(&app, &["alice", "bob"]).await;
    assert_eq!(graph.count("person").await.unwrap(), 2);
    assert_eq!(graph.count("knows").await.unwrap(), 1);

    run_graph(&app, &["alice"]).await;
    assert_eq!(graph.count("person").await.unwrap(), 1);
    assert_eq!(graph.count("knows").await.unwrap(), 0);
}

#[tokio::test]
async fn surrealdb_targets_e2e_conversation_graph_write_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_e2e_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB e2e test; no local SurrealDB connection");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("surrealdb_target_e2e")
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap();

    run_conversation_graph(&app, true).await;
    assert_eq!(graph.count("session").await.unwrap(), 1);
    assert_eq!(graph.count("statement").await.unwrap(), 1);
    assert_eq!(graph.count("person").await.unwrap(), 1);
    assert_eq!(graph.count("tech").await.unwrap(), 1);
    assert_eq!(graph.count("org").await.unwrap(), 1);
    assert_eq!(graph.count("session_statement").await.unwrap(), 1);
    assert_eq!(graph.count("statement_mentions").await.unwrap(), 3);

    run_conversation_graph(&app, false).await;
    assert_eq!(graph.count("session").await.unwrap(), 1);
    assert_eq!(graph.count("statement").await.unwrap(), 1);
    assert_eq!(graph.count("person").await.unwrap(), 1);
    assert_eq!(graph.count("tech").await.unwrap(), 0);
    assert_eq!(graph.count("org").await.unwrap(), 0);
    assert_eq!(graph.count("session_statement").await.unwrap(), 1);
    assert_eq!(graph.count("statement_mentions").await.unwrap(), 1);
}

async fn run_graph(app: &App, people: &'static [&'static str]) {
    app.run(move |ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let person_schema = TableSchema::new([("name", ColumnDef::new("string"))])?;
        let person =
            surrealdb::mount_table_target_with_schema(&ctx, graph, "person", Some(person_schema))
                .await?;
        let knows =
            surrealdb::mount_relation_target(&ctx, graph, "knows", &person, &person).await?;

        for name in people {
            person.declare_record(&ctx, *name, &serde_json::json!({ "name": name }))?;
        }
        if people.contains(&"alice") && people.contains(&"bob") {
            knows.declare_relation(&ctx, "alice", "bob")?;
        }
        Ok(())
    })
    .await
    .unwrap();
}

async fn run_conversation_graph(app: &App, include_all_entities: bool) {
    app.run(move |ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let session_schema = TableSchema::new([
            ("youtube_id", ColumnDef::new("string")),
            ("name", ColumnDef::new("string")),
        ])?;
        let statement_schema = TableSchema::new([("statement", ColumnDef::new("string"))])?;
        let entity_schema = TableSchema::new([("name", ColumnDef::new("string"))])?;

        let session =
            surrealdb::mount_table_target_with_schema(&ctx, graph, "session", Some(session_schema))
                .await?;
        let statement = surrealdb::mount_table_target_with_schema(
            &ctx,
            graph,
            "statement",
            Some(statement_schema),
        )
        .await?;
        let person = surrealdb::mount_table_target_with_schema(
            &ctx,
            graph,
            "person",
            Some(entity_schema.clone()),
        )
        .await?;
        let tech = surrealdb::mount_table_target_with_schema(
            &ctx,
            graph,
            "tech",
            Some(entity_schema.clone()),
        )
        .await?;
        let org =
            surrealdb::mount_table_target_with_schema(&ctx, graph, "org", Some(entity_schema))
                .await?;
        let session_statement = surrealdb::mount_relation_target(
            &ctx,
            graph,
            "session_statement",
            &session,
            &statement,
        )
        .await?;
        let statement_mentions = surrealdb::mount_relation_target_many(
            &ctx,
            graph,
            "statement_mentions",
            &[&statement],
            &[&person, &tech, &org],
            None,
        )
        .await?;

        session.declare_record(
            &ctx,
            100_i64,
            &serde_json::json!({ "youtube_id": "local-demo", "name": "Demo" }),
        )?;
        statement.declare_record(
            &ctx,
            200_i64,
            &serde_json::json!({ "statement": "CocoIndex writes Rust targets" }),
        )?;
        person.declare_record(
            &ctx,
            "George He",
            &serde_json::json!({ "name": "George He" }),
        )?;
        session_statement.declare_relation(&ctx, 100_i64, 200_i64)?;
        statement_mentions.declare_relation_between(
            &ctx,
            "statement",
            200_i64,
            "person",
            "George He",
        )?;

        if include_all_entities {
            tech.declare_record(&ctx, "Rust", &serde_json::json!({ "name": "Rust" }))?;
            org.declare_record(
                &ctx,
                "CocoIndex",
                &serde_json::json!({ "name": "CocoIndex" }),
            )?;
            statement_mentions.declare_relation_between(
                &ctx,
                "statement",
                200_i64,
                "tech",
                "Rust",
            )?;
            statement_mentions.declare_relation_between(
                &ctx,
                "statement",
                200_i64,
                "org",
                "CocoIndex",
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
}

fn chrono_like_timestamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[tokio::test]
async fn surrealdb_declare_row_derives_id_from_row_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_declrow_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB declare_row test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_declare_row").await;

    app.run(|ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let people = surrealdb::mount_table_target(&ctx, graph, "human").await?;
        // `declare_row` takes the record id from the row's own `id` field.
        people.declare_row(&ctx, &serde_json::json!({ "id": "alice", "age": 30 }))?;
        people.declare_row(&ctx, &serde_json::json!({ "id": "bob", "age": 41 }))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(graph.count("human").await.unwrap(), 2);
}

async fn temp_app(graph: &Graph, name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap();
    (app, dir)
}

// ---------------------------------------------------------------------------
// Vector index attachment: create -> change metric (recreate) -> orphan remove
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surrealdb_vector_index_attachment_lifecycle_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_vidx_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB vector-index test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_vector_index").await;

    async fn run(app: &App, metric: &'static str, declare_index: bool) {
        app.run(move |ctx| async move {
            let graph = ctx.get_key(&GRAPH)?;
            let docs = surrealdb::mount_table_target(&ctx, graph, "doc").await?;
            if declare_index {
                docs.declare_vector_index(
                    &ctx,
                    "embedding",
                    3,
                    VectorIndexOptions {
                        metric,
                        method: "hnsw",
                        ..Default::default()
                    },
                )?;
            }
            docs.declare_record(
                &ctx,
                "d1",
                &serde_json::json!({ "name": "a", "embedding": [1.0, 2.0, 3.0] }),
            )?;
            Ok(())
        })
        .await
        .unwrap();
    }

    let idx = "idx_doc__embedding".to_string();

    // Create the index.
    run(&app, "cosine", true).await;
    assert!(graph.index_names("doc").await.unwrap().contains(&idx));

    // Change the metric → drop + recreate; still present.
    run(&app, "euclidean", true).await;
    assert!(graph.index_names("doc").await.unwrap().contains(&idx));

    // Stop declaring the index → orphan-removed.
    run(&app, "euclidean", false).await;
    assert!(!graph.index_names("doc").await.unwrap().contains(&idx));
}

// ---------------------------------------------------------------------------
// Schema evolution: adding a column defines the new field on re-run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surrealdb_schema_evolution_adds_field_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_schema_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB schema-evolution test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_schema_evolution").await;

    async fn run(app: &App, with_email: bool) {
        app.run(move |ctx| async move {
            let graph = ctx.get_key(&GRAPH)?;
            let columns: Vec<(&str, ColumnDef)> = if with_email {
                vec![
                    ("name", ColumnDef::new("string")),
                    ("email", ColumnDef::new("string")),
                ]
            } else {
                vec![("name", ColumnDef::new("string"))]
            };
            let schema = TableSchema::new(columns)?;
            let person =
                surrealdb::mount_table_target_with_schema(&ctx, graph, "member", Some(schema))
                    .await?;
            if with_email {
                person.declare_record(
                    &ctx,
                    "p1",
                    &serde_json::json!({ "name": "Ann", "email": "ann@example.com" }),
                )?;
            } else {
                person.declare_record(&ctx, "p1", &serde_json::json!({ "name": "Ann" }))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }

    run(&app, false).await;
    let fields = graph.field_names("member").await.unwrap();
    assert!(fields.contains(&"name".to_string()));
    assert!(!fields.contains(&"email".to_string()));

    // Re-run with an added column → the new field is defined; records still reconcile.
    run(&app, true).await;
    let fields = graph.field_names("member").await.unwrap();
    assert!(fields.contains(&"name".to_string()));
    assert!(fields.contains(&"email".to_string()));
    assert_eq!(graph.count("member").await.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Table drop: a table no longer declared on re-run is removed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surrealdb_table_dropped_when_no_longer_declared_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_drop_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB table-drop test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_table_drop").await;

    // Declare the table + a record.
    app.run(|ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let widget = surrealdb::mount_table_target(&ctx, graph, "widget").await?;
        widget.declare_record(&ctx, "w1", &serde_json::json!({ "name": "gear" }))?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        graph
            .table_names()
            .await
            .unwrap()
            .contains(&"widget".to_string())
    );
    assert_eq!(graph.count("widget").await.unwrap(), 1);

    // Re-register the table provider but declare nothing on it: the table state
    // declared in the previous run is now orphaned, so the system-managed table
    // is dropped (`REMOVE TABLE`) and its rows go with it.
    app.run(|ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let _ = surrealdb::table_target(&ctx, graph, "widget")?;
        Ok(())
    })
    .await
    .unwrap();
    let after = graph.table_names().await.unwrap();
    assert!(
        !after.contains(&"widget".to_string()),
        "after orphaning the table: tables={after:?}"
    );
}

// ---------------------------------------------------------------------------
// Schema evolution: a field no longer declared is REMOVEd (was silently kept
// before — DEFINE FIELD IF NOT EXISTS can't drop)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surrealdb_schema_evolution_drops_field_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_dropfield_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB field-drop test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_drop_field").await;

    // v1: name + email.
    app.run(|ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let schema = TableSchema::new(vec![
            ("name", ColumnDef::new("string")),
            ("email", ColumnDef::new("string")),
        ])?;
        let t =
            surrealdb::mount_table_target_with_schema(&ctx, graph, "acct", Some(schema)).await?;
        t.declare_record(
            &ctx,
            "p1",
            &serde_json::json!({ "name": "Ann", "email": "a@x.com" }),
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        graph
            .field_names("acct")
            .await
            .unwrap()
            .contains(&"email".to_string())
    );

    // v2: drop the email field. The undeclared field must be REMOVEd.
    app.run(|ctx| async move {
        let graph = ctx.get_key(&GRAPH)?;
        let schema = TableSchema::new(vec![("name", ColumnDef::new("string"))])?;
        let t =
            surrealdb::mount_table_target_with_schema(&ctx, graph, "acct", Some(schema)).await?;
        t.declare_record(&ctx, "p1", &serde_json::json!({ "name": "Ann" }))?;
        Ok(())
    })
    .await
    .unwrap();
    let fields = graph.field_names("acct").await.unwrap();
    assert!(
        !fields.contains(&"email".to_string()),
        "the undeclared email field should be dropped; fields={fields:?}"
    );
    assert!(fields.contains(&"name".to_string()));
}
