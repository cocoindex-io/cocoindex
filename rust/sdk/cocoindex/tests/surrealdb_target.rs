#![cfg(feature = "surrealdb")]

use cocoindex::connectors::surrealdb::{self, ColumnDef, Graph, TableSchema, VectorIndexOptions};
use cocoindex::prelude::*;

cocoindex::context_key!(static GRAPH: Graph, key = "surrealdb_smoke_graph");

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
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap()
        .app("surrealdb_target_smoke")
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
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap()
        .app("surrealdb_target_e2e")
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
        let person_schema = TableSchema::new([("name", ColumnDef::new("string"))])?;
        let person =
            surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "person", Some(person_schema))
                .await?;
        let knows =
            surrealdb::mount_relation_target(&ctx, &GRAPH, "knows", &person, &person).await?;

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
        let session_schema = TableSchema::new([
            ("youtube_id", ColumnDef::new("string")),
            ("name", ColumnDef::new("string")),
        ])?;
        let statement_schema = TableSchema::new([("statement", ColumnDef::new("string"))])?;
        let entity_schema = TableSchema::new([("name", ColumnDef::new("string"))])?;

        let session = surrealdb::mount_table_target_with_schema(
            &ctx,
            &GRAPH,
            "session",
            Some(session_schema),
        )
        .await?;
        let statement = surrealdb::mount_table_target_with_schema(
            &ctx,
            &GRAPH,
            "statement",
            Some(statement_schema),
        )
        .await?;
        let person = surrealdb::mount_table_target_with_schema(
            &ctx,
            &GRAPH,
            "person",
            Some(entity_schema.clone()),
        )
        .await?;
        let tech = surrealdb::mount_table_target_with_schema(
            &ctx,
            &GRAPH,
            "tech",
            Some(entity_schema.clone()),
        )
        .await?;
        let org =
            surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "org", Some(entity_schema))
                .await?;
        let session_statement = surrealdb::mount_relation_target(
            &ctx,
            &GRAPH,
            "session_statement",
            &session,
            &statement,
        )
        .await?;
        let statement_mentions = surrealdb::mount_relation_target_many(
            &ctx,
            &GRAPH,
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
        let people = surrealdb::mount_table_target(&ctx, &GRAPH, "human").await?;
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
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap()
        .app(name)
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
            let docs = surrealdb::mount_table_target(&ctx, &GRAPH, "doc").await?;
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
                surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "member", Some(schema))
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
// Table removal: a table no longer declared on re-run, or on drop_state, is
// removed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surrealdb_undeclared_and_dropped_tables_are_removed_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_drop_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB table-removal test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_table_removal").await;

    async fn declare_widgets(app: &App) {
        app.run(|ctx| async move {
            let widget = surrealdb::mount_table_target(&ctx, &GRAPH, "widget").await?;
            let fits =
                surrealdb::mount_relation_target(&ctx, &GRAPH, "fits", &widget, &widget).await?;
            widget.declare_record(&ctx, "w1", &serde_json::json!({ "name": "gear" }))?;
            widget.declare_record(&ctx, "w2", &serde_json::json!({ "name": "axle" }))?;
            fits.declare_relation(&ctx, "w1", "w2")?;
            Ok(())
        })
        .await
        .unwrap();
    }
    async fn assert_tables(graph: &Graph, present: bool) {
        let tables = graph.table_names().await.unwrap();
        for name in ["widget", "fits"] {
            assert_eq!(
                tables.contains(&name.to_string()),
                present,
                "table {name}: tables={tables:?}"
            );
        }
    }

    declare_widgets(&app).await;
    assert_tables(&graph, true).await;
    assert_eq!(graph.count("widget").await.unwrap(), 2);
    assert_eq!(graph.count("fits").await.unwrap(), 1);

    // A run that no longer declares the tables removes them (`REMOVE TABLE`).
    app.run(|_ctx| async { Ok(()) }).await.unwrap();
    assert_tables(&graph, false).await;

    declare_widgets(&app).await;
    assert_tables(&graph, true).await;

    app.drop_state().await.unwrap();
    assert_tables(&graph, false).await;
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
        let schema = TableSchema::new(vec![
            ("name", ColumnDef::new("string")),
            ("email", ColumnDef::new("string")),
        ])?;
        let t =
            surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "acct", Some(schema)).await?;
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
        let schema = TableSchema::new(vec![("name", ColumnDef::new("string"))])?;
        let t =
            surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "acct", Some(schema)).await?;
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

// ---------------------------------------------------------------------------
// TableSchema::from_row: derive the schema from a row struct
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, cocoindex::SchemaFields)]
struct Article {
    title: String,
    subtitle: Option<String>,
    views: i64,
    rating: f64,
    published: bool,
    posted_at: chrono::DateTime<chrono::Utc>,
    uid: uuid::Uuid,
    price: rust_decimal::Decimal,
    read_time: std::time::Duration,
    cover: Vec<u8>,
    tags: Vec<String>,
    #[coco(vector = 3)]
    embedding: Vec<f32>,
    #[coco(type = "record<person>")]
    author: String,
}

#[test]
fn surrealdb_from_row_matches_the_hand_written_schema() {
    let derived = TableSchema::from_row::<Article>().unwrap();
    let by_hand = TableSchema::new([
        ("title", ColumnDef::new("string")),
        ("subtitle", ColumnDef::new("string").nullable()),
        ("views", ColumnDef::new("int")),
        ("rating", ColumnDef::new("float")),
        ("published", ColumnDef::new("bool")),
        ("posted_at", ColumnDef::new("datetime")),
        ("uid", ColumnDef::new("uuid")),
        ("price", ColumnDef::new("decimal")),
        ("read_time", ColumnDef::new("duration")),
        ("cover", ColumnDef::new("bytes")),
        ("tags", ColumnDef::new("any")),
        ("embedding", ColumnDef::new("array<float, 3>")),
        ("author", ColumnDef::new("record<person>")),
    ])
    .unwrap();
    assert_eq!(derived, by_hand);
}

#[test]
fn surrealdb_from_row_rejects_a_vector_without_a_dimension() {
    #[derive(cocoindex::SchemaFields)]
    #[allow(dead_code)]
    struct Unsized {
        #[coco(vector)]
        embedding: Vec<f32>,
    }
    let message = TableSchema::from_row::<Unsized>().unwrap_err().to_string();
    assert!(message.contains("embedding"), "{message}");
    assert!(message.contains("#[coco(vector = N)]"), "{message}");
}

#[tokio::test]
async fn surrealdb_from_row_schema_stores_typed_values_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_fromrow_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB from_row test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_from_row").await;

    app.run(|ctx| async move {
        let people = surrealdb::mount_table_target(&ctx, &GRAPH, "person").await?;
        people.declare_record(&ctx, "ann", &serde_json::json!({ "name": "Ann" }))?;
        let articles = surrealdb::mount_table_target_with_schema(
            &ctx,
            &GRAPH,
            "article",
            Some(TableSchema::from_row::<Article>()?),
        )
        .await?;
        articles.declare_record(
            &ctx,
            "a1",
            &Article {
                title: "Hello".to_string(),
                subtitle: None,
                views: 3,
                rating: 4.5,
                published: true,
                posted_at: "2024-05-01T10:00:00Z".parse().unwrap(),
                uid: "67e55044-10b1-426f-9247-bb680e5fe0c8".parse().unwrap(),
                price: "12.50".parse().unwrap(),
                read_time: std::time::Duration::from_secs(90),
                cover: b"hi".to_vec(),
                tags: vec!["a".to_string(), "b".to_string()],
                embedding: vec![1.0, 0.0, 0.5],
                author: "person:ann".to_string(),
            },
        )?;
        Ok(())
    })
    .await
    .unwrap();

    // The table is SCHEMAFULL with one field per struct field.
    let fields = graph.field_names("article").await.unwrap();
    for name in ["title", "subtitle", "posted_at", "embedding", "author"] {
        assert!(fields.contains(&name.to_string()), "fields={fields:?}");
    }
    let record = graph.record("article", "a1").await.unwrap().unwrap();
    assert_eq!(record["title"], "Hello");
    assert_eq!(record["views"], 3);
    assert_eq!(record["tags"], serde_json::json!(["a", "b"]));
    assert_eq!(record["embedding"], serde_json::json!([1.0, 0.0, 0.5]));
    // `None` is stored as an absent field, not as a null.
    assert!(
        record
            .get("subtitle")
            .is_none_or(serde_json::Value::is_null)
    );
}

// ---------------------------------------------------------------------------
// declare_fields: several components own different fields of one record
// ---------------------------------------------------------------------------

/// One field group: its name and the fields it contributes to `player:ann`.
type FieldGroup = (&'static str, serde_json::Value);

/// Declare the base record `player:ann` in the root component, and each field
/// group in a component of its own.
async fn run_players(app: &App, name: &'static str, groups: Vec<FieldGroup>) -> Result<()> {
    app.run(move |ctx| async move {
        let players = surrealdb::mount_table_target(&ctx, &GRAPH, "player").await?;
        players.declare_record(&ctx, "ann", &serde_json::json!({ "name": name }))?;
        ctx.mount_each(
            groups,
            |(group, _)| group.to_string(),
            move |ctx, (group, fields)| {
                let players = players.clone();
                async move { players.declare_fields(&ctx, "ann", group, &fields) }
            },
        )
        .await?;
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn surrealdb_declare_fields_gives_each_group_its_own_fields_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_fields_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB declare_fields test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_declare_fields").await;
    let ann = || async { graph.record("player", "ann").await.unwrap().unwrap() };
    let stats = |fields| ("stats", fields);
    let profile = || ("profile", serde_json::json!({ "city": "Oslo" }));

    // The base record exists before any group writes to it.
    run_players(&app, "Ann", vec![]).await.unwrap();
    assert_eq!(ann().await, serde_json::json!({ "name": "Ann" }));

    // Two groups add their fields; the base field is untouched.
    let both = serde_json::json!({ "score": 7, "rank": 1 });
    run_players(&app, "Ann", vec![stats(both.clone()), profile()])
        .await
        .unwrap();
    assert_eq!(
        ann().await,
        serde_json::json!({ "name": "Ann", "score": 7, "rank": 1, "city": "Oslo" })
    );

    // A group drops one field and changes another: only its own fields move.
    let score_only = serde_json::json!({ "score": 8 });
    run_players(&app, "Ann", vec![stats(score_only.clone()), profile()])
        .await
        .unwrap();
    assert_eq!(
        ann().await,
        serde_json::json!({ "name": "Ann", "score": 8, "city": "Oslo" })
    );

    // The record's owner rewrites it: the groups' fields survive.
    run_players(&app, "Anna", vec![stats(score_only.clone()), profile()])
        .await
        .unwrap();
    assert_eq!(
        ann().await,
        serde_json::json!({ "name": "Anna", "score": 8, "city": "Oslo" })
    );

    // A group goes away: its fields are unset, the other group's stay.
    run_players(&app, "Anna", vec![stats(score_only)])
        .await
        .unwrap();
    assert_eq!(
        ann().await,
        serde_json::json!({ "name": "Anna", "score": 8 })
    );
}

#[tokio::test]
async fn surrealdb_declare_fields_on_a_missing_record_fails_when_available() {
    let db_name = format!(
        "rust_sdk_surrealdb_fields_missing_{}_{}",
        std::process::id(),
        chrono_like_timestamp()
    );
    let Some(graph) = try_graph(&db_name).await else {
        eprintln!("skipping SurrealDB declare_fields test; no local SurrealDB connection");
        return;
    };
    let (app, _dir) = temp_app(&graph, "surrealdb_declare_fields_missing").await;

    let result = app
        .run(|ctx| async move {
            let players = surrealdb::mount_table_target(&ctx, &GRAPH, "player").await?;
            players.declare_fields(&ctx, "nobody", "stats", &serde_json::json!({ "score": 1 }))
        })
        .await;
    assert!(result.is_err(), "a fields write needs an existing record");
    assert!(graph.record("player", "nobody").await.unwrap().is_none());
}

#[tokio::test]
async fn surrealdb_declare_fields_takes_only_named_fields() {
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap()
        .app("surrealdb_declare_fields_shape")
        .await
        .unwrap();
    let result = app
        .run(|ctx| async move {
            let players = surrealdb::declare_table_target(&ctx, &GRAPH, "player")?;
            players.declare_fields(&ctx, "ann", "stats", &[1, 2, 3])
        })
        .await;
    let message = result.unwrap_err().to_string();
    assert!(message.contains("declare_fields"), "{message}");
}
