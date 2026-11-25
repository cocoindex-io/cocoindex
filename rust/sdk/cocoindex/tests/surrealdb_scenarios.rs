//! Edge-case scenarios for the SurrealDB target: relation updates, record id
//! forms, payload values, vectors, failed batches and recovery, out-of-band
//! changes and chunk boundaries. Each test runs on the SurrealDB server
//! `SURREALDB_URL` names (signing in as `SURREALDB_USER`/`SURREALDB_PASS`,
//! default `root`/`root`), else on the embedded in-memory engine (feature
//! `surrealdb_mem`), else on a local server at `127.0.0.1:8787`, skipping
//! when none answers.
#![cfg(feature = "surrealdb")]

use cocoindex::connectors::surrealdb::{self, ColumnDef, Graph, TableSchema, VectorIndexOptions};
use cocoindex::prelude::*;

cocoindex::context_key!(static GRAPH: Graph, key = "surrealdb_scenarios_graph");

/// Without `SURREALDB_URL`: the embedded in-memory engine when the
/// `surrealdb_mem` feature is on, else a local server (the tests skip when
/// none answers).
fn default_url() -> &'static str {
    if cfg!(feature = "surrealdb_mem") {
        "mem://"
    } else {
        "127.0.0.1:8787"
    }
}

/// A fresh database, plus an app on it.
async fn scenario(tag: &str) -> Option<(Graph, App, tempfile::TempDir)> {
    let url = std::env::var("SURREALDB_URL").unwrap_or_else(|_| default_url().to_string());
    let ns = std::env::var("SURREALDB_NS").unwrap_or_else(|_| "cocoindex".to_string());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_name = format!("rust_sdk_surrealdb_{tag}_{}_{nanos}", std::process::id());
    let credentials = if url.starts_with("mem://") {
        // The embedded engine runs without authentication.
        surrealdb::Credentials::None
    } else {
        surrealdb::Credentials::Root {
            username: std::env::var("SURREALDB_USER").unwrap_or_else(|_| "root".to_string()),
            password: std::env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".to_string()),
        }
    };
    let Ok(graph) = Graph::connect_with_credentials(&url, &ns, &db_name, credentials).await else {
        eprintln!("skipping SurrealDB {tag} test; no SurrealDB connection");
        return None;
    };
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .provide_key(&GRAPH, graph.clone())
        .build()
        .await
        .unwrap()
        .app(&format!("surrealdb_{tag}"))
        .await
        .unwrap();
    Some((graph, app, dir))
}

/// The rows a query returns, as JSON.
async fn query_json(graph: &Graph, query: &str) -> Vec<serde_json::Value> {
    let mut response = graph.client().query(query).await.unwrap().check().unwrap();
    response.take(0).unwrap()
}

/// The server's (major, minor) version.
async fn server_version(graph: &Graph) -> (u64, u64) {
    let version = graph.client().version().await.unwrap();
    (version.major, version.minor)
}

// ---------------------------------------------------------------------------
// Relations
// ---------------------------------------------------------------------------

/// Declare `person` records and `knows` relations with their fields.
async fn run_knows(
    app: &App,
    people: Vec<&'static str>,
    edges: Vec<(&'static str, &'static str, serde_json::Value)>,
) -> Result<()> {
    app.run(move |ctx| async move {
        let person = surrealdb::mount_table_target(&ctx, &GRAPH, "person").await?;
        let knows =
            surrealdb::mount_relation_target(&ctx, &GRAPH, "knows", &person, &person).await?;
        for name in people {
            person.declare_record(&ctx, name, &serde_json::json!({ "name": name }))?;
        }
        for (from, to, fields) in edges {
            knows.declare_relation_record(&ctx, from, to, &fields)?;
        }
        Ok(())
    })
    .await?;
    Ok(())
}

/// The `knows` edges as `[in, out, fields]`, sorted.
async fn knows_edges(graph: &Graph) -> Vec<serde_json::Value> {
    let mut edges = query_json(
        graph,
        "SELECT VALUE [<string>in, <string>out, (SELECT * OMIT id, in, out FROM ONLY $this.id)] \
         FROM knows",
    )
    .await;
    edges.sort_by_key(|edge| edge.to_string());
    edges
}

#[tokio::test]
async fn surrealdb_relation_fields_change_in_place_when_available() {
    let Some((graph, app, _dir)) = scenario("rel_update").await else {
        return;
    };
    let people = || vec!["alice", "bob"];
    run_knows(
        &app,
        people(),
        vec![
            (
                "alice",
                "bob",
                serde_json::json!({ "since": 2020, "note": "x" }),
            ),
            ("alice", "alice", serde_json::json!({ "since": 1990 })),
        ],
    )
    .await
    .unwrap();
    assert_eq!(knows_edges(&graph).await.len(), 2);

    // An edge's fields change and one goes away; the self-loop changes too.
    // This failed with "already exists" on SurrealDB 3.2, which cannot
    // re-create a record deleted in the same transaction.
    run_knows(
        &app,
        people(),
        vec![
            ("alice", "bob", serde_json::json!({ "since": 2021 })),
            ("alice", "alice", serde_json::json!({ "since": 1991 })),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        knows_edges(&graph).await,
        vec![
            serde_json::json!(["person:alice", "person:alice", { "since": 1991 }]),
            serde_json::json!(["person:alice", "person:bob", { "since": 2021 }]),
        ]
    );
    // Graph traversal follows the edges.
    assert_eq!(
        query_json(
            &graph,
            "SELECT VALUE <-knows<-person.name FROM ONLY person:bob"
        )
        .await,
        vec![serde_json::json!("alice")]
    );

    // An unchanged run leaves the edges alone; dropping them deletes them.
    run_knows(
        &app,
        people(),
        vec![("alice", "bob", serde_json::json!({ "since": 2021 }))],
    )
    .await
    .unwrap();
    assert_eq!(
        knows_edges(&graph).await,
        vec![serde_json::json!(["person:alice", "person:bob", { "since": 2021 }])]
    );
}

/// A relation with an explicit `id` whose endpoints change keeps its id and
/// moves to the new endpoints.
#[tokio::test]
async fn surrealdb_relation_with_explicit_id_moves_endpoints_when_available() {
    let Some((graph, app, _dir)) = scenario("rel_move").await else {
        return;
    };
    let people = || vec!["alice", "bob", "carol"];
    run_knows(
        &app,
        people(),
        vec![("alice", "bob", serde_json::json!({ "id": "e1", "w": 1 }))],
    )
    .await
    .unwrap();
    let result = run_knows(
        &app,
        people(),
        vec![("alice", "carol", serde_json::json!({ "id": "e1", "w": 2 }))],
    )
    .await;
    if server_version(&graph).await < (3, 3) {
        // SurrealDB 3.2 cannot re-create a record deleted in the same
        // transaction, so moving an edge needs 3.3 or later.
        let message = result.unwrap_err().to_string();
        assert!(message.contains("already exists"), "{message}");
        return;
    }
    result.unwrap();
    assert_eq!(
        knows_edges(&graph).await,
        vec![serde_json::json!(["person:alice", "person:carol", { "w": 2 }])]
    );
    assert_eq!(
        query_json(&graph, "SELECT VALUE <string>id FROM knows").await,
        vec![serde_json::json!("knows:e1")]
    );
}

#[tokio::test]
async fn surrealdb_many_relations_per_node_when_available() {
    let Some((graph, app, _dir)) = scenario("rel_fanout").await else {
        return;
    };
    async fn run(app: &App, n: usize, weight: i64) {
        app.run(move |ctx| async move {
            let node = surrealdb::mount_table_target(&ctx, &GRAPH, "node").await?;
            let link = surrealdb::mount_relation_target(&ctx, &GRAPH, "link", &node, &node).await?;
            node.declare_record(&ctx, "hub", &serde_json::json!({}))?;
            for i in 0..n {
                let leaf = format!("leaf{i}");
                node.declare_record(&ctx, leaf.as_str(), &serde_json::json!({}))?;
                link.declare_relation_record(
                    &ctx,
                    "hub",
                    leaf.as_str(),
                    &serde_json::json!({ "w": weight }),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }
    // More than two chunks of relations, then every edge changes, then most go.
    run(&app, 1201, 1).await;
    assert_eq!(graph.count("link").await.unwrap(), 1201);
    run(&app, 1201, 2).await;
    assert_eq!(
        query_json(&graph, "SELECT VALUE count(->link) FROM ONLY node:hub").await,
        vec![serde_json::json!(1201)]
    );
    assert!(
        query_json(&graph, "SELECT VALUE w FROM link WHERE w != 2")
            .await
            .is_empty()
    );
    run(&app, 3, 2).await;
    assert_eq!(graph.count("link").await.unwrap(), 3);
    assert_eq!(graph.count("node").await.unwrap(), 4);
}

/// Deleting a record makes SurrealDB delete its edges, behind the back of an
/// edge that stays declared. The edge is not written again when the endpoint
/// comes back, since its own declaration did not change (a known gap, as in
/// the Python connector); a full reprocess restores it.
#[tokio::test]
async fn surrealdb_relation_endpoint_deleted_and_restored_when_available() {
    let Some((graph, app, _dir)) = scenario("rel_endpoint").await else {
        return;
    };
    let edge = || vec![("alice", "bob", serde_json::json!({ "w": 1 }))];
    let expected = vec![serde_json::json!(["person:alice", "person:bob", { "w": 1 }])];
    run_knows(&app, vec!["alice", "bob"], edge()).await.unwrap();
    assert_eq!(knows_edges(&graph).await, expected);

    // bob's record goes away while the edge stays declared: SurrealDB deletes
    // the edge with it.
    run_knows(&app, vec!["alice"], edge()).await.unwrap();
    assert!(knows_edges(&graph).await.is_empty());

    // bob comes back; the edge's declaration is unchanged, so it stays gone.
    run_knows(&app, vec!["alice", "bob"], edge()).await.unwrap();
    assert!(knows_edges(&graph).await.is_empty());

    app.update_with_options(
        cocoindex::app::UpdateOptions {
            full_reprocess: true,
            ..Default::default()
        },
        |ctx| async move {
            let person = surrealdb::mount_table_target(&ctx, &GRAPH, "person").await?;
            let knows =
                surrealdb::mount_relation_target(&ctx, &GRAPH, "knows", &person, &person).await?;
            for name in ["alice", "bob"] {
                person.declare_record(&ctx, name, &serde_json::json!({ "name": name }))?;
            }
            knows.declare_relation_record(&ctx, "alice", "bob", &serde_json::json!({ "w": 1 }))
        },
    )
    .await
    .unwrap();
    assert_eq!(knows_edges(&graph).await, expected);
}

// ---------------------------------------------------------------------------
// Record ids and values
// ---------------------------------------------------------------------------

const STRING_IDS: &[&str] = &[
    "plain",
    "with space",
    "a/b:c",
    "back`tick",
    "back\\slash",
    "⟨angle⟩",
    "ünïcødé 😀",
    "123",
    "-7",
    "",
    "550e8400-e29b-41d4-a716-446655440000",
    "x; REMOVE TABLE thing; --",
];

#[tokio::test]
async fn surrealdb_record_id_forms_round_trip_when_available() {
    let Some((graph, app, _dir)) = scenario("ids").await else {
        return;
    };
    async fn run(app: &App, declare: bool) {
        app.run(move |ctx| async move {
            let t = surrealdb::mount_table_target(&ctx, &GRAPH, "thing").await?;
            if declare {
                for (i, id) in STRING_IDS.iter().enumerate() {
                    t.declare_record(&ctx, *id, &serde_json::json!({ "i": i }))?;
                }
                for n in [0i64, -1, i64::MIN, i64::MAX] {
                    t.declare_record(&ctx, n, &serde_json::json!({ "n": n.to_string() }))?;
                }
                t.declare_record(&ctx, u64::MAX, &serde_json::json!({ "n": "u64max" }))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }
    run(&app, true).await;
    let expected = STRING_IDS.len() + 5;
    assert_eq!(graph.count("thing").await.unwrap(), expected);
    for (i, id) in STRING_IDS.iter().enumerate() {
        assert_eq!(
            graph.record("thing", *id).await.unwrap(),
            Some(serde_json::json!({ "i": i })),
            "id {id:?}"
        );
    }
    // A numeric string and a number are distinct ids.
    for n in [-1i64, i64::MIN, i64::MAX] {
        assert_eq!(
            graph.record("thing", n).await.unwrap(),
            Some(serde_json::json!({ "n": n.to_string() })),
            "id {n}"
        );
    }
    assert_eq!(
        graph.record("thing", u64::MAX).await.unwrap(),
        Some(serde_json::json!({ "n": "u64max" }))
    );
    // An unchanged re-run writes nothing new; then every id is deleted.
    run(&app, true).await;
    assert_eq!(graph.count("thing").await.unwrap(), expected);
    run(&app, false).await;
    assert_eq!(graph.count("thing").await.unwrap(), 0);
}

#[tokio::test]
async fn surrealdb_payload_values_round_trip_when_available() {
    let Some((graph, app, _dir)) = scenario("values").await else {
        return;
    };
    let mut deep = serde_json::json!({ "leaf": true });
    for i in 0..40 {
        deep = serde_json::json!({ format!("k{i}"): deep, "arr": [i, [i], { "x": i }] });
    }
    let big_array: Vec<f64> = (0..100_000).map(|i| f64::from(i) * 0.5).collect();
    let row = serde_json::json!({
        "b": true,
        "i": i64::MIN,
        "u": i64::MAX,
        "f": -1.5e300,
        "s": "quote ' \" ` \\ \n \t end",
        "null_field": null,
        "empty_s": "",
        "empty_a": [],
        "empty_o": {},
        "mixed": [1, "two", 3.5, null, false, { "k": [] }],
        "deep": deep,
        "big_text": "é".repeat(2 * 1024 * 1024),
        "big_array": big_array,
    });
    let declared = row.clone();
    app.run(move |ctx| async move {
        let t = surrealdb::mount_table_target(&ctx, &GRAPH, "blob").await?;
        t.declare_record(&ctx, "r1", &declared)
    })
    .await
    .unwrap();
    let stored = graph.record("blob", "r1").await.unwrap().unwrap();
    let mut expected = row.as_object().unwrap().clone();
    // `null` is NONE: the field is absent.
    expected.remove("null_field");
    for (name, value) in &expected {
        assert_eq!(stored.get(name), Some(value), "field {name}");
    }
    assert_eq!(stored.as_object().unwrap().len(), expected.len());
}

#[tokio::test]
async fn surrealdb_non_finite_floats_are_rejected_at_declare() {
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join("lmdb"))
        .build()
        .await
        .unwrap()
        .app("surrealdb_non_finite")
        .await
        .unwrap();
    #[derive(serde::Serialize)]
    struct Doc {
        v: Vec<f32>,
    }
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let message = app
            .run(move |ctx| async move {
                let t = surrealdb::declare_table_target(&ctx, &GRAPH, "doc")?;
                t.declare_record(&ctx, "d", &Doc { v: vec![1.0, bad] })
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("non-finite"), "{message}");
    }
}

// ---------------------------------------------------------------------------
// Vectors, failures and recovery
// ---------------------------------------------------------------------------

/// A schemafull vector column: a wrong-length or empty vector fails the run,
/// and the next run with valid data writes every record, including those of a
/// chunk that committed before the failing one.
#[tokio::test]
async fn surrealdb_failed_chunk_is_written_on_the_next_run_when_available() {
    let Some((graph, app, _dir)) = scenario("midbatch").await else {
        return;
    };
    async fn run(app: &App, bad: Option<(usize, Vec<f32>)>, value: i64) -> Result<()> {
        app.run(move |ctx| async move {
            let schema = TableSchema::new([
                ("v", ColumnDef::new("int")),
                ("embedding", ColumnDef::new("array<float, 3>")),
            ])?;
            let t = surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "vec", Some(schema))
                .await?;
            for i in 0..800 {
                let embedding = match &bad {
                    Some((at, vector)) if *at == i => vector.clone(),
                    _ => vec![i as f32, 0.0, 1.0],
                };
                t.declare_record(
                    &ctx,
                    i as i64,
                    &serde_json::json!({ "v": value, "embedding": embedding }),
                )?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }
    // Record 700 lands in the second chunk of 500.
    for bad in [vec![1.0, 2.0], vec![1.0, 2.0, 3.0, 4.0], vec![]] {
        let message = run(&app, Some((700, bad.clone())), 1)
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("embedding"), "{bad:?}: {message}");
    }
    run(&app, None, 1).await.unwrap();
    assert_eq!(graph.count("vec").await.unwrap(), 800);
    assert_eq!(
        graph.record("vec", 700i64).await.unwrap().unwrap()["embedding"],
        serde_json::json!([700.0, 0.0, 1.0])
    );
    // An update that fails in its second chunk, then succeeds.
    run(&app, Some((650, vec![0.0])), 2).await.unwrap_err();
    run(&app, None, 2).await.unwrap();
    assert!(
        query_json(&graph, "SELECT VALUE <string>id FROM vec WHERE v != 2")
            .await
            .is_empty()
    );
}

/// The vector column's dimension, and the index's dimension and element type,
/// change between runs.
#[tokio::test]
async fn surrealdb_vector_dimension_and_type_change_between_runs_when_available() {
    let Some((graph, app, _dir)) = scenario("vecdim").await else {
        return;
    };
    async fn run(app: &App, dim: usize, vector_type: &'static str) {
        app.run(move |ctx| async move {
            let schema =
                TableSchema::new([("embedding", ColumnDef::new(format!("array<float, {dim}>")))])?;
            let t = surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "emb", Some(schema))
                .await?;
            t.declare_vector_index(
                &ctx,
                "embedding",
                dim,
                VectorIndexOptions {
                    vector_type,
                    ..Default::default()
                },
            )?;
            for i in 0..5 {
                let v: Vec<f64> = (0..dim).map(|d| (i * 10 + d) as f64 + 0.25).collect();
                t.declare_record(&ctx, i as i64, &serde_json::json!({ "embedding": v }))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }
    let index_def = || async {
        let info = query_json(&graph, "INFO FOR TABLE emb").await;
        info[0]["indexes"]["idx_emb__embedding"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    run(&app, 3, "f32").await;
    let def = index_def().await;
    assert!(def.contains("DIMENSION 3"), "{def}");
    run(&app, 4, "f32").await;
    let def = index_def().await;
    assert!(def.contains("DIMENSION 4"), "{def}");
    assert_eq!(
        graph.record("emb", 1i64).await.unwrap().unwrap()["embedding"],
        serde_json::json!([10.25, 11.25, 12.25, 13.25])
    );
    run(&app, 4, "f64").await;
    let def = index_def().await;
    assert!(def.contains("F64"), "{def}");
    // The index answers a nearest-neighbor query.
    assert_eq!(
        query_json(
            &graph,
            "SELECT VALUE <string>id FROM emb WHERE embedding <|1,40|> [20.25, 21.25, 22.25, 23.25]"
        )
        .await,
        vec![serde_json::json!("emb:2")]
    );
}

/// A table removed behind CocoIndex's back is defined again on the next run;
/// its unchanged records come back with a full reprocess.
#[tokio::test]
async fn surrealdb_table_removed_out_of_band_is_restored_when_available() {
    let Some((graph, app, _dir)) = scenario("oob").await else {
        return;
    };
    async fn run(app: &App, full_reprocess: bool) {
        app.update_with_options(
            cocoindex::app::UpdateOptions {
                full_reprocess,
                ..Default::default()
            },
            |ctx| async move {
                let schema = TableSchema::new([("name", ColumnDef::new("string"))])?;
                let t =
                    surrealdb::mount_table_target_with_schema(&ctx, &GRAPH, "gone", Some(schema))
                        .await?;
                t.declare_record(&ctx, "a", &serde_json::json!({ "name": "A" }))?;
                t.declare_record(&ctx, "b", &serde_json::json!({ "name": "B" }))?;
                Ok(())
            },
        )
        .await
        .unwrap();
    }
    run(&app, false).await;
    assert_eq!(graph.count("gone").await.unwrap(), 2);
    query_json(&graph, "REMOVE TABLE gone").await;
    run(&app, false).await;
    assert!(
        graph
            .table_names()
            .await
            .unwrap()
            .contains(&"gone".to_string())
    );
    assert!(
        graph
            .field_names("gone")
            .await
            .unwrap()
            .contains(&"name".to_string())
    );
    run(&app, true).await;
    assert_eq!(graph.count("gone").await.unwrap(), 2);
}

// ---------------------------------------------------------------------------
// Field groups and chunk boundaries
// ---------------------------------------------------------------------------

/// Declare `player:ann` in the root component, and each field group in a
/// component of its own.
async fn run_players(app: &App, groups: Vec<(&'static str, serde_json::Value)>) -> Result<()> {
    app.run(move |ctx| async move {
        let players = surrealdb::mount_table_target(&ctx, &GRAPH, "player").await?;
        players.declare_record(&ctx, "ann", &serde_json::json!({ "name": "Ann" }))?;
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

/// Field groups declared in the same run as their base record: the groups'
/// components commit before the record's, so their writes fail (logged) and
/// land on the next run.
#[tokio::test]
async fn surrealdb_field_groups_declared_with_their_record_land_on_the_next_run_when_available() {
    let Some((graph, app, _dir)) = scenario("fields_same_run").await else {
        return;
    };
    let groups = || {
        vec![
            ("a", serde_json::json!({ "a_only": true })),
            ("b", serde_json::json!({ "score": 2 })),
        ]
    };
    run_players(&app, groups()).await.unwrap();
    run_players(&app, groups()).await.unwrap();
    assert_eq!(
        graph.record("player", "ann").await.unwrap(),
        Some(serde_json::json!({ "name": "Ann", "a_only": true, "score": 2 }))
    );
}

/// A vector index declared on a user-managed table is CocoIndex's, as in
/// Python: it is created, and removed when no longer declared. (Rust created
/// none, yet removed it when undeclared.)
#[tokio::test]
async fn surrealdb_vector_index_on_a_user_managed_table_when_available() {
    let Some((graph, app, _dir)) = scenario("user_index").await else {
        return;
    };
    query_json(&graph, "DEFINE TABLE ext SCHEMALESS").await;
    async fn run(app: &App, declare_index: bool) {
        app.run(move |ctx| async move {
            let t = surrealdb::mount_table_target_with_schema_and_options(
                &ctx,
                &GRAPH,
                "ext",
                None,
                cocoindex::statediff::ManagedTargetOptions::user_managed(),
            )
            .await?;
            if declare_index {
                t.declare_vector_index(&ctx, "embedding", 2, VectorIndexOptions::default())?;
            }
            t.declare_record(&ctx, "e1", &serde_json::json!({ "embedding": [1.0, 0.0] }))
        })
        .await
        .unwrap();
    }
    let idx = "idx_ext__embedding".to_string();
    run(&app, true).await;
    assert!(graph.index_names("ext").await.unwrap().contains(&idx));
    run(&app, false).await;
    assert!(!graph.index_names("ext").await.unwrap().contains(&idx));
    run(&app, true).await;
    app.drop_state().await.unwrap();
    // The table is the user's and stays.
    assert!(
        graph
            .table_names()
            .await
            .unwrap()
            .contains(&"ext".to_string())
    );
    // `drop_state` leaves a user-managed table's contents alone, its records
    // and the index alike.
    assert!(graph.index_names("ext").await.unwrap().contains(&idx));
    assert_eq!(graph.count("ext").await.unwrap(), 1);
}

/// Batches at the transaction chunk boundaries (500 actions per chunk).
#[tokio::test]
async fn surrealdb_batches_at_chunk_boundaries_when_available() {
    let Some((graph, app, _dir)) = scenario("chunks").await else {
        return;
    };
    for n in [499usize, 500, 501, 1000, 1001] {
        app.run(move |ctx| async move {
            let t = surrealdb::mount_table_target(&ctx, &GRAPH, "chunk").await?;
            let r = surrealdb::mount_relation_target(&ctx, &GRAPH, "next", &t, &t).await?;
            for i in 0..n {
                t.declare_record(&ctx, i as i64, &serde_json::json!({ "n": n }))?;
                if i > 0 {
                    r.declare_relation(&ctx, (i - 1) as i64, i as i64)?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(graph.count("chunk").await.unwrap(), n);
        assert_eq!(graph.count("next").await.unwrap(), n - 1);
        assert!(
            query_json(
                &graph,
                &format!("SELECT VALUE <string>id FROM chunk WHERE n != {n}")
            )
            .await
            .is_empty(),
            "n={n}"
        );
    }
}

/// A large declaration, written twice (soak). Run with
/// `SURREALDB_URL=127.0.0.1:8000 cargo test -p cocoindex --features surrealdb,testing --test surrealdb_scenarios -- --ignored`.
#[tokio::test]
#[ignore = "soak: needs a SurrealDB server and takes tens of seconds"]
async fn surrealdb_very_large_batch_soak() {
    let Some((graph, app, _dir)) = scenario("soak").await else {
        return;
    };
    let n = 50_000usize;
    for round in 0..2i64 {
        let started = std::time::Instant::now();
        app.run(move |ctx| async move {
            let t = surrealdb::mount_table_target(&ctx, &GRAPH, "soak").await?;
            let r = surrealdb::mount_relation_target(&ctx, &GRAPH, "soak_next", &t, &t).await?;
            for i in 0..n {
                t.declare_record(
                    &ctx,
                    i as i64,
                    &serde_json::json!({ "round": round, "text": format!("record {i}") }),
                )?;
                if i > 0 {
                    r.declare_relation_record(
                        &ctx,
                        (i - 1) as i64,
                        i as i64,
                        &serde_json::json!({ "round": round }),
                    )?;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
        eprintln!("round {round}: {:?}", started.elapsed());
        assert_eq!(graph.count("soak").await.unwrap(), n);
        assert_eq!(graph.count("soak_next").await.unwrap(), n - 1);
        assert!(
            query_json(
                &graph,
                &format!("SELECT VALUE <string>id FROM soak_next WHERE round != {round}")
            )
            .await
            .is_empty()
        );
    }
}
