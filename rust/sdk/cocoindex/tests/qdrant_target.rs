//! Live-Qdrant integration test for the `qdrant` collection target connector.
//!
//! Skips gracefully when `QDRANT_URL` is unset. Run with a Qdrant server:
//!   QDRANT_URL=http://localhost:6334 \
//!     cargo test -p cocoindex --features qdrant --test qdrant_target
//!
//! Exercises the full managed-target reconcile path over the public target-state
//! collection create, point upsert, skip-unchanged, vector search,
//! in-place update, orphan delete, and schema-change collection recreate; and
//! sparse vectors beside a dense one, over a caller-built client.
#![cfg(feature = "qdrant")]

use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::connectors::qdrant::{
    self, CollectionSchema, Distance, NamedPointVector, QdrantConnection, QdrantNamedVectorDef,
    QdrantSparseVectorDef, QdrantVectorDef, SparseModifier,
};
use cocoindex::{Environment, Result};
use serde_json::json;

cocoindex::context_key!(
    static DB: QdrantConnection, key = "qdrant_test"
);

type Point = (u64, Vec<f32>, &'static str, &'static str);

fn payload(filename: &str, text: &str) -> serde_json::Map<String, serde_json::Value> {
    json!({ "filename": filename, "text": text })
        .as_object()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn qdrant_target_creates_upserts_searches_and_reconciles() -> Result<()> {
    let Ok(url) = std::env::var("QDRANT_URL") else {
        eprintln!("skipping live Qdrant test; QDRANT_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let collection = format!("coco_qdrant_test_{nonce}");

    let conn = QdrantConnection::connect(&url).await?;
    let tempdir = tempfile::tempdir().unwrap();
    let coco_db = tempdir.path().join(".cocoindex_db");

    let run = |size: u64, points: Vec<Point>| {
        let conn = conn.clone();
        let collection = collection.clone();
        let coco_db = coco_db.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap()
                .app("QdrantTargetTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let collection = collection.clone();
                let points = points.clone();
                async move {
                    let target = qdrant::mount_collection_target(
                        &ctx,
                        &DB,
                        &collection,
                        CollectionSchema::new(size, Distance::Cosine),
                    )
                    .await?;
                    for (id, v, filename, text) in &points {
                        target.declare_point(&ctx, *id, v.clone(), payload(filename, text))?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    let count = |conn: QdrantConnection, name: String| async move {
        conn.client()
            .collection_info(name)
            .await
            .unwrap()
            .result
            .unwrap()
            .points_count
            .unwrap_or(0)
    };

    let p = |id, v: [f32; 3], f, t| (id, v.to_vec(), f, t);

    // --- Run 1: create collection + 3 points ---
    run(
        3,
        vec![
            p(1, [1.0, 0.0, 0.0], "a.md", "alpha"),
            p(2, [0.0, 1.0, 0.0], "b.md", "beta"),
            p(3, [0.0, 0.0, 1.0], "c.md", "gamma"),
        ],
    )
    .await;
    assert_eq!(count(conn.clone(), collection.clone()).await, 3, "3 points");

    // --- Vector search returns the nearest point's payload ---
    let hits = qdrant::vector_search(&conn, &collection, vec![0.0, 0.9, 0.1], 1).await?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload["text"], "beta");

    // --- Run 2: unchanged → still 3 points ---
    run(
        3,
        vec![
            p(1, [1.0, 0.0, 0.0], "a.md", "alpha"),
            p(2, [0.0, 1.0, 0.0], "b.md", "beta"),
            p(3, [0.0, 0.0, 1.0], "c.md", "gamma"),
        ],
    )
    .await;
    assert_eq!(count(conn.clone(), collection.clone()).await, 3, "no dup");

    // --- Run 3: update point 1's payload + drop point 3 ---
    run(
        3,
        vec![
            p(1, [1.0, 0.0, 0.0], "a.md", "alpha-updated"),
            p(2, [0.0, 1.0, 0.0], "b.md", "beta"),
        ],
    )
    .await;
    assert_eq!(
        count(conn.clone(), collection.clone()).await,
        2,
        "orphan deleted"
    );
    let hit = qdrant::vector_search(&conn, &collection, vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(hit[0].payload["text"], "alpha-updated", "payload updated");

    // --- Run 4: change vector schema (dim 3 -> 4) → collection recreated,
    // points cleared, the single new 4-dim point re-declared. ---
    run(4, vec![(1, vec![1.0, 0.0, 0.0, 0.0], "a.md", "alpha")]).await;
    assert_eq!(
        count(conn.clone(), collection.clone()).await,
        1,
        "schema change recreated the collection with just the new point"
    );

    // cleanup
    let _ = conn.client().delete_collection(&collection).await;
    Ok(())
}

#[tokio::test]
async fn qdrant_target_supports_uuid_point_ids() -> Result<()> {
    let Ok(url) = std::env::var("QDRANT_URL") else {
        eprintln!("skipping live Qdrant UUID test; QDRANT_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let collection = format!("coco_qdrant_uuid_{nonce}");
    let conn = QdrantConnection::connect(&url).await?;
    let tempdir = tempfile::tempdir().unwrap();
    let coco_db = tempdir.path().join(".cocoindex_db");

    let uuid_a = "11111111-1111-1111-1111-111111111111";
    let uuid_b = "22222222-2222-2222-2222-222222222222";

    let run = |ids: Vec<String>| {
        let conn = conn.clone();
        let collection = collection.clone();
        let coco_db = coco_db.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap()
                .app("QdrantUuidTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let collection = collection.clone();
                let ids = ids.clone();
                async move {
                    let target = qdrant::mount_collection_target(
                        &ctx,
                        &DB,
                        &collection,
                        CollectionSchema::new(3, Distance::Cosine),
                    )
                    .await?;
                    for (i, id) in ids.iter().enumerate() {
                        let v = if i == 0 {
                            vec![1.0, 0.0, 0.0]
                        } else {
                            vec![0.0, 1.0, 0.0]
                        };
                        // String/UUID point id (via From<&str>).
                        target.declare_point(&ctx, id.as_str(), v, payload("f.md", id))?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    let count = |conn: QdrantConnection, name: String| async move {
        conn.client()
            .collection_info(name)
            .await
            .unwrap()
            .result
            .unwrap()
            .points_count
            .unwrap_or(0)
    };

    // Upsert two UUID-keyed points.
    run(vec![uuid_a.to_string(), uuid_b.to_string()]).await;
    assert_eq!(
        count(conn.clone(), collection.clone()).await,
        2,
        "two UUID points"
    );
    let hits = qdrant::vector_search(&conn, &collection, vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(
        hits[0].payload["text"], uuid_a,
        "UUID-keyed point is searchable"
    );

    // Drop one UUID point → orphan reconciled away by UUID id. The kept point
    // is declared in another text form of its UUID: that names the same point
    // and the same target state, so it is updated in place, not duplicated.
    let upper_a = uuid_a.to_uppercase();
    run(vec![upper_a.clone()]).await;
    assert_eq!(
        count(conn.clone(), collection.clone()).await,
        1,
        "orphaned UUID point deleted; the upper-case form updates the same point"
    );
    let hits = qdrant::vector_search(&conn, &collection, vec![1.0, 0.0, 0.0], 1).await?;
    assert_eq!(
        hits[0].payload["text"], upper_a,
        "the point declared under the upper-case form carries its new payload"
    );

    let _ = conn.client().delete_collection(&collection).await;
    Ok(())
}

#[tokio::test]
async fn qdrant_undeclared_and_dropped_collections_are_removed_when_available() -> Result<()> {
    let Ok(url) = std::env::var("QDRANT_URL") else {
        eprintln!("skipping live Qdrant collection-removal test; QDRANT_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let collection = format!("coco_qdrant_removal_{nonce}");
    let conn = QdrantConnection::connect(&url).await?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn.clone())
        .build()
        .await?
        .app("QdrantCollectionRemovalTest")
        .await?;
    let declare = |collection: String| {
        move |ctx: cocoindex::Ctx| async move {
            let target = qdrant::mount_collection_target(
                &ctx,
                &DB,
                &collection,
                CollectionSchema::new(3, Distance::Cosine),
            )
            .await?;
            target.declare_point(&ctx, 1u64, vec![1.0, 0.0, 0.0], payload("a.md", "alpha"))?;
            Ok::<_, cocoindex::Error>(())
        }
    };
    let exists = |name: String| {
        let conn = conn.clone();
        async move { conn.client().collection_exists(name).await.unwrap() }
    };

    app.run(declare(collection.clone())).await?;
    assert!(exists(collection.clone()).await);

    // A run that no longer declares the collection drops it.
    app.run(|_ctx| async { Ok(()) }).await?;
    assert!(
        !exists(collection.clone()).await,
        "a collection no longer declared is dropped"
    );

    app.run(declare(collection.clone())).await?;
    assert!(exists(collection.clone()).await);

    app.drop_state().await?;
    assert!(
        !exists(collection.clone()).await,
        "drop_state drops the collection"
    );

    let _ = conn.client().delete_collection(&collection).await;
    Ok(())
}

/// Dense + sparse named vectors over a connection wrapped with `from_client`:
/// the collection gets both configs, sparse values are stored and searchable,
/// an updated sparse vector is rewritten, and changing the sparse modifier
/// recreates the collection.
#[tokio::test]
async fn qdrant_target_stores_sparse_vectors_over_a_caller_built_client() -> Result<()> {
    let Ok(url) = std::env::var("QDRANT_URL") else {
        eprintln!("skipping live Qdrant sparse-vector test; QDRANT_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let collection = format!("coco_qdrant_sparse_{nonce}");

    // The caller owns the client's configuration (API key, TLS, timeouts).
    let client = qdrant_client::Qdrant::from_url(&url)
        .api_key(std::env::var("QDRANT_API_KEY").ok())
        .build()
        .unwrap();
    let conn = QdrantConnection::from_client("qdrant-under-test", client);
    assert_eq!(conn.state_id(), "qdrant-under-test");

    let tempdir = tempfile::tempdir().unwrap();
    let coco_db = tempdir.path().join(".cocoindex_db");

    type SparsePoint = (u64, [f32; 3], Vec<u32>, Vec<f32>, &'static str);
    let run = |modifier: Option<SparseModifier>, points: Vec<SparsePoint>| {
        let conn = conn.clone();
        let collection = collection.clone();
        let coco_db = coco_db.clone();
        async move {
            let app = Environment::builder()
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap()
                .app("QdrantSparseTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let collection = collection.clone();
                let points = points.clone();
                async move {
                    let schema = CollectionSchema::named([
                        (
                            "dense",
                            QdrantNamedVectorDef::from(QdrantVectorDef::f32(3, Distance::Cosine)?),
                        ),
                        ("sparse", QdrantSparseVectorDef { modifier }.into()),
                    ])?;
                    let target =
                        qdrant::mount_collection_target(&ctx, &DB, &collection, schema).await?;
                    for (id, dense, indices, values, text) in points {
                        target.declare_named_vectors_point(
                            &ctx,
                            id,
                            [
                                ("dense", NamedPointVector::Single(dense.to_vec())),
                                ("sparse", NamedPointVector::Sparse { indices, values }),
                            ],
                            payload("f.md", text),
                        )?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    // The text of the best hit for a sparse query on one dimension.
    let sparse_top = |index: u32| {
        let conn = conn.clone();
        let collection = collection.clone();
        async move {
            let response = conn
                .client()
                .query(
                    qdrant_client::qdrant::QueryPointsBuilder::new(&collection)
                        .query(qdrant_client::qdrant::VectorInput::new_sparse(
                            vec![index],
                            vec![1.0],
                        ))
                        .using("sparse")
                        .limit(1)
                        .with_payload(true),
                )
                .await
                .unwrap();
            response.result.first().map(|hit| {
                hit.payload["text"]
                    .as_str()
                    .map(String::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
        }
    };

    run(
        None,
        vec![
            (1, [1.0, 0.0, 0.0], vec![10, 20], vec![1.0, 0.5], "alpha"),
            (2, [0.0, 1.0, 0.0], vec![30], vec![2.0], "beta"),
        ],
    )
    .await;

    let info = conn
        .client()
        .collection_info(&collection)
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(info.points_count, Some(2));
    let params = info.config.unwrap().params.unwrap();
    let sparse_config = params.sparse_vectors_config.unwrap();
    assert_eq!(sparse_config.map.keys().collect::<Vec<_>>(), ["sparse"]);
    assert_eq!(sparse_config.map["sparse"].modifier.unwrap_or(0), 0);

    assert_eq!(sparse_top(30).await.as_deref(), Some("beta"));
    assert_eq!(sparse_top(10).await.as_deref(), Some("alpha"));
    // The dense field of the same points still answers.
    let hits =
        qdrant::named_vector_search(&conn, &collection, "dense", vec![0.0, 1.0, 0.0], 1).await?;
    assert_eq!(hits[0].payload["text"], "beta");

    // Only the sparse vector of point 1 changes: it moves to dimension 40.
    run(
        None,
        vec![
            (1, [1.0, 0.0, 0.0], vec![40], vec![1.0], "alpha"),
            (2, [0.0, 1.0, 0.0], vec![30], vec![2.0], "beta"),
        ],
    )
    .await;
    assert_eq!(sparse_top(40).await.as_deref(), Some("alpha"));
    assert_eq!(sparse_top(10).await, None, "the old sparse value is gone");

    // A different modifier is a schema change: the collection is recreated
    // with it and holds only what this run declares.
    run(
        Some(SparseModifier::Idf),
        vec![(2, [0.0, 1.0, 0.0], vec![30], vec![2.0], "beta")],
    )
    .await;
    let info = conn
        .client()
        .collection_info(&collection)
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(info.points_count, Some(1));
    let sparse_config = info
        .config
        .unwrap()
        .params
        .unwrap()
        .sparse_vectors_config
        .unwrap();
    assert_eq!(
        sparse_config.map["sparse"].modifier,
        Some(qdrant_client::qdrant::Modifier::Idf as i32)
    );

    let _ = conn.client().delete_collection(&collection).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Battle tests: id forms, malformed vectors, payload values, batch sizes,
// out-of-band changes, failed writes, handles as mount arguments.
// ---------------------------------------------------------------------------

use std::sync::Arc;

use cocoindex::connectors::qdrant::{CollectionTarget, QdrantPointId};
use cocoindex::{Ctx, UpdateOptions};
use qdrant_client::qdrant::{ScrollPointsBuilder, point_id::PointIdOptions};

type Declare = Arc<dyn Fn(&Ctx, &CollectionTarget) -> Result<()> + Send + Sync>;

/// One live collection with its own CocoIndex app, deleted by `cleanup`.
struct Live {
    conn: QdrantConnection,
    collection: String,
    app: cocoindex::App,
    _dir: tempfile::TempDir,
}

impl Live {
    async fn new(tag: &str) -> Option<Self> {
        let Ok(url) = std::env::var("QDRANT_URL") else {
            eprintln!("skipping live Qdrant test; QDRANT_URL is not set");
            return None;
        };
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let conn = QdrantConnection::connect(&url).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&DB, conn.clone())
            .build()
            .await
            .unwrap()
            .app(&format!("QdrantBattle_{tag}"))
            .await
            .unwrap();
        Some(Self {
            conn,
            collection: format!("coco_qdrant_{tag}_{nonce}"),
            app,
            _dir: dir,
        })
    }

    async fn run_with(
        &self,
        options: UpdateOptions,
        schema: CollectionSchema,
        user_managed: bool,
        declare: Declare,
    ) -> Result<()> {
        let collection = self.collection.clone();
        self.app
            .update_with_options(options, move |ctx| async move {
                let target = if user_managed {
                    qdrant::mount_collection_target_with_options(
                        &ctx,
                        &DB,
                        &collection,
                        schema,
                        cocoindex::ManagedTargetOptions::user_managed(),
                    )
                    .await?
                } else {
                    qdrant::mount_collection_target(&ctx, &DB, &collection, schema).await?
                };
                declare(&ctx, &target)
            })
            .await
    }

    async fn run(&self, schema: CollectionSchema, declare: Declare) -> Result<()> {
        self.run_with(UpdateOptions::default(), schema, false, declare)
            .await
    }

    async fn count(&self) -> u64 {
        self.conn
            .client()
            .count(qdrant_client::qdrant::CountPointsBuilder::new(&self.collection).exact(true))
            .await
            .unwrap()
            .result
            .unwrap()
            .count
    }

    async fn exists(&self) -> bool {
        self.conn
            .client()
            .collection_exists(&self.collection)
            .await
            .unwrap()
    }

    /// Every point's id (as text) and payload, sorted by id text.
    async fn points(&self) -> Vec<(String, serde_json::Value)> {
        let response = self
            .conn
            .client()
            .scroll(
                ScrollPointsBuilder::new(&self.collection)
                    .limit(100_000)
                    .with_payload(true),
            )
            .await
            .unwrap();
        let mut points: Vec<_> = response
            .result
            .into_iter()
            .map(|p| {
                let id = match p.id.unwrap().point_id_options.unwrap() {
                    PointIdOptions::Num(n) => n.to_string(),
                    PointIdOptions::Uuid(u) => u,
                };
                let payload: serde_json::Value = qdrant_client::Payload::from(p.payload).into();
                (id, payload)
            })
            .collect();
        points.sort_by(|a, b| a.0.cmp(&b.0));
        points
    }

    async fn cleanup(self) {
        let _ = self.conn.client().delete_collection(&self.collection).await;
    }
}

fn dense3() -> CollectionSchema {
    CollectionSchema::new(3, Distance::Cosine)
}

fn declare_points(points: Vec<(QdrantPointId, Vec<f32>, &'static str)>) -> Declare {
    Arc::new(move |ctx, target| {
        for (id, v, text) in &points {
            target.declare_point(ctx, id.clone(), v.clone(), payload("f.md", text))?;
        }
        Ok(())
    })
}

/// All text forms of one UUID name one point and one target state; integer
/// ids cover the whole `u64` range; a string that is not a UUID is rejected at
/// declare time.
#[tokio::test]
async fn qdrant_point_id_forms_name_one_point_when_available() -> Result<()> {
    let Some(live) = Live::new("ids").await else {
        return Ok(());
    };
    let canonical = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let forms = [
        ("{6BA7B810-9DAD-11D1-80B4-00C04FD430C8}", "braced"),
        ("urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8", "urn"),
        ("6ba7b8109dad11d180b400c04fd430c8", "simple"),
        ("6BA7B810-9dad-11D1-80b4-00C04FD430C8", "mixed"),
    ];
    for (form, text) in forms {
        live.run(
            dense3(),
            declare_points(vec![
                (QdrantPointId::from(form), vec![1.0, 0.0, 0.0], text),
                (QdrantPointId::from(0u64), vec![0.0, 1.0, 0.0], "zero"),
                (QdrantPointId::from(u64::MAX), vec![0.0, 0.0, 1.0], "max"),
            ]),
        )
        .await?;
        let points = live.points().await;
        let ids: Vec<&str> = points.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["0", "18446744073709551615", canonical], "form {form}");
        assert_eq!(
            points[2].1["text"], text,
            "form {form} updates the same point"
        );
    }

    // The canonical form names the same point too.
    live.run(
        dense3(),
        declare_points(vec![(
            QdrantPointId::from(canonical),
            vec![1.0, 0.0, 0.0],
            "typed",
        )]),
    )
    .await?;
    let points = live.points().await;
    assert_eq!(
        points,
        [(
            canonical.to_string(),
            json!({"filename": "f.md", "text": "typed"})
        )],
        "the integer points are removed"
    );

    // Strings that are not UUIDs fail at declare time and write nothing.
    for bad in [
        "42",
        "doc/1",
        "",
        "6ba7b810-9dad-11d1-80b4",
        "ü-🙂 id with spaces",
    ] {
        let err = live
            .run(
                dense3(),
                declare_points(vec![(QdrantPointId::from(bad), vec![1.0, 0.0, 0.0], "bad")]),
            )
            .await
            .expect_err(bad);
        assert!(err.to_string().contains("must be UUIDs"), "{bad}: {err}");
    }
    assert_eq!(live.points().await.len(), 1, "failed runs write nothing");
    live.cleanup().await;
    Ok(())
}

/// Two text forms of one UUID declared in one run are one target state
/// declared twice: the run fails, naming the key.
#[tokio::test]
async fn qdrant_two_forms_of_one_uuid_in_one_run_when_available() -> Result<()> {
    let Some(live) = Live::new("dupid").await else {
        return Ok(());
    };
    let result = live
        .run(
            dense3(),
            declare_points(vec![
                (
                    QdrantPointId::from("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                    vec![1.0, 0.0, 0.0],
                    "lower",
                ),
                (
                    QdrantPointId::from("6BA7B810-9DAD-11D1-80B4-00C04FD430C8"),
                    vec![0.0, 1.0, 0.0],
                    "upper",
                ),
            ]),
        )
        .await;
    let err = result.expect_err("one target state declared twice");
    assert!(
        err.to_string().contains("already declared"),
        "the error names the duplicate: {err}"
    );
    assert!(!live.exists().await || live.count().await == 0);
    live.cleanup().await;
    Ok(())
}

/// Vectors that do not fit the schema fail at declare time, with the point
/// named, before anything is written: wrong length, empty, non-finite, and
/// malformed multivectors and sparse vectors. A later valid run succeeds.
#[tokio::test]
async fn qdrant_malformed_vectors_fail_at_declare_when_available() -> Result<()> {
    let Some(live) = Live::new("badvec").await else {
        return Ok(());
    };
    let cases: Vec<(&str, Vec<f32>)> = vec![
        ("short", vec![1.0, 0.0]),
        ("long", vec![1.0, 0.0, 0.0, 0.0]),
        ("empty", vec![]),
        ("nan", vec![f32::NAN, 0.0, 0.0]),
        ("inf", vec![f32::INFINITY, 0.0, 0.0]),
    ];
    for (name, v) in cases {
        let err = live
            .run(
                dense3(),
                declare_points(vec![
                    (QdrantPointId::from(1u64), vec![1.0, 0.0, 0.0], "ok"),
                    (QdrantPointId::from(2u64), v, "bad"),
                ]),
            )
            .await
            .expect_err(name);
        let msg = err.to_string();
        assert!(msg.contains("qdrant point 2"), "{name}: {msg}");
    }
    assert!(
        !live.exists().await || live.count().await == 0,
        "nothing written"
    );

    // Multivectors: no vectors, or one of the wrong length or non-finite.
    for (name, vs) in [
        ("no vectors", vec![]),
        ("wrong inner length", vec![vec![1.0, 0.0, 0.0], vec![1.0]]),
        ("nan inner", vec![vec![f32::NAN, 0.0, 0.0]]),
    ] {
        let vs: Vec<Vec<f32>> = vs;
        let err = live
            .run(
                CollectionSchema::multivector(3, Distance::Cosine),
                Arc::new(move |ctx, target| {
                    target.declare_multivector_point(ctx, 5u64, vs.clone(), payload("m", "m"))
                }),
            )
            .await
            .expect_err(name);
        assert!(err.to_string().contains("qdrant point 5"), "{name}: {err}");
    }

    // Sparse vectors: unsorted indices and an empty vector are valid; a
    // repeated index, mismatched lengths and a NaN weight are not.
    let sparse_schema = || {
        CollectionSchema::named([
            (
                "dense",
                QdrantNamedVectorDef::from(QdrantVectorDef::f32(3, Distance::Cosine).unwrap()),
            ),
            ("sparse", QdrantSparseVectorDef::default().into()),
        ])
        .unwrap()
    };
    let named_point = |dense: Vec<f32>, indices: Vec<u32>, values: Vec<f32>| -> Declare {
        Arc::new(move |ctx, target| {
            target.declare_named_vectors_point(
                ctx,
                9u64,
                [
                    ("dense", NamedPointVector::Single(dense.clone())),
                    (
                        "sparse",
                        NamedPointVector::Sparse {
                            indices: indices.clone(),
                            values: values.clone(),
                        },
                    ),
                ],
                payload("s", "s"),
            )
        })
    };
    let unit = || vec![1.0, 0.0, 0.0];
    live.run(
        sparse_schema(),
        named_point(unit(), vec![30, 10, 20], vec![3.0, 1.0, 2.0]),
    )
    .await?;
    live.run(sparse_schema(), named_point(unit(), vec![], vec![]))
        .await?;
    assert_eq!(live.count().await, 1);
    for (name, dense, indices, values) in [
        ("repeated index", unit(), vec![1, 1], vec![1.0, 2.0]),
        ("length mismatch", unit(), vec![1, 2], vec![1.0]),
        ("nan weight", unit(), vec![1], vec![f32::NAN]),
        ("named dense wrong length", vec![1.0], vec![], vec![]),
    ] {
        let err = live
            .run(sparse_schema(), named_point(dense, indices, values))
            .await
            .expect_err(name);
        assert!(err.to_string().contains("qdrant point 9"), "{name}: {err}");
    }
    assert_eq!(live.count().await, 1, "the valid point stays");
    live.cleanup().await;
    Ok(())
}

/// Payload values of every JSON type round-trip, nested objects and arrays
/// included, and a multi-megabyte payload is stored. An integer beyond `i64`
/// (Qdrant stores integers as `i64`) fails at declare time rather than being
/// rounded to a float.
#[tokio::test]
async fn qdrant_payload_values_round_trip_when_available() -> Result<()> {
    let Some(live) = Live::new("payload").await else {
        return Ok(());
    };
    let rich = json!({
        "null": null,
        "t": true,
        "f": false,
        "i_min": i64::MIN,
        "i_max": i64::MAX,
        "neg": -7,
        "float": 1.5,
        "tiny": 1e-300,
        "text": "héllo \"wörld\" \n\t 🙂 \u{0}",
        "empty_text": "",
        "empty_list": [],
        "empty_obj": {},
        "list": [1, "two", 3.5, null, [4, [5]], {"six": 6}],
        "nested": {"a": {"b": {"c": {"d": [true, {"e": "deep"}]}}}},
        "key with spaces and . dots": 1,
    });
    let big = "x".repeat(4 * 1024 * 1024);
    {
        let rich = rich.clone();
        let big = big.clone();
        live.run(
            dense3(),
            Arc::new(move |ctx, target| {
                target.declare_point(
                    ctx,
                    1u64,
                    vec![1.0, 0.0, 0.0],
                    rich.as_object().unwrap().clone(),
                )?;
                target.declare_point(
                    ctx,
                    2u64,
                    vec![0.0, 1.0, 0.0],
                    json!({ "big": big }).as_object().unwrap().clone(),
                )
            }),
        )
        .await?;
    }
    let points = live.points().await;
    assert_eq!(points[0].1, rich);
    assert_eq!(points[1].1["big"].as_str().map(str::len), Some(big.len()));

    for (name, value) in [
        ("u64 beyond i64", json!({ "n": u64::MAX })),
        (
            "nested u64 beyond i64",
            json!({ "a": [{ "n": (i64::MAX as u64) + 1 }] }),
        ),
    ] {
        let err = live
            .run(
                dense3(),
                Arc::new(move |ctx, target| {
                    target.declare_point(
                        ctx,
                        3u64,
                        vec![1.0, 0.0, 0.0],
                        value.as_object().unwrap().clone(),
                    )
                }),
            )
            .await
            .expect_err(name);
        assert!(err.to_string().contains("qdrant point 3"), "{name}: {err}");
    }
    live.cleanup().await;
    Ok(())
}

fn n_points(n: u64, tag: &'static str) -> Declare {
    Arc::new(move |ctx, target| {
        for i in 0..n {
            let x = (i % 97) as f32 + 1.0;
            target.declare_point(ctx, i, vec![x, 1.0, (i % 7) as f32], payload("f", tag))?;
        }
        Ok(())
    })
}

/// Batches on both sides of the 500 and 1000 marks, growing and shrinking.
#[tokio::test]
async fn qdrant_batches_around_chunk_boundaries_when_available() -> Result<()> {
    let Some(live) = Live::new("batch").await else {
        return Ok(());
    };
    for n in [499u64, 500, 501, 999, 1000, 1001, 2500, 1, 0, 1000] {
        live.run(dense3(), n_points(n, "v1")).await?;
        assert_eq!(live.count().await, n, "after declaring {n} points");
    }
    // Every point's payload changes at once.
    live.run(dense3(), n_points(1000, "v2")).await?;
    let points = live.points().await;
    assert!(points.iter().all(|(_, p)| p["text"] == "v2"));
    live.cleanup().await;
    Ok(())
}

/// A very large batch: 100k 128-dimension points in one run, then all of them
/// deleted. Slow; run with
///   QDRANT_URL=http://localhost:6334 cargo test -p cocoindex --features qdrant,testing \
///     --test qdrant_target qdrant_very_large_batch -- --ignored
#[tokio::test]
#[ignore]
async fn qdrant_very_large_batch_when_available() -> Result<()> {
    let Some(live) = Live::new("huge").await else {
        return Ok(());
    };
    const N: u64 = 100_000;
    let schema = || CollectionSchema::new(128, Distance::Dot);
    let start = std::time::Instant::now();
    live.run(
        schema(),
        Arc::new(|ctx, target| {
            for i in 0..N {
                let mut v = vec![0.0f32; 128];
                v[(i % 128) as usize] = 1.0;
                target.declare_point(ctx, i, v, payload("f", "huge"))?;
            }
            Ok(())
        }),
    )
    .await?;
    eprintln!("upserted {N} points in {:?}", start.elapsed());
    assert_eq!(live.count().await, N);
    let start = std::time::Instant::now();
    live.run(schema(), Arc::new(|_, _| Ok(()))).await?;
    eprintln!("deleted {N} points in {:?}", start.elapsed());
    assert_eq!(live.count().await, 0);
    live.cleanup().await;
    Ok(())
}

/// A collection deleted behind CocoIndex's back is recreated on the next run;
/// a full reprocess writes its points again.
#[tokio::test]
async fn qdrant_collection_deleted_out_of_band_is_recreated_when_available() -> Result<()> {
    let Some(live) = Live::new("oob").await else {
        return Ok(());
    };
    live.run(dense3(), n_points(10, "v1")).await?;
    live.conn
        .client()
        .delete_collection(&live.collection)
        .await
        .unwrap();
    live.run(dense3(), n_points(10, "v1")).await?;
    assert!(live.exists().await, "the collection is recreated");
    live.run_with(
        UpdateOptions {
            full_reprocess: true,
            ..Default::default()
        },
        dense3(),
        false,
        n_points(10, "v1"),
    )
    .await?;
    assert_eq!(
        live.count().await,
        10,
        "a full reprocess restores the points"
    );
    live.cleanup().await;
    Ok(())
}

/// A write that fails (the user-managed collection does not exist yet) fails
/// the run and records nothing: once the collection exists, the next run
/// writes every point.
#[tokio::test]
async fn qdrant_failed_write_is_retried_by_the_next_run_when_available() -> Result<()> {
    let Some(live) = Live::new("retry").await else {
        return Ok(());
    };
    let err = live
        .run_with(
            UpdateOptions::default(),
            dense3(),
            true,
            n_points(1200, "v1"),
        )
        .await
        .expect_err("the collection does not exist");
    assert!(err.to_string().contains("qdrant"), "{err}");

    live.conn
        .client()
        .create_collection(
            qdrant_client::qdrant::CreateCollectionBuilder::new(&live.collection).vectors_config(
                qdrant_client::qdrant::VectorParamsBuilder::new(
                    3,
                    qdrant_client::qdrant::Distance::Cosine,
                ),
            ),
        )
        .await
        .unwrap();
    live.run_with(
        UpdateOptions::default(),
        dense3(),
        true,
        n_points(1200, "v1"),
    )
    .await?;
    assert_eq!(live.count().await, 1200);
    live.cleanup().await;
    Ok(())
}

/// Changing the element type is a schema change: the collection is recreated
/// with the new datatype and refilled. Qdrant has no f64 vectors: an f64
/// schema is stored as f32.
#[tokio::test]
async fn qdrant_element_type_change_recreates_the_collection_when_available() -> Result<()> {
    use cocoindex::resources::schema::{VectorElementType, VectorSchema};
    let Some(live) = Live::new("dtype").await else {
        return Ok(());
    };
    let datatype = |live: &Live| {
        let conn = live.conn.clone();
        let name = live.collection.clone();
        async move {
            let info = conn
                .client()
                .collection_info(&name)
                .await
                .unwrap()
                .result
                .unwrap();
            let params = info.config.unwrap().params.unwrap().vectors_config.unwrap();
            match params.config.unwrap() {
                qdrant_client::qdrant::vectors_config::Config::Params(p) => p.datatype,
                other => panic!("unexpected {other:?}"),
            }
        }
    };
    let schema = |t: VectorElementType| {
        CollectionSchema::from_vector_schema(
            VectorSchema {
                element_type: t,
                ..VectorSchema::f32(3)
            },
            Distance::Cosine,
        )
        .unwrap()
    };
    live.run(schema(VectorElementType::Float32), n_points(5, "f32"))
        .await?;
    assert_eq!(datatype(&live).await.unwrap_or(0), 0);
    live.run(schema(VectorElementType::Float16), n_points(5, "f16"))
        .await?;
    assert_eq!(
        datatype(&live).await,
        Some(qdrant_client::qdrant::Datatype::Float16 as i32)
    );
    assert_eq!(
        live.count().await,
        5,
        "the recreated collection is refilled"
    );
    live.run(schema(VectorElementType::Float64), n_points(5, "f64"))
        .await?;
    assert_eq!(
        datatype(&live).await.unwrap_or(0),
        0,
        "f64 is stored as f32"
    );
    assert_eq!(live.count().await, 5);
    // A distance change recreates it too.
    live.run(
        CollectionSchema::new(3, Distance::Euclid),
        n_points(5, "l2"),
    )
    .await?;
    assert_eq!(live.count().await, 5);
    live.cleanup().await;
    Ok(())
}

static QDRANT_ARG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cocoindex::function]
async fn declare_qdrant_point_in_component(
    ctx: &Ctx,
    id: u64,
    target: CollectionTarget,
) -> Result<()> {
    QDRANT_ARG_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    target.declare_point(ctx, id, vec![1.0, 0.0, id as f32], payload("f", "arg"))
}

/// A collection handle is a `mount_each!` argument, keyed by its collection.
#[tokio::test]
async fn qdrant_collection_target_is_a_mount_argument_when_available() -> Result<()> {
    let Some(live) = Live::new("arg").await else {
        return Ok(());
    };
    let calls = || QDRANT_ARG_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    for round in 0..2 {
        let collection = live.collection.clone();
        live.app
            .run(move |ctx| async move {
                let target =
                    qdrant::mount_collection_target(&ctx, &DB, &collection, dense3()).await?;
                cocoindex::use_mount_each!(
                    vec![("1".to_string(), 1u64), ("2".to_string(), 2u64)],
                    |id| declare_qdrant_point_in_component(ctx, id, target.clone())
                )
                .await?;
                Ok(())
            })
            .await?;
        assert_eq!(calls(), 2, "round {round}: unchanged children are skipped");
        assert_eq!(live.count().await, 2);
    }
    live.cleanup().await;
    Ok(())
}
