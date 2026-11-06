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

    let run = |ids: Vec<&'static str>| {
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
                        target.declare_point(&ctx, *id, v, payload("f.md", id))?;
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
    run(vec![uuid_a, uuid_b]).await;
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

    // Drop one UUID point → orphan reconciled away by UUID id.
    run(vec![uuid_a]).await;
    assert_eq!(
        count(conn.clone(), collection.clone()).await,
        1,
        "orphaned UUID point deleted"
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
