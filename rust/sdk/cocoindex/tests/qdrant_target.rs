//! Live-Qdrant integration test for the `qdrant` collection target connector.
//!
//! Skips gracefully when `QDRANT_URL` is unset. Run with a Qdrant server:
//!   QDRANT_URL=http://localhost:6334 \
//!     cargo test -p cocoindex --features qdrant --test qdrant_target
//!
//! Exercises the full managed-target reconcile path over the public target-state
//! collection create, point upsert, skip-unchanged, vector search,
//! in-place update, orphan delete, and schema-change collection recreate.
#![cfg(feature = "qdrant")]

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::qdrant::{self, CollectionSchema, Distance, QdrantConnection};
use cocoindex::{App, ContextKey, Result};
use serde_json::json;

static DB: LazyLock<ContextKey<QdrantConnection>> = LazyLock::new(|| {
    ContextKey::new_with_state("qdrant_test", |c: &QdrantConnection| {
        c.state_id().to_string()
    })
});

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
            let app = App::builder("QdrantTargetTest")
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let collection = collection.clone();
                let points = points.clone();
                async move {
                    let conn = ctx.get_key(&DB)?;
                    let target = qdrant::mount_collection_target(
                        &ctx,
                        conn,
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
            let app = App::builder("QdrantUuidTest")
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let collection = collection.clone();
                let ids = ids.clone();
                async move {
                    let conn = ctx.get_key(&DB)?;
                    let target = qdrant::mount_collection_target(
                        &ctx,
                        conn,
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
