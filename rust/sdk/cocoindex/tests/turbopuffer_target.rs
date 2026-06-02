//! Live-Turbopuffer integration test for the `turbopuffer` namespace target.
//!
//! Skips gracefully unless `TURBOPUFFER_API_KEY` (and optionally
//! `TURBOPUFFER_REGION`) are set. Hits the real hosted service, so it uses a
//! unique namespace and deletes it at the end.
//!
//!   TURBOPUFFER_API_KEY=... TURBOPUFFER_REGION=gcp-us-central1 \
//!     cargo test -p cocoindex --features turbopuffer --test turbopuffer_target
#![cfg(feature = "turbopuffer")]

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::turbopuffer::{self, DistanceMetric, NamespaceSchema, TurbopufferConnection};
use cocoindex::{App, ContextKey, Result};
use serde_json::json;

static DB: LazyLock<ContextKey<TurbopufferConnection>> = LazyLock::new(|| {
    ContextKey::new_with_state("turbopuffer_test", |c: &TurbopufferConnection| {
        c.state_id().to_string()
    })
});

type RowSpec = (&'static str, Vec<f32>, &'static str);

fn attrs(text: &str) -> serde_json::Map<String, serde_json::Value> {
    json!({ "text": text }).as_object().unwrap().clone()
}

#[tokio::test]
async fn turbopuffer_target_upserts_searches_and_reconciles() -> Result<()> {
    let Ok(api_key) = std::env::var("TURBOPUFFER_API_KEY") else {
        eprintln!("skipping live Turbopuffer test; TURBOPUFFER_API_KEY is not set");
        return Ok(());
    };
    let region = std::env::var("TURBOPUFFER_REGION").unwrap_or_else(|_| "gcp-us-central1".into());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("coco_tpuf_test_{nonce}");

    let conn = TurbopufferConnection::new(&region, &api_key);
    let tempdir = tempfile::tempdir().unwrap();
    let coco_db = tempdir.path().join(".cocoindex_db");

    let run = |rows: Vec<RowSpec>| {
        let conn = conn.clone();
        let namespace = namespace.clone();
        let coco_db = coco_db.clone();
        async move {
            let app = App::builder("TurbopufferTargetTest")
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let namespace = namespace.clone();
                let rows = rows.clone();
                async move {
                    let conn = ctx.get_key(&DB)?;
                    let target = turbopuffer::mount_namespace_target(
                        &ctx,
                        conn,
                        &namespace,
                        NamespaceSchema::new(3, DistanceMetric::CosineDistance),
                    )
                    .await?;
                    for (id, v, text) in &rows {
                        target.declare_row(&ctx, *id, v.clone(), attrs(text))?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    let search = |q: Vec<f32>, k: usize| {
        let conn = conn.clone();
        let namespace = namespace.clone();
        async move {
            turbopuffer::vector_search(&conn, &namespace, q, k)
                .await
                .unwrap()
        }
    };

    // --- Run 1: upsert 3 rows ---
    run(vec![
        ("1", vec![1.0, 0.0, 0.0], "alpha"),
        ("2", vec![0.0, 1.0, 0.0], "beta"),
        ("3", vec![0.0, 0.0, 1.0], "gamma"),
    ])
    .await;
    assert_eq!(
        search(vec![0.0, 0.0, 0.0], 10).await.len(),
        3,
        "3 rows present"
    );
    let near = search(vec![0.0, 0.9, 0.1], 1).await;
    assert_eq!(near[0].attributes["text"], "beta", "nearest is beta");

    // --- Run 2: unchanged → still 3 rows (no dup) ---
    run(vec![
        ("1", vec![1.0, 0.0, 0.0], "alpha"),
        ("2", vec![0.0, 1.0, 0.0], "beta"),
        ("3", vec![0.0, 0.0, 1.0], "gamma"),
    ])
    .await;
    assert_eq!(search(vec![0.0, 0.0, 0.0], 10).await.len(), 3, "no dup");

    // --- Run 3: update row 1 text + drop row 3 ---
    run(vec![
        ("1", vec![1.0, 0.0, 0.0], "alpha-updated"),
        ("2", vec![0.0, 1.0, 0.0], "beta"),
    ])
    .await;
    assert_eq!(
        search(vec![0.0, 0.0, 0.0], 10).await.len(),
        2,
        "orphan deleted"
    );
    let hit = search(vec![1.0, 0.0, 0.0], 1).await;
    assert_eq!(hit[0].attributes["text"], "alpha-updated", "text updated");

    // cleanup: explicit namespace teardown (the nonce namespace is test-only).
    conn.delete_namespace(&namespace).await?;
    Ok(())
}
