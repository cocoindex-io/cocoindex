//! Live-Turbopuffer integration test for the `turbopuffer` namespace target.
//!
//! Skips gracefully unless `TURBOPUFFER_API_KEY` (and optionally
//! `TURBOPUFFER_REGION`) are set. Hits the real hosted service, so it uses a
//! unique namespace and deletes it at the end.
//!
//!   TURBOPUFFER_API_KEY=... TURBOPUFFER_REGION=gcp-us-central1 \
//!     cargo test -p cocoindex --features turbopuffer --test turbopuffer_target
#![cfg(feature = "turbopuffer")]

use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::connectors::turbopuffer::{
    self, DistanceMetric, NamespaceSchema, TurbopufferConnection,
};
use cocoindex::{Environment, Result};
use serde_json::json;

cocoindex::context_key!(
    static DB: TurbopufferConnection, key = "turbopuffer_test"
);

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
            let app = Environment::builder()
                .db_path(&coco_db)
                .provide_key(&DB, conn)
                .build()
                .await
                .unwrap()
                .app("TurbopufferTargetTest")
                .await
                .unwrap();
            app.run(move |ctx| {
                let namespace = namespace.clone();
                let rows = rows.clone();
                async move {
                    let target = turbopuffer::mount_namespace_target(
                        &ctx,
                        &DB,
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

#[tokio::test]
async fn turbopuffer_undeclared_and_dropped_namespaces_are_removed_when_available() -> Result<()> {
    let Ok(api_key) = std::env::var("TURBOPUFFER_API_KEY") else {
        eprintln!(
            "skipping live Turbopuffer namespace-removal test; TURBOPUFFER_API_KEY is not set"
        );
        return Ok(());
    };
    let region = std::env::var("TURBOPUFFER_REGION").unwrap_or_else(|_| "gcp-us-central1".into());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("coco_tpuf_removal_{nonce}");
    let conn = TurbopufferConnection::new(&region, &api_key);

    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn.clone())
        .build()
        .await?
        .app("TurbopufferNamespaceRemovalTest")
        .await?;
    let declare = |namespace: String| {
        move |ctx: cocoindex::Ctx| async move {
            let target = turbopuffer::mount_namespace_target(
                &ctx,
                &DB,
                &namespace,
                NamespaceSchema::new(3, DistanceMetric::CosineDistance),
            )
            .await?;
            target.declare_row(&ctx, "1", vec![1.0, 0.0, 0.0], attrs("alpha"))?;
            Ok::<_, cocoindex::Error>(())
        }
    };
    // Querying a deleted namespace fails with 404 Not Found.
    let exists = |namespace: String| {
        let conn = conn.clone();
        async move {
            match turbopuffer::vector_search(&conn, &namespace, vec![1.0, 0.0, 0.0], 1).await {
                Ok(_) => true,
                Err(e) if e.to_string().contains("404") => false,
                Err(e) => panic!("turbopuffer query: {e}"),
            }
        }
    };

    app.run(declare(namespace.clone())).await?;
    assert!(exists(namespace.clone()).await);

    // A run that no longer declares the namespace deletes it.
    app.run(|_ctx| async { Ok(()) }).await?;
    assert!(
        !exists(namespace.clone()).await,
        "a namespace no longer declared is deleted"
    );

    app.run(declare(namespace.clone())).await?;
    assert!(exists(namespace.clone()).await);

    app.drop_state().await?;
    assert!(
        !exists(namespace.clone()).await,
        "drop_state deletes the namespace"
    );

    conn.delete_namespace(&namespace).await?;
    Ok(())
}

/// The removal paths against a mock Turbopuffer API, so they run without an
/// API key: a namespace no longer declared, and `drop_state`, each send one
/// `DELETE /v2/namespaces/{namespace}`.
#[tokio::test]
async fn turbopuffer_undeclared_and_dropped_namespaces_are_deleted_on_mock_server() -> Result<()> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/namespaces/ns_mock"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v2/namespaces/ns_mock"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let deletes = || async {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "DELETE")
            .count()
    };

    let conn = TurbopufferConnection::new("mock", "key").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn)
        .build()
        .await?
        .app("TurbopufferNamespaceRemovalMockTest")
        .await?;
    let declare = |ctx: cocoindex::Ctx| async move {
        let target = turbopuffer::mount_namespace_target(
            &ctx,
            &DB,
            "ns_mock",
            NamespaceSchema::new(3, DistanceMetric::CosineDistance),
        )
        .await?;
        target.declare_row(&ctx, "1", vec![1.0, 0.0, 0.0], attrs("alpha"))?;
        Ok::<_, cocoindex::Error>(())
    };

    app.run(declare).await?;
    assert_eq!(deletes().await, 0);

    app.run(|_ctx| async { Ok(()) }).await?;
    assert_eq!(
        deletes().await,
        1,
        "a namespace no longer declared is deleted"
    );

    app.run(declare).await?;
    app.drop_state().await?;
    assert_eq!(deletes().await, 2, "drop_state deletes the namespace");
    Ok(())
}
