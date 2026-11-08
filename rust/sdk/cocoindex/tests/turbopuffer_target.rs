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

/// An integer row id is written as a JSON number, in an upsert and in a
/// delete, so Turbopuffer stores a `u64` id, not its text.
#[tokio::test]
async fn turbopuffer_integer_row_ids_are_written_as_numbers_on_mock_server() -> Result<()> {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/namespaces/ns_int_ids"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v2/namespaces/ns_int_ids"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let write_bodies = || async {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "POST")
            .map(|r| r.body_json::<serde_json::Value>().unwrap())
            .collect::<Vec<_>>()
    };

    let conn = TurbopufferConnection::new("mock", "key").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn)
        .build()
        .await?
        .app("TurbopufferIntegerIdsMockTest")
        .await?;
    let declare = |ids: Vec<u64>| {
        move |ctx: cocoindex::Ctx| {
            let ids = ids.clone();
            async move {
                let target = turbopuffer::mount_namespace_target(
                    &ctx,
                    &DB,
                    "ns_int_ids",
                    NamespaceSchema::new(3, DistanceMetric::CosineDistance),
                )
                .await?;
                for id in ids {
                    target.declare_row(&ctx, id, vec![1.0, 0.0, 0.0], attrs("alpha"))?;
                }
                target.declare_row(&ctx, "s1", vec![0.0, 1.0, 0.0], attrs("beta"))?;
                Ok::<_, cocoindex::Error>(())
            }
        }
    };

    app.run(declare(vec![7, u64::from(u32::MAX) + 1])).await?;
    let bodies = write_bodies().await;
    assert_eq!(bodies.len(), 1, "one write per batch");
    let mut ids: Vec<serde_json::Value> = bodies[0]["upsert_rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].clone())
        .collect();
    ids.sort_by_key(|id| id.to_string());
    assert_eq!(ids, vec![json!("s1"), json!(4294967296u64), json!(7)]);

    // Row 7 is no longer declared: its delete carries the number too.
    app.run(declare(vec![u64::from(u32::MAX) + 1])).await?;
    let bodies = write_bodies().await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[1]["deletes"], json!([7]));
    assert!(
        bodies[1].get("upsert_rows").is_none(),
        "unchanged rows are skipped"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Battle tests on a mock Turbopuffer API: malformed vectors, row ids,
// attribute values, batch sizes, schema changes, a failed write retried by the
// next run, handles as mount arguments.
// ---------------------------------------------------------------------------

use std::sync::Arc;

use cocoindex::Ctx;
use cocoindex::connectors::turbopuffer::{NamespaceTarget, RowId, VectorDef};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

type Declare = Arc<dyn Fn(&Ctx, &NamespaceTarget) -> Result<()> + Send + Sync>;

/// A mock Turbopuffer server answering every write and delete of `namespace`
/// with 200, and an app writing to it.
struct Mocked {
    server: MockServer,
    namespace: &'static str,
    app: cocoindex::App,
    _dir: tempfile::TempDir,
}

impl Mocked {
    async fn new(namespace: &'static str) -> Self {
        let server = MockServer::start().await;
        for verb in ["POST", "DELETE"] {
            Mock::given(method(verb))
                .and(path(format!("/v2/namespaces/{namespace}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                .mount(&server)
                .await;
        }
        let conn = TurbopufferConnection::new("mock", "key").with_base_url(server.uri());
        let dir = tempfile::tempdir().unwrap();
        let app = Environment::builder()
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&DB, conn)
            .build()
            .await
            .unwrap()
            .app(namespace)
            .await
            .unwrap();
        Self {
            server,
            namespace,
            app,
            _dir: dir,
        }
    }

    async fn run(&self, schema: NamespaceSchema, declare: Declare) -> Result<()> {
        let namespace = self.namespace;
        self.app
            .update(move |ctx| async move {
                let target =
                    turbopuffer::mount_namespace_target(&ctx, &DB, namespace, schema).await?;
                declare(&ctx, &target)
            })
            .await
    }

    /// Every request so far: `(method, JSON body or Null)`.
    async fn requests(&self) -> Vec<(String, serde_json::Value)> {
        self.server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.method.to_string(),
                    r.body_json().unwrap_or(serde_json::Value::Null),
                )
            })
            .collect()
    }

    async fn writes(&self) -> Vec<serde_json::Value> {
        self.requests()
            .await
            .into_iter()
            .filter(|(m, _)| m == "POST")
            .map(|(_, body)| body)
            .collect()
    }
}

fn schema3() -> NamespaceSchema {
    NamespaceSchema::new(3, DistanceMetric::CosineDistance)
}

fn n_rows(n: u64, text: &'static str) -> Declare {
    Arc::new(move |ctx, target| {
        for i in 0..n {
            target.declare_row(ctx, i, vec![1.0, (i % 5) as f32, 0.0], attrs(text))?;
        }
        Ok(())
    })
}

fn array_len(body: &serde_json::Value, key: &str) -> usize {
    body.get(key).and_then(|v| v.as_array()).map_or(0, Vec::len)
}

/// Vectors that do not fit the schema fail at declare time with the row
/// named, and nothing is sent: wrong length, empty, non-finite, and a named
/// field of the wrong length. Reserved attribute names fail the same way.
#[tokio::test]
async fn turbopuffer_malformed_rows_fail_at_declare_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_bad_rows").await;
    for (name, v) in [
        ("short", vec![1.0, 0.0]),
        ("long", vec![1.0, 0.0, 0.0, 0.0]),
        ("empty", vec![]),
        ("nan", vec![f32::NAN, 0.0, 0.0]),
        ("inf", vec![0.0, f32::INFINITY, 0.0]),
    ] {
        let err = mock
            .run(
                schema3(),
                Arc::new(move |ctx, target| {
                    target.declare_row(ctx, "ok", vec![1.0, 0.0, 0.0], attrs("ok"))?;
                    target.declare_row(ctx, "bad", v.clone(), attrs("bad"))
                }),
            )
            .await
            .expect_err(name);
        assert!(err.to_string().contains("row \"bad\""), "{name}: {err}");
    }
    for reserved in ["id", "vector"] {
        let err = mock
            .run(
                schema3(),
                Arc::new(move |ctx, target| {
                    let attributes = json!({ reserved: 1 }).as_object().unwrap().clone();
                    target.declare_row(ctx, "r", vec![1.0, 0.0, 0.0], attributes)
                }),
            )
            .await
            .expect_err(reserved);
        assert!(err.to_string().contains("reserved"), "{reserved}: {err}");
    }
    let named = NamespaceSchema::named(
        [
            ("text_vec", VectorDef::f32(3).unwrap()),
            ("image_vec", VectorDef::f32(2).unwrap()),
        ],
        DistanceMetric::CosineDistance,
    )?;
    let err = mock
        .run(
            named,
            Arc::new(|ctx, target| {
                target.declare_named_row(
                    ctx,
                    "n",
                    [
                        ("text_vec", vec![1.0, 0.0, 0.0]),
                        ("image_vec", vec![1.0, 0.0, 0.0]),
                    ],
                    attrs("n"),
                )
            }),
        )
        .await
        .expect_err("named wrong length");
    assert!(err.to_string().contains("\"image_vec\""), "{err}");
    assert!(mock.writes().await.is_empty(), "nothing is sent");
    Ok(())
}

/// Row ids: a string with any characters is sent as is, a string `"7"` and an
/// integer `7` are two rows, and an integer beyond `i64` fails at declare.
#[tokio::test]
async fn turbopuffer_row_id_forms_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_row_ids").await;
    let ids: Vec<RowId> = vec![
        RowId::from("7"),
        RowId::from(7u64),
        RowId::from("a/b c:d?e#f 🙂 \"q\""),
        RowId::from(""),
        RowId::from(0u64),
        RowId::from(i64::MAX as u64),
    ];
    {
        let ids = ids.clone();
        mock.run(
            schema3(),
            Arc::new(move |ctx, target| {
                for id in &ids {
                    target.declare_row(ctx, id.clone(), vec![1.0, 0.0, 0.0], attrs("x"))?;
                }
                Ok(())
            }),
        )
        .await?;
    }
    let writes = mock.writes().await;
    let mut sent: Vec<String> = writes[0]["upsert_rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].to_string())
        .collect();
    sent.sort();
    let mut expected: Vec<String> = [
        json!("7"),
        json!(7),
        json!("a/b c:d?e#f 🙂 \"q\""),
        json!(""),
        json!(0),
        json!(i64::MAX),
    ]
    .iter()
    .map(|v| v.to_string())
    .collect();
    expected.sort();
    assert_eq!(sent, expected);

    let err = mock
        .run(
            schema3(),
            Arc::new(|ctx, target| {
                target.declare_row(ctx, u64::MAX, vec![1.0, 0.0, 0.0], attrs("x"))
            }),
        )
        .await
        .expect_err("u64 beyond i64");
    assert!(err.to_string().contains("out of range"), "{err}");
    Ok(())
}

/// Attribute values of every JSON type, nested ones included, are sent
/// unchanged.
#[tokio::test]
async fn turbopuffer_attribute_values_are_sent_unchanged_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_attrs").await;
    let rich = json!({
        "null": null,
        "t": true,
        "i_min": i64::MIN,
        "u_max": u64::MAX,
        "float": 0.1,
        "text": "héllo \"wörld\" \n 🙂",
        "list": [1, "two", [3], {"four": 4}],
        "nested": {"a": {"b": {"c": [null, {"d": "deep"}]}}},
        "big": "z".repeat(1024 * 1024),
    });
    {
        let rich = rich.clone();
        mock.run(
            schema3(),
            Arc::new(move |ctx, target| {
                target.declare_row(
                    ctx,
                    1u64,
                    vec![1.0, 0.0, 0.0],
                    rich.as_object().unwrap().clone(),
                )
            }),
        )
        .await?;
    }
    let row = mock.writes().await[0]["upsert_rows"][0].clone();
    for (k, v) in rich.as_object().unwrap() {
        assert_eq!(&row[k], v, "attribute {k}");
    }
    Ok(())
}

/// Every batch goes out as one write, whatever its size, and reconciles to
/// the right upserts and deletes around the 500 and 1000 marks.
#[tokio::test]
async fn turbopuffer_batches_around_chunk_boundaries_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_batches").await;
    let mut prev = 0u64;
    let mut expected_writes = 0;
    for n in [499u64, 500, 501, 999, 1000, 1001, 2500, 1, 0] {
        mock.run(schema3(), n_rows(n, "v1")).await?;
        let writes = mock.writes().await;
        let added = n.saturating_sub(prev) as usize;
        let removed = prev.saturating_sub(n) as usize;
        if added + removed > 0 {
            expected_writes += 1;
        }
        assert_eq!(
            writes.len(),
            expected_writes,
            "{n} rows: one write per batch"
        );
        if added + removed > 0 {
            let last = writes.last().unwrap();
            assert_eq!(array_len(last, "upsert_rows"), added, "{n} rows: upserts");
            assert_eq!(array_len(last, "deletes"), removed, "{n} rows: deletes");
        }
        prev = n;
    }
    Ok(())
}

/// A write the server rejects fails the run and records nothing; the next
/// run sends every row again.
#[tokio::test]
async fn turbopuffer_failed_write_is_retried_by_the_next_run_on_mock_server() -> Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/namespaces/ns_retry"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v2/namespaces/ns_retry"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .with_priority(2)
        .mount(&server)
        .await;
    let conn = TurbopufferConnection::new("mock", "key").with_base_url(server.uri());
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, conn)
        .build()
        .await?
        .app("TurbopufferRetry")
        .await?;
    let run = || {
        app.update(|ctx| async move {
            let target =
                turbopuffer::mount_namespace_target(&ctx, &DB, "ns_retry", schema3()).await?;
            n_rows(700, "v1")(&ctx, &target)
        })
    };
    let err = run().await.expect_err("the first write fails");
    assert!(err.to_string().contains("500"), "{err}");
    run().await?;
    // Unchanged after a successful write: nothing more is sent.
    run().await?;
    let bodies: Vec<serde_json::Value> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| r.body_json().unwrap())
        .collect();
    assert_eq!(bodies.len(), 2);
    assert_eq!(
        array_len(&bodies[1], "upsert_rows"),
        700,
        "every row is resent"
    );
    Ok(())
}

/// A dimension change deletes the namespace and writes every row again with
/// the new schema.
#[tokio::test]
async fn turbopuffer_dimension_change_clears_and_refills_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_dims").await;
    mock.run(schema3(), n_rows(3, "v1")).await?;
    mock.run(
        NamespaceSchema::new(4, DistanceMetric::CosineDistance),
        Arc::new(|ctx, target| {
            for i in 0..3u64 {
                target.declare_row(ctx, i, vec![1.0, 0.0, 0.0, 0.0], attrs("v2"))?;
            }
            Ok(())
        }),
    )
    .await?;
    let requests = mock.requests().await;
    let methods: Vec<&str> = requests.iter().map(|(m, _)| m.as_str()).collect();
    assert_eq!(methods, ["POST", "DELETE", "POST"]);
    let last = &requests[2].1;
    assert_eq!(array_len(last, "upsert_rows"), 3);
    assert_eq!(last["schema"]["vector"]["type"], "[4]f32");
    Ok(())
}

static TPUF_ARG_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cocoindex::function]
async fn declare_tpuf_row_in_component(ctx: &Ctx, id: u64, target: NamespaceTarget) -> Result<()> {
    TPUF_ARG_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    target.declare_row(ctx, id, vec![1.0, 0.0, 0.0], attrs("arg"))
}

/// A namespace handle is a `mount_each!` argument, keyed by its namespace.
#[tokio::test]
async fn turbopuffer_namespace_target_is_a_mount_argument_on_mock_server() -> Result<()> {
    let mock = Mocked::new("ns_handle_arg").await;
    let calls = || TPUF_ARG_CALLS.load(std::sync::atomic::Ordering::SeqCst);
    for round in 0..2 {
        mock.app
            .run(|ctx| async move {
                let target =
                    turbopuffer::mount_namespace_target(&ctx, &DB, "ns_handle_arg", schema3())
                        .await?;
                cocoindex::use_mount_each!(
                    vec![("1".to_string(), 1u64), ("2".to_string(), 2u64)],
                    |id| declare_tpuf_row_in_component(ctx, id, target.clone())
                )
                .await?;
                Ok(())
            })
            .await?;
        assert_eq!(calls(), 2, "round {round}: unchanged children are skipped");
        // Each child component writes its own row; the second round writes
        // nothing.
        assert_eq!(mock.writes().await.len(), 2, "round {round}");
    }
    Ok(())
}
