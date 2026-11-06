//! Tests for the `bigquery` table target.
//!
//! The reconcile lifecycle runs against a `wiremock` stand-in for the BigQuery
//! v2 REST API (every `jobs.query` call is recorded and answered as complete),
//! so no Google account is needed: table DDL on first run, batched `MERGE` for
//! declared rows, `DELETE` for un-declared ones, the no-change skip, `DROP
//! TABLE` when the table is no longer declared, the user-managed mode, and the
//! service-account token exchange.
//!
//! The live test at the end hits a real project and skips unless
//! `BIGQUERY_PROJECT` and `BIGQUERY_DATASET` are set (credentials come from
//! `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials):
//!   BIGQUERY_PROJECT=my-proj BIGQUERY_DATASET=cocoindex_test \
//!     cargo test -p cocoindex --features bigquery --test bigquery_target
#![cfg(feature = "bigquery")]

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::connectors::bigquery::{
    self, BigQueryConfig, BigQueryConnection, BigQueryTableOptions, ColumnDef, TableSchema,
};
use cocoindex::statediff::ManagedBy;
use cocoindex::{Environment, Result};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

cocoindex::context_key!(static BQ: BigQueryConnection, key = "bigquery_target_test");

#[derive(Serialize, Clone, cocoindex::SchemaFields)]
struct Item {
    id: i64,
    name: String,
    #[coco(json)]
    tags: Vec<String>,
}

fn item(id: i64, name: &str, tags: &[&str]) -> Item {
    Item {
        id,
        name: name.to_string(),
        tags: tags.iter().map(|t| t.to_string()).collect(),
    }
}

async fn mock_server() -> Option<MockServer> {
    let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("skipping: cannot bind a local mock server ({e})");
            return None;
        }
    };
    Some(MockServer::builder().listener(listener).start().await)
}

/// A mock BigQuery that answers every query job as complete (no rows).
async fn bigquery_server() -> Option<MockServer> {
    let server = mock_server().await?;
    Mock::given(method("POST"))
        .and(path_regex(r"^/projects/[^/]+/queries$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"jobId": "job_1", "location": "US"},
        })))
        .mount(&server)
        .await;
    Some(server)
}

fn connection(server: &MockServer) -> BigQueryConnection {
    BigQueryConnection::from_static_token(
        BigQueryConfig {
            project: Some("demo-project".to_string()),
            credentials_path: None,
            location: Some("US".to_string()),
        },
        "test-token",
    )
    .unwrap()
    .with_base_url(server.uri())
}

/// The `query` text of every query job the server received so far, in order,
/// with the bound parameters.
async fn received_queries(server: &MockServer) -> Vec<(String, Vec<JsonValue>)> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .map(|r| {
            let body: JsonValue = serde_json::from_slice(&r.body).unwrap();
            (
                body["query"].as_str().unwrap().to_string(),
                body.get("queryParameters")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect()
}

fn param_value(param: &JsonValue) -> &JsonValue {
    &param["parameterValue"]["value"]
}

async fn declare(
    conn: &BigQueryConnection,
    db_path: &Path,
    rows: Vec<Item>,
    options: BigQueryTableOptions,
) -> Result<()> {
    let app = Environment::builder()
        .db_path(db_path)
        .provide_key(&BQ, conn.clone())
        .build()
        .await?
        .app("BigQueryTargetTest")
        .await?;
    app.run(move |ctx| {
        let rows = rows.clone();
        let options = options.clone();
        async move {
            let schema = TableSchema::from_row::<Item>(["id"])?;
            let target = bigquery::mount_table_target_with_options(
                &ctx,
                &BQ,
                "analytics",
                "items",
                schema,
                options,
            )
            .await?;
            for row in &rows {
                target.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_reconciles_table_and_rows_over_rest() -> Result<()> {
    let Some(server) = bigquery_server().await else {
        return Ok(());
    };
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    // Run 1: dataset + table DDL, then one MERGE carrying both rows. With no
    // `project` option the table is named relative to the connection's project.
    declare(
        &conn,
        &db,
        vec![item(1, "Alice", &["a"]), item(2, "Bob", &[])],
        BigQueryTableOptions::default(),
    )
    .await?;
    let queries = received_queries(&server).await;
    assert_eq!(queries.len(), 3, "{queries:?}");
    assert_eq!(queries[0].0, "CREATE SCHEMA IF NOT EXISTS `analytics`");
    assert_eq!(
        queries[1].0,
        "CREATE TABLE `analytics.items` (`id` INT64 NOT NULL, `name` STRING NOT \
         NULL, `tags` JSON NOT NULL, PRIMARY KEY (`id`) NOT ENFORCED)"
    );
    let (merge, params) = &queries[2];
    assert!(
        merge.starts_with("MERGE `analytics.items` AS target USING (SELECT @p0 AS `id`, @p1 AS `name`, PARSE_JSON(@p2) AS `tags` UNION ALL SELECT @p3 AS `id`, @p4 AS `name`, PARSE_JSON(@p5) AS `tags`) AS source ON target.`id` = source.`id`"),
        "{merge}"
    );
    assert_eq!(params.len(), 6);
    assert_eq!(params[0]["parameterType"]["type"], json!("INT64"));
    assert_eq!(param_value(&params[1]), &json!("Alice"));
    assert_eq!(params[2]["parameterType"]["type"], json!("STRING"));
    assert_eq!(param_value(&params[2]), &json!("[\"a\"]"));
    assert_eq!(param_value(&params[5]), &json!("[]"));
    // Every call carried the static bearer token and the configured location.
    for request in server.received_requests().await.unwrap() {
        assert_eq!(
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer test-token"
        );
        let body: JsonValue = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["location"], json!("US"));
    }

    // Run 2: row 1 changed, row 2 gone → one MERGE (row 1 only) + one DELETE.
    server.reset().await;
    let Some(server) = bigquery_server_on(server).await else {
        return Ok(());
    };
    declare(
        &conn,
        &db,
        vec![item(1, "Alice2", &["a", "b"])],
        BigQueryTableOptions::default(),
    )
    .await?;
    let queries = received_queries(&server).await;
    assert_eq!(queries.len(), 2, "{queries:?}");
    assert!(
        queries[0].0.starts_with("MERGE `analytics.items`"),
        "{}",
        queries[0].0
    );
    assert_eq!(queries[0].1.len(), 3);
    assert_eq!(param_value(&queries[0].1[1]), &json!("Alice2"));
    assert_eq!(
        queries[1].0,
        "DELETE FROM `analytics.items` WHERE `id` IN (@p0)"
    );
    assert_eq!(param_value(&queries[1].1[0]), &json!("2"));

    // Run 3: identical rows → nothing is sent.
    server.reset().await;
    let Some(server) = bigquery_server_on(server).await else {
        return Ok(());
    };
    declare(
        &conn,
        &db,
        vec![item(1, "Alice2", &["a", "b"])],
        BigQueryTableOptions::default(),
    )
    .await?;
    assert!(received_queries(&server).await.is_empty());

    // Run 4: the table is no longer declared → DROP TABLE.
    server.reset().await;
    let Some(server) = bigquery_server_on(server).await else {
        return Ok(());
    };
    let app = Environment::builder()
        .db_path(&db)
        .provide_key(&BQ, conn.clone())
        .build()
        .await?
        .app("BigQueryTargetTest")
        .await?;
    app.run(|_ctx| async { Ok(()) }).await?;
    let queries = received_queries(&server).await;
    assert_eq!(
        queries.iter().map(|(q, _)| q.as_str()).collect::<Vec<_>>(),
        vec!["DROP TABLE IF EXISTS `analytics.items`"]
    );
    Ok(())
}

/// Re-mount the standard responder on a server after `reset()`.
async fn bigquery_server_on(server: MockServer) -> Option<MockServer> {
    Mock::given(method("POST"))
        .and(path_regex(r"^/projects/[^/]+/queries$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"jobId": "job_1", "location": "US"},
        })))
        .mount(&server)
        .await;
    Some(server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_user_managed_table_gets_rows_but_no_ddl() -> Result<()> {
    let Some(server) = bigquery_server().await else {
        return Ok(());
    };
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    declare(
        &conn,
        &db,
        vec![item(1, "Alice", &[])],
        BigQueryTableOptions {
            project: Some("other-project".to_string()),
            managed_by: ManagedBy::User,
        },
    )
    .await?;
    let queries = received_queries(&server).await;
    assert_eq!(queries.len(), 1, "{queries:?}");
    assert!(
        queries[0]
            .0
            .starts_with("MERGE `other-project.analytics.items` AS target"),
        "{}",
        queries[0].0
    );

    // Undeclaring a user-managed table drops nothing.
    server.reset().await;
    let Some(server) = bigquery_server_on(server).await else {
        return Ok(());
    };
    let app = Environment::builder()
        .db_path(&db)
        .provide_key(&BQ, conn.clone())
        .build()
        .await?
        .app("BigQueryTargetTest")
        .await?;
    app.run(|_ctx| async { Ok(()) }).await?;
    assert!(received_queries(&server).await.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_query_errors_fail_the_run() -> Result<()> {
    let Some(server) = mock_server().await else {
        return Ok(());
    };
    Mock::given(method("POST"))
        .and(path_regex(r"^/projects/[^/]+/queries$"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": 400, "message": "Syntax error: boom", "status": "INVALID_ARGUMENT"}
        })))
        .mount(&server)
        .await;
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let error = declare(
        &conn,
        &tmp.path().join("db"),
        vec![item(1, "Alice", &[])],
        BigQueryTableOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Syntax error: boom"), "{error}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_query_polls_incomplete_jobs_and_decodes_rows() -> Result<()> {
    let Some(server) = mock_server().await else {
        return Ok(());
    };
    Mock::given(method("POST"))
        .and(path("/projects/demo-project/queries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"jobId": "job_slow", "location": "US"},
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/projects/demo-project/queries/job_slow"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"jobId": "job_slow", "location": "US"},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"},
            ]},
            "rows": [
                {"f": [{"v": "1"}, {"v": "Alice"}]},
                {"f": [{"v": "2"}, {"v": null}]},
            ],
        })))
        .mount(&server)
        .await;
    let conn = connection(&server);
    let rows = conn.query("SELECT id, name FROM t").await?;
    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "name": "Alice"})
                .as_object()
                .unwrap()
                .clone(),
            json!({"id": 2, "name": null}).as_object().unwrap().clone(),
        ]
    );
    let poll = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.method == "GET")
        .unwrap();
    assert_eq!(
        poll.url
            .query_pairs()
            .find(|(k, _)| k == "location")
            .unwrap()
            .1,
        "US"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_service_account_exchanges_a_signed_jwt_for_a_token() -> Result<()> {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};

    let Some(server) = bigquery_server().await else {
        return Ok(());
    };
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "sa-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        })))
        .mount(&server)
        .await;

    let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let key_path = tmp.path().join("sa.json");
    std::fs::write(
        &key_path,
        json!({
            "type": "service_account",
            "project_id": "sa-project",
            "client_email": "svc@sa-project.iam.gserviceaccount.com",
            "private_key": pem.as_str(),
            "token_uri": format!("{}/token", server.uri()),
        })
        .to_string(),
    )
    .unwrap();

    // The project comes from the key file.
    let conn = BigQueryConnection::connect(BigQueryConfig {
        project: None,
        credentials_path: Some(key_path),
        location: None,
    })?
    .with_base_url(server.uri());
    assert_eq!(conn.project(), "sa-project");
    assert_eq!(
        conn.state_id(),
        "bigquery:service_account:svc@sa-project.iam.gserviceaccount.com:sa-project:"
    );

    conn.query("SELECT 1").await?;
    conn.query("SELECT 2").await?;
    let requests = server.received_requests().await.unwrap();
    let token_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path() == "/token")
        .collect();
    assert_eq!(
        token_requests.len(),
        1,
        "the token is cached across queries"
    );
    let form = String::from_utf8(token_requests[0].body.clone()).unwrap();
    assert!(
        form.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"),
        "{form}"
    );
    let assertion = form
        .split('&')
        .find_map(|kv| kv.strip_prefix("assertion="))
        .unwrap();
    assert_eq!(assertion.split('.').count(), 3);
    let queries: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .collect();
    assert_eq!(queries.len(), 2);
    assert_eq!(
        queries[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer sa-token"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Live test
// ---------------------------------------------------------------------------

fn live_config() -> Option<(BigQueryConfig, String)> {
    let project = std::env::var("BIGQUERY_PROJECT")
        .ok()
        .filter(|v| !v.is_empty())?;
    let dataset = std::env::var("BIGQUERY_DATASET")
        .ok()
        .filter(|v| !v.is_empty())?;
    let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
        .map(Into::into);
    Some((
        BigQueryConfig {
            project: Some(project),
            credentials_path,
            location: std::env::var("BIGQUERY_LOCATION")
                .ok()
                .filter(|v| !v.is_empty()),
        },
        dataset,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bigquery_live_upsert_and_delete() -> Result<()> {
    let Some((config, dataset)) = live_config() else {
        eprintln!("skipping live BigQuery test; BIGQUERY_PROJECT / BIGQUERY_DATASET are not set");
        return Ok(());
    };
    let conn = BigQueryConnection::connect(config)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("cocoindex_test_{nonce}");
    let qualified = format!("`{}.{dataset}.{table}`", conn.project());
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    let schema = TableSchema::new(
        [
            ("id", ColumnDef::new("INT64").not_null()),
            ("payload", ColumnDef::new("JSON").parse_json()),
        ],
        ["id"],
    )?;
    let run = |rows: Vec<JsonValue>| {
        let conn = conn.clone();
        let db = db.clone();
        let dataset = dataset.clone();
        let table = table.clone();
        let schema = schema.clone();
        async move {
            let app = Environment::builder()
                .db_path(&db)
                .provide_key(&BQ, conn)
                .build()
                .await?
                .app("BigQueryLiveTest")
                .await?;
            app.run(move |ctx| {
                let rows = rows.clone();
                let dataset = dataset.clone();
                let table = table.clone();
                let schema = schema.clone();
                async move {
                    let target =
                        bigquery::mount_table_target(&ctx, &BQ, dataset, table, schema).await?;
                    for row in &rows {
                        target.declare_row(&ctx, row)?;
                    }
                    Ok(())
                }
            })
            .await?;
            Ok::<_, cocoindex::Error>(())
        }
    };

    let result: Result<()> = async {
        run(vec![
            json!({"id": 1, "payload": {"v": "one"}}),
            json!({"id": 2, "payload": {"v": "two"}}),
        ])
        .await?;
        run(vec![json!({"id": 1, "payload": {"v": "one-updated"}})]).await?;
        let rows = conn
            .query(&format!(
                "SELECT `id`, JSON_VALUE(`payload`, '$.v') AS v FROM {qualified} ORDER BY `id`"
            ))
            .await?;
        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "v": "one-updated"})
                    .as_object()
                    .unwrap()
                    .clone()
            ]
        );
        Ok(())
    }
    .await;
    let _ = conn
        .query(&format!("DROP TABLE IF EXISTS {qualified}"))
        .await;
    result
}
