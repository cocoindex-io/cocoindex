//! Tests for the `snowflake` table target.
//!
//! The reconcile lifecycle runs against a `wiremock` stand-in for Snowflake's
//! session / query-request HTTP protocol (login, statements, session close are
//! recorded and answered as successful), so no Snowflake account is needed:
//! database + schema + table DDL on first run, batched `MERGE INTO` for
//! declared rows, `DELETE` for un-declared ones, the no-change skip, `DROP
//! TABLE` when the table is no longer declared, the user-managed mode, the
//! session token on every statement, and the warehouse / role login options.
//!
//! The live test at the end hits a real account and skips unless
//! `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
//! `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` are set
//! (`SNOWFLAKE_ROLE` is optional):
//!   SNOWFLAKE_ACCOUNT=org-acct SNOWFLAKE_USER=me SNOWFLAKE_PASSWORD=... \
//!     SNOWFLAKE_WAREHOUSE=wh SNOWFLAKE_DATABASE=db SNOWFLAKE_SCHEMA=public \
//!     cargo test -p cocoindex --features snowflake --test snowflake_target
#![cfg(feature = "snowflake")]

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::connectors::snowflake::{
    self, ColumnDef, SnowflakeConfig, SnowflakeConnection, SnowflakeTableOptions, TableSchema,
};
use cocoindex::statediff::ManagedBy;
use cocoindex::{Environment, Result};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

cocoindex::context_key!(static SF: SnowflakeConnection, key = "snowflake_target_test");

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

/// Mount a mock Snowflake that logs any user in and answers every statement
/// with an empty successful result.
async fn mount_snowflake(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/session/v1/login-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "message": null,
            "code": null,
            "data": {"token": "session-token", "masterToken": "master-token"},
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/queries/v1/query-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "message": null,
            "code": null,
            "data": {"rowtype": [], "rowset": [], "queryId": "q1"},
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/session"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
        .mount(server)
        .await;
}

async fn snowflake_server() -> Option<MockServer> {
    let server = mock_server().await?;
    mount_snowflake(&server).await;
    Some(server)
}

fn connection(server: &MockServer) -> SnowflakeConnection {
    SnowflakeConnection::new(
        SnowflakeConfig::new("org-acct", "me", "hunter2")
            .warehouse("COMPUTE_WH")
            .role("SYSADMIN"),
    )
    .with_base_url(server.uri())
}

/// Every statement the server received so far, in order, with its bindings.
async fn received_statements(server: &MockServer) -> Vec<(String, JsonValue)> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path() == "/queries/v1/query-request")
        .map(|r| {
            let body: JsonValue = serde_json::from_slice(&r.body).unwrap();
            (
                body["sqlText"].as_str().unwrap().to_string(),
                body.get("bindings").cloned().unwrap_or(JsonValue::Null),
            )
        })
        .collect()
}

async fn count_requests(server: &MockServer, request_path: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path() == request_path)
        .count()
}

fn options() -> SnowflakeTableOptions {
    SnowflakeTableOptions {
        database: Some("ANALYTICS".to_string()),
        schema: Some("PUBLIC".to_string()),
        managed_by: ManagedBy::System,
    }
}

async fn declare(
    conn: &SnowflakeConnection,
    db_path: &Path,
    rows: Vec<Item>,
    options: SnowflakeTableOptions,
) -> Result<()> {
    let app = Environment::builder()
        .db_path(db_path)
        .provide_key(&SF, conn.clone())
        .build()
        .await?
        .app("SnowflakeTargetTest")
        .await?;
    app.run(move |ctx| {
        let rows = rows.clone();
        let options = options.clone();
        async move {
            let schema = TableSchema::from_row::<Item>(["id"])?;
            let target =
                snowflake::mount_table_target_with_options(&ctx, &SF, "items", schema, options)
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

async fn run_empty(conn: &SnowflakeConnection, db_path: &Path) -> Result<()> {
    let app = Environment::builder()
        .db_path(db_path)
        .provide_key(&SF, conn.clone())
        .build()
        .await?
        .app("SnowflakeTargetTest")
        .await?;
    app.run(|_ctx| async { Ok(()) }).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_reconciles_table_and_rows_over_http() -> Result<()> {
    let Some(server) = snowflake_server().await else {
        return Ok(());
    };
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    // Run 1: database + schema + table DDL, then one MERGE carrying both rows.
    declare(
        &conn,
        &db,
        vec![item(1, "Alice", &["a"]), item(2, "Bob", &[])],
        options(),
    )
    .await?;
    let statements = received_statements(&server).await;
    assert_eq!(statements.len(), 4, "{statements:?}");
    assert_eq!(
        statements[0].0,
        "CREATE DATABASE IF NOT EXISTS \"ANALYTICS\""
    );
    assert_eq!(
        statements[1].0,
        "CREATE SCHEMA IF NOT EXISTS \"ANALYTICS\".\"PUBLIC\""
    );
    assert_eq!(
        statements[2].0,
        "CREATE TABLE \"ANALYTICS\".\"PUBLIC\".\"items\" (\"id\" NUMBER NOT NULL, \"name\" \
         VARCHAR NOT NULL, \"tags\" VARIANT NOT NULL, PRIMARY KEY (\"id\"))"
    );
    let (merge, bindings) = &statements[3];
    assert!(
        merge.starts_with(
            "MERGE INTO \"ANALYTICS\".\"PUBLIC\".\"items\" AS target USING (SELECT ? AS \"id\", \
             ? AS \"name\", PARSE_JSON(?) AS \"tags\" UNION ALL SELECT ? AS \"id\", ? AS \
             \"name\", PARSE_JSON(?) AS \"tags\") AS source ON target.\"id\" = source.\"id\""
        ),
        "{merge}"
    );
    assert_eq!(bindings["1"], json!({"type": "FIXED", "value": "1"}));
    assert_eq!(bindings["2"], json!({"type": "TEXT", "value": "Alice"}));
    assert_eq!(bindings["3"], json!({"type": "TEXT", "value": "[\"a\"]"}));
    assert_eq!(bindings["6"], json!({"type": "TEXT", "value": "[]"}));

    // Login carried the warehouse and role, the password only in the body, and
    // every statement the session token; the DDL session and the row session
    // were both closed.
    let requests = server.received_requests().await.unwrap();
    let logins: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path() == "/session/v1/login-request")
        .collect();
    assert_eq!(logins.len(), 2, "one session for DDL, one for rows");
    let login_query: Vec<(String, String)> = logins[0]
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(
        login_query.contains(&("warehouse".to_string(), "COMPUTE_WH".to_string())),
        "{login_query:?}"
    );
    assert!(
        login_query.contains(&("roleName".to_string(), "SYSADMIN".to_string())),
        "{login_query:?}"
    );
    let login_body: JsonValue = serde_json::from_slice(&logins[0].body).unwrap();
    assert_eq!(login_body["data"]["ACCOUNT_NAME"], json!("org-acct"));
    assert_eq!(login_body["data"]["LOGIN_NAME"], json!("me"));
    assert_eq!(login_body["data"]["PASSWORD"], json!("hunter2"));
    for request in requests
        .iter()
        .filter(|r| r.url.path() != "/session/v1/login-request")
    {
        assert_eq!(
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Snowflake Token=\"session-token\""
        );
    }
    assert_eq!(count_requests(&server, "/session").await, 2);

    // Run 2: row 1 changed, row 2 gone → one MERGE (row 1 only) + one DELETE.
    server.reset().await;
    mount_snowflake(&server).await;
    declare(&conn, &db, vec![item(1, "Alice2", &["a", "b"])], options()).await?;
    let statements = received_statements(&server).await;
    assert_eq!(statements.len(), 2, "{statements:?}");
    assert!(
        statements[0]
            .0
            .starts_with("MERGE INTO \"ANALYTICS\".\"PUBLIC\".\"items\""),
        "{}",
        statements[0].0
    );
    assert_eq!(
        statements[0].1["2"],
        json!({"type": "TEXT", "value": "Alice2"})
    );
    assert!(statements[0].1.get("4").is_none());
    assert_eq!(
        statements[1].0,
        "DELETE FROM \"ANALYTICS\".\"PUBLIC\".\"items\" WHERE \"id\" IN (?)"
    );
    assert_eq!(statements[1].1["1"], json!({"type": "FIXED", "value": "2"}));

    // Run 3: identical rows → no session is even opened.
    server.reset().await;
    mount_snowflake(&server).await;
    declare(&conn, &db, vec![item(1, "Alice2", &["a", "b"])], options()).await?;
    assert!(server.received_requests().await.unwrap().is_empty());

    // Run 4: the table is no longer declared → DROP TABLE.
    server.reset().await;
    mount_snowflake(&server).await;
    run_empty(&conn, &db).await?;
    assert_eq!(
        received_statements(&server)
            .await
            .iter()
            .map(|(s, _)| s.as_str())
            .collect::<Vec<_>>(),
        vec!["DROP TABLE IF EXISTS \"ANALYTICS\".\"PUBLIC\".\"items\""]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_user_managed_table_gets_rows_but_no_ddl() -> Result<()> {
    let Some(server) = snowflake_server().await else {
        return Ok(());
    };
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    // No database / schema: the table name stands alone.
    declare(
        &conn,
        &db,
        vec![item(1, "Alice", &[])],
        SnowflakeTableOptions {
            database: None,
            schema: None,
            managed_by: ManagedBy::User,
        },
    )
    .await?;
    let statements = received_statements(&server).await;
    assert_eq!(statements.len(), 1, "{statements:?}");
    assert!(
        statements[0]
            .0
            .starts_with("MERGE INTO \"items\" AS target"),
        "{}",
        statements[0].0
    );

    server.reset().await;
    mount_snowflake(&server).await;
    run_empty(&conn, &db).await?;
    assert!(received_statements(&server).await.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_failed_statement_fails_the_run_and_closes_the_session() -> Result<()> {
    let Some(server) = mock_server().await else {
        return Ok(());
    };
    mount_snowflake(&server).await;
    // Override the statement responder with a failure.
    Mock::given(method("POST"))
        .and(path("/queries/v1/query-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": false,
            "message": "SQL compilation error: Database 'ANALYTICS' does not exist",
            "code": "002003",
            "data": null,
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let error = declare(
        &conn,
        &tmp.path().join("db"),
        vec![item(1, "Alice", &[])],
        options(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("002003"), "{error}");
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert_eq!(
        count_requests(&server, "/session").await,
        1,
        "the session is closed on failure"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_login_failure_is_reported() -> Result<()> {
    let Some(server) = mock_server().await else {
        return Ok(());
    };
    Mock::given(method("POST"))
        .and(path("/session/v1/login-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": false,
            "message": "Incorrect username or password was specified.",
            "code": "390100",
        })))
        .mount(&server)
        .await;
    let error = connection(&server).query("SELECT 1").await.unwrap_err();
    assert!(error.to_string().contains("390100"), "{error}");
    assert!(error.to_string().contains("Incorrect username"), "{error}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_query_polls_in_progress_results_and_decodes_rows() -> Result<()> {
    let Some(server) = mock_server().await else {
        return Ok(());
    };
    mount_snowflake(&server).await;
    Mock::given(method("POST"))
        .and(path("/queries/v1/query-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "code": "333334",
            "message": "Query in progress",
            "data": {"queryId": "q_slow", "getResultUrl": "/queries/q_slow/result"},
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/queries/q_slow/result"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": true,
            "code": null,
            "data": {
                "rowtype": [
                    {"name": "id", "type": "fixed"},
                    {"name": "name", "type": "text"},
                    {"name": "ok", "type": "boolean"},
                ],
                "rowset": [["1", "Alice", "1"], ["2", null, "0"]],
            },
        })))
        .mount(&server)
        .await;
    let rows = connection(&server)
        .query("SELECT id, name, ok FROM t")
        .await?;
    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "name": "Alice", "ok": true})
                .as_object()
                .unwrap()
                .clone(),
            json!({"id": 2, "name": null, "ok": false})
                .as_object()
                .unwrap()
                .clone(),
        ]
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Live test
// ---------------------------------------------------------------------------

struct LiveEnv {
    config: SnowflakeConfig,
    database: String,
    schema: String,
}

fn live_env() -> Option<LiveEnv> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let mut config = SnowflakeConfig::new(
        var("SNOWFLAKE_ACCOUNT")?,
        var("SNOWFLAKE_USER")?,
        var("SNOWFLAKE_PASSWORD")?,
    )
    .warehouse(var("SNOWFLAKE_WAREHOUSE")?);
    if let Some(role) = var("SNOWFLAKE_ROLE") {
        config = config.role(role);
    }
    Some(LiveEnv {
        config,
        database: var("SNOWFLAKE_DATABASE")?,
        schema: var("SNOWFLAKE_SCHEMA")?,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_live_upsert_and_delete() -> Result<()> {
    let Some(env) = live_env() else {
        eprintln!("skipping live Snowflake test; SNOWFLAKE_* are not set");
        return Ok(());
    };
    let conn = SnowflakeConnection::new(env.config);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("COCOINDEX_TEST_{nonce}");
    let qualified = format!("\"{}\".\"{}\".\"{table}\"", env.database, env.schema);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    let schema = TableSchema::new(
        [
            ("id", ColumnDef::new("NUMBER").not_null()),
            ("payload", ColumnDef::new("VARIANT").parse_json()),
        ],
        ["id"],
    )?;
    let options = SnowflakeTableOptions {
        database: Some(env.database.clone()),
        schema: Some(env.schema.clone()),
        managed_by: ManagedBy::System,
    };
    let run = |rows: Vec<JsonValue>| {
        let conn = conn.clone();
        let db = db.clone();
        let table = table.clone();
        let schema = schema.clone();
        let options = options.clone();
        async move {
            let app = Environment::builder()
                .db_path(&db)
                .provide_key(&SF, conn)
                .build()
                .await?
                .app("SnowflakeLiveTest")
                .await?;
            app.run(move |ctx| {
                let rows = rows.clone();
                let table = table.clone();
                let schema = schema.clone();
                let options = options.clone();
                async move {
                    let target = snowflake::mount_table_target_with_options(
                        &ctx, &SF, table, schema, options,
                    )
                    .await?;
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
                "SELECT \"id\", \"payload\":v::string AS v FROM {qualified} ORDER BY \"id\""
            ))
            .await?;
        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "V": "one-updated"})
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

/// Rows per `MERGE` (bindings / 3 columns) of every `MERGE` the server got.
async fn merge_row_counts(server: &MockServer) -> Vec<usize> {
    received_statements(server)
        .await
        .iter()
        .filter(|(sql, _)| sql.starts_with("MERGE"))
        .map(|(_, bindings)| bindings.as_object().map_or(0, |b| b.len()) / 3)
        .collect()
}

/// Rows go in `MERGE` statements of at most 4,000 bindings: 1,333 rows of 3
/// columns per statement. A statement the server rejects mid-batch fails the
/// run, and the next run sends every row again, so nothing is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snowflake_batches_follow_the_binding_limit_and_a_failure_is_retried() -> Result<()> {
    let items = |n: i64| -> Vec<Item> { (0..n).map(|id| item(id, "x", &[])).collect() };
    for (n, expected) in [(1, vec![1]), (1333, vec![1333]), (1334, vec![1333, 1])] {
        let Some(server) = snowflake_server().await else {
            return Ok(());
        };
        let tmp = tempfile::tempdir().unwrap();
        declare(
            &connection(&server),
            &tmp.path().join("db"),
            items(n),
            options(),
        )
        .await?;
        let mut counts = merge_row_counts(&server).await;
        counts.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(counts, expected, "{n} rows");
    }

    let Some(server) = mock_server().await else {
        return Ok(());
    };
    // The first MERGE fails; everything else succeeds.
    Mock::given(method("POST"))
        .and(path("/queries/v1/query-request"))
        .and(wiremock::matchers::body_string_contains("MERGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "success": false,
            "message": "boom",
            "code": "100038",
            "data": null,
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_snowflake(&server).await;
    let conn = connection(&server);
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");
    let err = declare(&conn, &db, items(3000), options())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("boom"), "{err}");
    server.reset().await;
    mount_snowflake(&server).await;
    declare(&conn, &db, items(3000), options()).await?;
    let sent: usize = merge_row_counts(&server).await.iter().sum();
    assert_eq!(
        sent, 3000,
        "the next run sends every row of the failed batch"
    );
    Ok(())
}
