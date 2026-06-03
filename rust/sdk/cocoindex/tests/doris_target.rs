//! Live-Doris integration tests for the `doris` table target.
//!
//! Skips gracefully when `DORIS_FE_HOST` is unset. Run with a Doris cluster on
//! localhost (e.g. the `apache/doris:doris-all-in-one-2.1.0` container) and a
//! pre-created database:
//!   DORIS_FE_HOST=localhost DORIS_HTTP_PORT=8030 DORIS_QUERY_PORT=9030 \
//!     DORIS_DATABASE=cocoindex_test \
//!     cargo test -p cocoindex --features doris --test doris_target
//!
//! These exercise the full reconcile lifecycle against a real cluster: table
//! DDL (DUPLICATE KEY model), Stream Load row ingestion (with the FE→BE 307
//! redirect), delete-before-insert on update, SQL deletes for undeclared rows,
//! the no-change skip (which must not duplicate rows), and an inverted index.
//!
//! Note: `USING ANN` vector indexes require Doris 3.x; the all-in-one 2.1 image
//! rejects that syntax, so the vector-index DDL is covered by a `src/doris.rs`
//! unit test rather than live here.
#![cfg(feature = "doris")]

use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::doris::{
    self, ColumnDef, DorisConfig, DorisConnection, DorisTableOptions, InvertedIndexDef, TableSchema,
};
use cocoindex::{App, Result};
use serde::Serialize;
use sqlx::Row;

#[derive(Serialize, Clone)]
struct Item {
    id: String,
    name: String,
    value: i64,
}

fn config() -> Option<DorisConfig> {
    let fe_host = std::env::var("DORIS_FE_HOST").ok()?;
    let database = std::env::var("DORIS_DATABASE").unwrap_or_else(|_| "cocoindex_test".to_string());
    let http_port = std::env::var("DORIS_HTTP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8030);
    let query_port = std::env::var("DORIS_QUERY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9030);
    let mut cfg = DorisConfig::new(fe_host, database)
        .fe_http_port(http_port)
        .query_port(query_port);
    if let Ok(user) = std::env::var("DORIS_USERNAME") {
        cfg = cfg.username(user);
    }
    if let Ok(pw) = std::env::var("DORIS_PASSWORD") {
        cfg = cfg.password(pw);
    }
    Some(cfg)
}

fn unique_table(label: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("coco_doris_{label}_{nonce}")
}

fn item_schema() -> TableSchema {
    TableSchema::new(
        [
            ("id", ColumnDef::new("TEXT")),
            ("name", ColumnDef::new("TEXT")),
            ("value", ColumnDef::new("BIGINT")),
        ],
        ["id"],
    )
    .unwrap()
}

/// Declare `rows` on `table` through a fresh App that reuses `db_path`, so
/// successive calls reconcile against the previous run (enabling update/delete).
async fn declare(
    conn: &DorisConnection,
    db_path: &std::path::Path,
    table: &str,
    rows: Vec<Item>,
) -> Result<()> {
    let conn = conn.clone();
    let table = table.to_string();
    let app = App::builder("DorisTargetTest")
        .db_path(db_path)
        .build()
        .await?;
    app.run(move |ctx| {
        let conn = conn.clone();
        let table = table.clone();
        let rows = rows.clone();
        async move {
            let target = doris::mount_table_target(&ctx, &conn, table, item_schema()).await?;
            for row in &rows {
                target.declare_row(&ctx, row)?;
            }
            Ok(())
        }
    })
    .await?;
    Ok(())
}

async fn fetch_items(conn: &DorisConnection, table: &str) -> Vec<(String, String, i64)> {
    let sql = format!(
        "SELECT id, name, value FROM `{}`.`{}` ORDER BY id",
        conn.config().database,
        table
    );
    // Doris only prepares point-query SELECTs; use the unprepared text protocol.
    let rows = sqlx::raw_sql(&sql).fetch_all(conn.pool()).await.unwrap();
    rows.into_iter()
        .map(|r| {
            (
                r.get::<String, _>("id"),
                r.get::<String, _>("name"),
                r.get::<i64, _>("value"),
            )
        })
        .collect()
}

async fn drop_table(conn: &DorisConnection, table: &str) {
    let sql = format!(
        "DROP TABLE IF EXISTS `{}`.`{}`",
        conn.config().database,
        table
    );
    let _ = sqlx::raw_sql(&sql).execute(conn.pool()).await;
}

fn item(id: &str, name: &str, value: i64) -> Item {
    Item {
        id: id.to_string(),
        name: name.to_string(),
        value,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_create_table_and_insert_rows() -> Result<()> {
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("insert");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    declare(
        &conn,
        &db,
        &table,
        vec![item("1", "Alice", 10), item("2", "Bob", 20)],
    )
    .await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![
            ("1".into(), "Alice".into(), 10),
            ("2".into(), "Bob".into(), 20)
        ]
    );

    // Add a third row in a second run.
    declare(
        &conn,
        &db,
        &table,
        vec![
            item("1", "Alice", 10),
            item("2", "Bob", 20),
            item("3", "Carol", 30),
        ],
    )
    .await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![
            ("1".into(), "Alice".into(), 10),
            ("2".into(), "Bob".into(), 20),
            ("3".into(), "Carol".into(), 30)
        ]
    );

    drop_table(&conn, &table).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_update_row() -> Result<()> {
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("update");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    declare(
        &conn,
        &db,
        &table,
        vec![item("1", "Alice", 10), item("2", "Bob", 20)],
    )
    .await?;
    // Update row 1 (name + value). Delete-before-insert must replace, not append.
    declare(
        &conn,
        &db,
        &table,
        vec![item("1", "Alice2", 99), item("2", "Bob", 20)],
    )
    .await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![
            ("1".into(), "Alice2".into(), 99),
            ("2".into(), "Bob".into(), 20)
        ],
        "update must replace the row in the DUPLICATE KEY model, not duplicate it"
    );

    drop_table(&conn, &table).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_delete_row() -> Result<()> {
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("delete");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    declare(
        &conn,
        &db,
        &table,
        vec![
            item("1", "Alice", 10),
            item("2", "Bob", 20),
            item("3", "Carol", 30),
        ],
    )
    .await?;
    // Undeclare row 2 — reconcile should DELETE it.
    declare(
        &conn,
        &db,
        &table,
        vec![item("1", "Alice", 10), item("3", "Carol", 30)],
    )
    .await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![
            ("1".into(), "Alice".into(), 10),
            ("3".into(), "Carol".into(), 30)
        ]
    );

    drop_table(&conn, &table).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_value_map_rows() -> Result<()> {
    // Rows declared as a serde_json map (the dict-rows analogue).
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("maprows");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    let conn2 = conn.clone();
    let table2 = table.clone();
    let app = App::builder("DorisMapRows").db_path(&db).build().await?;
    app.run(move |ctx| {
        let conn = conn2.clone();
        let table = table2.clone();
        async move {
            let target = doris::mount_table_target(&ctx, &conn, table, item_schema()).await?;
            target.declare_row(
                &ctx,
                &serde_json::json!({"id": "1", "name": "Zoe", "value": 7}),
            )?;
            target.declare_row(
                &ctx,
                &serde_json::json!({"id": "2", "name": "Yan", "value": 8}),
            )?;
            Ok(())
        }
    })
    .await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![("1".into(), "Zoe".into(), 7), ("2".into(), "Yan".into(), 8)]
    );

    drop_table(&conn, &table).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_no_change_does_not_duplicate() -> Result<()> {
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("nochange");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    let rows = vec![item("1", "Alice", 10), item("2", "Bob", 20)];
    declare(&conn, &db, &table, rows.clone()).await?;
    // Re-run with identical rows: the row handler should skip unchanged rows,
    // and even if it re-loaded, delete-before-insert must keep exactly one copy.
    declare(&conn, &db, &table, rows).await?;
    assert_eq!(
        fetch_items(&conn, &table).await,
        vec![
            ("1".into(), "Alice".into(), 10),
            ("2".into(), "Bob".into(), 20)
        ],
        "re-running with identical rows must not duplicate rows"
    );

    drop_table(&conn, &table).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doris_inverted_index_table() -> Result<()> {
    let Some(cfg) = config() else {
        eprintln!("skipping live Doris test; DORIS_FE_HOST is not set");
        return Ok(());
    };
    let conn = DorisConnection::connect(cfg).await?;
    let table = unique_table("inv");
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("db");

    let schema = TableSchema::new(
        [
            ("id", ColumnDef::new("TEXT")),
            ("content", ColumnDef::new("TEXT")),
        ],
        ["id"],
    )
    .unwrap();
    let options = DorisTableOptions {
        inverted_indexes: vec![InvertedIndexDef::new("content").parser("english")],
        ..Default::default()
    };

    let conn2 = conn.clone();
    let table2 = table.clone();
    let app = App::builder("DorisInvertedIndex")
        .db_path(&db)
        .build()
        .await?;
    app.run(move |ctx| {
        let conn = conn2.clone();
        let table = table2.clone();
        let schema = schema.clone();
        let options = options.clone();
        async move {
            let target =
                doris::mount_table_target_with_options(&ctx, &conn, table, schema, options).await?;
            target.declare_row(
                &ctx,
                &serde_json::json!({"id": "1", "content": "the quick brown fox"}),
            )?;
            Ok(())
        }
    })
    .await?;

    // The created table carries the inverted index, and the row is loaded.
    let show_row = sqlx::raw_sql(&format!(
        "SHOW CREATE TABLE `{}`.`{}`",
        conn.config().database,
        table
    ))
    .fetch_one(conn.pool())
    .await
    .unwrap();
    let ddl: String = show_row.get::<String, _>(1);
    assert!(
        ddl.to_uppercase().contains("USING INVERTED"),
        "expected an INVERTED index in DDL: {ddl}"
    );

    let count_row = sqlx::raw_sql(&format!(
        "SELECT COUNT(*) AS c FROM `{}`.`{}`",
        conn.config().database,
        table
    ))
    .fetch_one(conn.pool())
    .await
    .unwrap();
    assert_eq!(count_row.get::<i64, _>("c"), 1);

    drop_table(&conn, &table).await;
    Ok(())
}
