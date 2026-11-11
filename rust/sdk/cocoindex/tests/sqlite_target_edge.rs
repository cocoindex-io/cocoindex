//! Edge cases of the SQLite table target: schema evolution, identifiers, value
//! edge cases, batch boundaries, atomic batches, a locked database, tables
//! changed by someone else, `drop_state`, and `vec0` tables when the
//! `sqlite-vec` library is available (`SQLITE_VEC_EXTENSION`).
#![cfg(feature = "sqlite")]

use cocoindex::connectors::sqlite;
use cocoindex::{App, Environment, Result};
use serde_json::{Value, json};
use sqlx::Row as _;

cocoindex::context_key!(
    static DB: sqlite::Database, key = "sqlite_target_edge_test_db"
);

mod common;

async fn temp_db() -> (sqlite::Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target.db");
    let db = sqlite::Database::connect(path.to_str().unwrap())
        .await
        .unwrap();
    (db, dir)
}

async fn build_app(db: &sqlite::Database) -> (App, tempfile::TempDir) {
    common::record_sql_statements();
    let dir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide_key(&DB, db.clone())
        .build()
        .await
        .unwrap()
        .app("SqliteEdgeTest")
        .await
        .unwrap();
    (app, dir)
}

/// A column list, each `(name, type, nullable)`.
type Cols = Vec<(&'static str, &'static str, bool)>;

fn schema_of(cols: &Cols, pk: &[&str]) -> Result<sqlite::TableSchema> {
    sqlite::TableSchema::new(
        cols.iter().map(|(name, ty, nullable)| {
            let def = sqlite::ColumnDef::new(*ty);
            (*name, if *nullable { def } else { def.not_null() })
        }),
        pk.iter().copied(),
    )
}

async fn run_rows_with(
    app: &App,
    table: &'static str,
    cols: Cols,
    pk: &'static [&'static str],
    options: sqlite::SqliteTableOptions,
    rows: Vec<Value>,
) -> Result<()> {
    app.run(move |ctx| async move {
        let target = sqlite::mount_table_target_with_options(
            &ctx,
            &DB,
            table,
            schema_of(&cols, pk)?,
            options,
        )
        .await?;
        for row in &rows {
            target.declare_row(&ctx, row)?;
        }
        Ok(())
    })
    .await?;
    Ok(())
}

async fn run_rows(
    app: &App,
    table: &'static str,
    cols: Cols,
    pk: &'static [&'static str],
    rows: Vec<Value>,
) -> Result<()> {
    run_rows_with(app, table, cols, pk, Default::default(), rows).await
}

/// Every row of `table` as a JSON object, ordered by `order_by`.
async fn rows_json(db: &sqlite::Database, table: &str, order_by: &str) -> Vec<Value> {
    let rows = sqlx::query(&format!("SELECT * FROM {table} ORDER BY {order_by}"))
        .fetch_all(db.pool())
        .await
        .unwrap();
    rows.iter()
        .map(|row| {
            use sqlx::{Column as _, TypeInfo as _, ValueRef as _};
            let mut map = serde_json::Map::new();
            for (i, col) in row.columns().iter().enumerate() {
                let raw = row.try_get_raw(i).unwrap();
                let value = if raw.is_null() {
                    Value::Null
                } else {
                    match raw.type_info().name() {
                        "INTEGER" => json!(row.get::<i64, _>(i)),
                        "REAL" => json!(row.get::<f64, _>(i)),
                        "BLOB" => json!(row.get::<Vec<u8>, _>(i)),
                        _ => json!(row.get::<String, _>(i)),
                    }
                };
                map.insert(col.name().to_string(), value);
            }
            Value::Object(map)
        })
        .collect()
}

async fn count(db: &sqlite::Database, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(db.pool())
        .await
        .unwrap()
}

/// Making a column required and back keeps the rows: SQLite has no `ALTER
/// COLUMN`, so the column is dropped and added again and the rows rewritten,
/// as in Python.
#[tokio::test]
async fn nullable_column_becomes_required_and_back() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let nullable: Cols = vec![("id", "INTEGER", false), ("note", "TEXT", true)];
    let required: Cols = vec![("id", "INTEGER", false), ("note", "TEXT", false)];
    let rows = vec![json!({"id": 1, "note": "a"})];
    run_rows(&app, "t", nullable.clone(), &["id"], rows.clone())
        .await
        .unwrap();
    run_rows(&app, "t", required, &["id"], rows).await.unwrap();
    assert_eq!(
        rows_json(&db, "t", "id").await,
        vec![json!({"id": 1, "note": "a"})]
    );
    run_rows(
        &app,
        "t",
        nullable,
        &["id"],
        vec![
            json!({"id": 1, "note": "a"}),
            json!({"id": 2, "note": null}),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        rows_json(&db, "t", "id").await,
        vec![
            json!({"id": 1, "note": "a"}),
            json!({"id": 2, "note": null})
        ]
    );
}

/// A renamed column is dropped and the new one added and filled.
#[tokio::test]
async fn renamed_and_retyped_columns_are_repopulated() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    run_rows(
        &app,
        "t",
        vec![
            ("id", "INTEGER", false),
            ("a", "TEXT", true),
            ("n", "INTEGER", true),
        ],
        &["id"],
        vec![json!({"id": 1, "a": "v", "n": 5})],
    )
    .await
    .unwrap();
    run_rows(
        &app,
        "t",
        vec![
            ("id", "INTEGER", false),
            ("b", "TEXT", true),
            ("n", "REAL", true),
        ],
        &["id"],
        vec![json!({"id": 1, "b": "v", "n": 5.5})],
    )
    .await
    .unwrap();
    assert_eq!(
        rows_json(&db, "t", "id").await,
        vec![json!({"id": 1, "b": "v", "n": 5.5})]
    );
}

/// A primary-key change rebuilds the table with the rows rewritten.
#[tokio::test]
async fn primary_key_change_rebuilds_the_table() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let cols: Cols = vec![
        ("a", "TEXT", false),
        ("b", "INTEGER", false),
        ("v", "TEXT", true),
    ];
    let rows = vec![
        json!({"a": "x", "b": 1, "v": "one"}),
        json!({"a": "x", "b": 2, "v": "two"}),
    ];
    run_rows(&app, "t", cols.clone(), &["a"], vec![rows[0].clone()])
        .await
        .unwrap();
    run_rows(&app, "t", cols, &["a", "b"], rows.clone())
        .await
        .unwrap();
    assert_eq!(rows_json(&db, "t", "a, b").await, rows);
}

/// Reserved words and mixed case are quoted in DDL and DML.
#[tokio::test]
async fn reserved_and_mixed_case_identifiers() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let cols: Cols = vec![
        ("select", "INTEGER", false),
        ("Order", "TEXT", false),
        ("group", "TEXT", true),
    ];
    run_rows(
        &app,
        "Table",
        cols.clone(),
        &["select", "Order"],
        vec![
            json!({"select": 1, "Order": "x", "group": "g"}),
            json!({"select": 2, "Order": "y"}),
        ],
    )
    .await
    .unwrap();
    run_rows(
        &app,
        "Table",
        cols,
        &["select", "Order"],
        vec![json!({"select": 2, "Order": "y", "group": "h"})],
    )
    .await
    .unwrap();
    assert_eq!(
        rows_json(&db, "\"Table\"", "\"select\"").await,
        vec![json!({"select": 2, "Order": "y", "group": "h"})]
    );
    for bad in ["", "a\"b", "a b", "1x", "名前", "a;b"] {
        assert!(
            schema_of(&vec![(bad, "TEXT", false)], &[bad]).is_err(),
            "{bad:?}"
        );
    }
}

#[tokio::test]
async fn edge_values_round_trip() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let cols: Cols = vec![
        ("id", "INTEGER", false),
        ("t", "TEXT", true),
        ("i", "INTEGER", true),
        ("f", "REAL", true),
        ("b", "BLOB", true),
        ("j", "JSON", true),
    ];
    let huge = "y".repeat(4 * 1024 * 1024);
    let rows = vec![
        json!({"id": 1, "t": "", "i": i64::MAX, "f": -0.0, "b": [], "j": {"a": [1, {"b": null}]}}),
        json!({"id": 2, "t": "nul\u{0}inside ünïcödé 🎉", "i": i64::MIN, "f": 1e308, "b": [0, 255]}),
        json!({"id": 3, "t": huge.clone(), "b": vec![9u8; 1 << 20]}),
    ];
    run_rows(&app, "t", cols.clone(), &["id"], rows)
        .await
        .unwrap();
    let got = rows_json(&db, "t", "id").await;
    assert_eq!(got[0]["t"], json!(""));
    assert_eq!(got[0]["i"], json!(i64::MAX));
    assert_eq!(got[0]["b"], json!([]));
    assert_eq!(got[0]["j"], json!("{\"a\":[1,{\"b\":null}]}"));
    assert_eq!(got[1]["t"], json!("nul\u{0}inside ünïcödé 🎉"));
    assert_eq!(got[1]["i"], json!(i64::MIN));
    assert_eq!(got[2]["t"].as_str().map(str::len), Some(huge.len()));
    assert_eq!(got[2]["b"].as_array().map(Vec::len), Some(1 << 20));

    for (row, expected) in [
        (json!({"id": 4, "i": u64::MAX}), "integer column"),
        (json!({"id": 4, "i": 1.5}), "integer column"),
        (json!({"id": 4, "f": "x"}), "real column"),
        (json!({"id": 4, "b": [256]}), "BLOB array"),
        (json!({"id": null}), "null"),
    ] {
        let err = run_rows(&app, "t", cols.clone(), &["id"], vec![row.clone()])
            .await
            .unwrap_err();
        assert!(err.to_string().contains(expected), "{row}: {err}");
    }
}

#[derive(serde::Serialize)]
struct FloatRow {
    id: i64,
    f: f64,
}

#[tokio::test]
async fn non_finite_floats_are_rejected() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = app
            .run(move |ctx| async move {
                let cols: Cols = vec![("id", "INTEGER", false), ("f", "REAL", true)];
                let t =
                    sqlite::mount_table_target(&ctx, &DB, "t", schema_of(&cols, &["id"])?).await?;
                t.declare_row(&ctx, &FloatRow { id: 1, f: value })
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{err}");
    }
}

/// A two-column table fits 16,383 rows in one statement (the 32,766-parameter
/// bind limit): 16,383 rows → 1 statement, 16,384 → 2.
#[tokio::test]
async fn batch_boundaries_follow_the_bind_limit() {
    let cols: Cols = vec![("id", "INTEGER", false), ("v", "TEXT", true)];
    for (n, statements) in [(1usize, 1usize), (16_383, 1), (16_384, 2)] {
        let (db, _d) = temp_db().await;
        let (app, _a) = build_app(&db).await;
        let table: &'static str = Box::leak(format!("b{n}").into_boxed_str());
        let rows = (0..n as i64)
            .map(|id| json!({"id": id, "v": "x"}))
            .collect();
        run_rows(&app, table, cols.clone(), &["id"], rows)
            .await
            .unwrap();
        assert_eq!(count(&db, table).await, n as i64);
        let inserts = common::sql_statements_containing(&format!("INSERT INTO \"{table}\" "));
        assert_eq!(inserts.len(), statements, "{n} rows");
    }
}

/// A batch is one transaction: a row the database rejects (here a CHECK
/// constraint on a user-managed table) rolls back the whole batch, and the
/// next run with the row fixed writes all of them.
#[tokio::test]
async fn a_batch_is_atomic_and_the_next_run_repairs() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, v INTEGER CHECK (v < 1000))")
        .execute(db.pool())
        .await
        .unwrap();
    let cols: Cols = vec![("id", "INTEGER", false), ("v", "INTEGER", true)];
    let user = || sqlite::SqliteTableOptions {
        managed_by: cocoindex::ManagedBy::User,
        ..Default::default()
    };
    let rows = |bad: bool| -> Vec<Value> {
        (0..40_000)
            .map(|id| json!({"id": id, "v": if bad && id == 39_999 { 5000 } else { id % 1000 }}))
            .collect()
    };
    let err = run_rows_with(&app, "t", cols.clone(), &["id"], user(), rows(true))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("CHECK"), "{err}");
    assert_eq!(count(&db, "t").await, 0, "the failed batch is rolled back");
    run_rows_with(&app, "t", cols, &["id"], user(), rows(false))
        .await
        .unwrap();
    assert_eq!(count(&db, "t").await, 40_000);
}

/// Another connection holding the write lock past the busy timeout fails the
/// run; once it lets go, the next run applies the change.
#[tokio::test]
async fn a_locked_database_fails_the_run_and_the_next_run_repairs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("locked.db");
    let db = sqlite::Database::connect_with_options(
        path.to_str().unwrap(),
        sqlite::ConnectOptions {
            busy_timeout: std::time::Duration::from_millis(200),
            vec_extension: None,
        },
    )
    .await
    .unwrap();
    let (app, _a) = build_app(&db).await;
    let cols: Cols = vec![("id", "INTEGER", false), ("v", "TEXT", true)];
    run_rows(
        &app,
        "t",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1, "v": "a"})],
    )
    .await
    .unwrap();

    let other = sqlite::Database::connect(path.to_str().unwrap())
        .await
        .unwrap();
    let mut conn = other.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    let err = run_rows(
        &app,
        "t",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1, "v": "b"})],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("locked"), "{err}");
    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();
    drop(conn);

    run_rows(&app, "t", cols, &["id"], vec![json!({"id": 1, "v": "b"})])
        .await
        .unwrap();
    assert_eq!(
        rows_json(&db, "t", "id").await,
        vec![json!({"id": 1, "v": "b"})]
    );
}

/// A table someone else dropped between runs is created again (empty but for
/// the rows that changed), as in Python; a column someone else dropped is not
/// noticed until the schema changes (the target tracks the schema it applied).
#[tokio::test]
async fn a_table_dropped_by_someone_else_is_created_again() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let cols: Cols = vec![("id", "INTEGER", false), ("v", "TEXT", true)];
    run_rows(
        &app,
        "t",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1, "v": "a"})],
    )
    .await
    .unwrap();
    sqlx::query("DROP TABLE t")
        .execute(db.pool())
        .await
        .unwrap();
    run_rows(
        &app,
        "t",
        cols,
        &["id"],
        vec![json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})],
    )
    .await
    .unwrap();
    assert_eq!(
        rows_json(&db, "t", "id").await,
        vec![json!({"id": 2, "v": "b"})]
    );
}

/// Declaring one primary key twice in a component fails the run.
#[tokio::test]
async fn duplicate_primary_key_in_one_run() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    let err = run_rows(
        &app,
        "t",
        vec![("id", "INTEGER", false), ("v", "TEXT", true)],
        &["id"],
        vec![json!({"id": 1, "v": "a"}), json!({"id": 1, "v": "b"})],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("already declared"), "{err}");
}

/// `drop_state` drops the tables the app created and nothing else.
#[tokio::test]
async fn drop_state_removes_exactly_what_the_app_created() {
    let (db, _d) = temp_db().await;
    let (app, _a) = build_app(&db).await;
    sqlx::query("CREATE TABLE other (id INTEGER)")
        .execute(db.pool())
        .await
        .unwrap();
    let cols: Cols = vec![("id", "INTEGER", false)];
    app.run(move |ctx| async move {
        for name in ["a", "b"] {
            let t = sqlite::mount_table_target(&ctx, &DB, name, schema_of(&cols, &["id"])?).await?;
            t.declare_row(&ctx, &json!({"id": 1}))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    app.drop_state().await.unwrap();
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(tables, ["other"]);
}

/// With `sqlite-vec`: a vector dimension change rebuilds the `vec0` table
/// with the new vectors, and a vector of the wrong length fails the run.
#[tokio::test]
async fn vec0_dimension_change_and_wrong_length() {
    let Ok(extension) = std::env::var("SQLITE_VEC_EXTENSION") else {
        eprintln!("skipping sqlite-vec test; SQLITE_VEC_EXTENSION is not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let db =
        sqlite::Database::connect_with_vec(dir.path().join("v.db").to_str().unwrap(), &extension)
            .await
            .unwrap();
    let (app, _a) = build_app(&db).await;
    let vec0 = || sqlite::SqliteTableOptions {
        virtual_table_def: Some(sqlite::Vec0TableDef::default()),
        ..Default::default()
    };
    run_rows_with(
        &app,
        "v",
        vec![("id", "INTEGER", false), ("e", "float[3]", false)],
        &["id"],
        vec0(),
        vec![json!({"id": 1, "e": [1.0, 0.0, 0.0]})],
    )
    .await
    .unwrap();
    run_rows_with(
        &app,
        "v",
        vec![("id", "INTEGER", false), ("e", "float[4]", false)],
        &["id"],
        vec0(),
        vec![
            json!({"id": 1, "e": [1.0, 0.0, 0.0, 0.0]}),
            json!({"id": 2, "e": [0.0, 1.0, 0.0, 0.0]}),
        ],
    )
    .await
    .unwrap();
    let nearest: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM v WHERE e MATCH '[0, 0.9, 0.1, 0]' AND k = 1")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(nearest, vec![2]);
    let err = run_rows_with(
        &app,
        "v",
        vec![("id", "INTEGER", false), ("e", "float[4]", false)],
        &["id"],
        vec0(),
        vec![json!({"id": 3, "e": [1.0, 2.0]})],
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("dimension"),
        "{err}"
    );
}
