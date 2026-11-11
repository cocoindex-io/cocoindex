//! Edge cases of the Postgres table target against a live server: schema
//! evolution, identifiers, value edge cases, batch boundaries, failures that
//! the next run repairs, and an outage of the server between runs. Each test
//! skips unless `POSTGRES_URL` names a server, and works in a schema of its own
//! that it drops at the end.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cocoindex::connectors::postgres;
use cocoindex::{App, Environment, ManagedTargetOptions, Result};
use serde_json::{Value, json};
use sqlx::Row as _;

cocoindex::context_key!(
    static PG: postgres::Database, key = "postgres_target_edge_test_db"
);

mod common;

/// A unique schema name for one test.
fn unique_schema(test: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "coco_edge_{test}_{nonce}_{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// A connection and a fresh schema, or `None` when no server is configured.
async fn setup(test: &str) -> Result<Option<(postgres::Database, String)>> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres test; POSTGRES_URL is not set");
        return Ok(None);
    };
    let db = postgres::Database::connect(&url).await?;
    Ok(Some((db, unique_schema(test))))
}

async fn teardown(db: &postgres::Database, schema: &str) -> Result<()> {
    exec(db, &format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE")).await
}

async fn build_app(db: &postgres::Database, name: &str) -> Result<(App, tempfile::TempDir)> {
    common::record_sql_statements();
    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?
        .app(name)
        .await?;
    Ok((app, tempdir))
}

async fn exec(db: &postgres::Database, sql: &str) -> Result<()> {
    sqlx::query(sql)
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres `{sql}`: {e}")))?;
    Ok(())
}

/// Every row of `table` as JSON, ordered by `order_by`.
async fn rows_json(db: &postgres::Database, table: &str, order_by: &str) -> Result<Vec<Value>> {
    let sql = format!("SELECT to_jsonb(r__) AS j FROM {table} r__ ORDER BY {order_by}");
    let rows = sqlx::query(&sql)
        .fetch_all(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres `{sql}`: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|r| r.try_get::<Value, _>("j").unwrap())
        .collect())
}

async fn count(db: &postgres::Database, table: &str) -> Result<i64> {
    let sql = format!("SELECT count(*) FROM {table}");
    sqlx::query_scalar(&sql)
        .fetch_one(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres `{sql}`: {e}")))
}

/// `(column, data type, is nullable)` of every column of a table.
async fn columns(
    db: &postgres::Database,
    schema: &str,
    table: &str,
) -> Vec<(String, String, bool)> {
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT column_name::text, format_type(a.atttypid, a.atttypmod), is_nullable::text
         FROM information_schema.columns c
         JOIN pg_attribute a ON a.attname = c.column_name
           AND a.attrelid = format('%I.%I', c.table_schema, c.table_name)::regclass
         WHERE c.table_schema = $1 AND c.table_name = $2
         ORDER BY column_name",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(db.pool())
    .await
    .unwrap()
    .into_iter()
    .map(|(name, ty, nullable)| (name, ty, nullable == "YES"))
    .collect()
}

/// A column list, each `(name, type, nullable)`.
type Cols = Vec<(&'static str, &'static str, bool)>;

fn schema_of(cols: &Cols, pk: &[&str]) -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        cols.iter().map(|(name, ty, nullable)| {
            let def = postgres::ColumnDef::new(*ty);
            (*name, if *nullable { def.nullable() } else { def })
        }),
        pk.iter().copied(),
    )
}

/// One run declaring `rows` (JSON objects) into `schema.table`.
async fn run_rows(
    app: &App,
    schema: &str,
    table: &'static str,
    cols: Cols,
    pk: &'static [&'static str],
    rows: Vec<Value>,
) -> Result<()> {
    let schema = schema.to_string();
    app.run(move |ctx| async move {
        let target =
            postgres::mount_table_target(&ctx, &PG, table, schema_of(&cols, pk)?, Some(&schema))
                .await?;
        for row in &rows {
            target.declare_row(&ctx, row)?;
        }
        Ok(())
    })
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

/// Making a column non-nullable and back changes its `NOT NULL` constraint, as
/// Python's connector does (`ALTER COLUMN … SET/DROP NOT NULL`), and keeps the
/// rows. Without it, a column made nullable again keeps `NOT NULL` and a NULL
/// value fails every later run.
#[tokio::test]
async fn nullable_column_becomes_required_and_back() -> Result<()> {
    let Some((db, schema)) = setup("nullable").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeNullable").await?;
    let nullable: Cols = vec![("id", "bigint", false), ("note", "text", true)];
    let required: Cols = vec![("id", "bigint", false), ("note", "text", false)];

    run_rows(
        &app,
        &schema,
        "t",
        nullable.clone(),
        &["id"],
        vec![json!({"id": 1, "note": "a"})],
    )
    .await?;
    assert!(columns(&db, &schema, "t").await[1].2);

    run_rows(
        &app,
        &schema,
        "t",
        required,
        &["id"],
        vec![json!({"id": 1, "note": "a"})],
    )
    .await?;
    assert!(
        !columns(&db, &schema, "t").await[1].2,
        "note became NOT NULL"
    );

    run_rows(
        &app,
        &schema,
        "t",
        nullable,
        &["id"],
        vec![
            json!({"id": 1, "note": "a"}),
            json!({"id": 2, "note": null}),
        ],
    )
    .await?;
    assert!(
        columns(&db, &schema, "t").await[1].2,
        "note is nullable again"
    );
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![
            json!({"id": 1, "note": "a"}),
            json!({"id": 2, "note": null})
        ]
    );
    teardown(&db, &schema).await
}

/// A column renamed in the declared schema is dropped and the new one added;
/// the rows are rewritten so the new column holds the values.
#[tokio::test]
async fn renamed_column_is_repopulated() -> Result<()> {
    let Some((db, schema)) = setup("rename").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeRename").await?;
    run_rows(
        &app,
        &schema,
        "t",
        vec![("id", "bigint", false), ("old_name", "text", true)],
        &["id"],
        vec![json!({"id": 1, "old_name": "v"})],
    )
    .await?;
    run_rows(
        &app,
        &schema,
        "t",
        vec![("id", "bigint", false), ("new_name", "text", true)],
        &["id"],
        vec![json!({"id": 1, "new_name": "v"})],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, "new_name": "v"})]
    );
    teardown(&db, &schema).await
}

/// Changing a vector column's dimension retypes it (dropping the old vectors,
/// which no cast converts) and rewrites the rows with the new vectors; the
/// vector index on it is rebuilt for the new type.
#[tokio::test]
async fn vector_dimension_change_rewrites_vectors_and_index() -> Result<()> {
    let Some((db, schema)) = setup("vecdim").await? else {
        return Ok(());
    };
    if let Err(e) = exec(&db, "CREATE EXTENSION IF NOT EXISTS vector").await {
        eprintln!("skipping: pgvector is unavailable: {e}");
        return Ok(());
    }
    let (app, _dir) = build_app(&db, "PgEdgeVecDim").await?;
    for (ty, vector) in [
        ("vector(3)", json!([1.0, 2.0, 3.0])),
        ("vector(4)", json!([1.0, 2.0, 3.0, 4.0])),
    ] {
        let schema = schema.clone();
        app.run(move |ctx| async move {
            let cols: Cols = vec![("id", "bigint", false), ("v", ty, false)];
            let target = postgres::mount_table_target(
                &ctx,
                &PG,
                "t",
                schema_of(&cols, &["id"])?,
                Some(&schema),
            )
            .await?;
            target.declare_vector_index(&ctx, "v", postgres::VectorIndexOptions::default())?;
            target.declare_row(&ctx, &json!({"id": 1, "v": vector}))?;
            Ok(())
        })
        .await?;
    }
    // No cast converts the vectors, so the column is dropped and added again:
    // nullable, as Python's connector adds it (a NOT NULL add fails on rows).
    assert_eq!(
        columns(&db, &schema, "t").await[1],
        ("v".to_string(), "vector(4)".to_string(), true)
    );
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, "v": "[1,2,3,4]"})]
    );
    let index_def: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = 't__vector__v'",
    )
    .bind(&schema)
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert!(index_def.contains("vector_cosine_ops"), "{index_def}");
    teardown(&db, &schema).await
}

/// A column name longer than Postgres' 63-byte identifier limit is truncated
/// by the server. The column reconciliation must compare truncated names, or
/// it drops the column on every run as one it does not know.
#[tokio::test]
async fn column_name_longer_than_63_bytes_survives_runs() -> Result<()> {
    let Some((db, schema)) = setup("longname").await? else {
        return Ok(());
    };
    const LONG: &str = "a_column_name_that_is_much_longer_than_the_postgres_identifier_limit_of_63";
    let (app, _dir) = build_app(&db, "PgEdgeLongName").await?;
    let cols: Cols = vec![("id", "bigint", false), (LONG, "text", true)];
    run_rows(
        &app,
        &schema,
        "t",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1, LONG: "kept"})],
    )
    .await?;
    // An unchanged second run must not touch the column.
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        vec![json!({"id": 1, LONG: "kept"})],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, &LONG[..63]: "kept"})]
    );
    teardown(&db, &schema).await
}

/// Reserved words and mixed-case names are quoted everywhere: create, upsert,
/// delete, a column change, and an unchanged run.
#[tokio::test]
async fn reserved_and_mixed_case_identifiers() -> Result<()> {
    let Some((db, schema)) = setup("Reserved_Mixed").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeReserved").await?;
    let cols: Cols = vec![
        ("select", "bigint", false),
        ("Order", "text", false),
        ("user", "text", true),
    ];
    let rows = vec![
        json!({"select": 1, "Order": "x", "user": "u1"}),
        json!({"select": 2, "Order": "y", "user": null}),
    ];
    run_rows(
        &app,
        &schema,
        "Table",
        cols.clone(),
        &["select", "Order"],
        rows,
    )
    .await?;
    run_rows(
        &app,
        &schema,
        "Table",
        cols,
        &["select", "Order"],
        vec![json!({"select": 2, "Order": "y", "user": "u2"})],
    )
    .await?;
    let table = format!("\"{schema}\".\"Table\"");
    assert_eq!(
        rows_json(&db, &table, "\"select\"").await?,
        vec![json!({"select": 2, "Order": "y", "user": "u2"})]
    );
    // Retype a reserved-word column.
    run_rows(
        &app,
        &schema,
        "Table",
        vec![
            ("select", "bigint", false),
            ("Order", "text", false),
            ("user", "varchar(10)", true),
        ],
        &["select", "Order"],
        vec![json!({"select": 2, "Order": "y", "user": "u2"})],
    )
    .await?;
    assert_eq!(
        columns(&db, &schema, "Table").await[2].1,
        "character varying(10)"
    );
    teardown(&db, &schema).await
}

/// Names outside the plain-identifier allow-list are rejected before any SQL
/// is built (as in Python, which mirrors the Doris CVE-2026-28438 fix).
#[test]
fn names_outside_the_identifier_allow_list_are_rejected() {
    for bad in ["", "a\"b", "a b", "1abc", "名前", "a;drop", "a-b"] {
        assert!(
            postgres::TableSchema::new([(bad, postgres::ColumnDef::new("text"))], [bad]).is_err(),
            "{bad:?}"
        );
    }
    for bad_type in ["text; DROP TABLE x", "text\"", "", "text[]--"] {
        assert!(
            postgres::TableSchema::new([("a", postgres::ColumnDef::new(bad_type))], ["a"]).is_err(),
            "{bad_type:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn edge_values_round_trip() -> Result<()> {
    let Some((db, schema)) = setup("values").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeValues").await?;
    let cols: Cols = vec![
        ("id", "bigint", false),
        ("t", "text", true),
        ("i", "bigint", true),
        ("s", "smallint", true),
        ("f", "double precision", true),
        ("d", "numeric", true),
        ("ts", "timestamptz", true),
        ("j", "jsonb", true),
        ("b", "bytea", true),
        ("u", "uuid", true),
    ];
    let huge = "x".repeat(8 * 1024 * 1024);
    let rows = vec![
        json!({"id": 1, "t": "", "i": i64::MAX, "s": -32768, "f": -0.0, "d": "79228162514264337593543950335",
               "ts": "2024-03-10T02:30:00+05:30", "j": {"a": [1, {"b\u{0}": "c\u{0}"}], "e": {}},
               "b": [], "u": "6F9619FF-8B86-D011-B42D-00C04FC964FF"}),
        json!({"id": 2, "t": "nul\u{0}inside ünïcödé 🎉", "i": i64::MIN, "f": 1e308, "d": "-0.000000000001",
               "ts": "1970-01-01T00:00:00Z", "j": null, "b": [0, 255, 0]}),
        json!({"id": 3, "t": huge.clone(), "j": "just a string", "b": vec![7u8; 1 << 20]}),
    ];
    run_rows(&app, &schema, "t", cols, &["id"], rows).await?;
    let table = format!("\"{schema}\".\"t\"");
    type Got = (
        i64,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i32>,
    );
    let got: Vec<Got> =
        sqlx::query_as(&format!(
            "SELECT id, t, i, d::text, (ts AT TIME ZONE 'UTC')::text, length(b) FROM {table} ORDER BY id"
        ))
        .fetch_all(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(
        got[0].1.as_deref(),
        Some(""),
        "empty string stays empty, not NULL"
    );
    assert_eq!(got[0].2, Some(i64::MAX));
    assert_eq!(got[0].3.as_deref(), Some("79228162514264337593543950335"));
    assert_eq!(got[0].4.as_deref(), Some("2024-03-09 21:00:00"));
    assert_eq!(got[0].5, Some(0));
    assert_eq!(got[1].1.as_deref(), Some("nulinside ünïcödé 🎉"));
    assert_eq!(got[1].2, Some(i64::MIN));
    assert_eq!(got[1].3.as_deref(), Some("-0.000000000001"));
    assert_eq!(got[1].5, Some(3));
    assert_eq!(got[2].1.as_ref().map(String::len), Some(huge.len()));
    assert_eq!(got[2].5, Some(1 << 20));
    let j: Vec<Option<Value>> = sqlx::query_scalar(&format!("SELECT j FROM {table} ORDER BY id"))
        .fetch_all(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(
        j,
        vec![
            Some(json!({"a": [1, {"b": "c"}], "e": {}})),
            None,
            Some(json!("just a string"))
        ]
    );
    let u: Option<String> =
        sqlx::query_scalar(&format!("SELECT u::text FROM {table} WHERE id = 1"))
            .fetch_one(db.pool())
            .await
            .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(u.as_deref(), Some("6f9619ff-8b86-d011-b42d-00c04fc964ff"));
    teardown(&db, &schema).await
}

#[derive(serde::Serialize)]
struct FloatRow {
    id: i64,
    f: f64,
}

/// NaN and ±Inf are rejected when declared (they'd serialize to JSON null),
/// in a plain column and inside a vector.
#[tokio::test]
async fn non_finite_floats_are_rejected() -> Result<()> {
    let Some((db, schema)) = setup("nonfinite").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeNonFinite").await?;
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let schema = schema.clone();
        let err = app
            .run(move |ctx| async move {
                let cols: Cols = vec![("id", "bigint", false), ("f", "double precision", false)];
                let target = postgres::mount_table_target(
                    &ctx,
                    &PG,
                    "t",
                    schema_of(&cols, &["id"])?,
                    Some(&schema),
                )
                .await?;
                target.declare_row(&ctx, &FloatRow { id: 1, f: value })?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-finite"), "{err}");
    }
    teardown(&db, &schema).await
}

/// Values that don't fit their column fail the run with the server's message,
/// and a later run with valid values succeeds.
#[tokio::test]
async fn out_of_range_and_wrong_shape_values_fail_then_repair() -> Result<()> {
    let Some((db, schema)) = setup("range").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeRange").await?;
    let cols: Cols = vec![
        ("id", "bigint", false),
        ("small", "smallint", true),
        ("t", "text", true),
    ];
    let cases = [
        (json!({"id": 1, "small": 40000}), "out of range"),
        (json!({"id": 1, "small": u64::MAX}), "out of range"),
        (json!({"id": 1, "t": 5}), "text column requires string"),
        (json!({"id": 1, "small": "12"}), "numeric column"),
        (json!({"id": u64::MAX}), "unsupported numeric primary key"),
        (json!({"id": 1.5}), "unsupported numeric primary key"),
        (json!({"id": null}), "primary key value cannot be null"),
        (json!({"t": "no id"}), "primary key value cannot be null"),
    ];
    for (row, expected) in cases {
        let err = run_rows(&app, &schema, "t", cols.clone(), &["id"], vec![row.clone()])
            .await
            .unwrap_err();
        assert!(err.to_string().contains(expected), "{row}: {err}");
    }
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        vec![json!({"id": 1, "small": 7})],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, "small": 7, "t": null})]
    );
    teardown(&db, &schema).await
}

/// A vector of the wrong length fails the run; fixing it repairs the table.
#[tokio::test]
async fn vector_of_wrong_length_fails_then_repairs() -> Result<()> {
    let Some((db, schema)) = setup("veclen").await? else {
        return Ok(());
    };
    if let Err(e) = exec(&db, "CREATE EXTENSION IF NOT EXISTS vector").await {
        eprintln!("skipping: pgvector is unavailable: {e}");
        return Ok(());
    }
    let (app, _dir) = build_app(&db, "PgEdgeVecLen").await?;
    let cols: Cols = vec![("id", "bigint", false), ("v", "vector(3)", false)];
    let err = run_rows(
        &app,
        &schema,
        "t",
        cols.clone(),
        &["id"],
        vec![
            json!({"id": 1, "v": [1.0, 2.0, 3.0]}),
            json!({"id": 2, "v": [1.0, 2.0]}),
        ],
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("dimensions"), "{err}");
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        vec![
            json!({"id": 1, "v": [1.0, 2.0, 3.0]}),
            json!({"id": 2, "v": [4.0, 5.0, 6.0]}),
        ],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(count(&db, &table).await?, 2);
    teardown(&db, &schema).await
}

// ---------------------------------------------------------------------------
// Row identity
// ---------------------------------------------------------------------------

/// A row whose primary key changes is a new row: the old one is deleted.
#[tokio::test]
async fn row_whose_primary_key_changes_replaces_the_old_row() -> Result<()> {
    let Some((db, schema)) = setup("pkchange").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgePkChange").await?;
    let cols: Cols = vec![
        ("k1", "text", false),
        ("k2", "bigint", false),
        ("v", "text", true),
    ];
    run_rows(
        &app,
        &schema,
        "t",
        cols.clone(),
        &["k1", "k2"],
        vec![json!({"k1": "a", "k2": 1, "v": "x"})],
    )
    .await?;
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["k1", "k2"],
        vec![json!({"k1": "a", "k2": 2, "v": "x"})],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "k2").await?,
        vec![json!({"k1": "a", "k2": 2, "v": "x"})]
    );
    teardown(&db, &schema).await
}

/// Declaring the same primary key twice in one component fails the run with a
/// message naming the key, instead of writing one of the two silently.
#[tokio::test]
async fn duplicate_primary_key_in_one_run() -> Result<()> {
    let Some((db, schema)) = setup("dupkey").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeDupKey").await?;
    let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
    let result = run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        vec![
            json!({"id": 1, "v": "first"}),
            json!({"id": 1, "v": "second"}),
        ],
    )
    .await;
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("already declared with key: Int(1)"),
        "{err}"
    );
    teardown(&db, &schema).await
}

/// Two child components declare rows into one table: each owns its rows, and
/// unmounting one deletes only its rows.
#[tokio::test]
async fn two_components_share_one_table() -> Result<()> {
    let Some((db, schema)) = setup("shared").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeShared").await?;
    let run = |groups: Vec<(&'static str, Vec<i64>)>| {
        let schema = schema.clone();
        let app = &app;
        async move {
            app.run(move |ctx| async move {
                let cols: Cols = vec![("id", "bigint", false), ("grp", "text", false)];
                let target = postgres::mount_table_target(
                    &ctx,
                    &PG,
                    "t",
                    schema_of(&cols, &["id"])?,
                    Some(&schema),
                )
                .await?;
                ctx.mount_each(
                    groups,
                    |(name, _)| name.to_string(),
                    move |child, (name, ids)| {
                        let target = target.clone();
                        async move {
                            for id in ids {
                                target.declare_row(&child, &json!({"id": id, "grp": name}))?;
                            }
                            Ok(())
                        }
                    },
                )
                .await?;
                Ok(())
            })
            .await
        }
    };
    run(vec![("a", (0..300).collect()), ("b", (300..600).collect())]).await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(count(&db, &table).await?, 600);
    run(vec![("b", (300..600).collect())]).await?;
    assert_eq!(count(&db, &table).await?, 300);
    let grps: Vec<String> = sqlx::query_scalar(&format!("SELECT DISTINCT grp FROM {table}"))
        .fetch_all(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(grps, vec!["b".to_string()]);
    teardown(&db, &schema).await
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------

/// Upserts go in statements of at most 2,000 rows: 1 → 1 statement, 1,999 → 1,
/// 2,000 → 1, 2,001 → 2.
#[tokio::test]
async fn batch_boundaries_issue_the_expected_statement_count() -> Result<()> {
    let Some((db, schema)) = setup("batch").await? else {
        return Ok(());
    };
    let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
    for (i, (n, statements)) in [(1usize, 1usize), (1999, 1), (2000, 1), (2001, 2)]
        .into_iter()
        .enumerate()
    {
        let (app, _dir) = build_app(&db, &format!("PgEdgeBatch{i}")).await?;
        let table: &'static str = Box::leak(format!("t{n}").into_boxed_str());
        let rows = (0..n as i64)
            .map(|id| json!({"id": id, "v": "x"}))
            .collect();
        run_rows(&app, &schema, table, cols.clone(), &["id"], rows).await?;
        let qualified = format!("\"{schema}\".\"{table}\"");
        assert_eq!(count(&db, &qualified).await?, n as i64);
        let inserts = common::sql_statements_containing(&format!("INSERT INTO {qualified} "));
        assert_eq!(inserts.len(), statements, "{n} rows");
    }
    teardown(&db, &schema).await
}

/// A statement that fails in the middle of a multi-statement batch fails the
/// run; the statements that committed stay, and the next run, with the bad
/// value fixed, converges to exactly the declared rows.
#[tokio::test]
async fn failure_mid_batch_is_repaired_by_the_next_run() -> Result<()> {
    let Some((db, schema)) = setup("midbatch").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeMidBatch").await?;
    let cols: Cols = vec![("id", "bigint", false), ("small", "smallint", true)];
    let rows = |bad: bool| -> Vec<Value> {
        (0..5000)
            .map(|id| {
                let small = if bad && id == 4321 {
                    100_000
                } else {
                    id % 1000
                };
                json!({"id": id, "small": small})
            })
            .collect()
    };
    let err = run_rows(&app, &schema, "t", cols.clone(), &["id"], rows(true))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("out of range"), "{err}");
    run_rows(&app, &schema, "t", cols.clone(), &["id"], rows(false)).await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(count(&db, &table).await?, 5000);
    let bad: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table} WHERE small <> id % 1000"
    ))
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(bad, 0);
    // Shrinking after the failure deletes exactly the undeclared rows.
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        rows(false)[..10].to_vec(),
    )
    .await?;
    assert_eq!(count(&db, &table).await?, 10);
    teardown(&db, &schema).await
}

/// 100,000 rows in one run, then all deleted. Slow (tens of seconds); run with
/// `POSTGRES_URL=… cargo test -p cocoindex --features postgres --test
/// postgres_target_edge -- --ignored`.
#[tokio::test]
#[ignore = "soak test: 100k rows against a live Postgres"]
async fn one_hundred_thousand_rows() -> Result<()> {
    let Some((db, schema)) = setup("soak").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeSoak").await?;
    let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
    let rows: Vec<Value> = (0..100_000)
        .map(|id| json!({"id": id, "v": format!("row {id}")}))
        .collect();
    let start = std::time::Instant::now();
    run_rows(&app, &schema, "t", cols.clone(), &["id"], rows).await?;
    eprintln!("100k upserts: {:?}", start.elapsed());
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(count(&db, &table).await?, 100_000);
    assert_eq!(
        common::sql_statements_containing(&format!("INSERT INTO {table} ")).len(),
        50
    );
    let start = std::time::Instant::now();
    run_rows(&app, &schema, "t", cols, &["id"], vec![]).await?;
    eprintln!("100k deletes: {:?}", start.elapsed());
    assert_eq!(count(&db, &table).await?, 0);
    teardown(&db, &schema).await
}

// ---------------------------------------------------------------------------
// Tables changed by someone else, user-managed tables, drop
// ---------------------------------------------------------------------------

/// A column someone else added to a system-managed table (for example by the
/// table's own SQL command attachment) is left alone, as Python's connector
/// leaves it: only columns this target declared before are dropped. The
/// attachment's SQL holds several statements.
#[tokio::test]
async fn column_added_by_an_attachment_is_kept() -> Result<()> {
    let Some((db, schema)) = setup("extcol").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeExtCol").await?;
    for label in ["a", "b"] {
        let schema = schema.clone();
        app.run(move |ctx| async move {
            let cols: Cols = vec![("id", "bigint", false), ("label", "text", true)];
            let target = postgres::mount_table_target(
                &ctx,
                &PG,
                "t",
                schema_of(&cols, &["id"])?,
                Some(&schema),
            )
            .await?;
            target.declare_sql_command_attachment(
                &ctx,
                "tsv",
                // Several statements in one attachment, as Python allows.
                format!(
                    "ALTER TABLE \"{schema}\".\"t\" ADD COLUMN IF NOT EXISTS tsv tsvector \
                     GENERATED ALWAYS AS (to_tsvector('simple', coalesce(label, ''))) STORED; \
                     CREATE INDEX IF NOT EXISTS t_tsv ON \"{schema}\".\"t\" USING gin (tsv)"
                ),
                Some(format!(
                    "DROP INDEX IF EXISTS \"{schema}\".t_tsv; \
                     ALTER TABLE \"{schema}\".\"t\" DROP COLUMN IF EXISTS tsv"
                )),
            )?;
            target.declare_row(&ctx, &json!({"id": 1, "label": label}))?;
            Ok(())
        })
        .await?;
    }
    let names: Vec<String> = columns(&db, &schema, "t")
        .await
        .into_iter()
        .map(|c| c.0)
        .collect();
    assert_eq!(names, ["id", "label", "tsv"]);
    let tsv: String = sqlx::query_scalar(&format!("SELECT tsv::text FROM \"{schema}\".\"t\""))
        .fetch_one(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(tsv, "'b':1");
    teardown(&db, &schema).await
}

/// A column someone else dropped from a system-managed table is added back on
/// the next run.
#[tokio::test]
async fn column_dropped_by_someone_else_is_added_back() -> Result<()> {
    let Some((db, schema)) = setup("extdrop").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeExtDrop").await?;
    let cols: Cols = vec![("id", "bigint", false), ("label", "text", true)];
    run_rows(
        &app,
        &schema,
        "t",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1, "label": "a"})],
    )
    .await?;
    exec(
        &db,
        &format!("ALTER TABLE \"{schema}\".\"t\" DROP COLUMN label"),
    )
    .await?;
    run_rows(
        &app,
        &schema,
        "t",
        cols,
        &["id"],
        vec![json!({"id": 2, "label": "b"})],
    )
    .await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 2, "label": "b"})]
    );
    teardown(&db, &schema).await
}

/// A user-managed table gets no DDL: a missing table fails the run (nothing
/// creates it), and rows the app stops declaring are deleted. `drop_state`
/// abandons the table with its contents: a container CocoIndex doesn't own is
/// not reconciled row by row when it goes (see "Deleting a container" in the
/// custom target connector guide), as in Python.
#[tokio::test]
async fn user_managed_table_is_never_created_or_dropped() -> Result<()> {
    let Some((db, schema)) = setup("usermanaged").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeUserManaged").await?;
    let run = |ids: Vec<i64>| {
        let schema = schema.clone();
        let app = &app;
        async move {
            app.run(move |ctx| async move {
                let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
                let target = postgres::mount_table_target_with_options(
                    &ctx,
                    &PG,
                    "t",
                    schema_of(&cols, &["id"])?,
                    Some(&schema),
                    ManagedTargetOptions::user_managed(),
                )
                .await?;
                for id in ids {
                    target.declare_row(&ctx, &json!({"id": id, "v": "app"}))?;
                }
                Ok(())
            })
            .await
        }
    };
    exec(&db, &format!("CREATE SCHEMA \"{schema}\"")).await?;
    let err = run(vec![1]).await.unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    exec(
        &db,
        &format!("CREATE TABLE \"{schema}\".\"t\" (id bigint PRIMARY KEY, v text, extra text)"),
    )
    .await?;
    exec(
        &db,
        &format!("INSERT INTO \"{schema}\".\"t\" VALUES (100, 'mine', 'e')"),
    )
    .await?;
    run(vec![1, 2]).await?;
    let table = format!("\"{schema}\".\"t\"");
    assert_eq!(count(&db, &table).await?, 3);
    run(vec![2]).await?;
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![
            json!({"id": 2, "v": "app", "extra": null}),
            json!({"id": 100, "v": "mine", "extra": "e"})
        ]
    );
    app.drop_state().await?;
    assert_eq!(count(&db, &table).await?, 2, "the table and its rows stay");
    teardown(&db, &schema).await
}

/// `drop_state` removes exactly the tables and indexes the app created and
/// leaves another table in the same schema.
#[tokio::test]
async fn drop_state_removes_exactly_what_the_app_created() -> Result<()> {
    let Some((db, schema)) = setup("dropstate").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeDropState").await?;
    exec(&db, &format!("CREATE SCHEMA \"{schema}\"")).await?;
    exec(
        &db,
        &format!("CREATE TABLE \"{schema}\".\"other\" (id bigint)"),
    )
    .await?;
    let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
    run_rows(
        &app,
        &schema,
        "a",
        cols.clone(),
        &["id"],
        vec![json!({"id": 1})],
    )
    .await?;
    run_rows(&app, &schema, "b", cols, &["id"], vec![json!({"id": 1})]).await?;
    app.drop_state().await?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables WHERE table_schema = $1 ORDER BY 1",
    )
    .bind(&schema)
    .fetch_all(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    // Only `b` was declared by the last run, so `a` was already dropped by it.
    assert_eq!(tables, ["other"]);
    teardown(&db, &schema).await
}

// ---------------------------------------------------------------------------
// Server outage
// ---------------------------------------------------------------------------

/// A TCP proxy in front of the Postgres server that can refuse connections
/// and cut the open ones, to stand in for a server that goes down.
struct Proxy {
    port: u16,
    up: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let up = Arc::new(AtomicBool::new(true));
        let generation = Arc::new(AtomicU64::new(0));
        let (up2, generation2) = (up.clone(), generation.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                if !up2.load(Ordering::SeqCst) {
                    continue; // dropping `client` closes it
                }
                let upstream = upstream.clone();
                let generation = generation2.clone();
                let born = generation.load(Ordering::SeqCst);
                tokio::spawn(async move {
                    let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let copy = tokio::io::copy_bidirectional(&mut client, &mut server);
                    let cut = async {
                        while generation.load(Ordering::SeqCst) == born {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    };
                    tokio::select! { _ = copy => {}, _ = cut => {} }
                });
            }
        });
        Self {
            port,
            up,
            generation,
        }
    }

    fn down(&self) {
        self.up.store(false, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    fn restore(&self) {
        self.up.store(true, Ordering::SeqCst);
    }
}

/// The server goes down during an update: the run fails with a connection
/// error. When it is back, the next run applies the missed changes.
#[tokio::test]
async fn server_down_during_an_update_then_back() -> Result<()> {
    let Some((direct, schema)) = setup("outage").await? else {
        return Ok(());
    };
    let url = reqwest::Url::parse(&std::env::var("POSTGRES_URL").unwrap()).unwrap();
    let upstream = format!(
        "{}:{}",
        url.host_str().unwrap_or("localhost"),
        url.port().unwrap_or(5432)
    );
    let proxy = Proxy::start(upstream).await;
    let mut proxied = url.clone();
    proxied.set_host(Some("127.0.0.1")).unwrap();
    proxied.set_port(Some(proxy.port)).unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect(proxied.as_str())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    let db = postgres::Database::from_pool("outage", pool);

    let (app, _dir) = build_app(&db, "PgEdgeOutage").await?;
    let cols: Cols = vec![("id", "bigint", false), ("v", "text", true)];
    let rows = |v: &str| -> Vec<Value> { (0..10).map(|id| json!({"id": id, "v": v})).collect() };
    run_rows(&app, &schema, "t", cols.clone(), &["id"], rows("one")).await?;

    proxy.down();
    let err = run_rows(&app, &schema, "t", cols.clone(), &["id"], rows("two"))
        .await
        .unwrap_err();
    eprintln!("outage error: {err}");

    proxy.restore();
    run_rows(&app, &schema, "t", cols, &["id"], rows("two")).await?;
    let table = format!("\"{schema}\".\"t\"");
    let distinct: Vec<String> = sqlx::query_scalar(&format!("SELECT DISTINCT v FROM {table}"))
        .fetch_all(direct.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(e.to_string()))?;
    assert_eq!(distinct, ["two"]);
    assert_eq!(count(&direct, &table).await?, 10);
    teardown(&direct, &schema).await
}

// ---------------------------------------------------------------------------
// Target handles as mount arguments
// ---------------------------------------------------------------------------

static HANDLE_ARG_CALLS: AtomicU64 = AtomicU64::new(0);

/// A component keyed by its arguments, the table handle among them.
#[cocoindex::function]
async fn declare_row_in_component(
    ctx: &cocoindex::Ctx,
    id: i64,
    table: postgres::TableTarget,
) -> Result<()> {
    HANDLE_ARG_CALLS.fetch_add(1, Ordering::SeqCst);
    table.declare_row(ctx, &json!({"id": id, "a": "x", "b": "x"}))
}

/// A table handle passed to `use_mount_each!` keys the child by the table's
/// name: an unchanged child is skipped and keeps its row. A schema change that
/// loses column data (a renamed column) re-runs the child even though its
/// arguments are unchanged, so the new column is filled.
#[tokio::test]
async fn table_handle_as_a_mount_argument() -> Result<()> {
    let Some((db, schema)) = setup("handlearg").await? else {
        return Ok(());
    };
    let (app, _dir) = build_app(&db, "PgEdgeHandleArg").await?;
    let calls = || HANDLE_ARG_CALLS.load(Ordering::SeqCst);
    let run = |col: &'static str| {
        let schema = schema.clone();
        let app = &app;
        async move {
            app.run(move |ctx| async move {
                let cols: Cols = vec![("id", "bigint", false), (col, "text", true)];
                let table = postgres::mount_table_target(
                    &ctx,
                    &PG,
                    "t",
                    schema_of(&cols, &["id"])?,
                    Some(&schema),
                )
                .await?;
                cocoindex::use_mount_each!(vec![("1".to_string(), 1_i64)], |id| {
                    declare_row_in_component(ctx, id, table.clone())
                })
                .await?;
                Ok(())
            })
            .await
        }
    };
    let table = format!("\"{schema}\".\"t\"");
    run("a").await?;
    assert_eq!(calls(), 1);
    run("a").await?;
    assert_eq!(calls(), 1, "unchanged: the child is skipped");
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, "a": "x"})]
    );
    run("b").await?;
    assert_eq!(
        rows_json(&db, &table, "id").await?,
        vec![json!({"id": 1, "b": "x"})]
    );
    teardown(&db, &schema).await
}
