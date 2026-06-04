#![cfg(feature = "postgres")]

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::{App, ContextKey, Ctx, Result, postgres};
use serde::{Deserialize, Serialize};
use sqlx::Row as _;

static PG: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("postgres_target_test_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});

#[derive(Clone, Serialize)]
struct TestRow {
    id: i64,
    label: String,
}

#[derive(Clone, Serialize)]
struct EmbeddingRow {
    id: i64,
    embedding: Vec<f32>,
}

async fn declare_rows(ctx: Ctx, schema: String, rows: Vec<TestRow>) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "rows",
        postgres::TableSchema::new(
            [
                ("id", postgres::ColumnDef::new("bigint")),
                ("label", postgres::ColumnDef::new("text")),
            ],
            ["id"],
        )?,
        Some(&schema),
    )
    .await?;
    for row in &rows {
        table.declare_row(&ctx, row)?;
    }
    Ok(())
}

async fn declare_vector_table(ctx: Ctx, schema: String, with_index: bool) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "vectors",
        postgres::TableSchema::new(
            [
                ("id", postgres::ColumnDef::new("bigint")),
                ("embedding", postgres::ColumnDef::new("vector(3)")),
            ],
            ["id"],
        )?,
        Some(&schema),
    )
    .await?;
    if with_index {
        table.declare_vector_index(
            &ctx,
            "embedding",
            postgres::VectorIndexOptions {
                name: Some("embedding_idx".to_string()),
                ..Default::default()
            },
        )?;
    }
    table.declare_row(
        &ctx,
        &EmbeddingRow {
            id: 1,
            embedding: vec![0.0, 1.0, 0.0],
        },
    )?;
    Ok(())
}

async fn declare_rows_with_sql_command(
    ctx: Ctx,
    schema: String,
    with_attachment: bool,
) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "rows",
        postgres::TableSchema::new(
            [
                ("id", postgres::ColumnDef::new("bigint")),
                ("label", postgres::ColumnDef::new("text")),
            ],
            ["id"],
        )?,
        Some(&schema),
    )
    .await?;
    if with_attachment {
        table.declare_sql_command_attachment(
            &ctx,
            "label_index",
            format!("CREATE INDEX IF NOT EXISTS rows_label_idx ON \"{schema}\".\"rows\" (label)"),
            Some(format!("DROP INDEX IF EXISTS \"{schema}\".rows_label_idx")),
        )?;
    }
    table.declare_row(
        &ctx,
        &TestRow {
            id: 1,
            label: "one".to_string(),
        },
    )?;
    Ok(())
}

async fn relation_exists(
    db: &postgres::Database,
    schema: &str,
    name: &str,
    relkind: &str,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = $3
        )",
    )
    .bind(schema)
    .bind(name)
    .bind(relkind)
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres relation lookup: {e}")))
}

async fn table_exists(db: &postgres::Database, schema: &str, table: &str) -> Result<bool> {
    relation_exists(db, schema, table, "r").await
}

async fn index_exists(db: &postgres::Database, schema: &str, index: &str) -> Result<bool> {
    relation_exists(db, schema, index, "i").await
}

#[tokio::test]
async fn postgres_table_target_reconciles_rows_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres target test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresTargetE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_rows(
                ctx,
                schema,
                vec![
                    TestRow {
                        id: 1,
                        label: "one".to_string(),
                    },
                    TestRow {
                        id: 2,
                        label: "two".to_string(),
                    },
                ],
            )
        }
    })
    .await?;

    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_rows(
                ctx,
                schema,
                vec![
                    TestRow {
                        id: 2,
                        label: "two-updated".to_string(),
                    },
                    TestRow {
                        id: 3,
                        label: "three".to_string(),
                    },
                ],
            )
        }
    })
    .await?;

    let rows = sqlx::query(&format!(
        "SELECT id, label FROM \"{schema}\".\"rows\" ORDER BY id"
    ))
    .fetch_all(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres query: {e}")))?;
    let actual = rows
        .into_iter()
        .map(|row| {
            let id: i64 = row.try_get("id").unwrap();
            let label: String = row.try_get("label").unwrap();
            (id, label)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![(2, "two-updated".to_string()), (3, "three".to_string())]
    );

    sqlx::query(&format!("DROP TABLE \"{schema}\".\"rows\""))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres external table drop: {e}")))?;
    app.run({
        let schema = schema.clone();
        move |ctx| declare_rows(ctx, schema, vec![])
    })
    .await?;
    assert!(
        table_exists(&db, &schema, "rows").await?,
        "delete-only reconciliation should recreate a missing table"
    );
    let row_count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{schema}\".\"rows\""))
        .fetch_one(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres count: {e}")))?;
    assert_eq!(row_count, 0);

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

#[tokio::test]
async fn postgres_sql_command_attachment_runs_setup_and_teardown_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres SQL-command-attachment test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_sqlcmd_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresSqlCommandE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    // Declare the attachment → setup SQL creates the index.
    app.run({
        let schema = schema.clone();
        move |ctx| declare_rows_with_sql_command(ctx, schema, true)
    })
    .await?;
    assert!(
        index_exists(&db, &schema, "rows_label_idx").await?,
        "setup_sql should create the index"
    );

    // Stop declaring it → teardown SQL drops the index (eager attachment cleanup).
    app.run({
        let schema = schema.clone();
        move |ctx| declare_rows_with_sql_command(ctx, schema, false)
    })
    .await?;
    assert!(
        !index_exists(&db, &schema, "rows_label_idx").await?,
        "teardown_sql should drop the index when the attachment is removed"
    );

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

#[tokio::test]
async fn postgres_read_table_items_keys_rows_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres source-items test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("src_items_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!(
        "CREATE TABLE \"{table}\" (id bigint primary key, name text)"
    ))
    .execute(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres create source table: {e}")))?;
    sqlx::query(&format!(
        "INSERT INTO \"{table}\" (id, name) VALUES (1, 'a'), (2, 'b')"
    ))
    .execute(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres insert: {e}")))?;

    #[derive(Deserialize)]
    struct SrcRow {
        id: i64,
        name: String,
    }

    let mut items: Vec<(cocoindex::StableKey, SrcRow)> =
        postgres::read_table_items(&db, &table, |row: &SrcRow| row.id).await?;
    items.sort_by_key(|(_, row)| row.id);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].0, cocoindex::StableKey::Int(1));
    assert_eq!(items[0].1.name, "a");
    assert_eq!(items[1].0, cocoindex::StableKey::Int(2));
    assert_eq!(items[1].1.name, "b");

    sqlx::query(&format!("DROP TABLE \"{table}\""))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres drop source table: {e}")))?;
    Ok(())
}

#[tokio::test]
async fn postgres_vector_index_target_reconciles_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres vector-index test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_vector_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    if let Err(e) = sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(db.pool())
        .await
    {
        eprintln!("skipping live Postgres vector-index test; pgvector is unavailable: {e}");
        return Ok(());
    }
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresVectorIndexE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    app.run({
        let schema = schema.clone();
        move |ctx| declare_vector_table(ctx, schema, true)
    })
    .await?;
    assert!(table_exists(&db, &schema, "vectors").await?);
    assert!(index_exists(&db, &schema, "vectors__vector__embedding_idx").await?);

    app.run({
        let schema = schema.clone();
        move |ctx| declare_vector_table(ctx, schema, false)
    })
    .await?;
    assert!(
        !index_exists(&db, &schema, "vectors__vector__embedding_idx").await?,
        "undeclared vector index should be dropped"
    );
    assert!(table_exists(&db, &schema, "vectors").await?);

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

#[tokio::test]
async fn postgres_strips_nul_from_text_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres NUL test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_nul_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresNulE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    // A label containing a U+0000 NUL — Postgres `text` cannot store it, so it
    // must be stripped before insert (otherwise the write errors).
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_rows(
                ctx,
                schema,
                vec![TestRow {
                    id: 1,
                    label: "a\u{0}b".to_string(),
                }],
            )
        }
    })
    .await?;

    let label: String = sqlx::query_scalar(&format!(
        "SELECT label FROM \"{schema}\".\"rows\" WHERE id = 1"
    ))
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres query: {e}")))?;
    assert_eq!(label, "ab", "NUL should be stripped from the stored text");

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

async fn declare_cols(
    ctx: Ctx,
    schema_ns: String,
    columns: Vec<(String, String)>,
    row: serde_json::Value,
) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let cols: Vec<(String, postgres::ColumnDef)> = columns
        .into_iter()
        .map(|(n, t)| (n, postgres::ColumnDef::new(t)))
        .collect();
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "rows",
        postgres::TableSchema::new(cols, ["id"])?,
        Some(&schema_ns),
    )
    .await?;
    table.declare_row(&ctx, &row)?;
    Ok(())
}

async fn column_exists(
    db: &postgres::Database,
    schema: &str,
    table: &str,
    col: &str,
) -> Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
    )
    .bind(schema)
    .bind(table)
    .bind(col)
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres column lookup: {e}")))?;
    Ok(count > 0)
}

fn cols(names: &[(&str, &str)]) -> Vec<(String, String)> {
    names
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

#[tokio::test]
async fn postgres_adds_and_drops_columns_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres schema-evolution test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_evolve_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresEvolveE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    // v1: (id, label)
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_cols(
                ctx,
                schema,
                cols(&[("id", "bigint"), ("label", "text")]),
                serde_json::json!({ "id": 1, "label": "a" }),
            )
        }
    })
    .await?;
    assert!(!column_exists(&db, &schema, "rows", "extra").await?);

    // v2: add the `extra` column.
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_cols(
                ctx,
                schema,
                cols(&[("id", "bigint"), ("label", "text"), ("extra", "text")]),
                serde_json::json!({ "id": 1, "label": "a", "extra": "x" }),
            )
        }
    })
    .await?;
    assert!(column_exists(&db, &schema, "rows", "extra").await?);
    let extra: Option<String> = sqlx::query_scalar(&format!(
        "SELECT extra FROM \"{schema}\".\"rows\" WHERE id = 1"
    ))
    .fetch_one(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres query: {e}")))?;
    assert_eq!(extra.as_deref(), Some("x"));

    // v3: drop the `extra` column.
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_cols(
                ctx,
                schema,
                cols(&[("id", "bigint"), ("label", "text")]),
                serde_json::json!({ "id": 1, "label": "a" }),
            )
        }
    })
    .await?;
    assert!(!column_exists(&db, &schema, "rows", "extra").await?);

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

#[tokio::test]
async fn postgres_column_drop_retries_after_failed_attempt_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres column-drop-retry test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_dropretry_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresDropRetryE2ETest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    // v1: table with (id, label, extra).
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_cols(
                ctx,
                schema,
                cols(&[("id", "bigint"), ("label", "text"), ("extra", "text")]),
                serde_json::json!({ "id": 1, "label": "a", "extra": "x" }),
            )
        }
    })
    .await?;

    // A view that depends on `extra` — this blocks DROP COLUMN.
    sqlx::query(&format!(
        "CREATE VIEW \"{schema}\".\"dep\" AS SELECT id, extra FROM \"{schema}\".\"rows\""
    ))
    .execute(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres create view: {e}")))?;

    // v2: drop `extra` — fails because the view depends on it.
    let failed = app
        .run({
            let schema = schema.clone();
            move |ctx| {
                declare_cols(
                    ctx,
                    schema,
                    cols(&[("id", "bigint"), ("label", "text")]),
                    serde_json::json!({ "id": 1, "label": "a" }),
                )
            }
        })
        .await;
    assert!(
        failed.is_err(),
        "DROP COLUMN blocked by a view should error"
    );
    assert!(
        column_exists(&db, &schema, "rows", "extra").await?,
        "the column must still exist after the failed drop"
    );

    // Remove the dependency, then retry — the drop now succeeds.
    sqlx::query(&format!("DROP VIEW \"{schema}\".\"dep\""))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres drop view: {e}")))?;
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_cols(
                ctx,
                schema,
                cols(&[("id", "bigint"), ("label", "text")]),
                serde_json::json!({ "id": 1, "label": "a" }),
            )
        }
    })
    .await?;
    assert!(
        !column_exists(&db, &schema, "rows", "extra").await?,
        "retry after removing the dependency should drop the column"
    );

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

#[derive(Clone, Serialize)]
struct BlobRow {
    id: i64,
    data: Vec<u8>,
}

async fn declare_blobs(ctx: Ctx, schema: String, rows: Vec<BlobRow>) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "blobs",
        postgres::TableSchema::new(
            [
                ("id", postgres::ColumnDef::new("bigint")),
                ("data", postgres::ColumnDef::new("bytea")),
            ],
            ["id"],
        )?,
        Some(&schema),
    )
    .await?;
    for row in &rows {
        table.declare_row(&ctx, row)?;
    }
    Ok(())
}

#[tokio::test]
async fn postgres_bytea_round_trips_byte_arrays_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres bytea test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_bytea_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresByteaTest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_blobs(
                ctx,
                schema,
                vec![
                    BlobRow {
                        id: 1,
                        data: vec![0u8, 1, 2, 255, 104, 105],
                    },
                    BlobRow {
                        id: 2,
                        data: Vec::new(),
                    },
                ],
            )
        }
    })
    .await?;

    let rows = sqlx::query(&format!(
        "SELECT id, data FROM \"{schema}\".\"blobs\" ORDER BY id"
    ))
    .fetch_all(db.pool())
    .await
    .map_err(|e| cocoindex::Error::engine(format!("postgres query: {e}")))?;
    let actual = rows
        .into_iter()
        .map(|row| {
            let id: i64 = row.try_get("id").unwrap();
            let data: Vec<u8> = row.try_get("data").unwrap();
            (id, data)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![(1, vec![0u8, 1, 2, 255, 104, 105]), (2, Vec::new())],
        "bytea byte arrays must round-trip exactly (hex-encoded literal)"
    );

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}

async fn declare_evo_table(
    ctx: Ctx,
    schema: String,
    cols: Vec<(&'static str, &'static str)>,
    pk: Vec<&'static str>,
    rows: Vec<serde_json::Value>,
) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        "evo",
        postgres::TableSchema::new(
            cols.into_iter()
                .map(|(n, t)| (n.to_string(), postgres::ColumnDef::new(t))),
            pk.into_iter().map(|s| s.to_string()),
        )?,
        Some(&schema),
    )
    .await?;
    for r in &rows {
        table.declare_row(&ctx, r)?;
    }
    Ok(())
}

async fn pg_col_type(db: &postgres::Database, schema: &str, col: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT data_type FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'evo' AND column_name = $2",
    )
    .bind(schema)
    .bind(col)
    .fetch_optional(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn postgres_schema_evolution_retype_and_pk_change_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres schema-evolution test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_evo_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("PostgresEvoTest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&PG, db.clone())
        .build()
        .await?;

    // v1: score is bigint.
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_evo_table(
                ctx,
                schema,
                vec![("id", "bigint"), ("name", "text"), ("score", "bigint")],
                vec!["id"],
                vec![
                    serde_json::json!({"id": 1, "name": "a", "score": 10}),
                    serde_json::json!({"id": 2, "name": "b", "score": 20}),
                ],
            )
        }
    })
    .await?;
    assert_eq!(
        pg_col_type(&db, &schema, "score").await.as_deref(),
        Some("bigint")
    );

    // v2: retype score bigint -> double precision (in-place ALTER, rows preserved).
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_evo_table(
                ctx,
                schema,
                vec![
                    ("id", "bigint"),
                    ("name", "text"),
                    ("score", "double precision"),
                ],
                vec!["id"],
                vec![
                    serde_json::json!({"id": 1, "name": "a", "score": 10.0}),
                    serde_json::json!({"id": 2, "name": "b", "score": 20.0}),
                ],
            )
        }
    })
    .await?;
    assert_eq!(
        pg_col_type(&db, &schema, "score").await.as_deref(),
        Some("double precision"),
        "score column should be retyped in place"
    );
    let scores: Vec<f64> = sqlx::query_scalar(&format!(
        "SELECT score FROM \"{schema}\".\"evo\" ORDER BY id"
    ))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(scores, vec![10.0, 20.0], "retype must preserve row data");

    // v3: change the primary key id -> name. This forces a destructive recreate;
    // rows are replayed via the Destructive child invalidation.
    app.run({
        let schema = schema.clone();
        move |ctx| {
            declare_evo_table(
                ctx,
                schema,
                vec![
                    ("id", "bigint"),
                    ("name", "text"),
                    ("score", "double precision"),
                ],
                vec!["name"],
                vec![
                    serde_json::json!({"id": 1, "name": "a", "score": 10.0}),
                    serde_json::json!({"id": 2, "name": "b", "score": 20.0}),
                ],
            )
        }
    })
    .await?;
    // The PK is now `name` (table was dropped + recreated), and rows are present.
    let pk_col: Option<String> = sqlx::query_scalar(
        "SELECT a.attname FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = ($1 || '.evo')::regclass AND i.indisprimary",
    )
    .bind(&schema)
    .fetch_optional(db.pool())
    .await
    .unwrap();
    assert_eq!(
        pk_col.as_deref(),
        Some("name"),
        "primary key should now be `name`"
    );
    let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{schema}\".\"evo\""))
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "rows should be replayed after the destructive recreate"
    );

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}
