#![cfg(feature = "postgres")]

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::{App, ContextKey, Ctx, Result, postgres};
use serde::Serialize;
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
