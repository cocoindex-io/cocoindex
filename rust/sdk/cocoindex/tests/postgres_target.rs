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
    )?;
    for row in &rows {
        table.declare_row(&ctx, row)?;
    }
    Ok(())
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

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(|e| cocoindex::Error::engine(format!("postgres cleanup: {e}")))?;
    Ok(())
}
