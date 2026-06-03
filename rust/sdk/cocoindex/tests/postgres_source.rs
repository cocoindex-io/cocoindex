//! Live-Postgres integration test for the `postgres::read_table` source.
//!
//! Skips gracefully when `POSTGRES_URL` is unset. Run with:
//!   POSTGRES_URL=postgres://cocoindex:cocoindex@localhost:5544/cocoindex \
//!     cargo test -p cocoindex --features postgres --test postgres_source
#![cfg(feature = "postgres")]

use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::{App, ContextKey, Ctx, Result, postgres};
use serde::{Deserialize, Serialize};

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("postgres_source_test_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
static CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Serialize, Deserialize)]
struct SourceRow {
    category: String,
    name: String,
    price: f64,
    amount: i32,
}

#[derive(Clone, Serialize, Deserialize)]
struct OutputRow {
    category: String,
    name: String,
    total_value: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SourceNameOnly {
    name: String,
}

/// Memoized per-row compute.
#[cocoindex::function(memo)]
async fn process(_ctx: &Ctx, row: &SourceRow) -> Result<OutputRow> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(OutputRow {
        category: row.category.clone(),
        name: row.name.clone(),
        total_value: row.price * row.amount as f64,
    })
}

fn out_schema() -> postgres::TableSchema {
    postgres::TableSchema::new(
        [
            ("category", postgres::ColumnDef::new("text")),
            ("name", postgres::ColumnDef::new("text")),
            ("total_value", postgres::ColumnDef::new("double precision")),
        ],
        ["category", "name"],
    )
    .unwrap()
}

#[tokio::test]
async fn postgres_source_reads_processes_and_reconciles_when_available() -> Result<()> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres source test; POSTGRES_URL is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let src_table = format!("src_products_{nonce}");
    let out_schema_name = format!("cocoindex_src_test_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    let pe = |e: sqlx::Error| cocoindex::Error::engine(format!("pg: {e}"));

    // --- Source table setup ---
    sqlx::query(&format!("DROP TABLE IF EXISTS \"{src_table}\""))
        .execute(db.pool())
        .await
        .map_err(pe)?;
    sqlx::query(&format!(
        "CREATE TABLE \"{src_table}\" (category text NOT NULL, name text NOT NULL, \
         price double precision, amount integer, modified_time timestamp NOT NULL DEFAULT now(), \
         PRIMARY KEY (category, name))"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    sqlx::query(&format!(
        "INSERT INTO \"{src_table}\" (category, name, price, amount) VALUES \
         ('Electronics','Headphones',100.0,2), ('Books','Rust Book',40.0,3)"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    sqlx::query(&format!(
        "DROP SCHEMA IF EXISTS \"{out_schema_name}\" CASCADE"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;

    // --- T1: typed read returns correct values + types ---
    {
        let mut rows: Vec<SourceRow> = postgres::read_table(&db, &src_table).await?;
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Headphones");
        assert_eq!(rows[0].price, 100.0);
        assert_eq!(rows[0].amount, 2);
    }
    {
        let mut rows: Vec<SourceNameOnly> = postgres::read_table_with_options(
            &db,
            &src_table,
            postgres::ReadTableOptions::new().columns(["name"]),
        )
        .await?;
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            rows,
            vec![
                SourceNameOnly {
                    name: "Headphones".to_string()
                },
                SourceNameOnly {
                    name: "Rust Book".to_string()
                }
            ]
        );
    }
    {
        let src_schema = format!("cocoindex_src_schema_{nonce}");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{src_schema}\" CASCADE"))
            .execute(db.pool())
            .await
            .map_err(pe)?;
        sqlx::query(&format!("CREATE SCHEMA \"{src_schema}\""))
            .execute(db.pool())
            .await
            .map_err(pe)?;
        sqlx::query(&format!(
            "CREATE TABLE \"{src_schema}\".\"source_names\" (name text PRIMARY KEY)"
        ))
        .execute(db.pool())
        .await
        .map_err(pe)?;
        sqlx::query(&format!(
            "INSERT INTO \"{src_schema}\".\"source_names\" (name) VALUES ('Schema Row')"
        ))
        .execute(db.pool())
        .await
        .map_err(pe)?;
        let rows: Vec<SourceNameOnly> = postgres::read_table_with_options(
            &db,
            "source_names",
            postgres::ReadTableOptions::new().pg_schema_name(src_schema.clone()),
        )
        .await?;
        assert_eq!(
            rows,
            vec![SourceNameOnly {
                name: "Schema Row".to_string()
            }]
        );
        sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{src_schema}\" CASCADE"))
            .execute(db.pool())
            .await
            .map_err(pe)?;
    }
    {
        let unsupported_table = format!("src_unsupported_{nonce}");
        sqlx::query(&format!("DROP TABLE IF EXISTS \"{unsupported_table}\""))
            .execute(db.pool())
            .await
            .map_err(pe)?;
        sqlx::query(&format!(
            "CREATE TABLE \"{unsupported_table}\" (id integer PRIMARY KEY, payload jsonb)"
        ))
        .execute(db.pool())
        .await
        .map_err(pe)?;
        sqlx::query(&format!(
            "INSERT INTO \"{unsupported_table}\" (id, payload) VALUES (1, '{{\"x\": 1}}')"
        ))
        .execute(db.pool())
        .await
        .map_err(pe)?;
        let err = postgres::read_table::<serde_json::Value>(&db, &unsupported_table)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported Postgres source column type"),
            "{err}"
        );
        sqlx::query(&format!("DROP TABLE IF EXISTS \"{unsupported_table}\""))
            .execute(db.pool())
            .await
            .map_err(pe)?;
    }

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join(".cocoindex_db");

    // Run the source→target pipeline once.
    let run = {
        let db = db.clone();
        let src_table = src_table.clone();
        let out_schema_name = out_schema_name.clone();
        move || {
            let db = db.clone();
            let src_table = src_table.clone();
            let out_schema_name = out_schema_name.clone();
            let db_path = db_path.clone();
            async move {
                let app = App::builder("PgSourceTest")
                    .db_path(&db_path)
                    .provide_key(&DB, db.clone())
                    .build()
                    .await
                    .unwrap();
                app.run(move |ctx| {
                    let src_table = src_table.clone();
                    let out_schema_name = out_schema_name.clone();
                    async move {
                        let db = ctx.get_key(&DB)?;
                        let target = postgres::mount_table_target(
                            &ctx,
                            db,
                            "output",
                            out_schema(),
                            Some(&out_schema_name),
                        )
                        .await?;
                        let rows: Vec<SourceRow> = postgres::read_table(db, &src_table).await?;
                        let outs = ctx
                            .mount_each(
                                rows,
                                |r| format!("{}|{}", r.category, r.name),
                                |child, r| async move { process(&child, &r).await },
                            )
                            .await?;
                        for o in &outs {
                            target.declare_row(&ctx, o)?;
                        }
                        Ok(())
                    }
                })
                .await
                .unwrap();
            }
        }
    };

    let out_count = |db: postgres::Database, schema: String| async move {
        let row: (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"{schema}\".\"output\""))
            .fetch_one(db.pool())
            .await
            .unwrap();
        row.0
    };
    let out_total = |db: postgres::Database, schema: String, name: String| async move {
        let row: (f64,) = sqlx::query_as(&format!(
            "SELECT total_value FROM \"{schema}\".\"output\" WHERE name = $1"
        ))
        .bind(name)
        .fetch_one(db.pool())
        .await
        .unwrap();
        row.0
    };

    // --- T2: first run processes all rows and writes outputs ---
    CALLS.store(0, Ordering::SeqCst);
    run().await;
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        2,
        "both rows processed on first run"
    );
    assert_eq!(out_count(db.clone(), out_schema_name.clone()).await, 2);
    assert_eq!(
        out_total(db.clone(), out_schema_name.clone(), "Headphones".into()).await,
        200.0
    );

    // --- T3/T5: unchanged source → memo skips processing ---
    CALLS.store(0, Ordering::SeqCst);
    run().await;
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "unchanged rows are memo-skipped"
    );
    assert_eq!(out_count(db.clone(), out_schema_name.clone()).await, 2);

    // --- T4: change a source row → that row is reprocessed and output updated ---
    sqlx::query(&format!(
        "UPDATE \"{src_table}\" SET price = 150.0 WHERE name = 'Headphones'"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    CALLS.store(0, Ordering::SeqCst);
    run().await;
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        1,
        "only the changed row is reprocessed"
    );
    assert_eq!(
        out_total(db.clone(), out_schema_name.clone(), "Headphones".into()).await,
        300.0,
        "changed row's derived output is updated"
    );

    // --- T6: delete a source row → its output is reconciled away ---
    sqlx::query(&format!(
        "DELETE FROM \"{src_table}\" WHERE name = 'Rust Book'"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    run().await;
    assert_eq!(
        out_count(db.clone(), out_schema_name.clone()).await,
        1,
        "deleted source row's output must be reconciled away"
    );

    // --- cleanup ---
    sqlx::query(&format!("DROP TABLE IF EXISTS \"{src_table}\""))
        .execute(db.pool())
        .await
        .map_err(pe)?;
    sqlx::query(&format!(
        "DROP SCHEMA IF EXISTS \"{out_schema_name}\" CASCADE"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    Ok(())
}
