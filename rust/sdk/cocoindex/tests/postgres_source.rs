//! Live-Postgres integration test for the `postgres::read_table` source.
//!
//! Skips gracefully when `POSTGRES_URL` is unset. Run with:
//!   POSTGRES_URL=postgres://cocoindex:cocoindex@localhost:5544/cocoindex \
//!     cargo test -p cocoindex --features postgres --test postgres_source
#![cfg(feature = "postgres")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::connectors::postgres;
use cocoindex::{Ctx, Environment, Result};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize};

cocoindex::context_key!(
    static DB: postgres::Database, key = "postgres_source_test_db"
);
static CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Serialize, Deserialize, cocoindex::MemoInput)]
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
        let mut rows: Vec<SourceRow> = postgres::read_table(&db, &src_table)
            .await?
            .try_collect()
            .await?;
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
        .await?
        .try_collect()
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
        .await?
        .try_collect()
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
        // The read starts (the table exists); the unsupported column fails
        // the stream at its first row.
        let err = postgres::read_table::<serde_json::Value>(&db, &unsupported_table)
            .await?
            .try_collect::<Vec<_>>()
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
                let app = Environment::builder()
                    .db_path(&db_path)
                    .provide_key(&DB, db.clone())
                    .build()
                    .await
                    .unwrap()
                    .app("PgSourceTest")
                    .await
                    .unwrap();
                app.run(move |ctx| {
                    let src_table = src_table.clone();
                    let out_schema_name = out_schema_name.clone();
                    async move {
                        let db = ctx.get_key(&DB)?;
                        let target = postgres::mount_table_target(
                            &ctx,
                            &DB,
                            "output",
                            out_schema(),
                            Some(&out_schema_name),
                        )
                        .await?;
                        let rows: Vec<SourceRow> = postgres::read_table(db, &src_table)
                            .await?
                            .try_collect()
                            .await?;
                        let outs = ctx
                            .use_mount_each(
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

#[derive(Debug, Deserialize)]
struct IdRow {
    id: i64,
    v: Option<f64>,
}

async fn stream_test_table(test: &str) -> Result<Option<(postgres::Database, String, String)>> {
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        eprintln!("skipping live Postgres stream test; POSTGRES_URL is not set");
        return Ok(None);
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("cocoindex_rust_stream_{test}_{nonce}");
    let db = postgres::Database::connect(&url).await?;
    let pe = |e: sqlx::Error| cocoindex::Error::engine(format!("postgres: {e}"));
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(db.pool())
        .await
        .map_err(pe)?;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".\"big\" AS SELECT g::bigint AS id, g::float8 AS v \
         FROM generate_series(1, 20000) g"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    Ok(Some((db, schema, url)))
}

/// A stream over a large table reads one snapshot: rows inserted, updated and
/// deleted by another connection while it reads don't show up in it.
#[tokio::test]
async fn read_table_streams_a_snapshot_under_concurrent_writes() -> Result<()> {
    let Some((db, schema, _)) = stream_test_table("snapshot").await? else {
        return Ok(());
    };
    let pe = |e: sqlx::Error| cocoindex::Error::engine(format!("postgres: {e}"));
    let mut stream = Box::pin(
        postgres::read_table_with_options::<IdRow>(
            &db,
            "big",
            postgres::ReadTableOptions::new().pg_schema_name(&schema),
        )
        .await?,
    );
    let first = stream.try_next().await?.expect("a first row");
    let mut ids = vec![first.id];
    let mut sum = first.v.unwrap_or_default();
    sqlx::raw_sql(&format!(
        "DELETE FROM \"{schema}\".\"big\" WHERE id % 2 = 0; \
         UPDATE \"{schema}\".\"big\" SET v = -1; \
         INSERT INTO \"{schema}\".\"big\" SELECT g, 0 FROM generate_series(20001, 30000) g"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    while let Some(row) = stream.try_next().await? {
        ids.push(row.id);
        sum += row.v.unwrap_or_default();
    }
    ids.sort_unstable();
    assert_eq!(ids, (1..=20000).collect::<Vec<i64>>());
    assert_eq!(sum, (1..=20000).map(|i| i as f64).sum::<f64>());
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(pe)?;
    Ok(())
}

/// Dropping a stream part way releases its connection: a pool of one
/// connection serves the next query, and the next stream, at once.
#[tokio::test]
async fn dropping_a_read_stream_releases_its_connection() -> Result<()> {
    let Some((direct, schema, url)) = stream_test_table("drop").await? else {
        return Ok(());
    };
    let pe = |e: sqlx::Error| cocoindex::Error::engine(format!("postgres: {e}"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&url)
        .await
        .map_err(pe)?;
    let db = postgres::Database::from_pool("one_connection", pool);
    for _ in 0..3 {
        let mut stream = Box::pin(
            postgres::read_table_with_options::<IdRow>(
                &db,
                "big",
                postgres::ReadTableOptions::new().pg_schema_name(&schema),
            )
            .await?,
        );
        assert!(stream.try_next().await?.is_some());
        drop(stream);
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(db.pool())
            .await
            .map_err(pe)?;
        assert_eq!(one, 1);
    }
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(direct.pool())
        .await
        .map_err(pe)?;
    Ok(())
}

/// A row that doesn't match the row type ends the stream with an error
/// naming the problem (no panic); the stream yields nothing after it.
#[tokio::test]
async fn read_table_reports_rows_that_do_not_match_the_type() -> Result<()> {
    let Some((db, schema, _)) = stream_test_table("mismatch").await? else {
        return Ok(());
    };
    let pe = |e: sqlx::Error| cocoindex::Error::engine(format!("postgres: {e}"));
    sqlx::query(&format!(
        "UPDATE \"{schema}\".\"big\" SET v = 'NaN' WHERE id = 5"
    ))
    .execute(db.pool())
    .await
    .map_err(pe)?;
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Strict {
        id: i64,
        v: f64,
    }
    let rows: Vec<Result<Strict>> = postgres::read_table_with_options::<Strict>(
        &db,
        "big",
        postgres::ReadTableOptions::new().pg_schema_name(&schema),
    )
    .await?
    .collect()
    .await;
    let errors: Vec<String> = rows
        .iter()
        .filter_map(|r| r.as_ref().err().map(ToString::to_string))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("does not match"), "{}", errors[0]);
    assert!(rows.last().unwrap().is_err(), "the error ends the stream");
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(db.pool())
        .await
        .map_err(pe)?;
    Ok(())
}
