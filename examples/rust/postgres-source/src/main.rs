//! Postgres Source — Rust port of the Python `postgres_source` example.
//!
//! Reads product rows from a Postgres source table, computes a derived field
//! (`total_value`) and an embedding per row, and writes the result into another
//! Postgres table (pgvector) via the declarative `TableTarget`.
//!
//!   cargo run -- index                 # read source -> embed -> write output (incremental)
//!   cargo run -- query "headphones"    # pgvector similarity search over the output
//!
//! Parallels the Python example:
//!   - source rows: `postgres::read_table::<SourceProduct>(db, "source_products")`
//!     (parity for Python's `PgTableSource(...).fetch_rows()`)
//!   - per-row compute (memo) : `#[cocoindex::function(memo)]` (skips unchanged rows)
//!   - output store           : `postgres::TableTarget` + `declare_vector_index`
//!   - embeddings             : `SentenceTransformerEmbedder` all-MiniLM-L6-v2 (same model as Python)
//!
//! Incrementality: unchanged source rows are memo-skipped; rows deleted from the
//! source have their derived output rows reconciled away automatically.

use std::sync::LazyLock;

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::postgres;
use cocoindex::prelude::*;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples_v1";
const TABLE: &str = "output";
const TOP_K: i64 = 5;

/// Target database.
static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("postgres_source_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
/// Source database. Defaults to the target URL, but can point elsewhere via
/// `SOURCE_DATABASE_URL`, matching the Python example.
static SOURCE_DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("source_pool", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

// ---------------------------------------------------------------------------
// Data models
// ---------------------------------------------------------------------------

/// One row of the `source_products` source table (extra columns are ignored).
#[derive(Clone, Serialize, Deserialize)]
struct SourceProduct {
    product_category: String,
    product_name: String,
    description: String,
    price: f64,
    amount: i64,
}

/// One row written to the output table.
#[derive(Clone, Serialize, Deserialize)]
struct OutputProduct {
    product_category: String,
    product_name: String,
    description: String,
    price: f64,
    amount: i64,
    total_value: f64,
    embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Compute the derived fields + embedding for one source row. Memoized by the
/// row content, so unchanged rows skip the embedding work on re-runs.
#[cocoindex::function(memo)]
async fn process_product(ctx: &Ctx, product: &SourceProduct) -> Result<OutputProduct> {
    let full_description = format!(
        "Category: {}\nName: {}\n\n{}",
        product.product_category, product.product_name, product.description
    );
    let total_value = product.price * product.amount as f64;
    let embedding = ctx.get_key(&EMBEDDER)?.embed(&full_description).await?;
    Ok(OutputProduct {
        product_category: product.product_category.clone(),
        product_name: product.product_name.clone(),
        description: product.description.clone(),
        price: product.price,
        amount: product.amount,
        total_value,
        embedding,
    })
}

fn output_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("product_category", postgres::ColumnDef::new("text")),
            ("product_name", postgres::ColumnDef::new("text")),
            ("description", postgres::ColumnDef::new("text")),
            ("price", postgres::ColumnDef::new("double precision")),
            ("amount", postgres::ColumnDef::new("bigint")),
            ("total_value", postgres::ColumnDef::new("double precision")),
            (
                "embedding",
                postgres::ColumnDef::new(format!("vector({EMBED_DIM})")),
            ),
        ],
        ["product_category", "product_name"],
    )
}

async fn app_main(ctx: Ctx) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let target = postgres::mount_table_target(&ctx, db, TABLE, output_schema()?, Some(PG_SCHEMA)).await?;
    target.declare_vector_index(
        &ctx,
        "embedding",
        postgres::VectorIndexOptions {
            method: "hnsw",
            ..Default::default()
        },
    )?;

    let source_db = ctx.get_key(&SOURCE_DB)?;
    let products: Vec<SourceProduct> = postgres::read_table(source_db, "source_products").await?;
    println!("read {} source product(s)", products.len());

    let outputs = ctx
        .mount_each(
            products,
            |p| format!("{}|{}", p.product_category, p.product_name),
            |child, product| async move { process_product(&child, &product).await },
        )
        .await?;

    for output in &outputs {
        target.declare_row(&ctx, output)?;
    }
    println!("wrote {} output row(s)", outputs.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// Query (pgvector similarity)
// ---------------------------------------------------------------------------

async fn query(pool: &PgPool, embedder: &SentenceTransformerEmbedder, q: &str) -> Result<()> {
    let vec = embedder.embed(q).await?;
    let vec_lit = format!(
        "[{}]",
        vec.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let rows = sqlx::query(&format!(
        "SELECT product_category, product_name, description, amount, total_value, \
            embedding <=> $1::vector AS distance \
         FROM \"{PG_SCHEMA}\".\"{TABLE}\" ORDER BY distance ASC LIMIT $2"
    ))
    .bind(vec_lit)
    .bind(TOP_K)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    println!("Top {} matches for {q:?}:", rows.len());
    println!("{}", "-".repeat(60));
    for r in &rows {
        let category: String = r.try_get("product_category").map_err(db_err)?;
        let name: String = r.try_get("product_name").map_err(db_err)?;
        let description: String = r.try_get("description").map_err(db_err)?;
        let amount: i64 = r.try_get("amount").map_err(db_err)?;
        let total_value: f64 = r.try_get("total_value").map_err(db_err)?;
        let distance: f64 = r.try_get("distance").map_err(db_err)?;
        println!(
            "[{:.3}] {category} | {name} | amount={amount} | total={total_value}",
            1.0 - distance
        );
        println!("    {description}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
}

fn source_database_url() -> String {
    std::env::var("SOURCE_DATABASE_URL").unwrap_or_else(|_| database_url())
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("query") => {
            let q = args[1..].join(" ");
            if q.trim().is_empty() {
                eprintln!("usage: cargo run -- query \"search text\"");
                std::process::exit(2);
            }
            let pool = PgPoolOptions::new()
                .connect(&database_url())
                .await
                .map_err(db_err)?;
            let embedder = load_embedder().await?;
            query(&pool, &embedder, &q).await?;
        }
        _ => {
            let db = postgres::Database::connect(&database_url()).await?;
            let source_db = postgres::Database::connect(&source_database_url()).await?;
            let embedder = load_embedder().await?;
            let app = App::builder("PostgresSourceRust")
                .db_path(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&SOURCE_DB, source_db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(app_main).await?;
            println!("{stats}");
        }
    }
    Ok(())
}

async fn load_embedder() -> Result<SentenceTransformerEmbedder> {
    SentenceTransformerEmbedder::load(EMBED_MODEL).await
}
