//! Code Embedding — Rust port of the Python `code_embedding` example.
//!
//! Pipeline: walk -> detect language -> tree-sitter chunk -> embed -> store in pgvector.
//!
//! Index (incremental; unchanged files are skipped via memoization):
//!     cargo run -- index [SOURCE_DIR]
//!
//! Query the index:
//!     cargo run -- query "your query"
//!
//! What the Rust SDK provides:
//!   - walk / memo / mount_each / ContextKey      -> from `cocoindex`
//!   - tree-sitter chunking + language detection  -> `cocoindex::ops::text`
//!   - embeddings (all-MiniLM-L6-v2, local ONNX)  -> `cocoindex::ops::sentence_transformers`
//!   - Postgres/pgvector TableTarget sync          -> from `cocoindex`

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter, detect_code_language};
use cocoindex::prelude::*;
use cocoindex::postgres;
use cocoindex::walk;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "code_embeddings";
const TOP_K: i64 = 5;

const INCLUDE_PATTERNS: &[&str] = &["**/*.py", "**/*.rs", "**/*.toml", "**/*.md", "**/*.mdx"];

/// Shared Postgres target database.
static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("code_embedding_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});

/// Shared embedder. `new_with_state` tracks the model name, so changing the model
/// invalidates memoized files — the parity for Python's
/// `ContextKey(..., detect_change=True)` + `Annotated[NDArray, EMBEDDER]`.
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

#[derive(Clone, Serialize, Deserialize)]
struct CodeEmbeddingRow {
    id: i64,
    filename: String,
    code: String,
    embedding: Vec<f32>,
    start_line: i32,
    end_line: i32,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Process one file into desired embedding rows. Memoized by the file's
/// fingerprint, while target row declarations still happen in the active
/// pipeline.
#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<Vec<CodeEmbeddingRow>> {
    let filename = file.key();
    let text = file.content_str()?;

    let language = detect_code_language(&filename);
    let splitter = RecursiveSplitter::new()?;
    let chunks = splitter.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: 1000,
            min_chunk_size: Some(300),
            chunk_overlap: Some(300),
            language,
        },
    );

    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    let embedder = ctx.get_key(&EMBEDDER)?;
    let codes: Vec<String> = chunks.iter().map(|c| c.text(&text).to_string()).collect();
    let embeddings = embedder.embed_batch(codes.clone()).await?;

    let mut id_gen = IdGenerator::new();
    let mut rows = Vec::with_capacity(chunks.len());
    for ((chunk, code), embedding) in chunks.iter().zip(codes.into_iter()).zip(embeddings) {
        let id = id_gen.next_id(&ctx, &code).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        rows.push(CodeEmbeddingRow {
            id,
            filename: filename.clone(),
            code,
            embedding,
            start_line: chunk.start.line as i32,
            end_line: chunk.end.line as i32,
        });
    }
    Ok(rows)
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table = postgres::mount_table_target(
        &ctx,
        db,
        TABLE,
        code_embedding_schema()?,
        Some(PG_SCHEMA),
    )
    .await?;
    table.declare_vector_index(
        &ctx,
        "embedding",
        postgres::VectorIndexOptions {
            name: Some("embedding".to_string()),
            method: "hnsw",
            ..Default::default()
        },
    )?;

    let files: Vec<FileEntry> = walk(&sourcedir, INCLUDE_PATTERNS)?
        .into_iter()
        .filter(|f| !is_excluded(&f.key()))
        .collect();
    println!(
        "indexing {} files from {}",
        files.len(),
        sourcedir.display()
    );

    let rows_by_file = ctx
        .mount_each(
            files,
            |f| f.key(),
            |child, file| async move { process_file(&child, &file).await },
        )
        .await?;

    let mut count = 0usize;
    for rows in &rows_by_file {
        count += rows.len();
        for row in rows {
            table.declare_row(&ctx, row)?;
        }
    }

    println!("indexed {count} chunks total");
    Ok(())
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

async fn query_once(pool: &PgPool, embedder: &SentenceTransformerEmbedder, query: &str) -> Result<()> {
    let query_vec = vector_param(&embedder.embed(query).await?);

    let rows = sqlx::query(&format!(
        "SELECT filename, code, start_line, end_line, embedding <=> $1::vector AS distance \
         FROM \"{PG_SCHEMA}\".\"{TABLE}\" ORDER BY distance ASC LIMIT $2"
    ))
    .bind(query_vec)
    .bind(TOP_K)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    for row in rows {
        let filename: String = row.try_get("filename").map_err(db_err)?;
        let code: String = row.try_get("code").map_err(db_err)?;
        let start_line: i32 = row.try_get("start_line").map_err(db_err)?;
        let end_line: i32 = row.try_get("end_line").map_err(db_err)?;
        let distance: f64 = row.try_get("distance").map_err(db_err)?;
        let score = 1.0 - distance;
        println!("[{score:.3}] {filename} (L{start_line}-L{end_line})");
        let snippet: String = code.chars().take(200).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema / helpers
// ---------------------------------------------------------------------------

fn code_embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("id", postgres::ColumnDef::new("bigint")),
            ("filename", postgres::ColumnDef::new("text")),
            ("code", postgres::ColumnDef::new("text")),
            (
                "embedding",
                postgres::ColumnDef::new(format!("vector({EMBED_DIM})")),
            ),
            ("start_line", postgres::ColumnDef::new("integer")),
            ("end_line", postgres::ColumnDef::new("integer")),
        ],
        ["id"],
    )
}

fn vector_param(vec: &[f32]) -> String {
    let values = vec.iter().map(f32::to_string).collect::<Vec<_>>();
    format!("[{}]", values.join(","))
}

fn is_excluded(key: &str) -> bool {
    key.split('/')
        .any(|part| part.starts_with('.') || part == "target" || part == "node_modules")
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
}

fn default_sourcedir() -> PathBuf {
    // Index the repository root, like the Python example.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

async fn connect_pool() -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url())
        .await
        .map_err(db_err)
}

async fn connect_target_db() -> Result<postgres::Database> {
    postgres::Database::connect(&database_url()).await
}

async fn load_embedder() -> Result<SentenceTransformerEmbedder> {
    SentenceTransformerEmbedder::load(EMBED_MODEL).await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("query") => {
            let q = args[1..].join(" ");
            if q.trim().is_empty() {
                eprintln!("usage: cargo run -- query \"your search text\"");
                std::process::exit(2);
            }
            let pool = connect_pool().await?;
            let embedder = load_embedder().await?;
            query_once(&pool, &embedder, &q).await?;
        }
        sub => {
            // "index" (or no subcommand) — optional source dir argument.
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let db = connect_target_db().await?;
            let embedder = load_embedder().await?;
            let app = App::builder("CodeEmbeddingRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
