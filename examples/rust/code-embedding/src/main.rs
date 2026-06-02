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
//! What the Rust SDK provides vs. what this example hand-rolls:
//!   - walk / memo / mount_each / ContextKey      -> from `cocoindex`
//!   - tree-sitter chunking + language detection  -> from `cocoindex_ops_text` (engine crate)
//!   - embeddings (all-MiniLM-L6-v2, local ONNX)  -> `fastembed` (no Rust embedder in the SDK)
//!   - Postgres/pgvector table + incremental sync -> hand-rolled via `sqlx` (no DB target in the SDK)
//!
//! Note on incremental deletes: the Python `TableTarget` auto-removes rows when a
//! source disappears. The Rust SDK has no declarative table target, so we reconcile
//! manually: per file we delete chunk-rows that vanished, and after the walk we
//! delete rows for files that no longer exist.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use cocoindex::prelude::*;
use cocoindex::walk;
use cocoindex_ops_text::prog_langs::detect_language;
use cocoindex_ops_text::split::{RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use pgvector::Vector;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "code_embeddings";
const TOP_K: i64 = 5;

const INCLUDE_PATTERNS: &[&str] = &["**/*.py", "**/*.rs", "**/*.toml", "**/*.md", "**/*.mdx"];

/// Shared Postgres pool. Plain key (a pool is not change-tracked).
static DB: LazyLock<ContextKey<PgPool>> = LazyLock::new(|| ContextKey::new("code_embedding_db"));

/// Shared embedder. `new_with_state` tracks the model name, so changing the model
/// invalidates memoized files — the parity for Python's
/// `ContextKey(..., detect_change=True)` + `Annotated[NDArray, EMBEDDER]`.
static EMBEDDER: LazyLock<ContextKey<Embedder>> =
    LazyLock::new(|| ContextKey::new_with_state("embedder", |e: &Embedder| e.model_name.clone()));

// ---------------------------------------------------------------------------
// Embedder (local fastembed model)
// ---------------------------------------------------------------------------

struct Embedder {
    model: Arc<TextEmbedding>,
    model_name: String,
}

impl Embedder {
    fn load(model_name: &str) -> Result<Self> {
        let model = TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2))
            .map_err(|e| Error::engine(format!("failed to load embedding model: {e}")))?;
        Ok(Self {
            model: Arc::new(model),
            model_name: model_name.to_string(),
        })
    }

    /// Embed a batch of texts in one call (fastembed batches internally). The
    /// blocking ONNX work runs off the async runtime.
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || model.embed(texts, None))
            .await
            .map_err(|e| Error::engine(format!("embedding task panicked: {e}")))?
            .map_err(|e| Error::engine(format!("embedding failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Process one file: chunk -> embed -> upsert rows, then drop rows for chunks
/// that disappeared. Memoized by the file's fingerprint, so unchanged files are
/// skipped entirely on later runs.
#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<usize> {
    let filename = file.key();
    let text = file.content_str()?;

    let language = detect_language(&filename).map(str::to_string);
    let chunker = RecursiveChunker::new(RecursiveSplitConfig::default())
        .map_err(|e| Error::engine(format!("chunker init: {e}")))?;
    let chunks = chunker.split(
        &text,
        RecursiveChunkConfig {
            chunk_size: 1000,
            min_chunk_size: Some(300),
            chunk_overlap: Some(300),
            language,
        },
    );

    let pool = ctx.get_key(&DB)?;
    if chunks.is_empty() {
        // No chunks now — make sure no stale rows linger for this file.
        delete_file_rows(pool, &filename).await?;
        return Ok(0);
    }

    let embedder = ctx.get_key(&EMBEDDER)?;
    let codes: Vec<String> = chunks
        .iter()
        .map(|c| {
            text.get(c.range.start..c.range.end)
                .unwrap_or("")
                .to_string()
        })
        .collect();
    let embeddings = embedder.embed(codes.clone()).await?;

    let mut id_gen = IdGenerator::new();
    let mut ids: Vec<i64> = Vec::with_capacity(chunks.len());
    for ((chunk, code), embedding) in chunks.iter().zip(codes.iter()).zip(embeddings) {
        let id = id_gen.next_id(&ctx, code).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        ids.push(id);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"{PG_SCHEMA}\".\"{TABLE}\" \
             (id, filename, code, embedding, start_line, end_line) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET \
               filename = EXCLUDED.filename, code = EXCLUDED.code, \
               embedding = EXCLUDED.embedding, start_line = EXCLUDED.start_line, \
               end_line = EXCLUDED.end_line"
        )))
        .bind(id)
        .bind(&filename)
        .bind(code)
        .bind(Vector::from(embedding))
        .bind(chunk.start.line as i32)
        .bind(chunk.end.line as i32)
        .execute(pool)
        .await
        .map_err(db_err)?;
    }

    // Delete rows for chunks that no longer exist in this file.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM \"{PG_SCHEMA}\".\"{TABLE}\" WHERE filename = $1 AND id <> ALL($2)"
    )))
    .bind(&filename)
    .bind(&ids)
    .execute(pool)
    .await
    .map_err(db_err)?;

    Ok(chunks.len())
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let pool = ctx.get_key(&DB)?;
    ensure_schema(pool).await?;

    let files: Vec<FileEntry> = walk(&sourcedir, INCLUDE_PATTERNS)?
        .into_iter()
        .filter(|f| !is_excluded(&f.key()))
        .collect();
    let seen: Vec<String> = files.iter().map(FileEntry::key).collect();
    println!(
        "indexing {} files from {}",
        files.len(),
        sourcedir.display()
    );

    let counts = ctx
        .mount_each(
            files,
            |f| f.key(),
            |child, file| async move { process_file(&child, &file).await },
        )
        .await?;

    // Reconcile: drop rows for files that no longer exist (the part Python's
    // declarative TableTarget would do automatically).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM \"{PG_SCHEMA}\".\"{TABLE}\" WHERE filename <> ALL($1)"
    )))
    .bind(&seen)
    .execute(pool)
    .await
    .map_err(db_err)?;

    println!("indexed {} chunks total", counts.iter().sum::<usize>());
    Ok(())
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

async fn query_once(pool: &PgPool, embedder: &Embedder, query: &str) -> Result<()> {
    let mut vecs = embedder.embed(vec![query.to_string()]).await?;
    let query_vec = Vector::from(vecs.remove(0));

    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT filename, code, start_line, end_line, embedding <=> $1 AS distance \
         FROM \"{PG_SCHEMA}\".\"{TABLE}\" ORDER BY distance ASC LIMIT $2"
    )))
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

async fn ensure_schema(pool: &PgPool) -> Result<()> {
    for stmt in [
        "CREATE EXTENSION IF NOT EXISTS vector".to_string(),
        format!("CREATE SCHEMA IF NOT EXISTS \"{PG_SCHEMA}\""),
        format!(
            "CREATE TABLE IF NOT EXISTS \"{PG_SCHEMA}\".\"{TABLE}\" (\
               id BIGINT PRIMARY KEY, \
               filename TEXT NOT NULL, \
               code TEXT NOT NULL, \
               embedding vector({EMBED_DIM}) NOT NULL, \
               start_line INT NOT NULL, \
               end_line INT NOT NULL)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {TABLE}_embedding_idx \
             ON \"{PG_SCHEMA}\".\"{TABLE}\" USING hnsw (embedding vector_cosine_ops)"
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt))
            .execute(pool)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn delete_file_rows(pool: &PgPool, filename: &str) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM \"{PG_SCHEMA}\".\"{TABLE}\" WHERE filename = $1"
    )))
    .bind(filename)
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
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

async fn load_embedder() -> Result<Embedder> {
    tokio::task::spawn_blocking(|| Embedder::load(EMBED_MODEL))
        .await
        .map_err(|e| Error::engine(format!("embedder load task panicked: {e}")))?
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

            let pool = connect_pool().await?;
            let embedder = load_embedder().await?;
            let app = App::builder("CodeEmbeddingRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, pool)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
