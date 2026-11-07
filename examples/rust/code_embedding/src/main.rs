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
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)
//!   - tree-sitter chunking + language detection  -> `cocoindex::ops::text`
//!   - embeddings (all-MiniLM-L6-v2, local ONNX)  -> `cocoindex::ops::sentence_transformers`
//!   - Postgres/pgvector TableTarget sync          -> from `cocoindex`

use std::path::PathBuf;

use cocoindex::connectors::postgres;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter, detect_code_language};
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "code_embeddings";
const TOP_K: i64 = 5;

const INCLUDE_PATTERNS: &[&str] = &["**/*.py", "**/*.rs", "**/*.toml", "**/*.md", "**/*.mdx"];
const EXCLUDE_PATTERNS: &[&str] = &["**/.*", "**/target", "**/node_modules"];

// Shared Postgres target database.
cocoindex::context_key!(static DB: postgres::Database);

// Shared embedder. The context key tracks the model name, so changing the model
// invalidates memoized files — the parity for Python's
// `ContextKey(..., detect_change=True)` + `Annotated[NDArray, EMBEDDER]`.
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct CodeEmbeddingRow {
    id: i64,
    filename: String,
    code: String,
    #[coco(vector)]
    embedding: Vec<f32>,
    start_line: i32,
    end_line: i32,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Embed one chunk and declare its row (cf. Python `process_chunk`).
#[cocoindex::function]
async fn process_chunk(
    ctx: &Ctx,
    chunk: Chunk,
    text: &str,
    filename: &str,
    id_gen: &IdGenerator,
    table: &postgres::TableTarget,
) -> Result<()> {
    let code = chunk.text(text);
    let id = i64::try_from(id_gen.next_id(ctx, code).await?)
        .map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
    table.declare_row(
        ctx,
        &CodeEmbeddingRow {
            id,
            filename: filename.to_string(),
            code: code.to_string(),
            embedding: ctx.get_key(&EMBEDDER)?.embed(ctx, code).await?,
            start_line: chunk.start.line as i32,
            end_line: chunk.end.line as i32,
        },
    )
}

/// Chunk and embed one file, declaring its rows (cf. Python `process_file`).
/// Memoized on the file: an unchanged file keeps its rows without re-running.
/// The table target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(table = skip))]
async fn process_file(ctx: &Ctx, file: &FileEntry, table: &postgres::TableTarget) -> Result<()> {
    let text = file.read_text().await?;
    let filename = file.key();
    let chunks = RecursiveSplitter::new()?.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: 1000,
            min_chunk_size: Some(300),
            chunk_overlap: Some(300),
            language: detect_code_language(&filename),
        },
    );
    let id_gen = IdGenerator::new();
    ctx.map(chunks, async |chunk| {
        process_chunk(ctx, chunk, &text, &filename, &id_gen, &table).await
    })
    .await?;
    Ok(())
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let table =
        postgres::mount_table_target(&ctx, &DB, TABLE, code_embedding_schema()?, Some(PG_SCHEMA))
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

    // The source supports live watch (cf. Python `localfs.walk_dir(live=True)`).
    let files = walk_dir(sourcedir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::new(
            INCLUDE_PATTERNS,
            EXCLUDE_PATTERNS,
        )?)
        .live();
    // One processing component per file: each file's rows sync and clean up
    // with that file (cf. Python `coco.mount_each(process_file, files.items(), target_table)`).
    ctx.mount_each_live(&"process_file", files, move |ctx, file: FileEntry| {
        let table = table.clone();
        async move { process_file(&ctx, &file, &table).await }
    })
    .await
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

async fn query_once(
    pool: &PgPool,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = vector_param(&embedder.embed_batch(vec![query.to_string()]).await?.remove(0));

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
    postgres::TableSchema::from_row::<CodeEmbeddingRow>(["id"])?
        .with_vector_dim("embedding", EMBED_DIM)
}

fn vector_param(vec: &[f32]) -> String {
    let values = vec.iter().map(f32::to_string).collect::<Vec<_>>();
    format!("[{}]", values.join(","))
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
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("CodeEmbeddingRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
