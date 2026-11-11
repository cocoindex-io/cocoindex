//! PDF Embedding — Rust port of the Python `pdf_embedding` example.
//!
//! Pipeline: walk local PDFs -> extract text -> chunk -> embed -> store in
//! Postgres/pgvector.
//!
//!   cargo run -- index [PDF_DIR]       # incremental (unchanged PDFs memo-skipped)
//!   cargo run -- query "your query"    # pgvector similarity search (no index)
//!
//! Parity note: the Python example converts PDFs to Markdown with `docling`
//! (a heavy ML pipeline). There is no Rust equivalent, so this port extracts
//! plain text with `lopdf` and chunks it with the markdown splitter — the same
//! Rust-native PDF approach used by the `paper_metadata` example. Everything
//! downstream (chunking 2000/500, MiniLM embeddings, Postgres target, query)
//! mirrors Python.
//!
//! Parallels the Python example:
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)

use std::path::PathBuf;

use cocoindex::connectors::postgres;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "pdf_embeddings";
const TOP_K: i64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct PdfEmbedding {
    id: i64,
    filename: String,
    chunk_start: i32,
    chunk_end: i32,
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

/// Extract all text from a PDF (Rust-native stand-in for `docling` markdown).
fn pdf_to_text(content: &[u8]) -> Result<String> {
    let doc = lopdf::Document::load_mem(content)
        .map_err(|e| Error::engine(format!("failed to parse PDF: {e}")))?;
    let pages: Vec<u32> = doc.get_pages().keys().copied().collect();
    if pages.is_empty() {
        return Ok(String::new());
    }
    doc.extract_text(&pages)
        .map_err(|e| Error::engine(format!("failed to extract PDF text: {e}")))
}

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
    let chunk_text = chunk.text(text);
    let id = i64::try_from(id_gen.next_id(ctx, chunk_text).await?)
        .map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
    table.declare_row(
        ctx,
        &PdfEmbedding {
            id,
            filename: filename.to_string(),
            chunk_start: chunk.start.char_offset as i32,
            chunk_end: chunk.end.char_offset as i32,
            text: chunk_text.to_string(),
            embedding: ctx.get_key(&EMBEDDER)?.embed(ctx, chunk_text).await?,
        },
    )
}

/// Extract, chunk and embed one PDF, declaring its rows (cf. Python `process_file`).
/// Memoized on the file: an unchanged PDF keeps its rows without re-running.
/// The table target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(table = skip))]
async fn process_file(ctx: &Ctx, file: &FileEntry, table: &postgres::TableTarget) -> Result<()> {
    let content = file.read().await?;
    let text = tokio::task::spawn_blocking(move || pdf_to_text(&content))
        .await
        .map_err(|e| Error::engine(format!("PDF parse task panicked: {e}")))??;
    let chunks = RecursiveSplitter::new()?.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: CHUNK_SIZE,
            min_chunk_size: None,
            chunk_overlap: Some(CHUNK_OVERLAP),
            language: Some("markdown".to_string()),
        },
    );
    let filename = file.key();
    let id_gen = IdGenerator::new();
    ctx.map(chunks, async |chunk| {
        process_chunk(ctx, chunk, &text, &filename, &id_gen, &table).await
    })
    .await?;
    Ok(())
}

fn pdf_embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::from_row::<PdfEmbedding>(["id"])?.with_vector_dim("embedding", EMBED_DIM)
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let table =
        postgres::mount_table_target(&ctx, &DB, TABLE, pdf_embedding_schema()?, Some(PG_SCHEMA))
            .await?;

    // The source supports live watch (cf. Python `localfs.walk_dir(live=True)`).
    let files = walk_dir(sourcedir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::include(["**/*.pdf"])?)
        .live();
    // One processing component per file: each file's rows sync and clean up
    // with that file (cf. Python `coco.mount_each(process_file, files.items(), target_table)`).
    ctx.mount_each_live(&"process_file", files, move |ctx, file: FileEntry| {
        let table = table.clone();
        async move { process_file(&ctx, &file, &table).await }
    })
    .await
}

async fn query_once(
    pool: &PgPool,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = vector_param(&embedder.embed_batch(vec![query.to_string()]).await?.remove(0));
    let rows = sqlx::query(&format!(
        "SELECT filename, text, embedding <=> $1::vector AS distance \
         FROM \"{PG_SCHEMA}\".\"{TABLE}\" ORDER BY distance ASC LIMIT $2"
    ))
    .bind(query_vec)
    .bind(TOP_K)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    for row in rows {
        let filename: String = row.try_get("filename").map_err(db_err)?;
        let text: String = row.try_get("text").map_err(db_err)?;
        let distance: f64 = row.try_get("distance").map_err(db_err)?;
        println!("[{:.3}] {filename}", 1.0 - distance);
        let snippet: String = text.chars().take(200).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn vector_param(vec: &[f32]) -> String {
    format!(
        "[{}]",
        vec.iter().map(f32::to_string).collect::<Vec<_>>().join(",")
    )
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

fn database_url() -> Result<String> {
    std::env::var("POSTGRES_URL").map_err(|_| Error::engine("POSTGRES_URL is not set"))
}

fn default_sourcedir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pdf_files")
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
            let pool = PgPoolOptions::new()
                .connect(&database_url()?)
                .await
                .map_err(db_err)?;
            let embedder = load_embedder().await?;
            query_once(&pool, &embedder, &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let db = postgres::Database::connect(&database_url()?).await?;
            let embedder = load_embedder().await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("PdfEmbeddingV1")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
