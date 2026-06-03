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
//! Rust-native PDF approach used by the `paper-metadata` example. Everything
//! downstream (chunking 2000/500, MiniLM embeddings, Postgres target, query)
//! mirrors Python.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::postgres;
use cocoindex::prelude::*;
use cocoindex::walk;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "pdf_embeddings";
const TOP_K: i64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("pdf_embedding_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

#[derive(Clone, Serialize, Deserialize)]
struct PdfEmbedding {
    id: i64,
    filename: String,
    chunk_start: i32,
    chunk_end: i32,
    text: String,
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

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<Vec<PdfEmbedding>> {
    let filename = file.key();
    let content = file.content()?;
    let text = tokio::task::spawn_blocking(move || pdf_to_text(&content))
        .await
        .map_err(|e| Error::engine(format!("PDF parse task panicked: {e}")))??;

    let splitter = RecursiveSplitter::new()?;
    let chunks = splitter.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: CHUNK_SIZE,
            min_chunk_size: None,
            chunk_overlap: Some(CHUNK_OVERLAP),
            language: Some("markdown".to_string()),
        },
    );
    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    let texts: Vec<String> = chunks.iter().map(|c| c.text(&text).to_string()).collect();
    let embeddings = ctx.get_key(&EMBEDDER)?.embed_batch(texts.clone()).await?;

    let mut id_gen = IdGenerator::new();
    let mut rows = Vec::with_capacity(texts.len());
    for ((chunk, chunk_text), embedding) in chunks.iter().zip(texts).zip(embeddings) {
        let id = id_gen.next_id(&ctx, &chunk_text).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        rows.push(PdfEmbedding {
            id,
            filename: filename.clone(),
            chunk_start: chunk.start.char_offset as i32,
            chunk_end: chunk.end.char_offset as i32,
            text: chunk_text,
            embedding,
        });
    }
    Ok(rows)
}

fn pdf_embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("id", postgres::ColumnDef::new("bigint")),
            ("filename", postgres::ColumnDef::new("text")),
            ("chunk_start", postgres::ColumnDef::new("integer")),
            ("chunk_end", postgres::ColumnDef::new("integer")),
            ("text", postgres::ColumnDef::new("text")),
            (
                "embedding",
                postgres::ColumnDef::new(format!("vector({EMBED_DIM})")),
            ),
        ],
        ["id"],
    )
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table =
        postgres::mount_table_target(&ctx, db, TABLE, pdf_embedding_schema()?, Some(PG_SCHEMA))
            .await?;

    let files: Vec<FileEntry> = walk(&sourcedir, &["**/*.pdf"])?;
    println!("indexing {} PDF(s) from {}", files.len(), sourcedir.display());

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
    println!("indexed {count} chunk(s) total");
    Ok(())
}

async fn query_once(
    pool: &PgPool,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = vector_param(&embedder.embed(query).await?);
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
            let app = App::builder("PdfEmbeddingV1")
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
