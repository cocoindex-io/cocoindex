//! Text Embedding with LanceDB — Rust port of the Python
//! `text_embedding_lancedb` example.
//!
//! Pipeline: walk markdown files -> chunk -> embed -> store in LanceDB.
//!
//!   cargo run -- index [SOURCE_DIR]    # incremental (unchanged files memo-skipped)
//!   cargo run -- query "your query"    # LanceDB vector search
//!
//! Same pipeline as the `text-embedding` example, but the target is the native
//! `cocoindex::lancedb` connector instead of Postgres/pgvector. Parallels the
//! Python example's use of `cocoindex.connectors.lancedb`.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::lancedb::{self, ColumnDef, ColumnType, LanceDatabase, TableSchema};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use cocoindex::walk;

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const TABLE: &str = "doc_embeddings";
const TOP_K: usize = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

static DB: LazyLock<ContextKey<LanceDatabase>> = LazyLock::new(|| {
    ContextKey::new_with_state("text_embedding_lancedb_db", |db: &LanceDatabase| {
        db.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

#[derive(Clone, Serialize, Deserialize)]
struct DocEmbedding {
    id: i64,
    filename: String,
    chunk_start: i64,
    chunk_end: i64,
    text: String,
    embedding: Vec<f32>,
}

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<Vec<DocEmbedding>> {
    let filename = file.key();
    let text = file.content_str()?;

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
        rows.push(DocEmbedding {
            id,
            filename: filename.clone(),
            chunk_start: chunk.start.char_offset as i64,
            chunk_end: chunk.end.char_offset as i64,
            text: chunk_text,
            embedding,
        });
    }
    Ok(rows)
}

fn doc_embedding_schema() -> Result<TableSchema> {
    TableSchema::new(
        [
            ("id", ColumnDef::new(ColumnType::Int64)),
            ("filename", ColumnDef::new(ColumnType::Text)),
            ("chunk_start", ColumnDef::new(ColumnType::Int64)),
            ("chunk_end", ColumnDef::new(ColumnType::Int64)),
            ("text", ColumnDef::new(ColumnType::Text)),
            ("embedding", ColumnDef::new(ColumnType::Vector(EMBED_DIM))),
        ],
        ["id"],
    )
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table = lancedb::mount_table_target(&ctx, db, TABLE, doc_embedding_schema()?).await?;

    let files: Vec<FileEntry> = walk(&sourcedir, &["**/*.md"])?;
    println!(
        "indexing {} markdown file(s) from {}",
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
    println!("indexed {count} chunk(s) total");
    Ok(())
}

async fn query_once(
    db: &LanceDatabase,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = embedder.embed(query).await?;
    let results = lancedb::vector_search(db, TABLE, "embedding", query_vec, TOP_K).await?;
    for r in results {
        let filename = r.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let text = r.get("text").and_then(|v| v.as_str()).unwrap_or("");
        let distance = r.get("_distance").and_then(|v| v.as_f64()).unwrap_or(0.0);
        println!("[{:.3}] {filename}", 1.0 - distance);
        let snippet: String = text.chars().take(200).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn lancedb_uri() -> String {
    std::env::var("LANCEDB_URI").unwrap_or_else(|_| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("lancedb_data")
            .to_string_lossy()
            .to_string()
    })
}

fn default_sourcedir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("markdown_files")
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
            let db = LanceDatabase::connect(&lancedb_uri()).await?;
            let embedder = load_embedder().await?;
            query_once(&db, &embedder, &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let db = LanceDatabase::connect(&lancedb_uri()).await?;
            let embedder = load_embedder().await?;
            let app = App::builder("TextEmbeddingLanceDBRust")
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
