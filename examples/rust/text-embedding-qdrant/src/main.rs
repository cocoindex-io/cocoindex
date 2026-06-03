//! Text Embedding with Qdrant — Rust port of the Python `text_embedding_qdrant`
//! example.
//!
//! Pipeline: walk markdown files -> chunk -> embed -> store in a Qdrant collection.
//!
//!   cargo run -- index [SOURCE_DIR]    # incremental (unchanged files memo-skipped)
//!   cargo run -- query "your query"    # Qdrant vector search
//!
//! Same pipeline as `text-embedding`, but the target is the native
//! `cocoindex::qdrant` collection connector (built on the public target-state
//! facade). Parallels the Python example's `cocoindex.connectors.qdrant`.
//!
//! Build note: the `qdrant-client` crate compiles protobufs, so a `protoc`
//! binary is required to build this example (set `PROTOC` or put it on `PATH`).

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use cocoindex::qdrant::{self, CollectionSchema, Distance, QdrantConnection};
use cocoindex::walk;
use serde_json::json;

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: u64 = 384;
const COLLECTION: &str = "TextEmbedding";
const TOP_K: u64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

static DB: LazyLock<ContextKey<QdrantConnection>> = LazyLock::new(|| {
    ContextKey::new_with_state("text_embedding_qdrant_db", |c: &QdrantConnection| {
        c.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

/// A computed point: id + vector + payload fields.
#[derive(Clone, Serialize, Deserialize)]
struct PointData {
    id: u64,
    vector: Vec<f32>,
    filename: String,
    chunk_start: i64,
    chunk_end: i64,
    text: String,
}

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<Vec<PointData>> {
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
    let mut points = Vec::with_capacity(texts.len());
    for ((chunk, chunk_text), vector) in chunks.iter().zip(texts).zip(embeddings) {
        let id = id_gen.next_id(&ctx, &chunk_text).await?;
        points.push(PointData {
            id,
            vector,
            filename: filename.clone(),
            chunk_start: chunk.start.char_offset as i64,
            chunk_end: chunk.end.char_offset as i64,
            text: chunk_text,
        });
    }
    Ok(points)
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let conn = ctx.get_key(&DB)?;
    let target = qdrant::mount_collection_target(
        &ctx,
        conn,
        COLLECTION,
        CollectionSchema::new(EMBED_DIM, Distance::Cosine),
    )
    .await?;

    let files: Vec<FileEntry> = walk(&sourcedir, &["**/*.md"])?;
    println!(
        "indexing {} markdown file(s) from {}",
        files.len(),
        sourcedir.display()
    );

    let points_by_file = ctx
        .mount_each(
            files,
            |f| f.key(),
            |child, file| async move { process_file(&child, &file).await },
        )
        .await?;

    let mut count = 0usize;
    for points in &points_by_file {
        count += points.len();
        for p in points {
            let payload = json!({
                "filename": p.filename,
                "chunk_start": p.chunk_start,
                "chunk_end": p.chunk_end,
                "text": p.text,
            })
            .as_object()
            .unwrap()
            .clone();
            target.declare_point(&ctx, p.id, p.vector.clone(), payload)?;
        }
    }
    println!("indexed {count} chunk(s) total");
    Ok(())
}

async fn query_once(
    conn: &QdrantConnection,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = embedder.embed(query).await?;
    let hits = qdrant::vector_search(conn, COLLECTION, query_vec, TOP_K).await?;
    for hit in hits {
        let filename = hit
            .payload
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let text = hit
            .payload
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        println!("[{:.3}] {filename}", hit.score);
        let snippet: String = text.chars().take(200).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn qdrant_url() -> String {
    std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string())
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
            let conn = QdrantConnection::connect(&qdrant_url()).await?;
            let embedder = load_embedder().await?;
            query_once(&conn, &embedder, &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let conn = QdrantConnection::connect(&qdrant_url()).await?;
            let embedder = load_embedder().await?;
            let app = App::builder("TextEmbeddingQdrantRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, conn)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
