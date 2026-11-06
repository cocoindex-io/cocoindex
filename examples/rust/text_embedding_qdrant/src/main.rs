//! Text Embedding with Qdrant — Rust port of the Python `text_embedding_qdrant`
//! example.
//!
//! Pipeline: walk markdown files -> chunk -> embed -> store in a Qdrant collection.
//!
//!   cargo run -- index [SOURCE_DIR]    # incremental (unchanged files memo-skipped)
//!   cargo run -- query "your query"    # Qdrant vector search
//!
//! Same pipeline as `text_embedding`, but the target is the native
//! `cocoindex::connectors::qdrant` collection connector (built on the public target-state
//! facade). Parallels the Python example's `cocoindex.connectors.qdrant`.
//!
//! Parallels the Python example:
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its points (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)
//!
//! Build note: the `qdrant-client` crate compiles protobufs, so a `protoc`
//! binary is required to build this example (set `PROTOC` or put it on `PATH`).

use std::path::PathBuf;

use cocoindex::connectors::qdrant::{
    self, CollectionSchema, Distance, NamedPointVector, QdrantConnection,
};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use serde_json::json;

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const COLLECTION: &str = "TextEmbedding";
const TOP_K: u64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

cocoindex::context_key!(static DB: QdrantConnection);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

/// The collection schema: id + vector + payload fields.
#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct PointData {
    id: u64,
    #[coco(vector)]
    vector: Vec<f32>,
    filename: String,
    chunk_start: i64,
    chunk_end: i64,
    text: String,
}

/// Embed one chunk and declare its point (cf. Python `process_chunk`).
#[cocoindex::function]
async fn process_chunk(
    ctx: &Ctx,
    chunk: Chunk,
    text: &str,
    filename: &str,
    id_gen: &IdGenerator,
    target: &qdrant::CollectionTarget,
) -> Result<()> {
    let chunk_text = chunk.text(text);
    let id = id_gen.next_id(ctx, chunk_text).await?;
    let vector = ctx.get_key(&EMBEDDER)?.embed(ctx, chunk_text).await?;
    let payload = json!({
        "filename": filename,
        "chunk_start": chunk.start.char_offset,
        "chunk_end": chunk.end.char_offset,
        "text": chunk_text,
    });
    let serde_json::Value::Object(payload) = payload else {
        return Err(Error::engine("point payload is not a JSON object"));
    };
    target.declare_named_vectors_point(
        ctx,
        id,
        [("vector", NamedPointVector::Single(vector))],
        payload,
    )
}

/// Chunk and embed one file, declaring its points (cf. Python `process_file`).
/// Memoized on the file: an unchanged file keeps its points without re-running.
/// The collection target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(target = skip))]
async fn process_file(
    ctx: &Ctx,
    file: &FileEntry,
    target: &qdrant::CollectionTarget,
) -> Result<()> {
    let text = file.read_text().await?;
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
        process_chunk(ctx, chunk, &text, &filename, &id_gen, &target).await
    })
    .await?;
    Ok(())
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let vector_dim = ctx.get_key(&EMBEDDER)?.dimension();
    let target = qdrant::mount_collection_target(
        &ctx,
        &DB,
        COLLECTION,
        CollectionSchema::from_row::<PointData>(Distance::Cosine)?
            .with_vector_dim("vector", vector_dim)?,
    )
    .await?;

    // The source supports live watch (cf. Python `localfs.walk_dir(live=True)`).
    let files = walk_dir(sourcedir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::include(["**/*.md"])?)
        .live();
    // One processing component per file: each file's points sync and clean up
    // with that file (cf. Python `coco.mount_each(process_file, files.items(), target_collection)`).
    ctx.mount_each_live(&"process_file", files, move |ctx, file: FileEntry| {
        let target = target.clone();
        async move { process_file(&ctx, &file, &target).await }
    })
    .await
}

async fn query_once(
    conn: &QdrantConnection,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = embedder.embed_batch(vec![query.to_string()]).await?.remove(0);
    let hits = qdrant::named_vector_search(conn, COLLECTION, "vector", query_vec, TOP_K).await?;
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
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, conn)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("TextEmbeddingQdrantRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
