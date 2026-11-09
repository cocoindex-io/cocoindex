//! Text Embedding with Turbopuffer — Rust port of the Python
//! `text_embedding_turbopuffer` example.
//!
//! Pipeline: walk markdown files -> chunk -> embed -> store in a Turbopuffer
//! namespace.
//!
//!   cargo run -- index [SOURCE_DIR]    # incremental (unchanged files memo-skipped)
//!   cargo run -- query "your query"    # Turbopuffer vector search
//!
//! Same pipeline as `text_embedding`, but the target is the native
//! `cocoindex::connectors::turbopuffer` namespace connector (built on the public
//! target-state facade). Parallels `cocoindex.connectors.turbopuffer`.
//!
//! Parallels the Python example:
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)
//!
//! Config (env, e.g. via `.env`):
//!   TURBOPUFFER_API_KEY   required
//!   TURBOPUFFER_REGION    default `gcp-us-central1`
//!   TURBOPUFFER_NAMESPACE default `TextEmbedding`

use std::path::PathBuf;

use cocoindex::connectors::turbopuffer::{
    self, DistanceMetric, NamespaceSchema, TurbopufferConnection,
};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use serde_json::json;

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const TOP_K: usize = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;

cocoindex::context_key!(static DB: TurbopufferConnection);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct RowData {
    id: String,
    #[coco(vector)]
    vector: Vec<f32>,
    filename: String,
    chunk_start: i64,
    chunk_end: i64,
    text: String,
}

/// Embed one chunk and declare its row (cf. Python `process_chunk`).
#[cocoindex::function]
async fn process_chunk(
    ctx: &Ctx,
    chunk: Chunk,
    text: &str,
    filename: &str,
    id_gen: &IdGenerator,
    target: &turbopuffer::NamespaceTarget,
) -> Result<()> {
    let chunk_text = chunk.text(text);
    let id = id_gen.next_id(ctx, chunk_text).await?;
    let vector = ctx.get_key(&EMBEDDER)?.embed(ctx, chunk_text).await?;
    let attributes = json!({
        "filename": filename,
        "chunk_start": chunk.start.char_offset,
        "chunk_end": chunk.end.char_offset,
        "text": chunk_text,
    });
    let serde_json::Value::Object(attributes) = attributes else {
        return Err(Error::engine("row attributes are not a JSON object"));
    };
    target.declare_named_row(ctx, id.to_string(), [("vector", vector)], attributes)
}

/// Chunk and embed one file, declaring its rows (cf. Python `process_file`).
/// Memoized on the file: an unchanged file keeps its rows without re-running.
/// The namespace target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(target = skip))]
async fn process_file(
    ctx: &Ctx,
    file: &FileEntry,
    target: &turbopuffer::NamespaceTarget,
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

async fn app_main(ctx: Ctx, sourcedir: PathBuf, namespace: String) -> Result<()> {
    let vector_dim = ctx.get_key(&EMBEDDER)?.dimension();
    let target = turbopuffer::mount_namespace_target(
        &ctx,
        &DB,
        &namespace,
        NamespaceSchema::from_row::<RowData>(DistanceMetric::CosineDistance)?
            .with_vector_dim("vector", vector_dim)?,
    )
    .await?;

    // The source supports live watch (cf. Python `localfs.walk_dir(live=True)`).
    let files = walk_dir(sourcedir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::include(["**/*.md"])?)
        .live();
    // One processing component per file: each file's rows sync and clean up
    // with that file (cf. Python `coco.mount_each(process_file, files.items(), target_namespace)`).
    ctx.mount_each_live(&"process_file", files, move |ctx, file: FileEntry| {
        let target = target.clone();
        async move { process_file(&ctx, &file, &target).await }
    })
    .await
}

async fn query_once(
    conn: &TurbopufferConnection,
    embedder: &SentenceTransformerEmbedder,
    namespace: &str,
    query: &str,
) -> Result<()> {
    let query_vec = embedder.embed_batch(vec![query.to_string()]).await?.remove(0);
    let hits =
        turbopuffer::vector_search_by_field(conn, namespace, "vector", query_vec, TOP_K).await?;
    for hit in hits {
        let filename = hit
            .attributes
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let text = hit
            .attributes
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        println!("[{:.3}] {filename}", 1.0 - hit.distance);
        let snippet: String = text.chars().take(200).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn connect() -> Result<TurbopufferConnection> {
    let api_key = std::env::var("TURBOPUFFER_API_KEY")
        .map_err(|_| Error::engine("TURBOPUFFER_API_KEY is required"))?;
    let region = std::env::var("TURBOPUFFER_REGION").unwrap_or_else(|_| "gcp-us-central1".into());
    Ok(TurbopufferConnection::new(&region, &api_key))
}

fn namespace() -> String {
    std::env::var("TURBOPUFFER_NAMESPACE").unwrap_or_else(|_| "TextEmbedding".into())
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
            let conn = connect()?;
            let embedder = load_embedder().await?;
            query_once(&conn, &embedder, &namespace(), &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let conn = connect()?;
            let embedder = load_embedder().await?;
            let ns = namespace();
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, conn)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("TextEmbeddingTurbopufferRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir, ns)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
