//! Code Embedding with LanceDB — Rust port of the Python
//! `code_embedding_lancedb` example.
//!
//! Pipeline: walk -> detect language -> tree-sitter chunk -> embed -> store in LanceDB.
//!
//!   cargo run -- index [SOURCE_DIR]    # incremental (unchanged files memo-skipped)
//!   cargo run -- query "your query"    # LanceDB vector search
//!
//! Same pipeline as the `code_embedding` example, but the target is the native
//! `cocoindex::connectors::lancedb` connector instead of Postgres/pgvector (parallels the
//! Python example's use of `cocoindex.connectors.lancedb`).
//!
//! Parallels the Python example:
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)

use std::path::PathBuf;

use cocoindex::connectors::lancedb::{self, LanceDatabase, TableSchema};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter, detect_code_language};
use cocoindex::prelude::*;

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const TABLE: &str = "code_embeddings";
const TOP_K: usize = 5;

const INCLUDE_PATTERNS: &[&str] = &["**/*.py", "**/*.rs", "**/*.toml", "**/*.md", "**/*.mdx"];
const EXCLUDE_PATTERNS: &[&str] = &["**/.*", "**/target", "**/node_modules"];

cocoindex::context_key!(static DB: LanceDatabase);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct CodeEmbedding {
    id: i64,
    filename: String,
    code: String,
    #[coco(vector)]
    embedding: Vec<f32>,
    start_line: i64,
    end_line: i64,
}

/// Embed one chunk and declare its row (cf. Python `process_chunk`).
#[cocoindex::function]
async fn process_chunk(
    ctx: &Ctx,
    chunk: Chunk,
    text: &str,
    filename: &str,
    id_gen: &IdGenerator,
    table: &lancedb::LanceTableTarget,
) -> Result<()> {
    let code = chunk.text(text);
    let id = i64::try_from(id_gen.next_id(ctx, code).await?)
        .map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
    table.declare_row(
        ctx,
        &CodeEmbedding {
            id,
            filename: filename.to_string(),
            code: code.to_string(),
            embedding: ctx.get_key(&EMBEDDER)?.embed(ctx, code).await?,
            start_line: chunk.start.line as i64,
            end_line: chunk.end.line as i64,
        },
    )
}

/// Chunk and embed one file, declaring its rows (cf. Python `process_file`).
/// Memoized on the file: an unchanged file keeps its rows without re-running.
/// The table target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(table = skip))]
async fn process_file(
    ctx: &Ctx,
    file: &FileEntry,
    table: &lancedb::LanceTableTarget,
) -> Result<()> {
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

fn code_embedding_schema() -> Result<TableSchema> {
    TableSchema::from_row::<CodeEmbedding>(["id"])?.with_vector_dim("embedding", EMBED_DIM)
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let table = lancedb::mount_table_target(&ctx, &DB, TABLE, code_embedding_schema()?).await?;

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

async fn query_once(
    db: &LanceDatabase,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = embedder.embed_batch(vec![query.to_string()]).await?.remove(0);
    let results = lancedb::vector_search(db, TABLE, "embedding", query_vec, TOP_K).await?;
    for r in results {
        let filename = r.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let code = r.get("code").and_then(|v| v.as_str()).unwrap_or("");
        let start_line = r.get("start_line").and_then(|v| v.as_i64()).unwrap_or(0);
        let end_line = r.get("end_line").and_then(|v| v.as_i64()).unwrap_or(0);
        let distance = r.get("_distance").and_then(|v| v.as_f64()).unwrap_or(0.0);
        println!(
            "[{:.3}] {filename} (L{start_line}-L{end_line})",
            1.0 - distance
        );
        let snippet: String = code.chars().take(200).collect();
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
    // Index the repository root, like the Python example.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
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
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("CodeEmbeddingLanceDBRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
