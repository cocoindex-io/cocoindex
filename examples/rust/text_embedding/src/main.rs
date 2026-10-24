//! Text Embedding — Rust port of the Python `text_embedding` example.
//!
//! Pipeline: walk markdown files -> chunk -> embed -> store in Postgres/pgvector.
//!
//!   cocoindex update                   # catch up (unchanged files are memo-skipped)
//!   cocoindex update -L                # keep watching the source dir for changes
//!   cargo run -- query "your query"    # pgvector similarity search
//!   cargo run -- index [SOURCE_DIR]    # one catch-up run without the CLI
//!
//! Parallels the Python example:
//!   - environment       : `#[cocoindex::lifespan]` (cf. `@coco.lifespan`)
//!   - app               : `#[cocoindex::app]` (cf. `coco.App(...)`)
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` (cf. `@coco.fn(memo=True)`)
//!   - chunking          : `cocoindex::ops::text::RecursiveSplitter` (cf. `RecursiveSplitter`)
//!   - embeddings        : `cocoindex::ops::sentence_transformers` all-MiniLM-L6-v2
//!   - target            : `postgres::TableTarget` + pgvector index

use std::path::PathBuf;
use std::time::Duration;

use cocoindex::connectors::postgres;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "doc_embeddings";
const TOP_K: i64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;
const APP_NAME: &str = "TextEmbeddingRust";
/// Overrides the source directory (default: `./markdown_files` in this crate).
const SOURCE_DIR_ENV: &str = "TEXT_EMBEDDING_SOURCE_DIR";

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct DocEmbedding {
    id: i64,
    filename: String,
    chunk_start: i32,
    chunk_end: i32,
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

/// Chunk and embed one file. Memoized on the file, so an unchanged file
/// returns its rows without re-chunking.
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
    let embedder = ctx.get_key(&EMBEDDER)?.clone();
    let embedding_ctx = ctx.clone();
    let embeddings = ctx
        .map(texts.clone(), move |chunk_text| {
            let embedder = embedder.clone();
            let ctx = embedding_ctx.clone();
            async move { embedder.embed(&ctx, chunk_text).await }
        })
        .await?;

    let mut id_gen = IdGenerator::new();
    let mut rows = Vec::with_capacity(texts.len());
    for ((chunk, chunk_text), embedding) in chunks.iter().zip(texts).zip(embeddings) {
        let id = id_gen.next_id(&ctx, &chunk_text).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        rows.push(DocEmbedding {
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

/// Environment setup, shared by `cocoindex` and `cargo run -- index`
/// (`Environment::from_registered`): the state db, Postgres and the embedder.
#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    let db = postgres::Database::connect(&database_url()).await?;
    let pool = db.pool().clone();
    let embedder = load_embedder().await?;
    Ok(builder
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&DB, db)
        .provide_key(&EMBEDDER, embedder)
        .on_shutdown(move || async move { pool.close().await }))
}

#[cocoindex::app(name = "TextEmbeddingRust")]
async fn app_main(ctx: Ctx) -> Result<()> {
    index(ctx, source_dir()).await
}

async fn index(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let vector_dim = ctx.get_key(&EMBEDDER)?.dimension();
    let table = postgres::mount_table_target(
        &ctx,
        &DB,
        TABLE,
        postgres::TableSchema::from_row::<DocEmbedding>(["id"])?
            .with_vector_dim("embedding", vector_dim)?,
        Some(PG_SCHEMA),
    )
    .await?;
    table.declare_vector_index(
        &ctx,
        "embedding",
        postgres::VectorIndexOptions {
            method: "hnsw",
            ..Default::default()
        },
    )?;

    // One component per markdown file; each declares its own rows. In live
    // mode (`cocoindex update -L`) the walker keeps watching the directory.
    let files = walk_dir(sourcedir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::new(
            ["**/*.md"],
            std::iter::empty::<&str>(),
        )?)
        .live()
        .poll_interval(Duration::from_secs(1));
    ctx.mount_each_live(&"files", files, move |ctx, file: FileEntry| {
        let table = table.clone();
        async move {
            for row in process_file(&ctx, &file).await? {
                table.declare_row(&ctx, &row)?;
            }
            Ok(())
        }
    })
    .await
}

async fn query_once(
    pool: &PgPool,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = vector_param(&Embedder::embed(embedder, query).await?);
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

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
}

fn source_dir() -> PathBuf {
    std::env::var_os(SOURCE_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("markdown_files"))
}

async fn load_embedder() -> Result<SentenceTransformerEmbedder> {
    SentenceTransformerEmbedder::load(EMBED_MODEL).await
}

/// `cocoindex ...` invocations are answered before this body runs.
#[cocoindex::main]
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
                .connect(&database_url())
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
            .unwrap_or_else(source_dir);

            let env = Environment::from_registered().await?;
            let app = env.app(APP_NAME).await?;
            let stats = app.run(move |ctx| index(ctx, dir)).await;
            env.shutdown().await;
            println!("{}", stats?);
        }
    }
    Ok(())
}
