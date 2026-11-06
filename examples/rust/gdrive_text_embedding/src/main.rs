//! Google Drive Text Embedding — Rust port of the Python `gdrive_text_embedding`
//! example.
//!
//! Pipeline: list Drive files -> read/export text -> chunk -> embed -> store in
//! Postgres/pgvector.
//!
//! Parallels the Python example:
//!   - source            : `GoogleDriveSource::items()` + `mount_each!` (cf. `GoogleDriveSource(..).items()` + `coco.mount_each`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)

use std::path::PathBuf;

use cocoindex::connectors::gdrive::{DriveFile, GoogleDriveClient, GoogleDriveSource};
use cocoindex::connectors::postgres;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples_v1";
const TABLE: &str = "doc_embeddings";
const TOP_K: i64 = 5;

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(static GDRIVE: GoogleDriveClient);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct DocEmbeddingRow {
    id: i64,
    filename: String,
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

/// Embed one chunk and declare its row (cf. Python `_emit_chunk`).
#[cocoindex::function]
async fn emit_chunk(
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
        &DocEmbeddingRow {
            id,
            filename: filename.to_string(),
            text: chunk_text.to_string(),
            embedding: ctx.get_key(&EMBEDDER)?.embed(ctx, chunk_text).await?,
        },
    )
}

/// Read, chunk and embed one Drive file, declaring its rows (cf. Python `process_file`).
/// Memoized on the file: an unchanged file keeps its rows without re-running.
/// The table target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(table = skip))]
async fn process_file(ctx: &Ctx, file: DriveFile, table: postgres::TableTarget) -> Result<()> {
    let text = file.read_text().await?;
    let chunks = RecursiveSplitter::new()?.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: 2000,
            min_chunk_size: None,
            chunk_overlap: Some(500),
            language: Some("markdown".to_string()),
        },
    );
    let filename = file.path().to_string();
    let id_gen = IdGenerator::new();
    ctx.map(chunks, async |chunk| {
        emit_chunk(ctx, chunk, &text, &filename, &id_gen, &table).await
    })
    .await?;
    Ok(())
}

async fn app_main(ctx: Ctx, root_folder_ids: Vec<String>) -> Result<()> {
    let table =
        postgres::mount_table_target(&ctx, &DB, TABLE, doc_embedding_schema()?, Some(PG_SCHEMA))
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

    let source = GoogleDriveSource::new(ctx.get_key(&GDRIVE)?.clone(), root_folder_ids);
    let files = source.items().await?;
    println!("indexing {} Google Drive files", files.len());

    // One processing component per file: each file's rows sync and clean up
    // with that file (cf. Python `coco.mount_each(process_file, source.items(), table)`).
    mount_each!(files, |file| process_file(ctx, file, table)).await?;
    Ok(())
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
        let score = 1.0 - distance;
        println!("[{score:.3}] {filename}");
        let snippet: String = text.chars().take(300).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn doc_embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::from_row::<DocEmbeddingRow>(["id"])?
        .with_vector_dim("embedding", EMBED_DIM)
}

fn vector_param(vec: &[f32]) -> String {
    let values = vec.iter().map(f32::to_string).collect::<Vec<_>>();
    format!("[{}]", values.join(","))
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
}

fn root_folder_ids() -> Result<Vec<String>> {
    let raw = std::env::var("GOOGLE_DRIVE_ROOT_FOLDER_IDS")
        .map_err(|_| Error::engine("GOOGLE_DRIVE_ROOT_FOLDER_IDS is required"))?;
    let ids: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if ids.is_empty() {
        Err(Error::engine(
            "GOOGLE_DRIVE_ROOT_FOLDER_IDS must contain at least one folder id",
        ))
    } else {
        Ok(ids)
    }
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

fn load_gdrive_client() -> Result<GoogleDriveClient> {
    let credential_path = std::env::var("GOOGLE_SERVICE_ACCOUNT_CREDENTIAL")
        .map_err(|_| Error::engine("GOOGLE_SERVICE_ACCOUNT_CREDENTIAL is required"))?;
    GoogleDriveClient::from_service_account_file(credential_path)
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
            if let Some(other) = sub
                && other != "index"
            {
                eprintln!("usage: cargo run -- [index] | cargo run -- query \"your search text\"");
                std::process::exit(2);
            }
            let db = connect_target_db().await?;
            let embedder = load_embedder().await?;
            let gdrive = load_gdrive_client()?;
            let root_folder_ids = root_folder_ids()?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .provide_key(&GDRIVE, gdrive)
                .build()
                .await?
                .app("GoogleDriveTextEmbeddingRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, root_folder_ids)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
