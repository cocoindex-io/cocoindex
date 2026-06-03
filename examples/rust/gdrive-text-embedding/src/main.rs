//! Google Drive Text Embedding — Rust port of the Python `gdrive_text_embedding`
//! example.
//!
//! Pipeline: list Drive files -> read/export text -> chunk -> embed -> store in
//! Postgres/pgvector.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::gdrive::{DriveFile, GoogleDriveClient, GoogleDriveSource};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::postgres;
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples_v1";
const TABLE: &str = "doc_embeddings";
const TOP_K: i64 = 5;

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("gdrive_text_embedding_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});

static GDRIVE: LazyLock<ContextKey<GoogleDriveClient>> = LazyLock::new(|| {
    ContextKey::new_with_state("gdrive_client", |client: &GoogleDriveClient| {
        client.state_id().to_string()
    })
});

static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});

#[derive(Clone, Serialize, Deserialize)]
struct DocEmbeddingRow {
    id: i64,
    filename: String,
    text: String,
    embedding: Vec<f32>,
}

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &DriveFile) -> Result<Vec<DocEmbeddingRow>> {
    let client = ctx.get_key(&GDRIVE)?;
    let text = client.read_text(&file).await?;
    let splitter = RecursiveSplitter::new()?;
    let chunks = splitter.split_with(
        &text,
        RecursiveChunkConfig {
            chunk_size: 2000,
            min_chunk_size: None,
            chunk_overlap: Some(500),
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
    for (chunk_text, embedding) in texts.into_iter().zip(embeddings) {
        let id = id_gen.next_id(&ctx, &chunk_text).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        rows.push(DocEmbeddingRow {
            id,
            filename: file.path().to_string(),
            text: chunk_text,
            embedding,
        });
    }
    Ok(rows)
}

async fn app_main(ctx: Ctx, root_folder_ids: Vec<String>) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table =
        postgres::mount_table_target(&ctx, db, TABLE, doc_embedding_schema()?, Some(PG_SCHEMA)).await?;
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
    let files = source.list_files().await?;
    println!("indexing {} Google Drive files", files.len());

    let rows_by_file = ctx
        .mount_each(
            files,
            |file| file.key(),
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

    println!("indexed {count} chunks total");
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
        let score = 1.0 - distance;
        println!("[{score:.3}] {filename}");
        let snippet: String = text.chars().take(300).collect();
        println!("    {}", snippet.replace('\n', "\n    "));
        println!("---");
    }
    Ok(())
}

fn doc_embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("id", postgres::ColumnDef::new("bigint")),
            ("filename", postgres::ColumnDef::new("text")),
            ("text", postgres::ColumnDef::new("text")),
            (
                "embedding",
                postgres::ColumnDef::new(format!("vector({EMBED_DIM})")),
            ),
        ],
        ["id"],
    )
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
            let app = App::builder("GoogleDriveTextEmbeddingRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&EMBEDDER, embedder)
                .provide_key(&GDRIVE, gdrive)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, root_folder_ids)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
