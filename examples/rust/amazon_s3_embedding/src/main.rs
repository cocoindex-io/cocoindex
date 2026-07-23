//! Amazon S3 Text Embedding — Rust port of the Python `amazon_s3_embedding`
//! example.
//!
//! Pipeline: list markdown files from an S3 bucket -> read -> chunk -> embed ->
//! store in Postgres/pgvector.
//!
//!   cargo run -- index                 # one-shot (the S3 source is not live)
//!   cargo run -- query "your query"    # pgvector similarity search
//!
//! Works against real S3 or an S3-compatible service (e.g. MinIO): set the
//! standard AWS env (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//! and `AWS_ENDPOINT_URL` for MinIO) plus `S3_BUCKET` (and optional `S3_PREFIX`).

use std::path::PathBuf;
use std::sync::Arc;

use cocoindex::connectors::amazon_s3::{self, ListOptions, S3Client, S3File};
use cocoindex::connectors::postgres;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use cocoindex::resources::file::PatternFilePathMatcher;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "amazon_s3_doc_embeddings";
const TOP_K: i64 = 5;

cocoindex::context_key!(
    static DB: postgres::Database = "s3_embedding_db",
    state = postgres::Database::state_id
);
cocoindex::context_key!(
    static S3: S3Client = "s3_client",
    state = S3Client::state_id
);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder = "embedder",
    state = SentenceTransformerEmbedder::model_name
);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct DocEmbeddingRow {
    id: i64,
    filename: String,
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

#[cocoindex::function]
async fn process_file(ctx: &Ctx, file: S3File) -> Result<Vec<DocEmbeddingRow>> {
    let text = ctx.get_key(&S3)?.read_text(&file).await?;
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

    let filename = file.key();
    let mut id_gen = IdGenerator::new();
    let mut rows = Vec::with_capacity(texts.len());
    for (chunk_text, embedding) in texts.into_iter().zip(embeddings) {
        let id = id_gen.next_id(&ctx, &chunk_text).await?;
        let id =
            i64::try_from(id).map_err(|_| Error::engine("generated id does not fit in BIGINT"))?;
        rows.push(DocEmbeddingRow {
            id,
            filename: filename.clone(),
            text: chunk_text,
            embedding,
        });
    }
    Ok(rows)
}

async fn app_main(ctx: Ctx, bucket: String, prefix: String) -> Result<()> {
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

    let files = amazon_s3::list_objects(
        ctx.get_key(&S3)?,
        &bucket,
        ListOptions {
            prefix,
            path_matcher: Some(Arc::new(
                PatternFilePathMatcher::include(["**/*.md"])
                    .map_err(|e| Error::engine(format!("matcher: {e}")))?,
            )),
            ..Default::default()
        },
    )
    .list()
    .await?;
    println!(
        "indexing {} markdown file(s) from s3://{bucket}",
        files.len()
    );

    let rows_by_file = mount_each!(
        files.into_iter().map(|f| (f.key(), f)),
        |file| process_file(ctx, file)
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
                .max_connections(8)
                .connect(&database_url())
                .await
                .map_err(db_err)?;
            let embedder = load_embedder().await?;
            query_once(&pool, &embedder, &q).await?;
        }
        sub => {
            if let Some(other) = sub
                && other != "index"
            {
                eprintln!("usage: cargo run -- [index] | cargo run -- query \"...\"");
                std::process::exit(2);
            }
            let bucket =
                std::env::var("S3_BUCKET").map_err(|_| Error::engine("S3_BUCKET is required"))?;
            let prefix = std::env::var("S3_PREFIX").unwrap_or_default();

            let db = postgres::Database::connect(&database_url()).await?;
            let s3 = S3Client::connect().await?;
            let embedder = load_embedder().await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&S3, s3)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("AmazonS3EmbeddingRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, bucket, prefix)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
