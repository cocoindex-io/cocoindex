//! OCI Object Storage Text Embedding — Rust port of the Python
//! `oci_object_storage_embedding` example.
//!
//! Pipeline: list markdown files from an OCI Object Storage bucket -> read ->
//! chunk -> embed -> store in Postgres/pgvector.
//!
//!   cargo run -- index                 # one-shot (the OCI source is not live)
//!   cargo run -- query "your query"    # pgvector similarity search
//!
//! Auth comes from an `~/.oci/config` profile (the same file the OCI CLI and
//! Python SDK use). Set `OCI_NAMESPACE` and `OCI_BUCKET` (and optional
//! `OCI_PREFIX`); `OCI_CONFIG_FILE` / `OCI_PROFILE` override the config location
//! and profile.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use cocoindex::file::PatternFilePathMatcher;
use cocoindex::oci_object_storage::{self, ListOptions, OciClient, OciFile};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::postgres;
use cocoindex::prelude::*;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const PG_SCHEMA: &str = "coco_examples";
const TABLE: &str = "oci_doc_embeddings";
const TOP_K: i64 = 5;

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("oci_embedding_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
static OCI: LazyLock<ContextKey<OciClient>> = LazyLock::new(|| {
    ContextKey::new_with_state("oci_client", |c: &OciClient| c.state_id().to_string())
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
async fn process_file(ctx: &Ctx, file: &OciFile) -> Result<Vec<DocEmbeddingRow>> {
    let text = ctx.get_key(&OCI)?.read_text(&file).await?;
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

async fn app_main(ctx: Ctx, namespace: String, bucket: String, prefix: String) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table =
        postgres::mount_table_target(&ctx, db, TABLE, doc_embedding_schema()?, Some(PG_SCHEMA))
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

    let files = oci_object_storage::list_objects(
        ctx.get_key(&OCI)?,
        &namespace,
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
        "indexing {} markdown file(s) from oci://{namespace}/{bucket}",
        files.len()
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
            let namespace = std::env::var("OCI_NAMESPACE")
                .map_err(|_| Error::engine("OCI_NAMESPACE is required"))?;
            let bucket =
                std::env::var("OCI_BUCKET").map_err(|_| Error::engine("OCI_BUCKET is required"))?;
            let prefix = std::env::var("OCI_PREFIX").unwrap_or_default();

            let db = postgres::Database::connect(&database_url()).await?;
            let oci = OciClient::connect().await?;
            let embedder = load_embedder().await?;
            let app = App::builder("OciObjectStorageEmbeddingRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&OCI, oci)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app
                .run(move |ctx| app_main(ctx, namespace, bucket, prefix))
                .await?;
            println!("{stats}");
        }
    }
    Ok(())
}
