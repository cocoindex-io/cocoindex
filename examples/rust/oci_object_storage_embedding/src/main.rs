//! OCI Object Storage Text Embedding — Rust port of the Python
//! `oci_object_storage_embedding` example.
//!
//! Pipeline: list markdown files from an OCI Object Storage bucket -> read ->
//! chunk -> embed -> store in Postgres/pgvector.
//!
//!   cargo run -- index                 # one-shot (the OCI source is not live)
//!   cargo run -- query "your query"    # pgvector similarity search
//!
//! Parallels the Python example:
//!   - source            : `oci_object_storage::list_objects(..).items()` + `mount_each!` (cf. `list_objects` + `coco.mount_each`;
//!                         the Python example's optional OCI Streaming live feed has its Rust analogue in `list_objects_live`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` declaring its rows (cf. `@coco.fn(memo=True)`)
//!   - per-chunk compute : `#[cocoindex::function]` under `ctx.map` (cf. `@coco.fn` under `coco.map`)
//!
//! Auth comes from an `~/.oci/config` profile (the same file the OCI CLI and
//! Python SDK use). Set `OCI_NAMESPACE` and `OCI_BUCKET` (and optional
//! `OCI_PREFIX`); `OCI_CONFIG_FILE` / `OCI_PROFILE` override the config location
//! and profile.

use std::path::PathBuf;
use std::sync::Arc;

use cocoindex::connectors::oci_object_storage::{self, ListOptions, OciClient, OciFile};
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
const TABLE: &str = "oci_doc_embeddings";
const TOP_K: i64 = 5;

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(static OCI: OciClient);
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

/// Embed one chunk and declare its row (cf. Python `process_chunk`).
#[cocoindex::function]
async fn process_chunk(
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

/// Chunk and embed one object, declaring its rows (cf. Python `process_file`).
/// Memoized on the object: an unchanged object keeps its rows without re-running.
/// The table target is one per app, so it is not part of the memo key.
#[cocoindex::function(memo, memo_key(table = skip))]
async fn process_file(ctx: &Ctx, file: OciFile, table: postgres::TableTarget) -> Result<()> {
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
    let filename = file.key();
    let id_gen = IdGenerator::new();
    ctx.map(chunks, async |chunk| {
        process_chunk(ctx, chunk, &text, &filename, &id_gen, &table).await
    })
    .await?;
    Ok(())
}

async fn app_main(ctx: Ctx, namespace: String, bucket: String, prefix: String) -> Result<()> {
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

    let files = oci_object_storage::list_objects(
        ctx.get_key(&OCI)?,
        &namespace,
        &bucket,
        ListOptions {
            prefix,
            path_matcher: Some(Arc::new(PatternFilePathMatcher::include(["**/*.md"])?)),
            ..Default::default()
        },
    )
    .items()
    .await?;
    println!(
        "indexing {} markdown file(s) from oci://{namespace}/{bucket}",
        files.len()
    );

    // One processing component per object: each object's rows sync and clean up
    // with that object (cf. Python `coco.mount_each(process_file, files.items(), target_table)`).
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
            let namespace = std::env::var("OCI_NAMESPACE")
                .map_err(|_| Error::engine("OCI_NAMESPACE is required"))?;
            let bucket =
                std::env::var("OCI_BUCKET").map_err(|_| Error::engine("OCI_BUCKET is required"))?;
            let prefix = std::env::var("OCI_PREFIX").unwrap_or_default();

            let db = postgres::Database::connect(&database_url()).await?;
            let oci = OciClient::connect().await?;
            let embedder = load_embedder().await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&OCI, oci)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("OciObjectStorageEmbeddingRust")
                .await?;
            let stats = app
                .run(move |ctx| app_main(ctx, namespace, bucket, prefix))
                .await?;
            println!("{stats}");
        }
    }
    Ok(())
}
