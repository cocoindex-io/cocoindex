//! Azure Blob Storage Text Embedding — Rust port of the Python
//! `azure_blob_embedding` example.
//!
//! Pipeline: list markdown blobs from a container -> read -> chunk -> embed ->
//! store in Postgres/pgvector.
//!
//!   cargo run -- index                 # one-shot (the source is not live)
//!   cargo run -- query "your query"    # pgvector similarity search
//!
//! Set `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_CONTAINER` (and optional
//! `AZURE_BLOB_PREFIX`), plus a credential: `AZURE_STORAGE_KEY` (account
//! key), `AZURE_STORAGE_SAS_TOKEN` or `AZURE_STORAGE_ACCESS_TOKEN` (a Microsoft
//! Entra ID token); with none of them, the client gets an Entra ID token as
//! `DefaultAzureCredential` does (`AZURE_TENANT_ID` / `AZURE_CLIENT_ID` /
//! `AZURE_CLIENT_SECRET`, a managed identity, or the `az login` account).
//! `AZURE_STORAGE_ACCOUNT_URL` overrides the account URL, for the Azurite
//! emulator.

use std::path::PathBuf;
use std::sync::Arc;

use cocoindex::connectors::azure_blob::{
    self, AzureBlobFile, ContainerClient, Credential, ListOptions, TokenCredential,
};
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
const TABLE: &str = "azure_blob_doc_embeddings";
const TOP_K: i64 = 5;

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(static CONTAINER: ContainerClient);
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

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: AzureBlobFile) -> Result<Vec<DocEmbeddingRow>> {
    let text = file.read_text().await?;
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
    let id_gen = IdGenerator::new();
    let mut rows = Vec::with_capacity(texts.len());
    for (chunk_text, embedding) in texts.into_iter().zip(embeddings) {
        let id = id_gen.next_id(ctx, &chunk_text).await?;
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

async fn app_main(ctx: Ctx, prefix: String) -> Result<()> {
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

    let client = ctx.get_key(&CONTAINER)?;
    let files = azure_blob::list_blobs(
        client,
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
        "indexing {} markdown file(s) from {}/{}",
        files.len(),
        client.account_name(),
        client.container_name()
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

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn required_env(name: &str) -> Result<String> {
    env_var(name).ok_or_else(|| Error::engine(format!("{name} is required")))
}

/// Build the container client from the environment (see the module docs).
fn container_client() -> Result<ContainerClient> {
    let account = required_env("AZURE_STORAGE_ACCOUNT")?;
    let container = required_env("AZURE_STORAGE_CONTAINER")?;
    let account_url = env_var("AZURE_STORAGE_ACCOUNT_URL")
        .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
    let credential = if let Some(key) = env_var("AZURE_STORAGE_KEY") {
        Credential::AccountKey(key)
    } else if let Some(sas) = env_var("AZURE_STORAGE_SAS_TOKEN") {
        Credential::SasToken(sas)
    } else if let Some(token) = env_var("AZURE_STORAGE_ACCESS_TOKEN") {
        Credential::BearerToken(token)
    } else {
        Credential::TokenCredential(TokenCredential::Default)
    };
    ContainerClient::new(&account_url, container, credential)
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
            let prefix = std::env::var("AZURE_BLOB_PREFIX").unwrap_or_default();

            let db = postgres::Database::connect(&database_url()).await?;
            let container = container_client()?;
            let embedder = load_embedder().await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&CONTAINER, container)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app("AzureBlobEmbeddingRust")
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, prefix)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
