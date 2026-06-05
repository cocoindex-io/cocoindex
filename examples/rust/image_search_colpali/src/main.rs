//! ColPali multi-vector image search with Qdrant — Rust port of the Python
//! `image_search_colpali` example.
//!
//! Pipeline: walk an image folder -> embed each image with ColPali into a
//! *list* of vectors (late interaction) -> store in a Qdrant MAX_SIM
//! multi-vector collection. Queries embed text into the same multi-vector shape
//! and run a MAX_SIM search.
//!
//!   cargo run -- index [IMAGE_DIR]      # incremental
//!   cargo run -- query "an invoice"     # ColPali query -> MAX_SIM search
//!
//! ColPali has no pure-Rust model, so this example offloads inference to an
//! external ColPali HTTP service (set `COLPALI_URL`, default
//! `http://localhost:8000`). The service must expose:
//!
//!   POST /embed-image   body: raw image bytes        -> {"embedding": [[f32; D]; N]}
//!   POST /embed-query   body: {"query": "<text>"}    -> {"embedding": [[f32; D]; M]}
//!
//! where `D` is the per-vector dimension (128 for `vidore/colpali-v1.2`). A
//! reference Python server is in the README. Everything else — the incremental
//! pipeline and the Qdrant MAX_SIM multi-vector collection — is native Rust via
//! `cocoindex::qdrant`.
//!
//! Build note: `qdrant-client` compiles protobufs, so `protoc` is required.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::prelude::*;
use cocoindex::qdrant::{self, CollectionSchema, Distance, QdrantConnection};
use cocoindex::walk;
use serde::Deserialize;
use serde_json::json;

/// ColPali per-vector dimension (`vidore/colpali-v1.2`).
const EMBED_DIM: u64 = 128;
const COLLECTION: &str = "ImageSearchColpali";
const TOP_K: u64 = 5;

const IMAGE_GLOBS: &[&str] = &[
    "**/*.jpg",
    "**/*.jpeg",
    "**/*.png",
    "**/*.webp",
    "**/*.gif",
    "**/*.bmp",
];

static DB: LazyLock<ContextKey<QdrantConnection>> = LazyLock::new(|| {
    ContextKey::new_with_state("image_search_colpali_db", |c: &QdrantConnection| {
        c.state_id().to_string()
    })
});
static COLPALI: LazyLock<ContextKey<ColpaliClient>> = LazyLock::new(|| {
    ContextKey::new_with_state("colpali", |c: &ColpaliClient| c.url.clone())
});

/// HTTP client for an external ColPali inference service (see module docs).
#[derive(Clone)]
struct ColpaliClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    /// One vector per image patch / query token (late-interaction).
    embedding: Vec<Vec<f32>>,
}

impl ColpaliClient {
    fn new(url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
        }
    }

    async fn embed_image(&self, bytes: Vec<u8>) -> Result<Vec<Vec<f32>>> {
        let resp = self
            .http
            .post(format!("{}/embed-image", self.url))
            .header("content-type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::engine(format!("colpali embed-image: {e}")))?;
        let out: EmbeddingResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("colpali embed-image decode: {e}")))?;
        Ok(out.embedding)
    }

    async fn embed_query(&self, text: &str) -> Result<Vec<Vec<f32>>> {
        let resp = self
            .http
            .post(format!("{}/embed-query", self.url))
            .json(&json!({ "query": text }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::engine(format!("colpali embed-query: {e}")))?;
        let out: EmbeddingResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("colpali embed-query decode: {e}")))?;
        Ok(out.embedding)
    }
}

/// A computed point: stable id + multi-vector embedding + source filename.
#[derive(Clone, Serialize, Deserialize)]
struct PointData {
    id: u64,
    vectors: Vec<Vec<f32>>,
    filename: String,
}

#[cocoindex::function(memo)]
async fn process_image(ctx: &Ctx, file: &FileEntry) -> Result<PointData> {
    let filename = file.key();
    let bytes = file.content()?;
    let vectors = ctx.get_key(&COLPALI)?.embed_image(bytes).await?;
    let mut id_gen = IdGenerator::new();
    let id = id_gen.next_id(&ctx, &filename).await?;
    Ok(PointData {
        id,
        vectors,
        filename,
    })
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let conn = ctx.get_key(&DB)?;
    let target = qdrant::mount_collection_target(
        &ctx,
        conn,
        COLLECTION,
        CollectionSchema::multivector(EMBED_DIM, Distance::Cosine),
    )
    .await?;

    let files: Vec<FileEntry> = walk(&sourcedir, IMAGE_GLOBS)?;
    println!(
        "indexing {} image(s) from {}",
        files.len(),
        sourcedir.display()
    );

    let points = ctx
        .mount_each(
            files,
            |f| f.key(),
            |child, file| async move { process_image(&child, &file).await },
        )
        .await?;

    for p in &points {
        let payload = json!({ "filename": p.filename })
            .as_object()
            .unwrap()
            .clone();
        target.declare_multivector_point(&ctx, p.id, p.vectors.clone(), payload)?;
    }
    println!("indexed {} image(s) total", points.len());
    Ok(())
}

async fn query_once(conn: &QdrantConnection, colpali: &ColpaliClient, query: &str) -> Result<()> {
    let query_vectors = colpali.embed_query(query).await?;
    let hits = qdrant::multivector_search(conn, COLLECTION, query_vectors, TOP_K).await?;
    for hit in hits {
        let filename = hit
            .payload
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        println!("[{:.3}] {filename}", hit.score);
    }
    Ok(())
}

fn qdrant_url() -> String {
    std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string())
}

fn colpali_url() -> String {
    std::env::var("COLPALI_URL").unwrap_or_else(|_| "http://localhost:8000".to_string())
}

fn default_sourcedir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("img")
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
            let colpali = ColpaliClient::new(colpali_url());
            query_once(&conn, &colpali, &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let conn = QdrantConnection::connect(&qdrant_url()).await?;
            let colpali = ColpaliClient::new(colpali_url());
            let app = App::builder("ImageSearchColpaliRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, conn)
                .provide_key(&COLPALI, colpali)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
