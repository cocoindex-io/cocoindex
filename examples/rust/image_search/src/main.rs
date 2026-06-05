//! Image Search with CLIP + Qdrant — Rust port of the Python `image_search`
//! example.
//!
//! Pipeline: walk an image folder -> embed each image with the CLIP ViT-B/32
//! vision tower -> store in a Qdrant collection. Queries embed text with the
//! matching CLIP *text* tower (same 512-dim space) and run a vector search, so
//! you can search images by natural-language text.
//!
//!   cargo run -- index [IMAGE_DIR]     # incremental (unchanged images memo-skipped)
//!   cargo run -- query "a red bicycle" # CLIP text -> image vector search
//!
//! Both embedders run locally via `fastembed` (ONNX, no Python). The Python
//! example uses `transformers` CLIPModel; here `cocoindex::ops::image` (vision)
//! and `cocoindex::ops::sentence_transformers` (text) load the Qdrant-published
//! CLIP ONNX towers, which share an embedding space.
//!
//! Build note: `qdrant-client` compiles protobufs, so a `protoc` binary is
//! required to build (set `PROTOC` or put it on `PATH`). `fastembed` downloads
//! the CLIP ONNX models on first run.

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::ops::image::ImageEmbedder;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::prelude::*;
use cocoindex::qdrant::{self, CollectionSchema, Distance, QdrantConnection};
use cocoindex::walk;
use serde_json::json;

/// CLIP ViT-B/32 vision tower (images) and text tower (queries). Both output
/// 512-dim vectors in a shared space.
const IMAGE_MODEL: &str = "Qdrant/clip-ViT-B-32-vision";
const TEXT_MODEL: &str = "Qdrant/clip-ViT-B-32-text";
const EMBED_DIM: u64 = 512;
const COLLECTION: &str = "ImageSearch";
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
    ContextKey::new_with_state("image_search_db", |c: &QdrantConnection| {
        c.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<ImageEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("image_embedder", |e: &ImageEmbedder| {
        e.model_name().to_string()
    })
});

/// A computed point: stable id + image vector + source filename.
#[derive(Clone, Serialize, Deserialize)]
struct PointData {
    id: u64,
    vector: Vec<f32>,
    filename: String,
}

#[cocoindex::function(memo)]
async fn process_image(ctx: &Ctx, file: &FileEntry) -> Result<PointData> {
    let filename = file.key();
    let bytes = file.content()?;
    let vector = ctx.get_key(&EMBEDDER)?.embed(bytes).await?;
    let mut id_gen = IdGenerator::new();
    let id = id_gen.next_id(&ctx, &filename).await?;
    Ok(PointData {
        id,
        vector,
        filename,
    })
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let conn = ctx.get_key(&DB)?;
    let target = qdrant::mount_collection_target(
        &ctx,
        conn,
        COLLECTION,
        CollectionSchema::new(EMBED_DIM, Distance::Cosine),
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
        target.declare_point(&ctx, p.id, p.vector.clone(), payload)?;
    }
    println!("indexed {} image(s) total", points.len());
    Ok(())
}

async fn query_once(
    conn: &QdrantConnection,
    text_embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = text_embedder.embed(query).await?;
    let hits = qdrant::vector_search(conn, COLLECTION, query_vec, TOP_K).await?;
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
            // Text tower shares CLIP's image embedding space.
            let text_embedder = SentenceTransformerEmbedder::load(TEXT_MODEL).await?;
            query_once(&conn, &text_embedder, &q).await?;
        }
        sub => {
            let dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(default_sourcedir);

            let conn = QdrantConnection::connect(&qdrant_url()).await?;
            let embedder = ImageEmbedder::load(IMAGE_MODEL).await?;
            let app = App::builder("ImageSearchRust")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, conn)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
