//! Paper Metadata — Rust port of the Python `paper_metadata` example.
//!
//! Pipeline: walk local PDFs -> extract first-page text + page count ->
//! LLM-extract title/authors/abstract -> embed title and abstract chunks ->
//! store in three Postgres tables (metadata, author↔paper, pgvector embeddings).
//!
//!   cargo run -- index [PAPERS_DIR]    # incremental (unchanged PDFs memo-skipped)
//!   cargo run -- query "your query"    # pgvector similarity search (no index)
//!
//! Parallels the Python example:
//!   - source           : `cocoindex::fs::walk` (cf. `localfs.walk_dir`)
//!   - per-file compute  : `#[cocoindex::function(memo)]` (cf. `@coco.fn(memo=True)`)
//!   - PDF parsing       : `lopdf` (cf. `pypdf` first-page text + page count)
//!   - LLM extraction    : OpenAI chat completions, JSON mode (cf. `openai` client)
//!   - chunking          : `cocoindex::ops::text::RecursiveSplitter` w/ a custom
//!                         "abstract" language (cf. `RecursiveSplitter` +
//!                         `CustomLanguageConfig`)
//!   - embeddings        : `cocoindex::ops::sentence_transformers` all-MiniLM-L6-v2
//!                         (same model as Python)
//!   - targets           : three `postgres::TableTarget`s (cf. three
//!                         `postgres.mount_table_target`s)
//!
//! Deviation from Python: embedding-row UUIDs are derived deterministically
//! from (filename, location, text) via the SDK's `UuidGenerator` rather than
//! `uuid.uuid4()`, so re-runs are stable. Like Python, no vector index is
//! created (the query demo does a sequential cosine scan).

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::id::UuidGenerator;
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{CustomLanguageConfig, RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::postgres;
use cocoindex::prelude::*;
use cocoindex::walk;
use serde::de::DeserializeOwned;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const TABLE_METADATA: &str = "paper_metadata";
const TABLE_AUTHOR_PAPERS: &str = "author_papers";
const TABLE_EMBEDDINGS: &str = "metadata_embeddings";
const PG_SCHEMA: &str = "coco_examples_v1";
const LLM_MODEL: &str = "gpt-4o";
const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const EMBED_DIM: usize = 384;
const TOP_K: i64 = 5;

const ABSTRACT_CHUNK_SIZE: usize = 500;
const ABSTRACT_MIN_CHUNK_SIZE: usize = 200;
const ABSTRACT_CHUNK_OVERLAP: usize = 150;
const LLM_INPUT_CHARS: usize = 4000;

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("paper_metadata_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});
static EMBEDDER: LazyLock<ContextKey<SentenceTransformerEmbedder>> = LazyLock::new(|| {
    ContextKey::new_with_state("embedder", |e: &SentenceTransformerEmbedder| {
        e.model_name().to_string()
    })
});
static LLM: LazyLock<ContextKey<LlmClient>> =
    LazyLock::new(|| ContextKey::new_with_state("llm_model", |c: &LlmClient| c.model.clone()));

// ---------------------------------------------------------------------------
// Clients: LLM (OpenAI JSON mode); embedder is `SentenceTransformerEmbedder`
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LlmClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl LlmClient {
    fn new(model: String) -> Result<Self> {
        let api_key =
            std::env::var("OPENAI_API_KEY").map_err(|_| Error::engine("set OPENAI_API_KEY"))?;
        let base_url =
            std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into());
        Ok(Self {
            http: reqwest::Client::new(),
            api_key,
            base_url,
            model,
        })
    }

    /// Chat completion in JSON-object mode (`temperature=0`); deserialize the
    /// response content into `T`. The expected shape must be described in `system`.
    async fn json<T: DeserializeOwned>(&self, system: &str, user: &str) -> Result<T> {
        let body = serde_json::json!({
            "model": self.model,
            "response_format": { "type": "json_object" },
            "temperature": 0,
            "messages": [
                { "role": "system", "content": system },
                { "role": "user", "content": user },
            ],
        });
        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::engine(format!("LLM request failed: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| Error::engine(format!("LLM response read failed: {e}")))?;
        if !status.is_success() {
            return Err(Error::engine(format!("LLM HTTP {status}: {text}")));
        }
        let envelope: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| Error::engine(format!("LLM response not JSON: {e}: {text}")))?;
        let content = envelope["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| Error::engine(format!("LLM response missing content: {text}")))?;
        serde_json::from_str::<T>(content).map_err(|e| {
            Error::engine(format!("LLM content not the expected JSON: {e}: {content}"))
        })
    }
}

// ---------------------------------------------------------------------------
// Data models
// ---------------------------------------------------------------------------

/// LLM-extracted author (cf. Python `AuthorModel`).
#[derive(Clone, Serialize, Deserialize)]
struct AuthorModel {
    name: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    affiliation: Option<String>,
}

/// LLM-extracted paper metadata (cf. Python `PaperMetadataModel`).
#[derive(Clone, Deserialize)]
struct PaperMetadataModel {
    title: String,
    #[serde(default)]
    authors: Vec<AuthorModel>,
    #[serde(default, rename = "abstract")]
    abstract_text: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct PaperMetadataRow {
    filename: String,
    title: String,
    authors: serde_json::Value, // jsonb: [{name, email, affiliation}]
    #[serde(rename = "abstract")]
    abstract_text: String,
    num_pages: i32,
}

#[derive(Clone, Serialize, Deserialize)]
struct AuthorPaperRow {
    author_name: String,
    filename: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct MetadataEmbeddingRow {
    id: String, // uuid
    filename: String,
    location: String,
    text: String,
    embedding: Vec<f32>,
}

/// Everything one paper contributes to the three tables. Returned by the
/// memoized `process_file` and declared (per child component) in `app_main`.
#[derive(Clone, Serialize, Deserialize)]
struct ProcessedPaper {
    metadata: PaperMetadataRow,
    authors: Vec<AuthorPaperRow>,
    embeddings: Vec<MetadataEmbeddingRow>,
}

// ---------------------------------------------------------------------------
// PDF + LLM extraction
// ---------------------------------------------------------------------------

/// Extract first-page text and total page count from a PDF (cf. Python's
/// `extract_basic_info` + `pdf_to_markdown`).
fn pdf_first_page_text_and_pages(content: &[u8]) -> Result<(String, i32)> {
    let doc = lopdf::Document::load_mem(content)
        .map_err(|e| Error::engine(format!("failed to parse PDF: {e}")))?;
    let pages = doc.get_pages();
    let num_pages = i32::try_from(pages.len()).unwrap_or(i32::MAX);
    let Some(&first_page) = pages.keys().next() else {
        return Ok((String::new(), num_pages));
    };
    let text = doc
        .extract_text(&[first_page])
        .map_err(|e| Error::engine(format!("failed to extract first-page text: {e}")))?;
    Ok((text, num_pages))
}

fn abstract_chunker() -> Result<RecursiveSplitter> {
    RecursiveSplitter::with_custom_languages(vec![CustomLanguageConfig {
        language_name: "abstract".to_string(),
        aliases: vec![],
        separators_regex: vec![
            r"[.?!]+\s+".to_string(),
            r"[:;]\s+".to_string(),
            r",\s+".to_string(),
            r"\s+".to_string(),
        ],
    }])
}

async fn extract_metadata(llm: &LlmClient, first_page_text: &str) -> Result<PaperMetadataModel> {
    let system = "You extract metadata from academic paper first pages. \
        Return only JSON with keys: title, authors, abstract. \
        authors is a list of {name, email, affiliation}. \
        Use null for missing fields.";
    let user: String = first_page_text.chars().take(LLM_INPUT_CHARS).collect();
    llm.json(system, &user).await
}

#[cocoindex::function(memo)]
async fn process_file(ctx: &Ctx, file: &FileEntry) -> Result<ProcessedPaper> {
    let filename = file.key();
    let content = file.content()?;
    let (first_page_text, num_pages) =
        tokio::task::spawn_blocking(move || pdf_first_page_text_and_pages(&content))
            .await
            .map_err(|e| Error::engine(format!("PDF parse task panicked: {e}")))??;

    let llm = ctx.get_key(&LLM)?;
    let metadata = extract_metadata(llm, &first_page_text).await?;
    let embedder = ctx.get_key(&EMBEDDER)?;

    let authors_json = serde_json::to_value(&metadata.authors)
        .map_err(|e| Error::engine(format!("serialize authors: {e}")))?;

    let author_rows: Vec<AuthorPaperRow> = metadata
        .authors
        .iter()
        .filter(|a| !a.name.is_empty())
        .map(|a| AuthorPaperRow {
            author_name: a.name.clone(),
            filename: filename.clone(),
        })
        .collect();

    // Deterministic per-paper UUIDs (stable across runs unlike Python's uuid4).
    let mut uuid_gen = UuidGenerator::with_deps(&filename)?;
    let mut embeddings = Vec::new();

    // Title embedding (one row).
    let title_vec = embedder.embed(&metadata.title).await?;
    let title_id = uuid_gen
        .next_uuid(&ctx, &("title", &metadata.title))
        .await?
        .to_string();
    embeddings.push(MetadataEmbeddingRow {
        id: title_id,
        filename: filename.clone(),
        location: "title".to_string(),
        text: metadata.title.clone(),
        embedding: title_vec,
    });

    // Abstract chunks (one row each).
    let chunker = abstract_chunker()?;
    let chunks = chunker.split_with(
        &metadata.abstract_text,
        RecursiveChunkConfig {
            chunk_size: ABSTRACT_CHUNK_SIZE,
            min_chunk_size: Some(ABSTRACT_MIN_CHUNK_SIZE),
            chunk_overlap: Some(ABSTRACT_CHUNK_OVERLAP),
            language: Some("abstract".to_string()),
        },
    );
    let chunk_texts: Vec<String> = chunks
        .iter()
        .map(|c| c.text(&metadata.abstract_text).to_string())
        .collect();
    if !chunk_texts.is_empty() {
        let vecs = embedder.embed_batch(chunk_texts.clone()).await?;
        for (text, embedding) in chunk_texts.into_iter().zip(vecs) {
            let id = uuid_gen.next_uuid(&ctx, &("abstract", &text)).await?.to_string();
            embeddings.push(MetadataEmbeddingRow {
                id,
                filename: filename.clone(),
                location: "abstract".to_string(),
                text,
                embedding,
            });
        }
    }

    Ok(ProcessedPaper {
        metadata: PaperMetadataRow {
            filename,
            title: metadata.title,
            authors: authors_json,
            abstract_text: metadata.abstract_text,
            num_pages,
        },
        authors: author_rows,
        embeddings,
    })
}

// ---------------------------------------------------------------------------
// Table schemas
// ---------------------------------------------------------------------------

fn metadata_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("filename", postgres::ColumnDef::new("text")),
            ("title", postgres::ColumnDef::new("text")),
            ("authors", postgres::ColumnDef::new("jsonb")),
            ("abstract", postgres::ColumnDef::new("text")),
            ("num_pages", postgres::ColumnDef::new("integer")),
        ],
        ["filename"],
    )
}

fn author_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("author_name", postgres::ColumnDef::new("text")),
            ("filename", postgres::ColumnDef::new("text")),
        ],
        ["author_name", "filename"],
    )
}

fn embedding_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("id", postgres::ColumnDef::new("uuid")),
            ("filename", postgres::ColumnDef::new("text")),
            ("location", postgres::ColumnDef::new("text")),
            ("text", postgres::ColumnDef::new("text")),
            (
                "embedding",
                postgres::ColumnDef::new(format!("vector({EMBED_DIM})")),
            ),
        ],
        ["id"],
    )
}

// ---------------------------------------------------------------------------
// Indexing
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let metadata_table =
        postgres::mount_table_target(&ctx, db, TABLE_METADATA, metadata_schema()?, Some(PG_SCHEMA))
            .await?;
    let author_table = postgres::mount_table_target(
        &ctx,
        db,
        TABLE_AUTHOR_PAPERS,
        author_schema()?,
        Some(PG_SCHEMA),
    )
    .await?;
    let embedding_table = postgres::mount_table_target(
        &ctx,
        db,
        TABLE_EMBEDDINGS,
        embedding_schema()?,
        Some(PG_SCHEMA),
    )
    .await?;

    let files: Vec<FileEntry> = walk(&sourcedir, &["**/*.pdf"])?;
    println!(
        "indexing {} PDF(s) from {}",
        files.len(),
        sourcedir.display()
    );

    ctx.mount_each(files, |f| f.key(), {
        let metadata_table = metadata_table.clone();
        let author_table = author_table.clone();
        let embedding_table = embedding_table.clone();
        move |child, file| {
            let metadata_table = metadata_table.clone();
            let author_table = author_table.clone();
            let embedding_table = embedding_table.clone();
            async move {
                let processed = process_file(&child, &file).await?;
                metadata_table.declare_row(&child, &processed.metadata)?;
                for row in &processed.authors {
                    author_table.declare_row(&child, row)?;
                }
                for row in &processed.embeddings {
                    embedding_table.declare_row(&child, row)?;
                }
                Ok(())
            }
        }
    })
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Query demo (no vector index — sequential cosine scan, like Python)
// ---------------------------------------------------------------------------

async fn query_once(pool: &PgPool, embedder: &SentenceTransformerEmbedder, query: &str) -> Result<()> {
    let query_vec = vector_param(&embedder.embed(query).await?);
    let rows = sqlx::query(&format!(
        "SELECT filename, location, text, embedding <=> $1::vector AS distance \
         FROM \"{PG_SCHEMA}\".\"{TABLE_EMBEDDINGS}\" ORDER BY distance ASC LIMIT $2"
    ))
    .bind(query_vec)
    .bind(TOP_K)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    for row in rows {
        let filename: String = row.try_get("filename").map_err(db_err)?;
        let location: String = row.try_get("location").map_err(db_err)?;
        let text: String = row.try_get("text").map_err(db_err)?;
        let distance: f64 = row.try_get("distance").map_err(db_err)?;
        println!("[{:.3}] {filename} ({location})", 1.0 - distance);
        println!("    {}", text.replace('\n', "\n    "));
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

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn database_url() -> Result<String> {
    std::env::var("POSTGRES_URL").map_err(|_| Error::engine("POSTGRES_URL is not set"))
}

fn default_sourcedir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("papers")
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
                .connect(&database_url()?)
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
            .unwrap_or_else(default_sourcedir);

            let db = postgres::Database::connect(&database_url()?).await?;
            let llm = LlmClient::new(LLM_MODEL.to_string())?;
            let embedder = load_embedder().await?;
            let app = App::builder("PaperMetadataV1")
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DB, db)
                .provide_key(&LLM, llm)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
