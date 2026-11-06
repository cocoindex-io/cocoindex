//! Entire Session Search — Rust port of the Python `entire_session_search`
//! example.
//!
//! Indexes AI coding session checkpoints from Entire (https://entire.io) —
//! transcripts, prompts, context summaries, and metadata — into Postgres with
//! pgvector for semantic search.
//!
//!   cargo run                              # catch-up over ./entire_checkpoints
//!   cargo run -- --live                    # keep watching for new sessions
//!   cargo run -- index [CHECKPOINTS_DIR] [--live]
//!   cargo run -- query "your query"        # pgvector similarity search
//!
//! A checkpoint folder holds four file types, and `process_file` routes on the
//! name: `full.jsonl` is parsed into per-turn transcript chunks, `prompt.txt` is
//! embedded whole, `context.md` is split into overlapping chunks, and
//! `metadata.json` becomes a structured row in a second table.
//!
//! Parallels the Python example:
//!   - source            : `walk_dir(..).live()` + `ctx.mount_each_live` (cf. `localfs.walk_dir(live=True)`)
//!   - per-file compute  : `#[cocoindex::function]` routing on the file name
//!   - chunk fan-out     : `ctx.map` (cf. `coco.map(process_chunk, ...)`)
//!   - chunking          : `cocoindex::ops::text::RecursiveSplitter` (cf. `RecursiveSplitter`)
//!   - embeddings        : `cocoindex::ops::sentence_transformers` all-MiniLM-L6-v2
//!   - targets           : two `postgres::TableTarget`s (embeddings + metadata)

use std::path::PathBuf;

use cocoindex::connectors::postgres::{self, Database, TableSchema};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use sqlx::Row as _;
use sqlx::postgres::{PgPool, PgPoolOptions};

const APP_NAME: &str = "EntireSessionSearch";
const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const TOP_K: i64 = 5;
const CHUNK_SIZE: usize = 2000;
const CHUNK_OVERLAP: usize = 500;
/// Transcript entries shorter than this are skipped as trivial.
const MIN_TRANSCRIPT_CHARS: usize = 20;

cocoindex::context_key!(static PG_DB: Database);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn database_url() -> String {
    env_or(
        "POSTGRES_URL",
        "postgres://cocoindex:cocoindex@localhost/cocoindex",
    )
}

fn table_embeddings() -> String {
    env_or("TABLE_EMBEDDINGS", "session_embeddings")
}

fn table_metadata() -> String {
    env_or("TABLE_METADATA", "session_metadata")
}

fn pg_schema_name() -> String {
    env_or("PG_SCHEMA_NAME", "entire")
}

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct SessionEmbeddingRow {
    id: i64,
    checkpoint_id: String,
    session_index: String,
    content_type: String, // "transcript", "prompt", or "context"
    role: String,         // "user", "assistant", or "" for non-transcript
    text: String,
    #[coco(vector)]
    embedding: Vec<f32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct SessionMetadataRow {
    checkpoint_id: String,
    session_index: String,
    prompt_summary: String,
    total_tokens: i64,
    files_touched: String, // JSON array
    agent_percentage: Option<f64>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Checkpoint and session identifiers extracted from a file path.
#[derive(Clone, Debug)]
struct SessionInfo {
    checkpoint_id: String,
    session_index: String,
}

/// A text chunk ready for embedding, with its metadata.
#[derive(Clone, Debug)]
struct ChunkInput {
    text: String,
    content_type: &'static str, // "transcript", "prompt", or "context"
    role: String,               // "user", "assistant", or "" for non-transcript
}

/// Entire layout: `<checkpoint_id[:2]>/<checkpoint_id[2:]>/<session_idx>/<filename>`.
/// The shard directory is the first 2 chars of the checkpoint id; the two are
/// concatenated to get the full 12-char id.
fn extract_session_info(file: &FileEntry) -> Result<SessionInfo> {
    let path = file.path();
    let parts: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let n = parts.len();
    if n < 4 {
        return Err(Error::engine(format!(
            "{} is not inside a <shard>/<checkpoint>/<session>/ layout",
            path.display()
        )));
    }
    Ok(SessionInfo {
        checkpoint_id: format!("{}{}", parts[n - 4], parts[n - 3]),
        session_index: parts[n - 2].clone(),
    })
}

/// Parse `full.jsonl` into transcript chunks, skipping trivial entries.
fn parse_transcript(content: &str) -> Vec<ChunkInput> {
    let mut chunks = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let role = entry["role"].as_str().unwrap_or("unknown").to_string();
        // Handle different content formats: a string, a list of parts (text,
        // tool_use, ...), or a top-level `text`.
        let text = match &entry["content"] {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Array(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    serde_json::Value::String(s) => Some(s.as_str()),
                    serde_json::Value::Object(_) if part["type"] == "text" => {
                        Some(part["text"].as_str().unwrap_or(""))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
            _ => entry["text"].as_str().unwrap_or("").to_string(),
        };
        let text = text.trim().to_string();
        if text.chars().count() < MIN_TRANSCRIPT_CHARS {
            continue;
        }
        chunks.push(ChunkInput {
            text,
            content_type: "transcript",
            role,
        });
    }
    chunks
}

// ---------------------------------------------------------------------------
// Processing
// ---------------------------------------------------------------------------

/// Embed and declare the chunks of one file concurrently (cf. Python's
/// `coco.map(process_chunk, ...)`). Each row's id is derived from its text, so
/// a turn that survives a re-parse keeps its row.
async fn declare_chunks(
    ctx: &Ctx,
    chunks: Vec<ChunkInput>,
    info: &SessionInfo,
    emb_table: &postgres::TableTarget,
) -> Result<()> {
    let mut id_gen = IdGenerator::new();
    let mut ids = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        ids.push(id_gen.next_id(ctx, &chunk.text).await? as i64);
    }
    let embedder = ctx.get_key(&EMBEDDER)?.clone();
    let embedding_ctx = ctx.clone();
    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
    let embeddings = ctx
        .map(texts, move |text| {
            let embedder = embedder.clone();
            let ctx = embedding_ctx.clone();
            async move { embedder.embed(&ctx, text).await }
        })
        .await?;
    for ((chunk, id), embedding) in chunks.into_iter().zip(ids).zip(embeddings) {
        emb_table.declare_row(
            ctx,
            &SessionEmbeddingRow {
                id,
                checkpoint_id: info.checkpoint_id.clone(),
                session_index: info.session_index.clone(),
                content_type: chunk.content_type.to_string(),
                role: chunk.role,
                text: chunk.text,
                embedding,
            },
        )?;
    }
    Ok(())
}

#[cocoindex::function]
async fn process_file(
    ctx: &Ctx,
    file: FileEntry,
    emb_table: postgres::TableTarget,
    meta_table: postgres::TableTarget,
) -> Result<()> {
    let info = extract_session_info(&file)?;
    let filename = file.file_path().name().to_string();

    match filename.as_str() {
        "full.jsonl" => {
            let chunks = parse_transcript(&file.content_str()?);
            declare_chunks(ctx, chunks, &info, &emb_table).await
        }
        "prompt.txt" => {
            let text = file.content_str()?.trim().to_string();
            if text.is_empty() {
                return Ok(());
            }
            declare_chunks(
                ctx,
                vec![ChunkInput {
                    text,
                    content_type: "prompt",
                    role: "user".to_string(),
                }],
                &info,
                &emb_table,
            )
            .await
        }
        "context.md" => {
            let text = file.content_str()?.trim().to_string();
            if text.is_empty() {
                return Ok(());
            }
            let splitter = RecursiveSplitter::new()?;
            let chunks = splitter
                .split_with(
                    &text,
                    RecursiveChunkConfig {
                        chunk_size: CHUNK_SIZE,
                        min_chunk_size: None,
                        chunk_overlap: Some(CHUNK_OVERLAP),
                        language: Some("markdown".to_string()),
                    },
                )
                .iter()
                .map(|c| ChunkInput {
                    text: c.text(&text).to_string(),
                    content_type: "context",
                    role: String::new(),
                })
                .collect();
            declare_chunks(ctx, chunks, &info, &emb_table).await
        }
        "metadata.json" => {
            let Ok(meta) = serde_json::from_str::<serde_json::Value>(&file.content_str()?) else {
                return Ok(());
            };
            let prompt_summary = meta["summary"]["intent"].as_str().unwrap_or("").to_string();
            let token_usage = &meta["token_usage"];
            let total_tokens = token_usage["input_tokens"].as_i64().unwrap_or(0)
                + token_usage["output_tokens"].as_i64().unwrap_or(0);
            let files_touched = match &meta["files_touched"] {
                list @ serde_json::Value::Array(_) => list.to_string(),
                serde_json::Value::Null => "[]".to_string(),
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let agent_percentage = meta["initial_attribution"]["agent_percentage"].as_f64();
            meta_table.declare_row(
                ctx,
                &SessionMetadataRow {
                    checkpoint_id: info.checkpoint_id,
                    session_index: info.session_index,
                    prompt_summary,
                    total_tokens,
                    files_touched,
                    agent_percentage,
                },
            )
        }
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, checkpoints_dir: PathBuf) -> Result<()> {
    let pg_schema = pg_schema_name();
    let emb_table = postgres::mount_table_target(
        &ctx,
        &PG_DB,
        table_embeddings(),
        TableSchema::from_row::<SessionEmbeddingRow>(["id"])?
            .with_vector_schema("embedding", ctx.get_key(&EMBEDDER)?)
            .await?,
        Some(&pg_schema),
    )
    .await?;
    let meta_table = postgres::mount_table_target(
        &ctx,
        &PG_DB,
        table_metadata(),
        TableSchema::from_row::<SessionMetadataRow>(["checkpoint_id", "session_index"])?,
        Some(&pg_schema),
    )
    .await?;

    // One component per checkpoint file. In live mode (`--live`) the walker
    // keeps watching the directory for new sessions.
    let files = walk_dir(checkpoints_dir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::new(
            [
                "**/full.jsonl",
                "**/prompt.txt",
                "**/context.md",
                "**/metadata.json",
            ],
            ["**/content_hash.txt"],
        )?)
        .live();
    ctx.mount_each_live(&"files", files, move |ctx, file: FileEntry| {
        let (emb_table, meta_table) = (emb_table.clone(), meta_table.clone());
        async move { process_file(&ctx, file, emb_table, meta_table).await }
    })
    .await
}

// ---------------------------------------------------------------------------
// Query demo
// ---------------------------------------------------------------------------

async fn query_once(
    pool: &PgPool,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
) -> Result<()> {
    let query_vec = vector_param(
        &embedder
            .embed_batch(vec![query.to_string()])
            .await?
            .remove(0),
    );
    let rows = sqlx::query(&format!(
        "SELECT checkpoint_id, session_index, content_type, role, text, \
         embedding <=> $1::vector AS distance \
         FROM \"{}\".\"{}\" ORDER BY distance ASC LIMIT $2",
        pg_schema_name(),
        table_embeddings()
    ))
    .bind(query_vec)
    .bind(TOP_K)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    for row in rows {
        let checkpoint_id: String = row.try_get("checkpoint_id").map_err(db_err)?;
        let session_index: String = row.try_get("session_index").map_err(db_err)?;
        let content_type: String = row.try_get("content_type").map_err(db_err)?;
        let role: String = row.try_get("role").map_err(db_err)?;
        let text: String = row.try_get("text").map_err(db_err)?;
        let distance: f64 = row.try_get("distance").map_err(db_err)?;
        let mut tag = content_type;
        if !role.is_empty() {
            tag = format!("{tag}/{role}");
        }
        println!(
            "[{:.3}] {checkpoint_id}/{session_index} ({tag})",
            1.0 - distance
        );
        let snippet: String = text.chars().take(200).collect();
        println!("    {snippet}");
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
                .connect(&database_url())
                .await
                .map_err(db_err)?;
            let embedder = SentenceTransformerEmbedder::load(EMBED_MODEL).await?;
            query_once(&pool, &embedder, &q).await
        }
        _ => {
            let live = args.iter().any(|a| a == "--live" || a == "-L");
            let checkpoints_dir = args
                .iter()
                .filter(|a| !a.starts_with('-') && a.as_str() != "index")
                .map(PathBuf::from)
                .next()
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("entire_checkpoints")
                });

            let db = Database::connect(&database_url()).await?;
            let embedder = SentenceTransformerEmbedder::load(EMBED_MODEL).await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&PG_DB, db)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app(APP_NAME)
                .await?;

            let body = move |ctx: Ctx| app_main(ctx, checkpoints_dir);
            if live {
                println!("watching for new sessions (Ctrl-C to stop)…");
                let handle = app.start_update_with_options(
                    UpdateOptions {
                        live: true,
                        ..UpdateOptions::default()
                    },
                    body,
                )?;
                handle.result().await?;
            } else {
                let stats = app.run(body).await?;
                println!("{stats}");
            }
            Ok(())
        }
    }
}
