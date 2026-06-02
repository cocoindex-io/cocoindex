//! Conversation to Knowledge — Rust port of the Python `conversation_to_knowledge` example.
//!
//! Pipeline: read sources -> (YouTube: yt-dlp + AssemblyAI | local transcript) ->
//! two LLM passes (speakers/metadata, then statements + entities) -> entity
//! resolution (embeddings + LLM) -> SurrealDB knowledge graph.
//!
//!   cargo run -- index [INPUT_DIR]   # default INPUT_DIR = ./input
//!
//! Input files in INPUT_DIR:
//!   - `*.txt`  : one YouTube URL per line (real path; needs yt-dlp + AssemblyAI)
//!   - `*.json` : a pre-transcribed session (cheap, audio-free) — see input/sample.json
//!
//! What the Rust SDK provides vs. what this example hand-rolls:
//!   - walk / memo / mount_each / map / ContextKey / IdGenerator  -> from `cocoindex`
//!   - entity resolution (candidate search + pair resolver)        -> from `cocoindex`
//!   - SurrealDB TableTarget / RelationTarget sync                 -> from `cocoindex`
//!   - LLM structured extraction + entity-pair confirmation        -> `reqwest` -> OpenAI JSON
//!   - audio download / diarized transcription                     -> `yt-dlp` + AssemblyAI REST
//!
//! Design note: costly per-session fetch+LLM work is memoized; graph writes are
//! declared through SurrealDB targets and reconciled by CocoIndex target state.

mod clients;
mod models;
mod pipeline;

use std::collections::HashMap;
use std::path::PathBuf;

use cocoindex::prelude::*;
use cocoindex::walk;
use serde::Deserialize;

use clients::{Embedder, Graph, LlmClient};
use models::*;
use pipeline::{collect_raw, create_knowledge_base, process_session, resolve_entities};

const INCLUDE: &[&str] = &["**/*.txt", "**/*.json"];

// ---------------------------------------------------------------------------
// Input parsing
// ---------------------------------------------------------------------------

/// Local pre-transcribed session input (`*.json`).
#[derive(Deserialize)]
struct LocalInput {
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    date: Option<String>,
    utterances: Vec<Utterance>,
}

fn parse_youtube_id(line: &str) -> Option<String> {
    let line = line.trim();
    for marker in ["watch?v=", "youtu.be/", "embed/"] {
        if let Some(pos) = line.find(marker) {
            let rest = &line[pos + marker.len()..];
            let id: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                .collect();
            if id.len() == 11 {
                return Some(id);
            }
        }
    }
    None
}

fn read_sources(dir: &PathBuf) -> Result<Vec<SessionSource>> {
    let mut sources = Vec::new();
    for file in walk(dir, INCLUDE)? {
        let content = file.content_str()?;
        if file.key().ends_with(".json") {
            let input: LocalInput = serde_json::from_str(&content)
                .map_err(|e| Error::engine(format!("invalid local input {}: {e}", file.key())))?;
            sources.push(SessionSource::Local {
                key: input.id.clone(),
                transcript: SessionTranscript {
                    utterances: input.utterances,
                    yt_channel: input.channel.unwrap_or_default(),
                    yt_title: input.title.unwrap_or(input.id),
                    yt_description: input.description,
                    yt_upload_date: input.date,
                },
            });
        } else {
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                match parse_youtube_id(line) {
                    Some(id) => sources.push(SessionSource::YouTube { youtube_id: id }),
                    None => eprintln!("skipping unrecognized line: {line}"),
                }
            }
        }
    }
    Ok(sources)
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, input_dir: PathBuf) -> Result<()> {
    let sources = read_sources(&input_dir)?;
    println!(
        "processing {} session(s) from {}",
        sources.len(),
        input_dir.display()
    );

    // Phase 1: per-session fetch + extract (memoized, concurrent).
    let processed: Vec<ProcessedSession> = ctx
        .mount_each(
            sources,
            |s| s.key(),
            |child, source| async move { process_session(&child, &source).await },
        )
        .await?;

    // Phase 2: entity resolution per type.
    let mut dedups: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();
    for kind in ENTITY_TYPES {
        let names = collect_raw(&processed, kind);
        let dedup = resolve_entities(&ctx, &kind.to_string(), &names).await?;
        dedups.insert(kind.to_string(), dedup);
    }

    // Phase 3: declare the desired knowledge graph.
    create_knowledge_base(&ctx, &processed, &dedups).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let input_dir = match args.first().map(String::as_str) {
        Some("index") => args.get(1).map(PathBuf::from),
        Some(other) => Some(PathBuf::from(other)),
        None => None,
    }
    .unwrap_or_else(|| PathBuf::from(env_or("INPUT_DIR", "./input")));

    // Connect SurrealDB + load LLM/embedder clients.
    let graph = Graph::connect(
        &env_or("SURREALDB_URL", "127.0.0.1:8787"),
        &env_or("SURREALDB_NS", "cocoindex"),
        &env_or("SURREALDB_DB", "yt_conversations"),
        &env_or("SURREALDB_USER", "root"),
        &env_or("SURREALDB_PASS", "root"),
    )
    .await?;
    let llm = LlmClient::new(env_or("LLM_MODEL", "gpt-4o-mini"))?;
    let resolver_llm = LlmClient::new(env_or("RESOLUTION_LLM_MODEL", "gpt-4o-mini"))?;
    let embedding_model = env_or("EMBEDDING_MODEL", "Snowflake/snowflake-arctic-embed-xs");
    let embedder = tokio::task::spawn_blocking(move || Embedder::load(&embedding_model))
        .await
        .map_err(|e| Error::engine(format!("embedder load panicked: {e}")))??;

    let graph_for_report = graph.clone();
    let app = App::builder("ConversationToKnowledge")
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&clients::GRAPH, graph)
        .provide_key(&clients::LLM, llm)
        .provide_key(&clients::RESOLVER_LLM, resolver_llm)
        .provide_key(&clients::EMBEDDER, embedder)
        .build()
        .await?;

    let stats = app.run(move |ctx| app_main(ctx, input_dir)).await?;
    println!(
        "graph: {} sessions, {} statements, {} persons, {} techs, {} orgs",
        graph_for_report.count("session").await?,
        graph_for_report.count("statement").await?,
        graph_for_report.count(PERSON).await?,
        graph_for_report.count(TECH).await?,
        graph_for_report.count(ORG).await?,
    );
    println!("{stats}");
    Ok(())
}
