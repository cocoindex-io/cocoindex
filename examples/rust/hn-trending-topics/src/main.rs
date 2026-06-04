//! HackerNews Trending Topics — Rust port of the Python `hn_trending_topics` example.
//!
//! Pipeline: scrape recent HN threads + comments (Algolia HN API) -> extract
//! topics per message via an LLM -> store in Postgres. A small CLI ranks
//! trending topics by mention score and searches messages by topic.
//!
//!   cargo run -- index                # fetch + extract + store (incremental)
//!   cargo run -- trending             # top trending topics
//!   cargo run -- search "rust"        # messages mentioning a topic
//!
//! What the Rust SDK provides vs. what this example hand-rolls:
//!   - mount_each / memo / ContextKey            -> from `cocoindex`
//!   - HN scraping (Algolia API) + LLM topics    -> `reqwest` (OpenAI JSON mode)
//!   - Postgres TableTarget incremental sync      -> from `cocoindex`
//!
//! Notes vs. Python: Python defaults to `gemini-2.5-flash`; this uses OpenAI
//! (`OPENAI_API_KEY`, default `gpt-4o-mini`). Per-thread work is memoized by
//! thread id; rows for threads that drop out of the latest list are reconciled.

use std::sync::LazyLock;

use cocoindex::prelude::*;
use cocoindex::postgres;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const THREAD_SCORE: i64 = 5;
const COMMENT_SCORE: i64 = 1;
const MAX_TEXT: usize = 4000;

static HTTP: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// Shared Postgres target database.
static PG: LazyLock<ContextKey<postgres::Database>> =
    LazyLock::new(|| ContextKey::new_with_state("hn_db", |db: &postgres::Database| db.state_id().to_string()));
/// LLM client; state-tracked on the model so changing it invalidates memos.
static LLM: LazyLock<ContextKey<LlmClient>> =
    LazyLock::new(|| ContextKey::new_with_state("llm_model", |c: &LlmClient| c.model.clone()));

// ---------------------------------------------------------------------------
// LLM client (OpenAI JSON mode)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LlmClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
}

#[derive(Deserialize)]
struct TopicsResponse {
    #[serde(default)]
    topics: Vec<String>,
}

// Mirrors the topic-extraction guidance in the Python example (models.py).
const TOPICS_PROMPT: &str = "Extract topics from the user's text. Return a JSON object \
    {\"topics\": [string, ...]}.\n\
    Each topic can be a product name, technology, model, people, company name, business \
    domain, etc. Capitalize for proper nouns and acronyms only. Use the form that is clear \
    alone. Avoid acronyms unless very popular and unambiguous for common people even without \
    context. Examples: \"Anthropic\" (not \"ANTHR\"); \"Claude\" (specific product name); \
    \"React\" (well-known library); \"PostgreSQL\" (canonical database name).\n\
    For topics that are a phrase combining multiple things, normalize into multiple topics \
    if needed. Examples: \"books for autistic kids\" -> \"book\", \"autistic\", \
    \"autistic kids\"; \"local Large Language Model\" -> \"local Large Language Model\", \
    \"Large Language Model\".\n\
    For people, use preferred name and last name. Example: \"Bill Clinton\" instead of \
    \"William Jefferson Clinton\".\n\
    When there are multiple common ways to refer to the same thing, use multiple topics. \
    Example: \"John Kennedy\", \"JFK\".";

impl LlmClient {
    fn new(model: String) -> Result<Self> {
        let api_key = std::env::var("OPENAI_API_KEY")
            .or_else(|_| std::env::var("LLM_API_KEY"))
            .map_err(|_| Error::engine("set OPENAI_API_KEY (or LLM_API_KEY)"))?;
        let base_url =
            std::env::var("LLM_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into());
        Ok(Self {
            http: reqwest::Client::new(),
            api_key,
            base_url,
            model,
        })
    }

    async fn extract_topics(&self, text: &str) -> Result<Vec<String>> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let snippet: String = trimmed.chars().take(MAX_TEXT).collect();
        let body = serde_json::json!({
            "model": self.model,
            "response_format": { "type": "json_object" },
            "messages": [
                { "role": "system", "content": TOPICS_PROMPT },
                { "role": "user", "content": format!("Extract topics from:\n\n{snippet}") },
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
        let txt = resp
            .text()
            .await
            .map_err(|e| Error::engine(format!("LLM read failed: {e}")))?;
        if !status.is_success() {
            return Err(Error::engine(format!("LLM HTTP {status}: {txt}")));
        }
        let env: serde_json::Value = serde_json::from_str(&txt)
            .map_err(|e| Error::engine(format!("LLM response not JSON: {e}")))?;
        let content = env["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| Error::engine("LLM response missing content"))?;
        let parsed: TopicsResponse = serde_json::from_str(content)
            .map_err(|e| Error::engine(format!("LLM topics not JSON: {e}: {content}")))?;
        Ok(parsed.topics)
    }
}

// ---------------------------------------------------------------------------
// HackerNews (Algolia) API
// ---------------------------------------------------------------------------

struct Message {
    id: String,
    content_type: &'static str,
    author: Option<String>,
    text: Option<String>,
    url: Option<String>,
    created_at: Option<String>,
}

struct Thread {
    id: String,
    messages: Vec<Message>, // [0] is the thread itself, rest are comments
}

#[derive(Clone, Serialize, Deserialize)]
struct HnMessageRow {
    id: String,
    thread_id: String,
    content_type: String,
    author: Option<String>,
    text: Option<String>,
    url: Option<String>,
    created_at: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct HnTopicRow {
    topic: String,
    message_id: String,
    thread_id: String,
    content_type: String,
    created_at: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ProcessedThread {
    messages: Vec<HnMessageRow>,
    topics: Vec<HnTopicRow>,
}

async fn fetch_thread_ids(max_threads: usize) -> Result<Vec<String>> {
    let v: serde_json::Value = HTTP
        .get("https://hn.algolia.com/api/v1/search_by_date")
        .query(&[
            ("tags", "story".to_string()),
            ("hitsPerPage", max_threads.to_string()),
        ])
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| Error::engine(format!("HN search failed: {e}")))?
        .json()
        .await
        .map_err(|e| Error::engine(format!("HN search response: {e}")))?;
    Ok(v["hits"]
        .as_array()
        .map(|hits| {
            hits.iter()
                .filter_map(|h| h["objectID"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default())
}

async fn fetch_thread(thread_id: &str, max_comments: usize) -> Result<Thread> {
    let data: serde_json::Value = HTTP
        .get(format!("https://hn.algolia.com/api/v1/items/{thread_id}"))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| Error::engine(format!("HN item failed: {e}")))?
        .json()
        .await
        .map_err(|e| Error::engine(format!("HN item response: {e}")))?;

    let mut text = data["title"].as_str().unwrap_or("").to_string();
    if let Some(more) = data["text"].as_str() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(more);
    }
    let mut messages = vec![Message {
        id: thread_id.to_string(),
        content_type: "thread",
        author: data["author"].as_str().map(str::to_string),
        text: Some(text),
        url: data["url"].as_str().map(str::to_string),
        created_at: data["created_at"].as_str().map(str::to_string),
    }];

    let mut comments = Vec::new();
    collect_comments(&data, thread_id, &mut comments);
    comments.truncate(max_comments);
    messages.extend(comments);

    Ok(Thread {
        id: thread_id.to_string(),
        messages,
    })
}

fn collect_comments(node: &serde_json::Value, _thread_id: &str, out: &mut Vec<Message>) {
    if let Some(children) = node["children"].as_array() {
        for child in children {
            if let Some(id) = child["id"].as_i64() {
                out.push(Message {
                    id: id.to_string(),
                    content_type: "comment",
                    author: child["author"].as_str().map(str::to_string),
                    text: child["text"].as_str().map(str::to_string),
                    url: None,
                    created_at: child["created_at"].as_str().map(str::to_string),
                });
            }
            collect_comments(child, _thread_id, out);
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

fn max_comments() -> usize {
    std::env::var("HN_MAX_COMMENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX)
}

/// Fetch one thread and extract topics for every message.
/// Memoized by thread id: re-runs skip already-processed threads, while target
/// row declarations still happen in the active pipeline.
#[cocoindex::function(memo)]
async fn process_thread(ctx: &Ctx, thread_id: &String) -> Result<ProcessedThread> {
    let llm = ctx.get_key(&LLM)?;
    let thread = fetch_thread(&thread_id, max_comments()).await?;
    let mut messages = Vec::with_capacity(thread.messages.len());
    let mut topic_rows = Vec::new();

    for msg in &thread.messages {
        let topics = match &msg.text {
            Some(t) => llm.extract_topics(t).await?,
            None => Vec::new(),
        };
        messages.push(HnMessageRow {
            id: msg.id.clone(),
            thread_id: thread.id.clone(),
            content_type: msg.content_type.to_string(),
            author: msg.author.clone(),
            text: msg.text.clone(),
            url: msg.url.clone(),
            created_at: msg.created_at.clone(),
        });
        for topic in topics {
            topic_rows.push(HnTopicRow {
                topic,
                message_id: msg.id.clone(),
                thread_id: thread.id.clone(),
                content_type: msg.content_type.to_string(),
                created_at: msg.created_at.clone(),
            });
        }
    }
    Ok(ProcessedThread {
        messages,
        topics: topic_rows,
    })
}

async fn app_main(ctx: Ctx, max_threads: usize) -> Result<()> {
    let db = ctx.get_key(&PG)?;
    let messages = postgres::mount_table_target(
        &ctx,
        db,
        "hn_messages",
        message_schema()?,
        Some("coco_examples"),
    )
    .await?;
    let topics = postgres::mount_table_target(
        &ctx,
        db,
        "hn_topics",
        topic_schema()?,
        Some("coco_examples"),
    )
    .await?;

    let thread_ids = fetch_thread_ids(max_threads).await?;
    println!("fetched {} threads from HackerNews", thread_ids.len());

    let processed = ctx
        .mount_each(
            thread_ids.clone(),
            |id| id.clone(),
            |child, id| async move { process_thread(&child, &id).await },
        )
        .await?;

    let mut count = 0usize;
    for thread in &processed {
        count += thread.messages.len();
        for row in &thread.messages {
            messages.declare_row(&ctx, row)?;
        }
        for row in &thread.topics {
            topics.declare_row(&ctx, row)?;
        }
    }

    println!("processed {count} messages across threads");
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema + queries
// ---------------------------------------------------------------------------

fn message_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("id", postgres::ColumnDef::new("text")),
            ("thread_id", postgres::ColumnDef::new("text")),
            ("content_type", postgres::ColumnDef::new("text")),
            ("author", postgres::ColumnDef::new("text").nullable()),
            ("text", postgres::ColumnDef::new("text").nullable()),
            ("url", postgres::ColumnDef::new("text").nullable()),
            ("created_at", postgres::ColumnDef::new("text").nullable()),
        ],
        ["id"],
    )
}

fn topic_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("topic", postgres::ColumnDef::new("text")),
            ("message_id", postgres::ColumnDef::new("text")),
            ("thread_id", postgres::ColumnDef::new("text")),
            ("content_type", postgres::ColumnDef::new("text")),
            ("created_at", postgres::ColumnDef::new("text").nullable()),
        ],
        ["topic", "message_id"],
    )
}

async fn show_trending(pool: &PgPool, limit: i64) -> Result<()> {
    let rows = sqlx::query(
        "SELECT topic, \
           SUM(CASE WHEN content_type = 'thread' THEN $1 ELSE $2 END)::bigint AS score, \
           COUNT(DISTINCT thread_id) AS threads \
         FROM coco_examples.hn_topics GROUP BY topic \
         ORDER BY score DESC, MAX(created_at) DESC LIMIT $3",
    )
    .bind(THREAD_SCORE)
    .bind(COMMENT_SCORE)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    println!("Top {} trending topics:", rows.len());
    println!("{}", "-".repeat(60));
    for (i, r) in rows.iter().enumerate() {
        let topic: String = r.try_get("topic").map_err(db_err)?;
        let score: i64 = r.try_get("score").map_err(db_err)?;
        let threads: i64 = r.try_get("threads").map_err(db_err)?;
        println!(
            "{:>2}. {topic:<32} (score: {score}, threads: {threads})",
            i + 1
        );
    }
    Ok(())
}

async fn search_topic(pool: &PgPool, topic: &str) -> Result<()> {
    let rows = sqlx::query(
        "SELECT m.content_type, m.author, m.text, m.thread_id, t.topic \
         FROM coco_examples.hn_topics t \
         JOIN coco_examples.hn_messages m ON t.message_id = m.id \
         WHERE LOWER(t.topic) LIKE LOWER($1) ORDER BY m.created_at DESC LIMIT 10",
    )
    .bind(format!("%{topic}%"))
    .fetch_all(pool)
    .await
    .map_err(db_err)?;

    println!("Messages mentioning '{topic}' ({} shown):", rows.len());
    println!("{}", "-".repeat(60));
    for r in &rows {
        let ct: String = r.try_get("content_type").map_err(db_err)?;
        let author: Option<String> = r.try_get("author").map_err(db_err)?;
        let text: Option<String> = r.try_get("text").map_err(db_err)?;
        let thread_id: String = r.try_get("thread_id").map_err(db_err)?;
        let snippet: String = text
            .unwrap_or_default()
            .chars()
            .take(120)
            .collect::<String>()
            .replace('\n', " ");
        println!(
            "[{ct}] by {} — https://news.ycombinator.com/item?id={thread_id}",
            author.as_deref().unwrap_or("?")
        );
        println!("    {snippet}");
    }
    Ok(())
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
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

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("trending") => {
            let pool = connect_pool().await?;
            show_trending(&pool, 20).await?;
        }
        Some("search") => {
            let q = args[1..].join(" ");
            if q.trim().is_empty() {
                eprintln!("usage: cargo run -- search \"topic\"");
                std::process::exit(2);
            }
            let pool = connect_pool().await?;
            search_topic(&pool, &q).await?;
        }
        _ => {
            // "index" or no subcommand.
            let max_threads = std::env::var("HN_MAX_THREADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10usize);
            let db = connect_target_db().await?;
            let llm = LlmClient::new(
                std::env::var("LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into()),
            )?;
            let app = App::builder("HNTrendingTopics")
                .db_path(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&PG, db)
                .provide_key(&LLM, llm)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, max_threads)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
