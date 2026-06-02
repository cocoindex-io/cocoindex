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
//!   - Postgres tables + incremental sync         -> hand-rolled `sqlx`
//!
//! Notes vs. Python: Python defaults to `gemini-2.5-flash`; this uses OpenAI
//! (`OPENAI_API_KEY`, default `gpt-4o-mini`). Per-thread work is memoized by
//! thread id; rows for threads that drop out of the latest list are reconciled.

use std::sync::LazyLock;

use cocoindex::prelude::*;
use serde::Deserialize;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const THREAD_SCORE: i64 = 5;
const COMMENT_SCORE: i64 = 1;
const MAX_TEXT: usize = 4000;

static HTTP: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// Shared Postgres pool.
static PG: LazyLock<ContextKey<PgPool>> = LazyLock::new(|| ContextKey::new("hn_db"));
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

const TOPICS_PROMPT: &str = "Extract topics from the user's text. Return a JSON object \
    {\"topics\": [string, ...]}. Each topic can be a product, technology, model, person, \
    company, or business domain. Capitalize proper nouns/acronyms only; use the clearest \
    standalone form; avoid obscure acronyms. Split combined phrases into multiple topics \
    when useful (e.g. \"local Large Language Model\" -> \"local Large Language Model\", \
    \"Large Language Model\"). For people use preferred + last name. When a thing has \
    multiple common names, include each (e.g. \"John Kennedy\", \"JFK\").";

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

/// Fetch one thread, extract topics for every message, and upsert rows.
/// Memoized by thread id: re-runs skip already-processed threads.
#[cocoindex::function(memo)]
async fn process_thread(ctx: &Ctx, thread_id: &String) -> Result<usize> {
    let pool = ctx.get_key(&PG)?;
    let llm = ctx.get_key(&LLM)?;
    let thread = fetch_thread(&thread_id, max_comments()).await?;

    for msg in &thread.messages {
        let topics = match &msg.text {
            Some(t) => llm.extract_topics(t).await?,
            None => Vec::new(),
        };
        upsert_message(pool, &thread.id, msg).await?;
        // Replace this message's topics.
        sqlx::query("DELETE FROM coco_examples.hn_topics WHERE message_id = $1")
            .bind(&msg.id)
            .execute(pool)
            .await
            .map_err(db_err)?;
        for topic in topics {
            sqlx::query(
                "INSERT INTO coco_examples.hn_topics \
                 (topic, message_id, thread_id, content_type, created_at) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (topic, message_id) DO NOTHING",
            )
            .bind(&topic)
            .bind(&msg.id)
            .bind(&thread.id)
            .bind(msg.content_type)
            .bind(&msg.created_at)
            .execute(pool)
            .await
            .map_err(db_err)?;
        }
    }
    Ok(thread.messages.len())
}

async fn upsert_message(pool: &PgPool, thread_id: &str, msg: &Message) -> Result<()> {
    sqlx::query(
        "INSERT INTO coco_examples.hn_messages \
         (id, thread_id, content_type, author, text, url, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (id) DO UPDATE SET \
           thread_id = EXCLUDED.thread_id, content_type = EXCLUDED.content_type, \
           author = EXCLUDED.author, text = EXCLUDED.text, url = EXCLUDED.url, \
           created_at = EXCLUDED.created_at",
    )
    .bind(&msg.id)
    .bind(thread_id)
    .bind(msg.content_type)
    .bind(&msg.author)
    .bind(&msg.text)
    .bind(&msg.url)
    .bind(&msg.created_at)
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn app_main(ctx: Ctx, max_threads: usize) -> Result<()> {
    let pool = ctx.get_key(&PG)?;
    ensure_schema(pool).await?;

    let thread_ids = fetch_thread_ids(max_threads).await?;
    println!("fetched {} threads from HackerNews", thread_ids.len());

    let counts = ctx
        .mount_each(
            thread_ids.clone(),
            |id| id.clone(),
            |child, id| async move { process_thread(&child, &id).await },
        )
        .await?;

    // Reconcile: drop messages/topics for threads no longer in the latest list.
    sqlx::query("DELETE FROM coco_examples.hn_topics WHERE thread_id <> ALL($1)")
        .bind(&thread_ids)
        .execute(pool)
        .await
        .map_err(db_err)?;
    sqlx::query("DELETE FROM coco_examples.hn_messages WHERE thread_id <> ALL($1)")
        .bind(&thread_ids)
        .execute(pool)
        .await
        .map_err(db_err)?;

    println!(
        "processed {} messages across threads",
        counts.iter().sum::<usize>()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema + queries
// ---------------------------------------------------------------------------

async fn ensure_schema(pool: &PgPool) -> Result<()> {
    for stmt in [
        "CREATE SCHEMA IF NOT EXISTS coco_examples",
        "CREATE TABLE IF NOT EXISTS coco_examples.hn_messages (\
           id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, content_type TEXT NOT NULL, \
           author TEXT, text TEXT, url TEXT, created_at TEXT)",
        "CREATE TABLE IF NOT EXISTS coco_examples.hn_topics (\
           topic TEXT NOT NULL, message_id TEXT NOT NULL, thread_id TEXT NOT NULL, \
           content_type TEXT NOT NULL, created_at TEXT, PRIMARY KEY (topic, message_id))",
    ] {
        sqlx::query(stmt).execute(pool).await.map_err(db_err)?;
    }
    Ok(())
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
            let pool = connect_pool().await?;
            let llm = LlmClient::new(
                std::env::var("LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into()),
            )?;
            let app = App::builder("HNTrendingTopics")
                .db_path(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&PG, pool)
                .provide_key(&LLM, llm)
                .build()
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, max_threads)).await?;
            println!("{stats}");
        }
    }
    Ok(())
}
