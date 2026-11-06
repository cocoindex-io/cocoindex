//! SEC EDGAR Analytics (Apache Doris) — Rust port of the Python
//! `sec_edgar_analytics` example.
//!
//! Ingest SEC filings from multiple formats — 10-K text and XBRL company-facts
//! JSON — into one unified, searchable index in Apache Doris. Each document is
//! scrubbed of PII, chunked, embedded, and tagged with risk/topic labels, then
//! written to Doris with both a vector index (for semantic search) and a
//! full-text index (for keyword search) — the foundation for hybrid retrieval.
//!
//!   cargo run                              # index ./data/{filings,company_facts}
//!   cargo run -- index [DATA_DIR]
//!   cargo run -- search "cybersecurity risk" [--source filing|facts]
//!
//! Parallels the Python example:
//!   - per-file compute  : `ctx.mount_each` + `#[cocoindex::function]` (cf. `coco.mount_each`)
//!   - chunking          : `cocoindex::ops::text::RecursiveSplitter` (cf. `RecursiveSplitter`)
//!   - embeddings        : `cocoindex::ops::sentence_transformers` all-MiniLM-L6-v2
//!   - target            : `doris::TableTarget` with `VectorIndexDef` + `InvertedIndexDef`
//!   - hybrid search     : `search` subcommand (vector + MATCH_ANY, fused with RRF; cf. `search.py`)

use std::path::PathBuf;

use cocoindex::connectors::doris::{
    self, DorisConfig, DorisConnection, DorisTableOptions, InvertedIndexDef, TableSchema,
    VectorIndexDef,
};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::ops::text::{RecursiveChunkConfig, RecursiveSplitter};
use cocoindex::prelude::*;
use regex::Regex;
use sqlx::Row as _;
use std::sync::LazyLock;

const APP_NAME: &str = "SECFilingAnalytics";
const TABLE: &str = "filing_chunks";
const EMBED_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const CHUNK_SIZE: usize = 1000;
const CHUNK_OVERLAP: usize = 200;
const SEARCH_LIMIT: usize = 5;

cocoindex::context_key!(static DORIS_DB: DorisConnection);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

// ---------------------------------------------------------------------------
// Doris row
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct FilingChunk {
    chunk_id: String,    // primary key — uuid5 of (filename, chunk offsets)
    source_type: String, // "filing" | "facts"
    doc_filename: String,
    cik: String,
    filing_date: String,
    form_type: String,
    text: String,
    topics: Vec<String>,
    #[coco(vector)]
    embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------

static SSN_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap());
static PHONE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b").unwrap());
static EMAIL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b").unwrap());

/// Redact SSNs, phone numbers and e-mail addresses *before* chunking, so PII
/// never enters the index.
fn scrub_pii(text: &str) -> String {
    let text = SSN_RE.replace_all(text, "[SSN REDACTED]");
    let text = PHONE_RE.replace_all(&text, "[PHONE REDACTED]");
    EMAIL_RE.replace_all(&text, "[EMAIL REDACTED]").into_owned()
}

const TOPIC_KEYWORDS: &[(&str, &[&str])] = &[
    (
        "RISK:CYBER",
        &[
            "cybersecurity",
            "data breach",
            "ransomware",
            "hacking",
            "cyber attack",
        ],
    ),
    (
        "RISK:CLIMATE",
        &[
            "climate change",
            "carbon",
            "sustainability",
            "emissions",
            "environmental",
        ],
    ),
    (
        "RISK:SUPPLY",
        &["supply chain", "logistics", "shortage", "disruption"],
    ),
    (
        "RISK:REGULATORY",
        &["compliance", "regulation", "sec", "enforcement"],
    ),
    (
        "TOPIC:AI",
        &[
            "artificial intelligence",
            "machine learning",
            "neural network",
        ],
    ),
    ("TOPIC:CLOUD", &["cloud computing", "aws", "azure", "saas"]),
    (
        "TOPIC:FINANCIAL",
        &[
            "revenue",
            "net income",
            "assets",
            "liabilities",
            "cash flow",
        ],
    ),
];

fn extract_topics(text: &str) -> Vec<String> {
    let low = text.to_lowercase();
    TOPIC_KEYWORDS
        .iter()
        .filter(|(_, keywords)| keywords.iter().any(|k| low.contains(k)))
        .map(|(topic, _)| topic.to_string())
        .collect()
}

/// Render XBRL company-facts JSON as searchable natural language.
fn company_facts_to_text(content: &str) -> Result<String> {
    let data: serde_json::Value = serde_json::from_str(content)
        .map_err(|e| Error::engine(format!("invalid company-facts JSON: {e}")))?;
    let str_or = |v: &serde_json::Value, default: &str| match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => default.to_string(),
        other => other.to_string(),
    };
    let mut lines = vec![
        format!(
            "# Company Financial Facts: {}",
            str_or(&data["entityName"], "Unknown")
        ),
        format!("CIK: {}\n", str_or(&data["cik"], "Unknown")),
        "## US-GAAP Financial Metrics\n".to_string(),
    ];
    let us_gaap = &data["facts"]["us-gaap"];
    for metric in [
        "Revenues",
        "NetIncomeLoss",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "ResearchAndDevelopmentExpense",
    ] {
        let md = &us_gaap[metric];
        let Some(units) = md["units"].as_object() else {
            continue;
        };
        let mut vals = Vec::new();
        for unit_data in units.values() {
            let Some(entries) = unit_data.as_array() else {
                continue;
            };
            let mut entries: Vec<&serde_json::Value> = entries.iter().collect();
            // Most recent period first.
            entries.sort_by(|a, b| {
                b["end"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(a["end"].as_str().unwrap_or(""))
            });
            for e in entries.into_iter().take(3) {
                let (Some(v), Some(end)) = (e["val"].as_f64(), e["end"].as_str()) else {
                    continue;
                };
                if v == 0.0 {
                    continue;
                }
                let fv = if v.abs() >= 1e9 {
                    format!("${:.1}B", v / 1e9)
                } else if v.abs() >= 1e6 {
                    format!("${:.1}M", v / 1e6)
                } else {
                    format!("${v:.0}")
                };
                vals.push(format!("{end}: {fv}"));
            }
        }
        if !vals.is_empty() {
            lines.push(format!("### {}", str_or(&md["label"], metric)));
            lines.push(format!(
                "Recent values: {}\n",
                vals.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    Ok(lines.join("\n"))
}

fn chunk_id(filename: &str, start: usize, end: usize) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{filename}|{start}|{end}").as_bytes(),
    )
    .to_string()
}

/// Document-level fields shared by every chunk row of one source file.
struct DocInfo {
    source_type: &'static str, // "filing" | "facts"
    filename: String,
    cik: String,
    filing_date: String,
    form_type: String,
}

/// Common path: scrub PII, chunk, embed, tag, declare one row per chunk.
async fn index_text(
    ctx: &Ctx,
    text: &str,
    doc: &DocInfo,
    table: &doris::TableTarget,
) -> Result<()> {
    let clean = scrub_pii(text);
    let splitter = RecursiveSplitter::new()?;
    let chunks = splitter.split_with(
        &clean,
        RecursiveChunkConfig {
            chunk_size: CHUNK_SIZE,
            min_chunk_size: None,
            chunk_overlap: Some(CHUNK_OVERLAP),
            language: Some("markdown".to_string()),
        },
    );
    let embedder = ctx.get_key(&EMBEDDER)?.clone();
    let embedding_ctx = ctx.clone();
    // Embed the chunks concurrently; the embedder memoizes each text and
    // batches concurrent misses.
    let texts: Vec<String> = chunks.iter().map(|c| c.text(&clean).to_string()).collect();
    let embeddings = ctx
        .map(texts.clone(), move |chunk_text| {
            let embedder = embedder.clone();
            let ctx = embedding_ctx.clone();
            async move { embedder.embed(&ctx, chunk_text).await }
        })
        .await?;
    for ((chunk, chunk_text), embedding) in chunks.iter().zip(texts).zip(embeddings) {
        table.declare_row(
            ctx,
            &FilingChunk {
                chunk_id: chunk_id(
                    &doc.filename,
                    chunk.start.char_offset,
                    chunk.end.char_offset,
                ),
                source_type: doc.source_type.to_string(),
                doc_filename: doc.filename.clone(),
                cik: doc.cik.clone(),
                filing_date: doc.filing_date.clone(),
                form_type: doc.form_type.clone(),
                topics: extract_topics(&chunk_text),
                text: chunk_text,
                embedding,
            },
        )?;
    }
    Ok(())
}

/// 10-K text filing: metadata from the `{CIK}_{date}_{form}.txt` filename.
#[cocoindex::function]
async fn process_filing(ctx: &Ctx, file: FileEntry, table: doris::TableTarget) -> Result<()> {
    let name = file.file_path().name().to_string();
    let parts: Vec<&str> = file.stem().split('_').collect();
    let cik = parts.first().copied().unwrap_or("unknown");
    let filing_date = parts.get(1).copied().unwrap_or("2024-01-01");
    let form_type = parts.get(2).copied().unwrap_or("10-K");
    let doc = DocInfo {
        source_type: "filing",
        filename: name,
        cik: cik.to_string(),
        filing_date: filing_date.to_string(),
        form_type: form_type.to_string(),
    };
    index_text(ctx, &file.content_str()?, &doc, &table).await
}

/// XBRL company-facts JSON: render to text, then index.
#[cocoindex::function]
async fn process_facts(ctx: &Ctx, file: FileEntry, table: doris::TableTarget) -> Result<()> {
    let name = file.file_path().name().to_string();
    let content = file.content_str()?;
    let cik = name.replace("CIK", "").replace(".json", "");
    let filing_date = if content.is_empty() {
        "2024-01-01".to_string()
    } else {
        let data: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| Error::engine(format!("invalid company-facts JSON {name}: {e}")))?;
        data["filingDate"]
            .as_str()
            .unwrap_or("2024-01-01")
            .to_string()
    };
    let doc = DocInfo {
        source_type: "facts",
        filename: name,
        cik,
        filing_date,
        form_type: "FACTS".to_string(),
    };
    index_text(ctx, &company_facts_to_text(&content)?, &doc, &table).await
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, data_dir: PathBuf) -> Result<()> {
    // One table, two index types: a vector (ANN) index for `l2_distance`
    // semantic search and an inverted index for `MATCH_ANY` keyword search.
    let table = doris::mount_table_target_with_options(
        &ctx,
        &DORIS_DB,
        TABLE,
        TableSchema::from_row::<FilingChunk>(["chunk_id"])?
            .with_vector_schema("embedding", ctx.get_key(&EMBEDDER)?)
            .await?,
        DorisTableOptions {
            vector_indexes: vec![VectorIndexDef::new("embedding").metric_type("l2_distance")],
            inverted_indexes: vec![InvertedIndexDef::new("text").parser("unicode")],
            ..DorisTableOptions::default()
        },
    )
    .await?;

    let filings = walk_items(data_dir.join("filings"), &["**/*.txt"])?;
    let facts = walk_items(data_dir.join("company_facts"), &["**/*.json"])?;
    println!(
        "indexing {} filing(s) and {} company-facts file(s) from {}",
        filings.len(),
        facts.len(),
        data_dir.display()
    );

    // Both source formats fan into the same table, one component per file.
    let filing_table = table.clone();
    ctx.mount_each(
        filings,
        |(key, _)| format!("filing/{key}"),
        move |ctx, (_, file)| {
            let table = filing_table.clone();
            async move { process_filing(&ctx, file, table).await }
        },
    )
    .await?;
    ctx.mount_each(
        facts,
        |(key, _)| format!("facts/{key}"),
        move |ctx, (_, file)| {
            let table = table.clone();
            async move { process_facts(&ctx, file, table).await }
        },
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Hybrid search (cf. `search.py`): vector + keyword ranks fused with RRF
// ---------------------------------------------------------------------------

async fn search(
    conn: &DorisConnection,
    embedder: &SentenceTransformerEmbedder,
    query: &str,
    source_type: Option<&str>,
) -> Result<()> {
    let vec = embedder
        .embed_batch(vec![query.to_string()])
        .await?
        .remove(0);
    let vstr = format!(
        "[{}]",
        vec.iter()
            .map(|x| format!("{x:.6}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let word_re = Regex::new(r"[A-Za-z]{3,}").unwrap();
    let keywords = word_re
        .find_iter(&query.to_lowercase())
        .map(|m| m.as_str().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    let where_clause = match source_type {
        Some(st) => format!("source_type = '{}'", st.replace('\'', "''")),
        None => "1 = 1".to_string(),
    };
    let sql = format!(
        "WITH semantic AS (
            SELECT chunk_id, doc_filename, source_type, topics, text,
                   ROW_NUMBER() OVER (ORDER BY l2_distance(embedding, {vstr})) AS rk
            FROM {TABLE} WHERE {where_clause}
        ),
        lexical AS (
            SELECT chunk_id,
                   ROW_NUMBER() OVER (ORDER BY CASE WHEN text MATCH_ANY ?
                                      THEN 0 ELSE 1 END) AS rk
            FROM {TABLE} WHERE {where_clause}
        )
        SELECT s.doc_filename, s.source_type, s.topics, s.text,
               1.0/(60 + s.rk) + 1.0/(60 + l.rk) AS rrf
        FROM semantic s JOIN lexical l USING (chunk_id)
        ORDER BY rrf DESC LIMIT {SEARCH_LIMIT}"
    );
    let rows = sqlx::query(&sql)
        .bind(keywords)
        .fetch_all(conn.pool())
        .await
        .map_err(|e| Error::engine(format!("doris query: {e}")))?;

    println!(
        "\nHybrid search: {query:?}{}",
        source_type
            .map(|s| format!(" [source={s}]"))
            .unwrap_or_default()
    );
    for row in rows {
        let filename: String = row.try_get("doc_filename").map_err(db_err)?;
        let st: String = row.try_get("source_type").map_err(db_err)?;
        let topics: String = row.try_get("topics").map_err(db_err)?;
        let text: String = row.try_get("text").map_err(db_err)?;
        let rrf: f64 = row.try_get("rrf").map_err(db_err)?;
        println!("\n[{rrf:.4}] {filename} ({st})  topics={topics}");
        let snippet: String = text.trim().chars().take(160).collect();
        println!("    {snippet}");
    }
    Ok(())
}

fn db_err(e: sqlx::Error) -> Error {
    Error::engine(format!("doris: {e}"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn port_env(key: &str, default: u16) -> Result<u16> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|e| Error::engine(format!("{key} must be a port number: {e}"))),
        Err(_) => Ok(default),
    }
}

async fn connect_doris() -> Result<DorisConnection> {
    let mut config = DorisConfig::new(
        env_or("DORIS_FE_HOST", "localhost"),
        env_or("DORIS_DATABASE", "sec_analytics"),
    )
    .fe_http_port(port_env("DORIS_HTTP_PORT", 8030)?)
    .query_port(port_env("DORIS_QUERY_PORT", 9030)?)
    .username(env_or("DORIS_USERNAME", "root"))
    .password(env_or("DORIS_PASSWORD", ""));
    // Stream Load redirects FE → BE; when the BE advertises an unreachable
    // address (e.g. a container IP), rewrite the host.
    if let Ok(host) = std::env::var("DORIS_BE_LOAD_HOST")
        && !host.is_empty()
    {
        config = config.be_load_host(host);
    }
    DorisConnection::connect(config).await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("search") => {
            let source_type = args
                .iter()
                .position(|a| a == "--source")
                .and_then(|i| args.get(i + 1))
                .cloned();
            let query = args[1..]
                .iter()
                .filter(|a| !a.starts_with("--") && Some(*a) != source_type.as_ref())
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            if query.trim().is_empty() {
                eprintln!("usage: cargo run -- search \"your query\" [--source filing|facts]");
                std::process::exit(2);
            }
            let conn = connect_doris().await?;
            let embedder = SentenceTransformerEmbedder::load(EMBED_MODEL).await?;
            search(&conn, &embedder, &query, source_type.as_deref()).await
        }
        sub => {
            let data_dir = match sub {
                Some("index") => args.get(1).map(PathBuf::from),
                Some(other) => Some(PathBuf::from(other)),
                None => None,
            }
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data"));

            let conn = connect_doris().await?;
            let embedder = SentenceTransformerEmbedder::load(EMBED_MODEL).await?;
            let app = Environment::builder()
                .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
                .provide_key(&DORIS_DB, conn)
                .provide_key(&EMBEDDER, embedder)
                .build()
                .await?
                .app(APP_NAME)
                .await?;
            let stats = app.run(move |ctx| app_main(ctx, data_dir)).await?;
            println!("{stats}");
            Ok(())
        }
    }
}
