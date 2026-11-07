//! Knowledge Graph for Docs (Neo4j) — Rust port of the Python
//! `docs_to_knowledge_graph` example.
//!
//! Build a concept knowledge graph from a folder of Markdown documentation. For
//! each document an LLM produces a short summary and a set of
//! (subject, predicate, object) relationship triples — "concepts, not code".
//! The triples become a property graph in Neo4j:
//!
//!   Document nodes — one per Markdown file (title + summary)
//!   Entity   nodes — one per distinct concept named in a triple
//!   Relationships:
//!     RELATIONSHIP  Entity   -> Entity    (predicate stored on the edge)
//!     MENTION       Document -> Entity    (which doc named which concept)
//!
//! The pipeline runs in two phases:
//!   1. Per-file extraction declares each Document node and carries its triples
//!      forward.
//!   2. A single graph-building pass declares the deduplicated Entity nodes and
//!      the RELATIONSHIP / MENTION edges across all documents.
//!
//!   cargo run                    # graph ./markdown_files
//!   cargo run -- [SOURCE_DIR]
//!
//! CocoIndex reconciles incrementally: editing one doc only re-extracts that doc
//! (the LLM calls are memoized by content), and the graph pass only changes what
//! the new set of triples changes.

use std::collections::BTreeSet;
use std::path::PathBuf;

use cocoindex::connectors::neo4j::{self, Graph, TableSchema};
use cocoindex::ops::api::{ApiChatClient, ChatMessage};
use cocoindex::prelude::*;

const APP_NAME: &str = "DocsToKnowledgeGraph";

// ---------------------------------------------------------------------------
// Context keys
// ---------------------------------------------------------------------------

cocoindex::context_key!(static KG_DB: Graph);
// The model id is a memo dependency (cf. Python's `detect_change=True`):
// swapping the model re-extracts the whole corpus against it.
cocoindex::context_key!(static LLM_MODEL: String, detect_change);

// ---------------------------------------------------------------------------
// Neo4j node / edge schemas
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Document {
    filename: String, // primary key
    title: String,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Entity {
    value: String, // primary key — the concept name
}

/// RELATIONSHIP edge payload. `id` is a stable hash of the triple so the same
/// (subject, predicate, object) always maps to a single edge; the `predicate`
/// is stored as an edge property.
#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Relationship {
    id: i64,
    predicate: String,
}

// MENTION carries no payload — declared without a schema, so the Neo4j
// connector derives its key from (from_id, to_id): one edge per (doc, entity).

// ---------------------------------------------------------------------------
// LLM extraction schemas (serde, for `ApiChatClient::chat_json`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DocumentSummary {
    title: String,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtractedRelationship {
    subject: String,
    predicate: String,
    object: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelationshipList {
    #[serde(default)]
    relationships: Vec<ExtractedRelationship>,
}

const SUMMARY_PROMPT: &str = "\
You are an expert technical writer. Summarize the documentation below.
Return a concise title and a one-paragraph summary of what it covers.

Respond with a JSON object: {\"title\": \"a concise title for the document\",
\"summary\": \"a one-paragraph summary of what the document covers\"}
";

const RELATIONSHIP_PROMPT: &str = "\
You extract a concept knowledge graph from technical documentation.
List the salient (subject, predicate, object) relationships between concepts.
Focus on concepts and ignore code examples and implementation details.
Use concise noun phrases for subjects and objects and a short verb phrase for
the predicate. Return only relationships supported by the text.

Respond with a JSON object: {\"relationships\": [{\"subject\": \"the concept the
statement is about, e.g. 'CocoIndex'\", \"predicate\": \"how subject relates to
object, e.g. 'supports'\", \"object\": \"the related concept, e.g. 'Incremental
Processing'\"}]}
";

/// The chat client for the configured model: an OpenAI-compatible endpoint
/// (`OPENAI_API_KEY`; `LLM_BASE_URL` for another provider, e.g. Ollama).
fn chat_client(ctx: &Ctx) -> Result<ApiChatClient> {
    let client = ApiChatClient::new(ctx.get_key(&LLM_MODEL)?);
    Ok(match std::env::var("LLM_BASE_URL") {
        Ok(url) => client.with_base_url(url),
        Err(_) => client,
    })
}

async fn chat_typed<T: serde::de::DeserializeOwned>(
    ctx: &Ctx,
    system: &str,
    content: &str,
) -> Result<T> {
    let json = chat_client(ctx)?
        .chat_json(&[ChatMessage::system(system), ChatMessage::user(content)])
        .await?;
    serde_json::from_str(&json)
        .map_err(|e| Error::engine(format!("LLM content is not the expected JSON: {e}: {json}")))
}

// ---------------------------------------------------------------------------
// Internal transfer type (Phase 1 -> Phase 2)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Triple {
    subject: String,
    predicate: String,
    object: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DocTriples {
    filename: String,
    triples: Vec<Triple>,
}

// ---------------------------------------------------------------------------
// LLM extraction (memoized by document content + model id)
// ---------------------------------------------------------------------------

#[cocoindex::function(memo)]
async fn extract_summary(ctx: &Ctx, content: &String) -> Result<DocumentSummary> {
    chat_typed(ctx, SUMMARY_PROMPT, &content).await
}

#[cocoindex::function(memo)]
async fn extract_relationships(ctx: &Ctx, content: &String) -> Result<Vec<Triple>> {
    let list: RelationshipList = chat_typed(ctx, RELATIONSHIP_PROMPT, &content).await?;
    Ok(list
        .relationships
        .into_iter()
        .map(|r| Triple {
            subject: r.subject,
            predicate: r.predicate,
            object: r.object,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Phase 1: per-file extraction — declare Document nodes, carry triples forward
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn process_file(
    ctx: &Ctx,
    file: FileEntry,
    document_table: neo4j::TableTarget,
) -> Result<DocTriples> {
    let content = file.content_str()?;
    let filename = file.key();

    let summary = extract_summary(ctx, &content).await?;
    document_table.declare_record(
        ctx,
        filename.as_str(),
        &Document {
            filename: filename.clone(),
            title: summary.title,
            summary: summary.summary,
        },
    )?;

    let triples = extract_relationships(ctx, &content).await?;
    Ok(DocTriples { filename, triples })
}

// ---------------------------------------------------------------------------
// Phase 2: build the concept graph — Entity nodes + RELATIONSHIP / MENTION edges
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn build_graph(
    ctx: &Ctx,
    docs: Vec<DocTriples>,
    entity_table: neo4j::TableTarget,
    relationship_rel: neo4j::RelationTarget,
    mention_rel: neo4j::RelationTarget,
) -> Result<()> {
    let mut entities = BTreeSet::new();
    let mut mentions = BTreeSet::new(); // (filename, entity value)

    for doc in &docs {
        for t in &doc.triples {
            entities.insert(t.subject.clone());
            entities.insert(t.object.clone());
            mentions.insert((doc.filename.clone(), t.subject.clone()));
            mentions.insert((doc.filename.clone(), t.object.clone()));

            let rel_id = generate_id(ctx, &(&t.subject, &t.predicate, &t.object)).await? as i64;
            relationship_rel.declare_relation_record(
                ctx,
                t.subject.as_str(),
                t.object.as_str(),
                &Relationship {
                    id: rel_id,
                    predicate: t.predicate.clone(),
                },
            )?;
        }
    }

    for value in &entities {
        entity_table.declare_record(
            ctx,
            value.as_str(),
            &Entity {
                value: value.clone(),
            },
        )?;
    }

    for (filename, entity) in &mentions {
        mention_rel.declare_relation(ctx, filename.as_str(), entity.as_str())?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// App main
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    // --- Mount node tables ---
    let document_table = neo4j::mount_table_target(
        &ctx,
        &KG_DB,
        "Document",
        TableSchema::from_row::<Document>("filename")?,
    )
    .await?;
    let entity_table = neo4j::mount_table_target(
        &ctx,
        &KG_DB,
        "Entity",
        TableSchema::from_row::<Entity>("value")?,
    )
    .await?;

    // --- Mount relationship targets ---
    // RELATIONSHIP carries a predicate; mounted with a schema so each distinct
    // triple (keyed by the hashed `id`) becomes its own edge.
    let relationship_rel = neo4j::mount_relation_target_with_schema(
        &ctx,
        &KG_DB,
        "RELATIONSHIP",
        &entity_table,
        &entity_table,
        TableSchema::from_row::<Relationship>("id")?,
    )
    .await?;
    // MENTION has no payload; the connector derives the key from the endpoints.
    let mention_rel =
        neo4j::mount_relation_target(&ctx, &KG_DB, "MENTION", &document_table, &entity_table)
            .await?;

    // --- Phase 1: per-file extraction, one component per document ---
    let files = walk_items(&sourcedir, &["**/*.md", "**/*.mdx"])?;
    println!(
        "extracting {} document(s) from {}",
        files.len(),
        sourcedir.display()
    );
    let docs: Vec<DocTriples> = ctx
        .use_mount_each(
            files,
            |(key, _)| format!("file/{key}"),
            move |ctx, (_, file)| {
                let document_table = document_table.clone();
                async move { process_file(&ctx, file, document_table).await }
            },
        )
        .await?;

    // --- Phase 2: build the concept graph ---
    ctx.mount(&"build_graph", move |ctx| async move {
        build_graph(&ctx, docs, entity_table, relationship_rel, mention_rel).await
    })
    .await?
    .ready()
    .await
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let sourcedir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("markdown_files"));

    let graph = Graph::connect(
        &env_or("NEO4J_URI", "bolt://localhost:7687"),
        &env_or("NEO4J_USER", "neo4j"),
        &env_or("NEO4J_PASSWORD", "cocoindex"),
        &env_or("NEO4J_DATABASE", "neo4j"),
    )
    .await?;

    let app = Environment::builder()
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&KG_DB, graph)
        .provide_key(&LLM_MODEL, env_or("LLM_MODEL", "gpt-5-mini"))
        .build()
        .await?
        .app(APP_NAME)
        .await?;

    let stats = app.run(move |ctx| app_main(ctx, sourcedir)).await?;
    println!("{stats}");
    Ok(())
}
