//! Product Recommendation Graph (Neo4j) — Rust port of the Python
//! `product_recommendation` example.
//!
//! For each product (a JSON file) an LLM extracts two kinds of taxonomy:
//!   - taxonomies: what the product *is* (e.g. "pen", "notebook")
//!   - complementary_taxonomies: what a buyer might also want (e.g. "ink refill")
//!
//! These become a product knowledge graph in Neo4j that powers recommendations
//! ("people who bought a pen often need ink refills"):
//!
//!   Product  nodes — one per product (title, price)
//!   Taxonomy nodes — one per distinct taxonomy label
//!   Relationships:
//!     PRODUCT_TAXONOMY               Product -> Taxonomy   (what it is)
//!     PRODUCT_COMPLEMENTARY_TAXONOMY Product -> Taxonomy   (what pairs with it)
//!
//! The pipeline runs in two phases:
//!   1. Per-product extraction declares each Product node and carries its
//!      taxonomy labels forward.
//!   2. A single graph-building pass declares the deduplicated Taxonomy nodes and
//!      the relationship edges across all products.
//!
//!   cargo run                    # graph ./products
//!   cargo run -- [SOURCE_DIR]

use std::collections::BTreeSet;
use std::path::PathBuf;

use cocoindex::connectors::neo4j::{self, Graph, TableSchema};
use cocoindex::ops::api::{ApiChatClient, ChatMessage};
use cocoindex::prelude::*;

const APP_NAME: &str = "ProductRecommendation";

// ---------------------------------------------------------------------------
// Context keys
// ---------------------------------------------------------------------------

cocoindex::context_key!(static KG_DB: Graph);
// The model id is a memo dependency (cf. Python's `detect_change=True`):
// swapping the model re-extracts every product against it.
cocoindex::context_key!(static LLM_MODEL: String, detect_change);

// ---------------------------------------------------------------------------
// Neo4j node / edge schemas
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Product {
    id: String, // primary key — the filename stem
    title: String,
    price: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Taxonomy {
    value: String, // primary key — the taxonomy label
}

// PRODUCT_TAXONOMY and PRODUCT_COMPLEMENTARY_TAXONOMY carry no payload — the
// connector derives each edge's key from (from_id, to_id): one edge per pair.

// ---------------------------------------------------------------------------
// Product JSON (the source files) and its Markdown rendering for the LLM
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ProductDescription {
    header: String,
    paragraph: String,
    bullets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawProduct {
    title: String,
    price: serde_json::Value,
    #[serde(default)]
    highlights: Vec<String>,
    #[serde(default)]
    description: ProductDescription,
}

impl RawProduct {
    /// `"$4.99"` / `"1,299"` / `4.99` -> `4.99` (cf. Python's
    /// `float(str(raw["price"]).lstrip("$").replace(",", ""))`).
    fn price(&self) -> Result<f64> {
        let text = match &self.price {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        text.trim_start_matches('$')
            .replace(',', "")
            .parse()
            .map_err(|e| Error::engine(format!("invalid product price {text:?}: {e}")))
    }

    /// The product detail the LLM reads (cf. Python's `PRODUCT_TEMPLATE`).
    fn render(&self) -> String {
        let mut out = format!("# {}\n\n## Highlights\n", self.title);
        for highlight in &self.highlights {
            out.push_str(&format!("- {highlight}\n"));
        }
        out.push_str(&format!(
            "\n## Description\n{}\n{}\n",
            self.description.header, self.description.paragraph
        ));
        for bullet in &self.description.bullets {
            out.push_str(&format!("- {bullet}\n"));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// LLM extraction schema (serde, for `ApiChatClient::chat_json`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProductTaxonomy {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProductTaxonomyInfo {
    #[serde(default)]
    taxonomies: Vec<ProductTaxonomy>,
    #[serde(default)]
    complementary_taxonomies: Vec<ProductTaxonomy>,
}

const TAXONOMY_PROMPT: &str = "\
You are an expert at categorizing retail products. Given the product details,
extract the taxonomies that describe what the product is, and the complementary
taxonomies a buyer might also need. Return only what the text supports.

Each taxonomy name is a concise noun (or short noun phrase) for the product's
core functionality, without branding or style. Most common US English,
lowercase, no punctuation unless a proper noun or acronym. Avoid broad
categories like 'office supplies'; prefer specific ones like 'pen' or 'printer'.

Respond with a JSON object: {\"taxonomies\": [{\"name\": \"what this product is\"}],
\"complementary_taxonomies\": [{\"name\": \"a complementary product a buyer of this
product might also need\"}]}
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

/// Memoized by the rendered product detail (and the model id): editing one
/// product re-tags only that product.
#[cocoindex::function(memo)]
async fn extract_taxonomy(ctx: &Ctx, detail: &String) -> Result<ProductTaxonomyInfo> {
    let json = chat_client(ctx)?
        .chat_json(&[
            ChatMessage::system(TAXONOMY_PROMPT),
            ChatMessage::user(detail.clone()),
        ])
        .await?;
    serde_json::from_str(&json)
        .map_err(|e| Error::engine(format!("LLM content is not the expected JSON: {e}: {json}")))
}

// ---------------------------------------------------------------------------
// Internal transfer type (Phase 1 -> Phase 2)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProductTaxonomies {
    product_id: String,
    taxonomies: Vec<String>,
    complementary: Vec<String>,
}

// ---------------------------------------------------------------------------
// Phase 1: per-product extraction — declare Product nodes, carry labels forward
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn process_file(
    ctx: &Ctx,
    file: FileEntry,
    product_table: neo4j::TableTarget,
) -> Result<ProductTaxonomies> {
    let raw: RawProduct = serde_json::from_str(&file.content_str()?)
        .map_err(|e| Error::engine(format!("invalid product JSON {}: {e}", file.key())))?;
    let product_id = file.stem().to_string();

    product_table.declare_record(
        ctx,
        product_id.as_str(),
        &Product {
            id: product_id.clone(),
            title: raw.title.clone(),
            price: raw.price()?,
        },
    )?;

    let info = extract_taxonomy(ctx, &raw.render()).await?;
    Ok(ProductTaxonomies {
        product_id,
        taxonomies: info.taxonomies.into_iter().map(|t| t.name).collect(),
        complementary: info
            .complementary_taxonomies
            .into_iter()
            .map(|t| t.name)
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// Phase 2: build the graph — Taxonomy nodes + relationship edges
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn build_graph(
    ctx: &Ctx,
    products: Vec<ProductTaxonomies>,
    taxonomy_table: neo4j::TableTarget,
    product_taxonomy_rel: neo4j::RelationTarget,
    complementary_rel: neo4j::RelationTarget,
) -> Result<()> {
    let labels: BTreeSet<&String> = products
        .iter()
        .flat_map(|p| p.taxonomies.iter().chain(p.complementary.iter()))
        .collect();
    for value in labels {
        taxonomy_table.declare_record(
            ctx,
            value.as_str(),
            &Taxonomy {
                value: value.clone(),
            },
        )?;
    }

    for p in &products {
        for t in p.taxonomies.iter().collect::<BTreeSet<_>>() {
            product_taxonomy_rel.declare_relation(ctx, p.product_id.as_str(), t.as_str())?;
        }
        for t in p.complementary.iter().collect::<BTreeSet<_>>() {
            complementary_rel.declare_relation(ctx, p.product_id.as_str(), t.as_str())?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// App main
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let product_table = neo4j::mount_table_target(
        &ctx,
        &KG_DB,
        "Product",
        TableSchema::from_row::<Product>("id")?,
    )
    .await?;
    let taxonomy_table = neo4j::mount_table_target(
        &ctx,
        &KG_DB,
        "Taxonomy",
        TableSchema::from_row::<Taxonomy>("value")?,
    )
    .await?;
    let product_taxonomy_rel = neo4j::mount_relation_target(
        &ctx,
        &KG_DB,
        "PRODUCT_TAXONOMY",
        &product_table,
        &taxonomy_table,
    )
    .await?;
    let complementary_rel = neo4j::mount_relation_target(
        &ctx,
        &KG_DB,
        "PRODUCT_COMPLEMENTARY_TAXONOMY",
        &product_table,
        &taxonomy_table,
    )
    .await?;

    // --- Phase 1: per-product extraction, one component per JSON file ---
    let files = walk_items(&sourcedir, &["**/*.json"])?;
    println!(
        "extracting {} product(s) from {}",
        files.len(),
        sourcedir.display()
    );
    let products: Vec<ProductTaxonomies> = ctx
        .mount_each(
            files,
            |(key, _)| format!("file/{key}"),
            move |ctx, (_, file)| {
                let product_table = product_table.clone();
                async move { process_file(&ctx, file, product_table).await }
            },
        )
        .await?;

    // --- Phase 2: build the graph ---
    ctx.mount(&"build_graph", move |ctx| async move {
        build_graph(
            &ctx,
            products,
            taxonomy_table,
            product_taxonomy_rel,
            complementary_rel,
        )
        .await
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
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("products"));

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
        .provide_key(&LLM_MODEL, env_or("LLM_MODEL", "gpt-4.1"))
        .build()
        .await?
        .app(APP_NAME)
        .await?;

    let stats = app.run(move |ctx| app_main(ctx, sourcedir)).await?;
    println!("{stats}");
    Ok(())
}
