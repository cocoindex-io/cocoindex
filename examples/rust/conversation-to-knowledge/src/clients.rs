//! Shared clients (OpenAI LLM, fastembed embedder, SurrealDB graph) and the
//! ContextKeys used to inject them into the pipeline.

use std::sync::{Arc, LazyLock};

use cocoindex::prelude::*;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use serde::de::DeserializeOwned;
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::types::RecordId;

// ---------------------------------------------------------------------------
// Context keys
// ---------------------------------------------------------------------------

/// LLM used for metadata/statement extraction. State-tracked on the model name,
/// so changing the model invalidates memoized extraction (parity with Python's
/// `LLM_MODEL = ContextKey(..., detect_change=True)`).
pub static LLM: LazyLock<ContextKey<LlmClient>> =
    LazyLock::new(|| ContextKey::new_with_state("llm_model", |c: &LlmClient| c.model.clone()));

/// LLM used to confirm entity-resolution pairs.
pub static RESOLVER_LLM: LazyLock<ContextKey<LlmClient>> = LazyLock::new(|| {
    ContextKey::new_with_state("resolution_llm_model", |c: &LlmClient| c.model.clone())
});

/// Local embedder for entity-resolution similarity.
pub static EMBEDDER: LazyLock<ContextKey<Embedder>> =
    LazyLock::new(|| ContextKey::new_with_state("embedder", |e: &Embedder| e.model_name.clone()));

/// SurrealDB connection (not change-tracked).
pub static GRAPH: LazyLock<ContextKey<Graph>> = LazyLock::new(|| ContextKey::new("surreal_db"));

// ---------------------------------------------------------------------------
// LLM client (OpenAI-compatible, JSON mode)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct LlmClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    pub model: String,
}

impl LlmClient {
    pub fn new(model: String) -> Result<Self> {
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

    /// Chat completion in JSON-object mode; deserialize the response content
    /// into `T`. The expected JSON shape must be described in `system`.
    pub async fn json<T: DeserializeOwned>(&self, system: &str, user: &str) -> Result<T> {
        let body = serde_json::json!({
            "model": self.model,
            "response_format": { "type": "json_object" },
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
// Embedder (local fastembed model)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Embedder {
    model: Arc<TextEmbedding>,
    pub model_name: String,
}

impl Embedder {
    pub fn load(model_name: &str) -> Result<Self> {
        let model = TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2))
            .map_err(|e| Error::engine(format!("failed to load embedding model: {e}")))?;
        Ok(Self {
            model: Arc::new(model),
            model_name: model_name.to_string(),
        })
    }

    /// Embed a batch of texts (normalized; dot product == cosine similarity).
    pub async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || model.embed(texts, None))
            .await
            .map_err(|e| Error::engine(format!("embedding task panicked: {e}")))?
            .map_err(|e| Error::engine(format!("embedding failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// SurrealDB graph
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Graph {
    db: Arc<Surreal<Client>>,
}

fn surreal_err(e: surrealdb::Error) -> Error {
    Error::engine(format!("surrealdb: {e}"))
}

impl Graph {
    pub async fn connect(
        url: &str,
        ns: &str,
        db_name: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self> {
        let db = Surreal::new::<Ws>(url).await.map_err(surreal_err)?;
        db.signin(Root {
            username: user.to_string(),
            password: pass.to_string(),
        })
        .await
        .map_err(surreal_err)?;
        db.use_ns(ns.to_string())
            .use_db(db_name.to_string())
            .await
            .map_err(surreal_err)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Delete all records in the given tables (idempotent full-rebuild support).
    pub async fn clear(&self, tables: &[&str]) -> Result<()> {
        for table in tables {
            // Table names here are fixed constants, safe to interpolate.
            self.db
                .query(format!("DELETE {table}"))
                .await
                .map_err(surreal_err)?;
        }
        Ok(())
    }

    /// Upsert a node `table:id` with the given content fields.
    pub async fn upsert(&self, id: RecordId, content: serde_json::Value) -> Result<()> {
        self.db
            .query("UPSERT $id CONTENT $data")
            .bind(("id", id))
            .bind(("data", content))
            .await
            .map_err(surreal_err)?
            .check()
            .map_err(surreal_err)?;
        Ok(())
    }

    /// Create a `from -[edge]-> to` relation. `edge` is a fixed constant.
    pub async fn relate(&self, from: RecordId, edge: &str, to: RecordId) -> Result<()> {
        self.db
            .query(format!("RELATE $f->{edge}->$t"))
            .bind(("f", from))
            .bind(("t", to))
            .await
            .map_err(surreal_err)?
            .check()
            .map_err(surreal_err)?;
        Ok(())
    }

    /// Count records in a table (for tests / reporting).
    pub async fn count(&self, table: &str) -> Result<usize> {
        let mut res = self
            .db
            .query(format!("SELECT VALUE id FROM {table}"))
            .await
            .map_err(surreal_err)?;
        let ids: Vec<RecordId> = res.take(0).map_err(surreal_err)?;
        Ok(ids.len())
    }
}
