//! Shared clients (OpenAI LLM, fastembed embedder, SurrealDB graph) and the
//! ContextKeys used to inject them into the pipeline.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use cocoindex::prelude::*;
pub use cocoindex::surrealdb::Graph;
use fastembed::{
    EmbeddingModel, InitOptions, InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel, read_file_to_bytes,
};
use hf_hub::api::sync::ApiBuilder;
use serde::de::DeserializeOwned;

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

/// SurrealDB connection. State-tracked on the target endpoint so changing the
/// external graph database invalidates local target-state reconciliation.
pub static GRAPH: LazyLock<ContextKey<Graph>> =
    LazyLock::new(|| ContextKey::new_with_state("surreal_db", |g: &Graph| g.state_id().to_string()));

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
        let model = match model_name {
            "Snowflake/snowflake-arctic-embed-xs" => load_snowflake_arctic_embed_xs()?,
            "sentence-transformers/all-MiniLM-L6-v2" | "all-MiniLM-L6-v2" => {
                TextEmbedding::try_new(InitOptions::new(EmbeddingModel::AllMiniLML6V2))
                    .map_err(|e| Error::engine(format!("failed to load embedding model: {e}")))?
            }
            other => {
                return Err(Error::engine(format!(
                    "unsupported embedding model {other:?}; supported: Snowflake/snowflake-arctic-embed-xs, sentence-transformers/all-MiniLM-L6-v2"
                )));
            }
        };
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

fn load_snowflake_arctic_embed_xs() -> Result<TextEmbedding> {
    let cache_dir = std::env::var("HF_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("FASTEMBED_CACHE_DIR").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from(".fastembed_cache"));
    let endpoint =
        std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string());
    let repo = ApiBuilder::new()
        .with_cache_dir(cache_dir)
        .with_endpoint(endpoint)
        .with_progress(true)
        .build()
        .map_err(|e| Error::engine(format!("failed to create Hugging Face client: {e}")))?
        .model("Snowflake/snowflake-arctic-embed-xs".to_string());

    let file = |path: &str| -> Result<Vec<u8>> {
        let local_path = repo
            .get(path)
            .map_err(|e| Error::engine(format!("failed to download {path}: {e}")))?;
        read_file_to_bytes(&local_path)
            .map_err(|e| Error::engine(format!("failed to read {}: {e}", local_path.display())))
    };

    let model = UserDefinedEmbeddingModel::new(
        file("onnx/model.onnx")?,
        TokenizerFiles {
            tokenizer_file: file("tokenizer.json")?,
            config_file: file("config.json")?,
            special_tokens_map_file: file("special_tokens_map.json")?,
            tokenizer_config_file: file("tokenizer_config.json")?,
        },
    )
    .with_pooling(Pooling::Cls);
    TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::default())
        .map_err(|e| Error::engine(format!("failed to load Snowflake embedding model: {e}")))
}
