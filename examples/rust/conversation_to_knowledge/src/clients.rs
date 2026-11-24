//! Shared clients (OpenAI LLM, the SDK's embedder and chat client, SurrealDB
//! graph) and the ContextKeys used to inject them into the pipeline.

pub use cocoindex::connectors::surrealdb::Graph;
pub use cocoindex::ops::api::ApiChatClient;
pub use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::prelude::*;
use serde::de::DeserializeOwned;

// ---------------------------------------------------------------------------
// Context keys
// ---------------------------------------------------------------------------

// LLM used for metadata/statement extraction. Its MemoInput tracks the model,
// so changing the model invalidates memoized extraction (parity with Python's
// `LLM_MODEL = ContextKey(..., detect_change=True)`).
cocoindex::context_key!(
    pub static LLM: LlmClient,
    detect_change
);

// LLM used to confirm entity-resolution pairs. Each decision's memo key holds
// the model, so no change detection on the key is needed.
cocoindex::context_key!(pub static RESOLVER_LLM: ApiChatClient);

// Local embedder for entity-resolution similarity.
cocoindex::context_key!(
    pub static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

// SurrealDB connection. Connection resources are not memo dependencies.
cocoindex::context_key!(pub static GRAPH: Graph);

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

/// The LLM API key: `OPENAI_API_KEY`, or `LLM_API_KEY` for another provider.
pub fn llm_api_key() -> Result<String> {
    std::env::var("OPENAI_API_KEY")
        .or_else(|_| std::env::var("LLM_API_KEY"))
        .map_err(|_| Error::engine("set OPENAI_API_KEY (or LLM_API_KEY)"))
}

/// The LLM API base URL (`LLM_BASE_URL`), OpenAI's by default.
pub fn llm_base_url() -> String {
    std::env::var("LLM_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into())
}

impl LlmClient {
    pub fn new(model: String) -> Result<Self> {
        let api_key = llm_api_key()?;
        let base_url = llm_base_url();
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

impl MemoInput for LlmClient {
    fn write_memo_key(&self, writer: &mut cocoindex::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&self.model)
    }
}
