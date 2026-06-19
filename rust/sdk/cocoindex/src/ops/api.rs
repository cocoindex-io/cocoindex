//! Remote embedding and transcription over OpenAI-compatible HTTP APIs.
//!
//! Rust-native equivalent of Python's `cocoindex.ops.litellm`
//! (`LiteLLMEmbedder` / `LiteLLMTranscriber`). Python routes to many providers
//! through the `litellm` library; there is no such router in Rust, so this
//! module talks directly to an OpenAI-compatible endpoint via `reqwest`. Point
//! it at any compatible base URL (OpenAI, Together, a local server, …) with
//! [`ApiEmbedder::with_base_url`] / [`ApiTranscriber::with_base_url`].
//!
//! - [`ApiEmbedder`] calls `POST {base_url}/embeddings` and implements
//!   [`VectorSchemaProvider`] (probing a `"hello"` embedding to discover the
//!   dimension, cached after the first call — same approach as Python).
//! - [`ApiTranscriber`] calls `POST {base_url}/audio/transcriptions` with the
//!   audio as a multipart upload, reading the bytes from any [`FileLike`].

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::file::FileLike;
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

fn base_url_or_default(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

async fn ensure_success(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    Err(Error::engine(format!("{what} failed ({status}): {body}")))
}

/// An embedder backed by an OpenAI-compatible `/embeddings` endpoint.
#[derive(Clone)]
pub struct ApiEmbedder {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: Option<String>,
    dimension: Arc<Mutex<Option<usize>>>,
}

impl ApiEmbedder {
    /// Create an embedder for `model`, defaulting to the OpenAI base URL and
    /// reading the API key from the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            model: model.into(),
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            dimension: Arc::new(Mutex::new(None)),
        }
    }

    /// Override the API base URL (e.g. a local or third-party endpoint).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    fn build_body(&self, input: &[String]) -> serde_json::Value {
        let mut body = json!({ "model": self.model, "input": input });
        // voyage/ and bedrock/ models reject `encoding_format="float"`; leave
        // them on their native defaults. Everyone else gets the float-decoded
        // payload (mirrors the Python LiteLLMEmbedder gating).
        if !(self.model.starts_with("voyage/") || self.model.starts_with("bedrock/")) {
            body["encoding_format"] = json!("float");
        }
        body
    }

    /// Embed a single text into an `f32` vector.
    pub async fn embed(&self, text: impl Into<String>) -> Result<Vec<f32>> {
        let mut out = self.embed_batch(vec![text.into()]).await?;
        out.pop()
            .ok_or_else(|| Error::engine("embedding API returned no vectors"))
    }

    /// Embed a batch of texts in one request.
    pub async fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let url = format!("{}/embeddings", base_url_or_default(&self.base_url));
        let mut req = self.client.post(&url).json(&self.build_body(&texts));
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::engine(format!("embedding request failed: {e}")))?;
        let resp = ensure_success(resp, "embedding request").await?;
        let parsed: EmbeddingResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("embedding response decode failed: {e}")))?;
        Ok(parsed.data.into_iter().map(|d| d.embedding).collect())
    }

    /// The embedding dimension, discovered by embedding a probe text and
    /// cached for subsequent calls.
    pub async fn dimension(&self) -> Result<usize> {
        let mut guard = self.dimension.lock().await;
        if let Some(dim) = *guard {
            return Ok(dim);
        }
        let dim = self.embed("hello").await?.len();
        *guard = Some(dim);
        Ok(dim)
    }
}

#[async_trait]
impl VectorSchemaProvider for ApiEmbedder {
    async fn vector_schema(&self) -> Result<VectorSchema> {
        Ok(VectorSchema {
            element_type: VectorElementType::Float32,
            size: self.dimension().await?,
        })
    }
}

#[async_trait]
impl crate::resources::embedder::Embedder for ApiEmbedder {
    // Delegate to the inherent methods (method-call resolution prefers inherent).
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        self.embed(text).await
    }
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.embed_batch(texts.to_vec()).await
    }
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

/// A speech-to-text transcriber backed by an OpenAI-compatible
/// `/audio/transcriptions` endpoint.
#[derive(Clone)]
pub struct ApiTranscriber {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: Option<String>,
    language: Option<String>,
}

impl ApiTranscriber {
    /// Create a transcriber for `model` (e.g. `"whisper-1"`), defaulting to the
    /// OpenAI base URL and the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            model: model.into(),
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            language: None,
        }
    }

    /// Override the API base URL.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the source-audio language hint (ISO-639-1, e.g. `"en"`).
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Transcribe the audio contained in a [`FileLike`].
    pub async fn transcribe<F: FileLike + ?Sized>(&self, file: &F) -> Result<String> {
        let bytes = file.read().await?;
        let filename = file.file_path().name().to_string();
        self.transcribe_bytes(bytes, filename).await
    }

    /// Transcribe raw audio bytes, using `filename` for the upload's filename
    /// (the extension lets the server infer the audio format).
    pub async fn transcribe_bytes(&self, bytes: Vec<u8>, filename: String) -> Result<String> {
        let url = format!(
            "{}/audio/transcriptions",
            base_url_or_default(&self.base_url)
        );
        let part = reqwest::multipart::Part::bytes(bytes).file_name(filename);
        let mut form = reqwest::multipart::Form::new()
            .text("model", self.model.clone())
            .part("file", part);
        if let Some(language) = &self.language {
            form = form.text("language", language.clone());
        }
        let mut req = self.client.post(&url).multipart(form);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::engine(format!("transcription request failed: {e}")))?;
        let resp = ensure_success(resp, "transcription request").await?;
        let parsed: TranscriptionResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("transcription response decode failed: {e}")))?;
        Ok(parsed.text)
    }
}

#[derive(Deserialize)]
struct TranscriptionResponse {
    text: String,
}

use crate::entity_resolution::{CanonicalSide, PairDecision, PairResolver};

/// A single chat message (`role` + `content`).
#[derive(Clone, Debug, serde::Serialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".into(),
            content: content.into(),
        }
    }
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: content.into(),
        }
    }
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: content.into(),
        }
    }
}

/// A chat-completion client backed by an OpenAI-compatible `/chat/completions`
/// endpoint.
///
/// Rust-native counterpart to the `litellm`/`instructor` chat calls in Python's
/// `cocoindex.ops.entity_resolution.llm_resolver`. Python routes through
/// `litellm`; Rust talks directly to an OpenAI-compatible endpoint via
/// `reqwest`, mirroring [`ApiEmbedder`].
#[derive(Clone)]
pub struct ApiChatClient {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: Option<String>,
}

impl ApiChatClient {
    /// Create a chat client for `model`, defaulting to the OpenAI base URL and
    /// reading the API key from the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            model: model.into(),
            api_key: std::env::var("OPENAI_API_KEY").ok(),
        }
    }

    /// Override the API base URL (e.g. a local or third-party endpoint).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// The model this client targets.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Run a chat completion, requesting a JSON-object response, and return the
    /// assistant message content (the raw JSON string).
    pub async fn chat_json(&self, messages: &[ChatMessage]) -> Result<String> {
        let url = format!("{}/chat/completions", base_url_or_default(&self.base_url));
        let body = json!({
            "model": self.model,
            "messages": messages,
            "response_format": { "type": "json_object" },
        });
        let mut req = self.client.post(&url).json(&body);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::engine(format!("chat request failed: {e}")))?;
        let resp = ensure_success(resp, "chat request").await?;
        let parsed: ChatResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("chat response decode failed: {e}")))?;
        parsed
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| Error::engine("chat API returned no choices"))
    }
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    #[serde(default)]
    content: String,
}

/// An [`PairResolver`] that asks an LLM (over [`ApiChatClient`]) whether a new
/// entity name refers to the same thing as one of a list of candidates.
///
/// Rust-native counterpart to Python's
/// `cocoindex.ops.entity_resolution.LlmPairResolver`. The prompt and retry
/// behaviour mirror the Python implementation; structured output is requested
/// via the endpoint's JSON-object response format instead of `instructor`.
#[derive(Clone)]
pub struct LlmPairResolver {
    client: ApiChatClient,
    entity_type: Option<String>,
    extra_guidance: Option<String>,
    retries: usize,
}

impl LlmPairResolver {
    /// Create a resolver using `model` (passed to [`ApiChatClient::new`]).
    pub fn new(model: impl Into<String>) -> Self {
        Self::with_client(ApiChatClient::new(model))
    }

    /// Use a pre-configured chat client (e.g. with a custom base URL/key).
    pub fn with_client(client: ApiChatClient) -> Self {
        Self {
            client,
            entity_type: None,
            extra_guidance: None,
            retries: 5,
        }
    }

    /// Override the API base URL.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.client = self.client.with_base_url(base_url);
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.client = self.client.with_api_key(api_key);
        self
    }

    /// Weave an entity-type hint into the prompt (e.g. `"person"`).
    pub fn with_entity_type(mut self, entity_type: impl Into<String>) -> Self {
        self.entity_type = Some(entity_type.into());
        self
    }

    /// Append domain guidance to the prompt. Do not include output-format
    /// instructions.
    pub fn with_extra_guidance(mut self, guidance: impl Into<String>) -> Self {
        self.extra_guidance = Some(guidance.into());
        self
    }

    /// Max retries when the LLM returns a `matched` value not in the candidate
    /// list. Default 5.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    fn system_prompt(&self) -> String {
        build_resolver_prompt(self.entity_type.as_deref(), self.extra_guidance.as_deref())
    }
}

fn build_resolver_prompt(entity_type: Option<&str>, extra_guidance: Option<&str>) -> String {
    let scope = match entity_type {
        Some(t) => format!(" of type {t:?}"),
        None => String::new(),
    };
    let mut prompt = format!(
        "You are resolving entity names{scope}. Given a new entity name and a \
list of existing canonical entity names, decide whether the new entity refers \
to the same thing as any existing one.\n\n\
Rules for the JSON response:\n\
- If no existing candidate refers to the same thing, set `matched` to null.\n\
- If the new entity matches an existing candidate, set `matched` to the exact \
candidate name (case-sensitive; it MUST be one of the candidates listed below).\n\
- When `matched` is set, also set `canonical`:\n\
  - `\"new\"`  — the new entity's name is a better canonical (more \
complete/correct); promote it and demote the matched name.\n\
  - `\"matched\"` (default) — the existing matched name stays canonical.\n\n\
If you are unsure whether two names refer to the same thing, err on the side of \
`matched` being null.\n\n\
Respond with only a JSON object of the form \
{{\"matched\": <candidate name or null>, \"canonical\": \"new\" | \"matched\"}}."
    );
    if let Some(guidance) = extra_guidance {
        prompt.push_str(&format!("\n\nAdditional guidance:\n{guidance}"));
    }
    prompt
}

fn build_user_message(entity: &str, candidates: &[String]) -> String {
    let lines = candidates
        .iter()
        .map(|c| format!("  - {c:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("New entity: {entity:?}\n\nExisting canonical candidates:\n{lines}")
}

#[derive(Deserialize)]
struct LlmDecision {
    #[serde(default)]
    matched: Option<String>,
    #[serde(default)]
    canonical: Option<String>,
}

#[async_trait]
impl PairResolver for LlmPairResolver {
    async fn resolve_pair(&self, entity: &str, candidates: &[String]) -> Result<PairDecision> {
        let mut messages = vec![
            ChatMessage::system(self.system_prompt()),
            ChatMessage::user(build_user_message(entity, candidates)),
        ];
        for _ in 0..=self.retries {
            let content = self.client.chat_json(&messages).await?;
            let decision: LlmDecision = serde_json::from_str(&content).map_err(|e| {
                Error::engine(format!("LLM resolver response was not valid JSON: {e}"))
            })?;
            let matched = decision.matched.filter(|m| !m.is_empty());
            match &matched {
                // No match, or a valid in-list candidate (and not self): accept.
                None => return Ok(PairDecision::no_match()),
                Some(m) if m != entity && candidates.iter().any(|c| c == m) => {
                    let canonical = match decision.canonical.as_deref() {
                        Some("new") => CanonicalSide::New,
                        _ => CanonicalSide::Matched,
                    };
                    return Ok(PairDecision::matched_with(m.clone(), canonical));
                }
                // Invalid `matched`: feed the model a correction and retry.
                Some(invalid) => {
                    messages.push(ChatMessage::assistant(content));
                    messages.push(ChatMessage::user(format!(
                        "{invalid:?} is not one of the listed candidates. Choose \
exactly one of the candidate names verbatim, or set `matched` to null."
                    )));
                }
            }
        }
        // Retries exhausted without a valid match: treat as no match (matches
        // the Python fallback).
        Ok(PairDecision::no_match())
    }
}
