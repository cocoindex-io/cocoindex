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
