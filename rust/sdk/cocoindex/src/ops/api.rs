//! Remote embedding, transcription and chat over OpenAI-compatible HTTP APIs.
//!
//! Rust-native equivalent of Python's `cocoindex.ops.litellm`
//! (`LiteLLMEmbedder` / `LiteLLMTranscriber`) and of the `litellm`-backed
//! `LlmPairResolver`. Python routes to many providers through the `litellm`
//! library; there is no such router in Rust, so this module talks directly to
//! an OpenAI-compatible endpoint via `reqwest`. Point it at any compatible base
//! URL (OpenAI, Together, vLLM, Ollama, …) with `with_base_url`.
//!
//! - [`ApiEmbedder`] calls `POST {base_url}/embeddings`. `embed` memoizes per
//!   text and batches concurrent misses (up to 64 texts per request), like
//!   Python's `@coco.fn(memo=True)` over a `batching=True` body. It implements
//!   [`VectorSchemaProvider`] (probing a `"hello"` embedding to discover the
//!   dimension, cached after the first call — same approach as Python).
//! - [`ApiTranscriber`] calls `POST {base_url}/audio/transcriptions` with the
//!   audio as a multipart upload, reading the bytes from any [`FileLike`].
//!   `transcribe` is memoized by the file's identity and content.
//! - [`ApiChatClient`] calls `POST {base_url}/chat/completions` in JSON mode;
//!   [`ApiChatClient::extract`] deserializes the answer into a type and
//!   memoizes it per conversation, and [`LlmPairResolver`] builds on the
//!   client and memoizes each decision.
//!
//! Every request is bounded by the op's `timeout` (10 minutes by default):
//! fast failures (408/409/425/429/5xx, a refused or reset connection) are
//! retried with exponential backoff inside it, a request that times out is not
//! retried, and expiry is an error. An active [`Ctx`] deadline can only shorten
//! the bound, never extend it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, Semaphore};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::memo::MemoInput;
use crate::resources::embedder::Embedder;
use crate::resources::file::FileLike;
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// Bound on one request, retries included (Python's `timeout` default).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Statuses worth another try: a fast failure that batch composition and time
/// can change (Python's `_RETRYABLE_HTTP_STATUS_CODES` plus every 5xx).
const RETRYABLE_STATUSES: &[u16] = &[408, 409, 425, 429];

fn base_url_or_default(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    RETRYABLE_STATUSES.contains(&status.as_u16()) || status.is_server_error()
}

/// Shared HTTP configuration of the three clients in this module.
#[derive(Clone)]
struct Endpoint {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    timeout: Duration,
    /// Permits for in-flight requests, when capped.
    inflight: Option<Arc<Semaphore>>,
}

impl Endpoint {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            timeout: DEFAULT_TIMEOUT,
            inflight: None,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{path}", base_url_or_default(&self.base_url))
    }

    fn authorize(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => req.bearer_auth(key),
            None => req,
        }
    }

    /// Send a request built by `build`, retrying transient failures until the
    /// request bound expires, and return the successful response. `build` is
    /// called once per attempt because a request body (such as a multipart
    /// upload) cannot be reused. `ctx`, when given, can shorten the bound.
    async fn send_with_retry(
        &self,
        ctx: Option<&Ctx>,
        build: impl Fn() -> reqwest::RequestBuilder,
        what: &str,
    ) -> Result<reqwest::Response> {
        let timeout = ctx
            .and_then(Ctx::remaining_deadline)
            .map_or(self.timeout, |remaining| remaining.min(self.timeout));
        let deadline = Instant::now() + timeout;
        let timed_out = || {
            if let Some(ctx) = ctx {
                ctx.check_cancellation()?;
            }
            Err(Error::engine(format!(
                "{what} did not complete within {timeout:?}"
            )))
        };
        let mut backoff = RETRY_INITIAL_BACKOFF;
        loop {
            // The permit covers one attempt only: a sleeping retry holds no
            // slot (cf. Python's `_inflight_permit`).
            let permit = match &self.inflight {
                Some(semaphore) => Some(
                    semaphore
                        .acquire()
                        .await
                        .map_err(|_| Error::engine("in-flight request limiter closed"))?,
                ),
                None => None,
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return timed_out();
            }
            let attempt = tokio::time::timeout(remaining, build().timeout(remaining).send()).await;
            drop(permit);
            let retry_after = match attempt {
                // A timeout has already spent its duration; never retry it.
                Err(_) => return timed_out(),
                Ok(Err(e)) if e.is_timeout() => return timed_out(),
                Ok(Ok(resp)) if resp.status().is_success() => return Ok(resp),
                Ok(Ok(resp)) => {
                    let status = resp.status();
                    if !is_retryable_status(status) {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::engine(format!("{what} failed ({status}): {body}")));
                    }
                    resp.headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(Duration::from_secs)
                }
                Ok(Err(e)) if e.is_connect() || e.is_request() => None,
                Ok(Err(e)) => return Err(Error::engine(format!("{what} failed: {e}"))),
            };
            let delay = retry_after.unwrap_or(backoff).min(RETRY_MAX_BACKOFF);
            if Instant::now() + delay >= deadline {
                return timed_out();
            }
            tokio::time::sleep(delay).await;
            backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
        }
    }
}

/// An embedder backed by an OpenAI-compatible `/embeddings` endpoint.
///
/// Rust counterpart of Python's `LiteLLMEmbedder`. Cheap to clone; clones
/// share the in-flight request cap and the probed dimension.
#[derive(Clone)]
pub struct ApiEmbedder {
    endpoint: Endpoint,
    model: String,
    dimensions: Option<usize>,
    dimension: Arc<Mutex<Option<usize>>>,
}

/// What identifies an embedder's output: the memo key of every embedding it
/// produces. `api_key`, `timeout` and `max_inflight_requests` are deliberately
/// absent: they authenticate, pace and bound requests but cannot change an
/// embedding, so tuning them must not invalidate every cached vector.
#[derive(Serialize)]
struct ApiEmbedderIdentity<'a> {
    model: &'a str,
    base_url: &'a str,
    dimensions: Option<usize>,
}

#[derive(Clone, crate::MemoInput)]
struct ScheduledApiEmbedder(ApiEmbedder);

impl Serialize for ScheduledApiEmbedder {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.identity().serialize(serializer)
    }
}

/// One physical `/embeddings` request for the misses of a batch of `embed`
/// calls sharing an `input_type`. The engine keys batches by `embedder` and
/// `input_type`, so calls with different values never share a request.
#[crate::function(memo, batching, max_batch_size = 64)]
async fn embed_api_scheduled(
    _ctx: &Ctx,
    texts: Vec<String>,
    embedder: ScheduledApiEmbedder,
    input_type: Option<String>,
) -> Result<Vec<Vec<f32>>> {
    embedder
        .0
        .request_embeddings(None, texts, input_type.as_deref())
        .await
}

impl ApiEmbedder {
    /// Create an embedder for `model`, defaulting to the OpenAI base URL and
    /// reading the API key from the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::new(),
            model: model.into(),
            dimensions: None,
            dimension: Arc::new(Mutex::new(None)),
        }
    }

    /// Override the API base URL (e.g. a local or third-party endpoint).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.endpoint.api_key = Some(api_key.into());
        self
    }

    /// Ask the model for vectors of this size (the request's `dimensions`
    /// field, for models such as `text-embedding-3-*` that support it).
    pub fn with_dimensions(mut self, dimensions: usize) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Bound each embedding request, retries included. Defaults to 10 minutes;
    /// raise it for a slow endpoint, since one request carries up to 64 texts.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.endpoint.timeout = timeout;
        self
    }

    /// Cap how many requests this embedder keeps open against the backend at
    /// once. Uncapped by default; useful for self-hosted endpoints that reject
    /// bursts with 429s. The cap is shared by all clones of this embedder.
    pub fn with_max_inflight_requests(mut self, max_inflight_requests: usize) -> Self {
        self.endpoint.inflight = Some(Arc::new(Semaphore::new(max_inflight_requests)));
        self
    }

    fn identity(&self) -> ApiEmbedderIdentity<'_> {
        ApiEmbedderIdentity {
            model: &self.model,
            base_url: &self.endpoint.base_url,
            dimensions: self.dimensions,
        }
    }

    /// Embed one text with per-text memoization and automatic batching.
    ///
    /// Concurrent cache misses in the same update are coalesced into requests
    /// of up to 64 texts. Repeated texts are served from the engine memo store.
    /// Use [`ApiEmbedder::embed_batch`] for an explicit raw request outside a
    /// CocoIndex update.
    pub async fn embed(&self, ctx: &Ctx, text: impl Into<String>) -> Result<Vec<f32>> {
        embed_api_scheduled(ctx, text.into(), ScheduledApiEmbedder(self.clone()), None).await
    }

    /// Like [`embed`](Self::embed), sending `input_type` with the request for
    /// asymmetric models (e.g. Cohere's `"search_query"` / `"search_document"`).
    /// Calls with different input types are memoized and batched separately.
    pub async fn embed_with_input_type(
        &self,
        ctx: &Ctx,
        text: impl Into<String>,
        input_type: impl Into<String>,
    ) -> Result<Vec<f32>> {
        embed_api_scheduled(
            ctx,
            text.into(),
            ScheduledApiEmbedder(self.clone()),
            Some(input_type.into()),
        )
        .await
    }

    /// Embed a batch of texts in one request, without memoization or
    /// batching: the path for code outside a CocoIndex update, such as
    /// embedding a search query.
    pub async fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        self.request_embeddings(None, texts, None).await
    }

    async fn request_embeddings(
        &self,
        ctx: Option<&Ctx>,
        texts: Vec<String>,
        input_type: Option<&str>,
    ) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let url = self.endpoint.url("embeddings");
        let mut body = json!({ "model": self.model, "input": texts });
        if let Some(dimensions) = self.dimensions {
            body["dimensions"] = json!(dimensions);
        }
        if let Some(input_type) = input_type {
            body["input_type"] = json!(input_type);
        }
        let resp = self
            .endpoint
            .send_with_retry(
                ctx,
                || {
                    self.endpoint
                        .authorize(self.endpoint.client.post(&url).json(&body))
                },
                "embedding request",
            )
            .await?;
        let parsed: EmbeddingResponse = resp
            .json()
            .await
            .map_err(|e| Error::engine(format!("embedding response decode failed: {e}")))?;
        embeddings_in_input_order(parsed.data, texts.len())
    }

    /// The embedding dimension, discovered by embedding a probe text and
    /// cached for subsequent calls.
    pub async fn dimension(&self) -> Result<usize> {
        let mut guard = self.dimension.lock().await;
        if let Some(dim) = *guard {
            return Ok(dim);
        }
        let dim = self
            .embed_batch(vec!["hello".to_string()])
            .await?
            .first()
            .map_or(0, Vec::len);
        *guard = Some(dim);
        Ok(dim)
    }
}

#[async_trait]
impl MemoInput for ApiEmbedder {
    fn write_memo_key(&self, writer: &mut crate::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&self.identity())
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
impl Embedder for ApiEmbedder {
    /// Memoized and batched, like the inherent [`embed`](ApiEmbedder::embed).
    async fn embed(&self, ctx: &Ctx, text: &str) -> Result<Vec<f32>> {
        ApiEmbedder::embed(self, ctx, text).await
    }
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
    /// Position of the input this embedding belongs to. OpenAI returns data
    /// in input order, but compatible servers are not required to.
    index: Option<usize>,
}

/// The embeddings ordered like the inputs, checking that there is exactly
/// one per input.
fn embeddings_in_input_order(
    mut data: Vec<EmbeddingData>,
    num_inputs: usize,
) -> Result<Vec<Vec<f32>>> {
    if data.len() != num_inputs {
        return Err(Error::engine(format!(
            "embedding API returned {} vectors for {num_inputs} inputs",
            data.len()
        )));
    }
    if data.iter().all(|d| d.index.is_some()) {
        data.sort_by_key(|d| d.index);
        if data.iter().enumerate().any(|(i, d)| d.index != Some(i)) {
            return Err(Error::engine(
                "embedding API returned duplicate or out-of-range indices",
            ));
        }
    }
    Ok(data.into_iter().map(|d| d.embedding).collect())
}

/// A speech-to-text transcriber backed by an OpenAI-compatible
/// `/audio/transcriptions` endpoint.
///
/// Rust counterpart of Python's `LiteLLMTranscriber`.
#[derive(Clone)]
pub struct ApiTranscriber {
    endpoint: Endpoint,
    model: String,
    language: Option<String>,
}

/// The memo identity of a transcriber's output; see [`ApiEmbedderIdentity`]
/// for why the key, timeout and in-flight cap are absent.
fn transcriber_identity(transcriber: &ApiTranscriber) -> (&str, &str, Option<&str>) {
    (
        &transcriber.model,
        &transcriber.endpoint.base_url,
        transcriber.language.as_deref(),
    )
}

/// One memoized transcription: keyed by the file's identity, validated by its
/// content state (cf. `FileLike`'s `MemoInput`), and by the transcriber.
#[crate::function(memo, memo_key(transcriber = transcriber_identity))]
async fn transcribe_memoized<F: FileLike + MemoInput + Clone + 'static>(
    ctx: &Ctx,
    file: &F,
    transcriber: &ApiTranscriber,
) -> Result<String> {
    let bytes = file.read().await?;
    let filename = file.file_path().name().to_string();
    transcriber
        .request_transcription(Some(ctx), bytes, filename)
        .await
}

impl ApiTranscriber {
    /// Create a transcriber for `model` (e.g. `"whisper-1"`), defaulting to the
    /// OpenAI base URL and the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::new(),
            model: model.into(),
            language: None,
        }
    }

    /// Override the API base URL.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.endpoint.api_key = Some(api_key.into());
        self
    }

    /// Set the source-audio language hint (ISO-639-1, e.g. `"en"`).
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Bound each transcription request, retries included. Defaults to 10
    /// minutes; raise it for long audio, since one request carries a whole file.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.endpoint.timeout = timeout;
        self
    }

    /// Transcribe the audio contained in a [`FileLike`], memoized by the file's
    /// identity and content: an unchanged file is not sent again.
    pub async fn transcribe<F: FileLike + MemoInput + Clone + 'static>(
        &self,
        ctx: &Ctx,
        file: &F,
    ) -> Result<String> {
        transcribe_memoized(ctx, file, self).await
    }

    /// Transcribe raw audio bytes directly, without memoization, using
    /// `filename` for the upload's filename (the extension lets the server
    /// infer the audio format).
    pub async fn transcribe_bytes(
        &self,
        bytes: impl Into<bytes::Bytes>,
        filename: String,
    ) -> Result<String> {
        self.request_transcription(None, bytes.into(), filename)
            .await
    }

    async fn request_transcription(
        &self,
        ctx: Option<&Ctx>,
        bytes: bytes::Bytes,
        filename: String,
    ) -> Result<String> {
        let url = self.endpoint.url("audio/transcriptions");
        let resp = self
            .endpoint
            .send_with_retry(
                ctx,
                || {
                    // A fresh body per attempt: the upload is consumed by a
                    // send. Cloning `Bytes` shares the buffer.
                    let part =
                        reqwest::multipart::Part::stream(bytes.clone()).file_name(filename.clone());
                    let mut form = reqwest::multipart::Form::new()
                        .text("model", self.model.clone())
                        .part("file", part);
                    if let Some(language) = &self.language {
                        form = form.text("language", language.clone());
                    }
                    self.endpoint
                        .authorize(self.endpoint.client.post(&url).multipart(form))
                },
                "transcription request",
            )
            .await?;
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
#[derive(Clone, Debug, Serialize, crate::MemoInput)]
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
    endpoint: Endpoint,
    model: String,
}

impl ApiChatClient {
    /// Create a chat client for `model`, defaulting to the OpenAI base URL and
    /// reading the API key from the `OPENAI_API_KEY` environment variable.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::new(),
            model: model.into(),
        }
    }

    /// Override the API base URL (e.g. a local or third-party endpoint).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    /// Set the bearer API key explicitly.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.endpoint.api_key = Some(api_key.into());
        self
    }

    /// The model this client targets.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Run a chat completion, requesting a JSON-object response, and return the
    /// assistant message content (the raw JSON string).
    pub async fn chat_json(&self, messages: &[ChatMessage]) -> Result<String> {
        self.request_chat_json(None, messages).await
    }

    /// Run a chat completion in JSON mode and deserialize the answer into
    /// `T`, memoized through the engine by the model, the endpoint and the
    /// messages: the way to extract structured data with an LLM inside an
    /// update (Python's `litellm` call with a `response_format` model). The
    /// prompt has to describe the JSON shape `T` deserializes from.
    pub async fn extract<T: DeserializeOwned>(
        &self,
        ctx: &Ctx,
        messages: &[ChatMessage],
    ) -> Result<T> {
        let content = chat_json_memoized(ctx, self, messages).await?;
        serde_json::from_str(&content)
            .map_err(|e| Error::engine(format!("chat response was not the expected JSON: {e}")))
    }

    async fn request_chat_json(
        &self,
        ctx: Option<&Ctx>,
        messages: &[ChatMessage],
    ) -> Result<String> {
        let url = self.endpoint.url("chat/completions");
        let body = json!({
            "model": self.model,
            "messages": messages,
            "response_format": { "type": "json_object" },
        });
        let resp = self
            .endpoint
            .send_with_retry(
                ctx,
                || {
                    self.endpoint
                        .authorize(self.endpoint.client.post(&url).json(&body))
                },
                "chat request",
            )
            .await?;
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

/// A client's memo identity is the model and the endpoint: what can change an
/// answer. The key, timeout and in-flight cap cannot, and are absent. So a
/// `detect_change` context key holding a client invalidates memoized work
/// when the model or endpoint changes.
#[async_trait]
impl MemoInput for ApiChatClient {
    fn write_memo_key(&self, writer: &mut crate::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&(&self.model, &self.endpoint.base_url))
    }
}

#[crate::function(memo)]
async fn chat_json_memoized(
    ctx: &Ctx,
    client: &ApiChatClient,
    messages: &[ChatMessage],
) -> Result<String> {
    client.request_chat_json(Some(ctx), messages).await
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

/// A [`PairResolver`] that asks an LLM (over [`ApiChatClient`]) whether a new
/// entity name refers to the same thing as one of a list of candidates.
///
/// Rust-native counterpart to Python's
/// `cocoindex.ops.entity_resolution.LlmPairResolver`. The prompt and retry
/// behaviour mirror the Python implementation; structured output is requested
/// via the endpoint's JSON-object response format instead of `instructor`.
/// Each `(entity, candidates)` decision is memoized through the engine and
/// persists across runs, as Python's `@coco.fn(memo=True)` does.
#[derive(Clone)]
pub struct LlmPairResolver {
    client: ApiChatClient,
    entity_type: Option<String>,
    extra_guidance: Option<String>,
    retries: usize,
}

/// Default for [`LlmPairResolver::with_retries`] (Python's default).
const DEFAULT_RESOLVER_RETRIES: usize = 2;

/// The memo identity of a resolver's decisions (cf. Python's
/// `_memo_key = memo_fingerprint((model, entity_type, extra_guidance))`);
/// `retries` cannot change a valid decision and is absent.
fn resolver_identity(resolver: &LlmPairResolver) -> (&str, &str, Option<&str>, Option<&str>) {
    (
        &resolver.client.model,
        &resolver.client.endpoint.base_url,
        resolver.entity_type.as_deref(),
        resolver.extra_guidance.as_deref(),
    )
}

#[crate::function(memo, memo_key(resolver = resolver_identity))]
async fn resolve_pair_memoized(
    ctx: &Ctx,
    entity: &str,
    candidates: &[String],
    resolver: &LlmPairResolver,
) -> Result<PairDecision> {
    resolver.request_decision(ctx, entity, candidates).await
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
            retries: DEFAULT_RESOLVER_RETRIES,
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
    /// list. Default 2.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    fn system_prompt(&self) -> String {
        build_resolver_prompt(self.entity_type.as_deref(), self.extra_guidance.as_deref())
    }

    async fn request_decision(
        &self,
        ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> Result<PairDecision> {
        let mut messages = vec![
            ChatMessage::system(self.system_prompt()),
            ChatMessage::user(build_user_message(entity, candidates)),
        ];
        for _ in 0..=self.retries {
            let content = self.client.request_chat_json(Some(ctx), &messages).await?;
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
    /// Memoized per `(entity, candidates)` through the engine.
    async fn resolve_pair(
        &self,
        ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> Result<PairDecision> {
        resolve_pair_memoized(ctx, entity, candidates, self).await
    }
}
