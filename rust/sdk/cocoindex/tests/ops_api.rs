//! Port of `python/tests/ops/test_embedder_refactor.py` and
//! `python/tests/ops/test_litellm_transcriber.py`, adapted for the Rust-native
//! `ops::api` HTTP embedder/transcriber. The Python tests mock the `litellm`
//! Python calls directly; here we stand up a mock HTTP server (wiremock) and
//! point the client at it, exercising the real request/response path.
//!
//! Run with: `cargo test -p cocoindex --features embed_api --test ops_api`.
#![cfg(feature = "embed_api")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use cocoindex::ops::api::{ApiEmbedder, ApiTranscriber, LlmPairResolver};
use cocoindex::prelude::{FileContentCache, FileLike, FileMetadata, FilePath};
use cocoindex::{CanonicalSide, PairResolver};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

async fn mock_server() -> Option<MockServer> {
    let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("skipping ops_api mock-server test; cannot bind localhost: {err}");
            return None;
        }
    };
    Some(MockServer::builder().listener(listener).start().await)
}

async fn embedding_server() -> Option<MockServer> {
    let server = mock_server().await?;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"embedding": [0.1, 0.2, 0.3, 0.4]}]
        })))
        .mount(&server)
        .await;
    Some(server)
}

#[tokio::test]
async fn api_embedder_single_text_api() {
    let Some(server) = embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model")
        .with_base_url(server.uri())
        .with_api_key("k");

    let vec = embedder.embed("hello").await.unwrap();

    // A single vector, not a batch.
    assert_eq!(vec.len(), 4);
    assert!((vec[0] - 0.1).abs() < 1e-6);

    // Exactly one request, carrying our single text in the batch.
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["input"], json!(["hello"]));
    assert_eq!(body["model"], json!("fake-model"));
}

#[tokio::test]
async fn api_embedder_vector_schema() {
    let Some(server) = embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    use cocoindex::VectorSchemaProvider;
    let schema = embedder.vector_schema().await.unwrap();
    assert_eq!(schema.size, 4);
    assert_eq!(schema.element_type, cocoindex::VectorElementType::Float32);
}

/// Mirror `test_litellm_encoding_format_gated_by_provider`: OpenAI-style models
/// request `encoding_format="float"`; voyage/bedrock models omit it.
#[tokio::test]
async fn api_embedder_encoding_format_gated_by_provider() {
    let cases: &[(&str, bool)] = &[
        ("text-embedding-3-small", true),
        ("openai/text-embedding-3-small", true),
        ("voyage/voyage-code-3", false),
        ("bedrock/amazon.titan-embed-text-v2:0", false),
    ];
    for (model, expects_float) in cases {
        let Some(server) = embedding_server().await else {
            return;
        };
        let embedder = ApiEmbedder::new(*model).with_base_url(server.uri());
        embedder.embed("hello").await.unwrap();

        let reqs = server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        if *expects_float {
            assert_eq!(
                body.get("encoding_format"),
                Some(&json!("float")),
                "model `{model}` should request float encoding"
            );
        } else {
            assert!(
                body.get("encoding_format").is_none(),
                "model `{model}` should omit encoding_format"
            );
        }
    }
}

/// A minimal in-memory [`FileLike`] for the transcriber test.
struct InMemoryFile {
    path: FilePath,
    data: Vec<u8>,
    cache: FileContentCache,
}

#[cocoindex::async_trait]
impl FileLike for InMemoryFile {
    fn file_path(&self) -> FilePath {
        self.path.clone()
    }

    fn cache(&self) -> &FileContentCache {
        &self.cache
    }

    async fn fetch_metadata(&self) -> cocoindex::Result<FileMetadata> {
        Ok(FileMetadata {
            size: self.data.len() as u64,
            modified: SystemTime::UNIX_EPOCH,
            content_fingerprint: None,
        })
    }

    async fn read_impl(&self, _size: Option<usize>) -> cocoindex::Result<Vec<u8>> {
        Ok(self.data.clone())
    }
}

#[tokio::test]
async fn api_transcriber_reads_file_and_sends_fields() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"text": "hello world"})))
        .mount(&server)
        .await;

    let file = InMemoryFile {
        path: FilePath::new("segment.mp3"),
        data: b"fake-audio".to_vec(),
        cache: FileContentCache::new(),
    };
    let transcriber = ApiTranscriber::new("fake-model")
        .with_base_url(server.uri())
        .with_api_key("k-default")
        .with_language("en");

    let text = transcriber.transcribe(&file).await.unwrap();
    assert_eq!(text, "hello world");

    // The multipart body carries the model, language, filename, and audio bytes
    // as plaintext form sections.
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body = String::from_utf8_lossy(&reqs[0].body);
    assert!(body.contains("fake-model"), "missing model in body");
    assert!(body.contains("segment.mp3"), "missing filename in body");
    assert!(body.contains("en"), "missing language in body");
    assert!(body.contains("fake-audio"), "missing audio bytes in body");
}

// ---------------------------------------------------------------------------
// LlmPairResolver — port of `python/tests/.../test_llm_resolver.py`. Python
// mocks `litellm`/`instructor`; here we stand up a mock `/chat/completions`
// endpoint and point the resolver's chat client at it.
// ---------------------------------------------------------------------------

/// A `/chat/completions` response whose assistant message content is `content`
/// (the JSON decision string the resolver parses).
fn chat_response(content: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "choices": [{"message": {"role": "assistant", "content": content}}]
    }))
}

#[tokio::test]
async fn llm_pair_resolver_parses_matched_and_canonical_new() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(
            r#"{"matched": "OpenAI Inc", "canonical": "new"}"#,
        ))
        .mount(&server)
        .await;

    let resolver = LlmPairResolver::new("fake-model")
        .with_base_url(server.uri())
        .with_api_key("k")
        .with_entity_type("organization");
    let decision = resolver
        .resolve_pair(
            "OpenAI, Inc.",
            &["OpenAI Inc".to_string(), "Anthropic".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(decision.matched.as_deref(), Some("OpenAI Inc"));
    assert_eq!(decision.canonical, CanonicalSide::New);

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["model"], json!("fake-model"));
    assert_eq!(body["response_format"]["type"], json!("json_object"));
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    // Entity-type hint woven into the system prompt.
    assert!(
        messages[0]["content"]
            .as_str()
            .unwrap()
            .contains("organization")
    );
}

#[tokio::test]
async fn llm_pair_resolver_null_match_returns_no_match() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(r#"{"matched": null}"#))
        .mount(&server)
        .await;

    let resolver = LlmPairResolver::new("fake-model").with_base_url(server.uri());
    let decision = resolver
        .resolve_pair("Foo", &["Bar".to_string()])
        .await
        .unwrap();
    assert!(decision.matched.is_none());
    assert_eq!(decision.canonical, CanonicalSide::Matched);
}

#[tokio::test]
async fn llm_pair_resolver_retries_on_invalid_candidate() {
    // First response names a candidate not in the list; the resolver retries,
    // and the second (valid) response wins.
    struct Sequenced {
        calls: Arc<AtomicUsize>,
    }
    impl Respond for Sequenced {
        fn respond(&self, _req: &Request) -> ResponseTemplate {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let content = if n == 0 {
                r#"{"matched": "Nonexistent"}"#
            } else {
                r#"{"matched": "Acme Corp", "canonical": "matched"}"#
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"role": "assistant", "content": content}}]
            }))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(Sequenced {
            calls: calls.clone(),
        })
        .mount(&server)
        .await;

    let resolver = LlmPairResolver::new("fake-model")
        .with_base_url(server.uri())
        .with_retries(3);
    let decision = resolver
        .resolve_pair("ACME", &["Acme Corp".to_string()])
        .await
        .unwrap();
    assert_eq!(decision.matched.as_deref(), Some("Acme Corp"));
    assert_eq!(calls.load(Ordering::SeqCst), 2, "should have retried once");
}

#[tokio::test]
async fn llm_pair_resolver_gives_up_after_retries() {
    // Always-invalid responses: exhaust retries, fall back to no-match.
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(r#"{"matched": "Nope"}"#))
        .mount(&server)
        .await;

    let resolver = LlmPairResolver::new("fake-model")
        .with_base_url(server.uri())
        .with_retries(2);
    let decision = resolver
        .resolve_pair("X", &["Y".to_string()])
        .await
        .unwrap();
    assert!(decision.matched.is_none());
    // 1 initial attempt + 2 retries = 3 calls.
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 3);
}
