//! Port of `python/tests/ops/test_embedder_refactor.py`,
//! `test_litellm_transcriber.py`, `test_litellm_timeouts.py`,
//! `test_litellm_concurrency.py` and `test_llm_resolver.py`, adapted for the
//! Rust-native `ops::api` HTTP clients. The Python tests mock the `litellm`
//! Python calls directly; here we stand up a mock HTTP server (wiremock) and
//! point the client at it, exercising the real request/response path.
//!
//! Run with: `cargo test -p cocoindex --features embed_api --test ops_api`.
#![cfg(feature = "embed_api")]

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use cocoindex::ops::api::{
    ApiChatClient, ApiEmbedder, ApiTranscriber, ChatMessage, LlmPairResolver,
};
use cocoindex::prelude::{FileContentCache, FileLike, FileMetadata, FilePath};
use cocoindex::{CanonicalSide, Ctx, PairResolver, walk_items};
use futures::future::join_all;
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

/// Run `f` as the body of one update on a fresh environment.
async fn in_update<T, F, Fut>(name: &str, f: F) -> cocoindex::Result<T>
where
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = cocoindex::Result<T>> + Send + 'static,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    let tempdir = tempfile::tempdir().unwrap();
    let app = cocoindex::Environment::builder()
        .db_path(tempdir.path().join("db"))
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    app.update(f).await
}

/// A fresh app in `tempdir`, so several updates share one memo store.
async fn app_in(tempdir: &tempfile::TempDir, name: &str) -> cocoindex::App {
    cocoindex::Environment::builder()
        .db_path(tempdir.path().join("db"))
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap()
}

/// An `/embeddings` server that answers each input with `[len]`, so a result
/// can be checked against its text.
struct LengthEmbeddings;

impl Respond for LengthEmbeddings {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let data: Vec<serde_json::Value> = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, text)| {
                json!({"index": index, "embedding": [text.as_str().unwrap().len() as f32]})
            })
            .collect();
        ResponseTemplate::new(200).set_body_json(json!({ "data": data }))
    }
}

async fn length_embedding_server() -> Option<MockServer> {
    let server = mock_server().await?;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(LengthEmbeddings)
        .mount(&server)
        .await;
    Some(server)
}

async fn request_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|req| serde_json::from_slice(&req.body).unwrap())
        .collect()
}

fn input_count(body: &serde_json::Value) -> usize {
    body["input"].as_array().unwrap().len()
}

#[tokio::test]
async fn api_embedder_single_text_api() {
    let Some(server) = length_embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model")
        .with_base_url(server.uri())
        .with_api_key("k");

    let vec = in_update("api_single", move |ctx| async move {
        embedder.embed(&ctx, "hello").await
    })
    .await
    .unwrap();

    // A single vector, not a batch.
    assert_eq!(vec, vec![5.0]);

    // Exactly one request, carrying our single text in the batch, with no
    // provider-specific fields.
    let bodies = request_bodies(&server).await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0],
        json!({"model": "fake-model", "input": ["hello"]})
    );
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs[0].headers.get("authorization").unwrap(), "Bearer k");
}

/// The Rust counterpart of `single_text_embed_batches_misses_and_memoizes_results`
/// for the HTTP embedder: concurrent misses share requests of at most 64
/// texts, and a second update is served from the memo store.
#[tokio::test]
async fn api_embedder_single_text_batches_misses_and_memoizes_results() {
    let Some(server) = length_embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let app = app_in(&tempdir, "api_batching").await;
    let texts: Vec<String> = (0..70).map(|index| format!("text-{index}")).collect();

    for _ in 0..2 {
        let embedder = embedder.clone();
        let texts = texts.clone();
        app.update(move |ctx| async move {
            let expected: Vec<Vec<f32>> =
                texts.iter().map(|text| vec![text.len() as f32]).collect();
            let results = join_all(texts.into_iter().map(|text| embedder.embed(&ctx, text)))
                .await
                .into_iter()
                .collect::<cocoindex::Result<Vec<_>>>()?;
            assert_eq!(results, expected);
            Ok(())
        })
        .await
        .unwrap();
    }

    let bodies = request_bodies(&server).await;
    assert_eq!(
        bodies.iter().map(input_count).sum::<usize>(),
        70,
        "the second update should be served entirely from memo entries"
    );
    assert!(
        bodies.iter().any(|body| input_count(body) > 1),
        "concurrent misses should be coalesced into a request"
    );
    assert!(bodies.iter().all(|body| input_count(body) <= 64));
}

/// `input_type` reaches the request, and calls with different input types
/// never share a request (cf. Python: "mixing values creates separate
/// batchers").
#[tokio::test]
async fn api_embedder_input_type_is_sent_and_separates_batches() {
    let Some(server) = length_embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    in_update("api_input_type", move |ctx| async move {
        let (query, doc, plain) = tokio::join!(
            embedder.embed_with_input_type(&ctx, "q", "search_query"),
            embedder.embed_with_input_type(&ctx, "doc", "search_document"),
            embedder.embed(&ctx, "plain"),
        );
        assert_eq!(query?, vec![1.0]);
        assert_eq!(doc?, vec![3.0]);
        assert_eq!(plain?, vec![5.0]);
        Ok(())
    })
    .await
    .unwrap();

    let mut sent: Vec<(Option<String>, Vec<String>)> = request_bodies(&server)
        .await
        .iter()
        .map(|body| {
            (
                body.get("input_type")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                body["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect(),
            )
        })
        .collect();
    sent.sort();
    assert_eq!(
        sent,
        vec![
            (None, vec!["plain".to_string()]),
            (Some("search_document".to_string()), vec!["doc".to_string()]),
            (Some("search_query".to_string()), vec!["q".to_string()]),
        ]
    );
}

#[tokio::test]
async fn api_embedder_sends_dimensions() {
    let Some(server) = length_embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model")
        .with_base_url(server.uri())
        .with_dimensions(256);

    embedder.embed_batch(vec!["a".to_string()]).await.unwrap();

    let bodies = request_bodies(&server).await;
    assert_eq!(bodies[0]["dimensions"], json!(256));
}

#[tokio::test]
async fn api_embedder_vector_schema() {
    let Some(server) = length_embedding_server().await else {
        return;
    };
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    use cocoindex::VectorSchemaProvider;
    let schema = embedder.vector_schema().await.unwrap();
    assert_eq!(schema.size, 1);
    assert_eq!(schema.element_type, cocoindex::VectorElementType::Float32);
}

/// Mirror `test_timeout_bounds_a_slow_backend` / `test_timeout_is_not_retried`:
/// a request that outlives `timeout` fails once, without a retry.
#[tokio::test]
async fn api_embedder_timeout_is_terminal() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(5))
                .set_body_json(json!({"data": [{"embedding": [1.0]}]})),
        )
        .mount(&server)
        .await;
    let embedder = ApiEmbedder::new("fake-model")
        .with_base_url(server.uri())
        .with_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let err = embedder
        .embed_batch(vec!["hello".to_string()])
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(4), "{err}");
    assert!(err.to_string().contains("did not complete within"), "{err}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

/// Mirror `test_limit_holds_through_the_public_api`: with a cap of one, the
/// backend never sees two requests at once.
#[tokio::test]
async fn api_embedder_bounds_inflight_requests() {
    let Some(server) = mock_server().await else {
        return;
    };
    const DELAY: Duration = Duration::from_millis(150);
    struct Timestamped(Arc<Mutex<Vec<Instant>>>);
    impl Respond for Timestamped {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            self.0.lock().unwrap().push(Instant::now());
            ResponseTemplate::new(200)
                .set_delay(DELAY)
                .set_body_json(json!({"data": [{"embedding": [1.0]}]}))
        }
    }
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(Timestamped(arrivals.clone()))
        .mount(&server)
        .await;
    let embedder = ApiEmbedder::new("fake-model")
        .with_base_url(server.uri())
        .with_max_inflight_requests(1);

    let results = join_all((0..3).map(|_| embedder.embed_batch(vec!["x".to_string()]))).await;
    assert!(results.iter().all(Result::is_ok));

    let arrivals = arrivals.lock().unwrap();
    assert_eq!(arrivals.len(), 3);
    for pair in arrivals.windows(2) {
        assert!(
            pair[1].duration_since(pair[0]) >= DELAY,
            "two requests overlapped despite the cap of one"
        );
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

    let bytes = file.read().await.unwrap();
    let text = transcriber
        .transcribe_bytes(bytes, file.file_path().name().to_string())
        .await
        .unwrap();
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

/// `transcribe` is memoized by the file: a second update with the same file
/// sends nothing, a changed file is sent again.
#[tokio::test]
async fn api_transcriber_memoizes_by_file_content() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"text": "hello world"})))
        .mount(&server)
        .await;
    let transcriber = ApiTranscriber::new("fake-model").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let audio_dir = tempdir.path().join("audio");
    std::fs::create_dir(&audio_dir).unwrap();
    std::fs::write(audio_dir.join("segment.mp3"), b"fake-audio").unwrap();
    let app = app_in(&tempdir, "api_transcribe").await;

    let transcribe_all = || {
        let transcriber = transcriber.clone();
        let audio_dir = audio_dir.clone();
        app.update(move |ctx| async move {
            let mut texts = Vec::new();
            for (_, file) in walk_items(&audio_dir, &["**/*.mp3"])? {
                texts.push(transcriber.transcribe(&ctx, &file).await?);
            }
            Ok(texts)
        })
    };

    assert_eq!(transcribe_all().await.unwrap(), vec!["hello world"]);
    assert_eq!(transcribe_all().await.unwrap(), vec!["hello world"]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "an unchanged file is served from the memo store"
    );

    std::fs::write(audio_dir.join("segment.mp3"), b"other-audio").unwrap();
    assert_eq!(transcribe_all().await.unwrap(), vec!["hello world"]);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
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

async fn resolve_pair_in_update(
    name: &str,
    resolver: LlmPairResolver,
    entity: &'static str,
    candidates: &'static [&'static str],
) -> cocoindex::Result<cocoindex::PairDecision> {
    in_update(name, move |ctx| async move {
        let candidates: Vec<String> = candidates.iter().map(|c| (*c).to_string()).collect();
        resolver.resolve_pair(&ctx, entity, &candidates).await
    })
    .await
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
    let decision = resolve_pair_in_update(
        "llm_matched",
        resolver,
        "OpenAI, Inc.",
        &["OpenAI Inc", "Anthropic"],
    )
    .await
    .unwrap();
    assert_eq!(decision.matched.as_deref(), Some("OpenAI Inc"));
    assert_eq!(decision.canonical, CanonicalSide::New);

    let bodies = request_bodies(&server).await;
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["model"], json!("fake-model"));
    assert_eq!(bodies[0]["response_format"]["type"], json!("json_object"));
    let messages = bodies[0]["messages"].as_array().unwrap();
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
    let decision = resolve_pair_in_update("llm_null", resolver, "Foo", &["Bar"])
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
            chat_response(content)
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
    let decision = resolve_pair_in_update("llm_retry", resolver, "ACME", &["Acme Corp"])
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
    let decision = resolve_pair_in_update("llm_give_up", resolver, "X", &["Y"])
        .await
        .unwrap();
    assert!(decision.matched.is_none());
    // 1 initial attempt + 2 retries = 3 calls.
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

/// Decisions persist across runs (cf. Python's `@coco.fn(memo=True)` on
/// `__call__`): the same pair is asked once, a different candidate list again.
#[tokio::test]
async fn llm_pair_resolver_memoizes_decisions() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(r#"{"matched": "Acme Corp"}"#))
        .mount(&server)
        .await;
    let resolver = LlmPairResolver::new("fake-model").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let app = app_in(&tempdir, "llm_memo").await;

    let resolve = |candidates: &'static [&'static str]| {
        let resolver = resolver.clone();
        app.update(move |ctx| async move {
            let candidates: Vec<String> = candidates.iter().map(|c| (*c).to_string()).collect();
            resolver.resolve_pair(&ctx, "ACME", &candidates).await
        })
    };

    for _ in 0..2 {
        let decision = resolve(&["Acme Corp"]).await.unwrap();
        assert_eq!(decision.matched.as_deref(), Some("Acme Corp"));
    }
    assert_eq!(server.received_requests().await.unwrap().len(), 1);

    resolve(&["Acme Corp", "Acme Inc"]).await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// ApiChatClient::extract — a typed, memoized JSON-mode completion
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Person {
    name: String,
    age: u32,
}

#[tokio::test]
async fn chat_client_extract_deserializes_and_memoizes() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(r#"{"name": "Ada", "age": 36}"#))
        .mount(&server)
        .await;
    let client = ApiChatClient::new("fake-model").with_base_url(server.uri());
    let tempdir = tempfile::tempdir().unwrap();
    let app = app_in(&tempdir, "chat_extract").await;

    let extract = |text: &'static str| {
        let client = client.clone();
        app.update(move |ctx| async move {
            let messages = [
                ChatMessage::system("Reply with JSON {name, age}."),
                ChatMessage::user(text),
            ];
            client.extract::<Person>(&ctx, &messages).await
        })
    };

    for _ in 0..2 {
        let person = extract("Ada is 36.").await.unwrap();
        assert_eq!(
            person,
            Person {
                name: "Ada".into(),
                age: 36
            }
        );
    }
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "the same conversation is served from the memo store"
    );

    extract("Ada, 36 years old.").await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);

    let requests = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["response_format"]["type"], "json_object");
}

cocoindex::context_key!(static CHAT: ApiChatClient, detect_change);

static CHAT_BODY_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Reads the chat client from a `detect_change` key and calls nothing, so
/// only the key's memo identity decides whether the body runs again.
#[cocoindex::function(memo)]
async fn uses_chat_client(ctx: &Ctx, text: &str) -> cocoindex::Result<String> {
    let client = ctx.get_key(&CHAT)?;
    CHAT_BODY_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Ok(format!("{}:{text}", client.model()))
}

/// `ApiChatClient` is a `MemoInput`, so it can sit in a `detect_change` key:
/// changing the model reruns memoized work, an unchanged client does not.
#[tokio::test]
async fn chat_client_in_a_detect_change_key_invalidates_on_model_change() {
    use std::sync::atomic::Ordering;
    let tempdir = tempfile::tempdir().unwrap();
    let run = |model: &'static str| {
        let db = tempdir.path().join("db");
        async move {
            let app = cocoindex::Environment::builder()
                .db_path(db)
                .provide_key(&CHAT, ApiChatClient::new(model))
                .build()
                .await
                .unwrap()
                .app("chat_detect_change")
                .await
                .unwrap();
            app.update(|ctx| async move { uses_chat_client(&ctx, "hi").await })
                .await
                .unwrap()
        }
    };

    assert_eq!(run("model-a").await, "model-a:hi");
    assert_eq!(run("model-a").await, "model-a:hi");
    assert_eq!(
        CHAT_BODY_CALLS.load(Ordering::SeqCst),
        1,
        "unchanged client: memo hit"
    );
    assert_eq!(run("model-b").await, "model-b:hi");
    assert_eq!(
        CHAT_BODY_CALLS.load(Ordering::SeqCst),
        2,
        "a new model reruns the body"
    );
}

#[tokio::test]
async fn chat_client_extract_rejects_an_answer_of_another_shape() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(chat_response(r#"{"name": "Ada"}"#))
        .mount(&server)
        .await;
    let client = ApiChatClient::new("fake-model").with_base_url(server.uri());
    let err = in_update("chat_extract_bad_shape", move |ctx| async move {
        client
            .extract::<Person>(&ctx, &[ChatMessage::user("Ada")])
            .await
            .map(|person| person.age)
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("expected JSON"),
        "unexpected error: {err}"
    );
}

/// A rate-limited (429) response is retried, honoring `Retry-After`.
#[tokio::test]
async fn api_embedder_retries_rate_limited_requests() {
    let Some(server) = mock_server().await else {
        return;
    };
    struct RateLimitedOnce(AtomicUsize);
    impl Respond for RateLimitedOnce {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(429).insert_header("retry-after", "0")
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "data": [{"embedding": [1.0, 2.0]}]
                }))
            }
        }
    }
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(RateLimitedOnce(AtomicUsize::new(0)))
        .mount(&server)
        .await;
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    assert_eq!(
        embedder
            .embed_batch(vec!["hello".to_string()])
            .await
            .unwrap(),
        vec![vec![1.0, 2.0]]
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

/// A client error is not retried.
#[tokio::test]
async fn api_embedder_does_not_retry_client_errors() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad input"))
        .mount(&server)
        .await;
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    let err = embedder
        .embed_batch(vec!["hello".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("bad input"), "{err}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

/// Results are matched to inputs by `index`, not by response order.
#[tokio::test]
async fn api_embedder_orders_batch_results_by_index() {
    let Some(server) = mock_server().await else {
        return;
    };
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"index": 1, "embedding": [2.0]},
                {"index": 0, "embedding": [1.0]}
            ]
        })))
        .mount(&server)
        .await;
    let embedder = ApiEmbedder::new("fake-model").with_base_url(server.uri());

    let out = embedder
        .embed_batch(vec!["a".to_string(), "b".to_string()])
        .await
        .unwrap();
    assert_eq!(out, vec![vec![1.0], vec![2.0]]);
}
