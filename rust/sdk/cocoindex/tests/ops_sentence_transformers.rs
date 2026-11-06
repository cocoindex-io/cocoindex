//! Fast, network-free checks for the fastembed-backed embedder. Loading a real
//! model downloads weights, so the live embedding path is exercised by the
//! examples rather than here; memoization and batching are covered by the
//! module's unit tests over a counting model.
//!
//! Run with: `cargo test -p cocoindex --features fastembed --test ops_sentence_transformers`.
#![cfg(feature = "fastembed")]

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;

/// A name outside `fastembed`'s registry is looked up on the Hugging Face Hub
/// (like Python's `SentenceTransformer(model_name_or_path)`), under the
/// `HF_ENDPOINT` / `HF_HOME` the Hub client reads. Pointing both at a closed
/// port and an empty cache makes that lookup fail fast and offline.
#[tokio::test]
async fn unknown_model_is_looked_up_on_the_hub_and_rejected_offline() {
    let cache = tempfile::tempdir().unwrap();
    // This test binary has no other test, so no concurrent reader of these
    // variables exists.
    unsafe {
        std::env::set_var("HF_ENDPOINT", "http://127.0.0.1:1");
        std::env::set_var("HF_HOME", cache.path());
    }
    let result = SentenceTransformerEmbedder::load("not-a-real-org/not-a-real-model").await;
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.contains("not-a-real-org/not-a-real-model") && msg.contains("Hugging Face Hub"),
        "got: {msg}"
    );
}

/// A local directory that is not a sentence-transformers export is rejected
/// with the missing file named, before any network access.
#[tokio::test]
async fn directory_without_an_onnx_export_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let result = SentenceTransformerEmbedder::load(dir.path().to_str().unwrap()).await;
    let msg = result.err().unwrap().to_string();
    assert!(msg.contains("onnx/model.onnx"), "got: {msg}");
}
