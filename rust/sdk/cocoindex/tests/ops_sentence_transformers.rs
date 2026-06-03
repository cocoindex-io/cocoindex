//! Fast, network-free checks for the fastembed-backed embedder. Loading a real
//! model downloads weights, so the live embedding path is exercised by the
//! examples rather than here; this only asserts the model-registry resolution.
//!
//! Run with: `cargo test -p cocoindex --features fastembed --test ops_sentence_transformers`.
#![cfg(feature = "fastembed")]

use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;

#[tokio::test]
async fn unknown_model_is_rejected_without_download() {
    // An unrecognized model name fails fast at registry lookup, before any
    // network access or ONNX initialization.
    let result = SentenceTransformerEmbedder::load("not-a-real-model/v0").await;
    assert!(result.is_err());
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.contains("unknown sentence-transformer model"),
        "got: {msg}"
    );
}
