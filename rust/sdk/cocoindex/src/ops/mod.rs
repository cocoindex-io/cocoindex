//! First-class SDK operations: text splitting, embedding, and transcription.
//!
//! Mirrors Python's `cocoindex.ops` package. Each submodule is gated behind a
//! feature so the heavy dependencies (tree-sitter grammars, ONNX runtime,
//! HTTP) are only pulled in when used:
//!
//! - [`text`] (`text` feature): language detection and recursive/separator
//!   chunking.
//! - [`code`] (`code_match` feature): structural code matching over reusable
//!   parsed ASTs (`CodeAst`, `CodePattern`, `match_code`, `index_terms`).
//! - [`sentence_transformers`] (`fastembed` feature): local sentence-transformer
//!   embeddings.
//! - [`image`] (`fastembed` feature): local image (CLIP) embeddings.
//! - [`api`] (`embed_api` feature): remote embeddings/transcription over an
//!   OpenAI-compatible HTTP API.

#[cfg(feature = "embed_api")]
pub mod api;
#[cfg(feature = "code_match")]
pub mod code;
#[cfg(feature = "fastembed")]
pub mod image;
#[cfg(feature = "fastembed")]
pub mod sentence_transformers;
#[cfg(feature = "text")]
pub mod text;
