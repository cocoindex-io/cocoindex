//! Shared embedder protocol (cf. Python `cocoindex.resources.embedder`).
//!
//! [`Embedder`] is the common contract for turning one text into a dense `f32`
//! vector inside a CocoIndex run. The SDK's `SentenceTransformerEmbedder`
//! (feature `fastembed`) and `ApiEmbedder` (feature `embed_api`) implement it,
//! and any `Embedder` is usable as an
//! [`EntityEmbedder`](crate::entity_resolution::EntityEmbedder) via the blanket
//! impl below — so `resolve_entities` accepts any embedder directly.
//!
//! The Python protocol is `async embed(text)`: the component context is
//! ambient there. In Rust the [`Ctx`] is explicit, so the built-in embedders
//! can memoize each text and coalesce concurrent misses into batches through
//! the engine, exactly as their Python counterparts do under
//! `@coco.fn(memo=True)` over a `batching=True` body.

use async_trait::async_trait;

use crate::ctx::Ctx;
use crate::error::Result;

/// A text embedder. Mirrors Python's `Embedder` protocol (`async embed(text)`).
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embed a single text into a dense vector.
    ///
    /// Implementations are expected to memoize per text and batch concurrent
    /// calls through `ctx` (see `#[cocoindex::function(memo, batching)]`).
    async fn embed(&self, ctx: &Ctx, text: &str) -> Result<Vec<f32>>;
}

/// Any [`Embedder`] is an [`EntityEmbedder`](crate::entity_resolution::EntityEmbedder),
/// so `resolve_entities` can take a `SentenceTransformerEmbedder`/`ApiEmbedder`
/// directly (matches Python's `resolve_entities(embedder: Embedder)`).
#[async_trait]
impl<E: Embedder> crate::entity_resolution::EntityEmbedder for E {
    async fn embed_entity(&self, ctx: &Ctx, entity: &str) -> Result<Vec<f32>> {
        self.embed(ctx, entity).await
    }
}
