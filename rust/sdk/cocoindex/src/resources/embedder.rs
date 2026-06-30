//! Shared embedder protocol (cf. Python `cocoindex.resources.embedder`).
//!
//! [`Embedder`] is the common contract for turning text into a dense `f32`
//! vector. The SDK's `SentenceTransformerEmbedder` (feature `fastembed`) and
//! `ApiEmbedder` (feature `embed_api`) implement it, and any `Embedder` is
//! usable as an [`EntityEmbedder`](crate::entity_resolution::EntityEmbedder) via
//! the blanket impl below — so `resolve_entities` accepts any embedder directly.

use async_trait::async_trait;

use crate::error::Result;

/// A text embedder. Mirrors Python's `Embedder` protocol (`async embed(text)`).
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Embed a single text into a dense vector.
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Embed a batch of texts. The default fans out to [`embed`](Self::embed);
    /// implementations with a native batch path should override it.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            out.push(self.embed(text).await?);
        }
        Ok(out)
    }
}

/// Any [`Embedder`] is an [`EntityEmbedder`](crate::entity_resolution::EntityEmbedder),
/// so `resolve_entities` can take a `SentenceTransformerEmbedder`/`ApiEmbedder`
/// directly (matches Python's `resolve_entities(embedder: Embedder)`).
#[async_trait]
impl<E: Embedder> crate::entity_resolution::EntityEmbedder for E {
    async fn embed_entity(&self, entity: &str) -> Result<Vec<f32>> {
        self.embed(entity).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An embedder that only implements `embed` (one char-code per char), to
    /// exercise the default `embed_batch` fan-out.
    struct CharCodeEmbedder;

    #[async_trait]
    impl Embedder for CharCodeEmbedder {
        async fn embed(&self, text: &str) -> Result<Vec<f32>> {
            Ok(text.chars().map(|c| c as u32 as f32).collect())
        }
    }

    #[tokio::test]
    async fn default_embed_batch_fans_out_in_order() {
        let e = CharCodeEmbedder;
        let out = e
            .embed_batch(&["ab".to_string(), "c".to_string()])
            .await
            .unwrap();
        assert_eq!(out, vec![vec![97.0, 98.0], vec![99.0]]);
        assert_eq!(e.embed("A").await.unwrap(), vec![65.0]);
    }

    #[tokio::test]
    async fn embedder_is_usable_as_entity_embedder() {
        use crate::entity_resolution::EntityEmbedder;
        let e = CharCodeEmbedder;
        // Goes through the blanket impl.
        assert_eq!(e.embed_entity("A").await.unwrap(), vec![65.0]);
    }
}
