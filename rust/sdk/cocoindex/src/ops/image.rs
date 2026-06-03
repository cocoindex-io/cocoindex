//! Local image embeddings via [`fastembed`].
//!
//! Rust-native image-embedding op with no Python runtime. The default model,
//! `Qdrant/clip-ViT-B-32-vision`, is the CLIP ViT-B/32 image tower; its 512-dim
//! output shares an embedding space with the matching CLIP text tower
//! (`Qdrant/clip-ViT-B-32-text`, loadable via
//! [`crate::ops::sentence_transformers::SentenceTransformerEmbedder`]), so a
//! text query can be searched against indexed image vectors — the basis of the
//! `image_search` example.
//!
//! Implements [`VectorSchemaProvider`] so a vector column can be defined from
//! the model's dimension.

use std::sync::Arc;

use async_trait::async_trait;
use fastembed::{ImageEmbedding, ImageInitOptions};

use crate::error::{Error, Result};
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

/// Wrapper around a locally-loaded `fastembed` image embedding model.
///
/// Cheap to clone: the underlying model is shared behind an [`Arc`].
#[derive(Clone)]
pub struct ImageEmbedder {
    model: Arc<ImageEmbedding>,
    model_name: String,
    dimension: usize,
}

impl ImageEmbedder {
    /// Load an image model by name (e.g. `"Qdrant/clip-ViT-B-32-vision"`).
    ///
    /// Matched against `fastembed`'s image-model registry first by exact
    /// (case-insensitive) model code, then by the trailing name after the org
    /// prefix, mirroring
    /// [`SentenceTransformerEmbedder::load`](crate::ops::sentence_transformers::SentenceTransformerEmbedder::load).
    /// Loading downloads and initializes the ONNX model, so it runs on a
    /// blocking thread.
    pub async fn load(model_name: impl Into<String>) -> Result<Self> {
        let model_name = model_name.into();
        tokio::task::spawn_blocking(move || Self::load_blocking(&model_name))
            .await
            .map_err(|e| Error::engine(format!("image embedder load task panicked: {e}")))?
    }

    fn load_blocking(model_name: &str) -> Result<Self> {
        let suffix = |code: &str| code.rsplit('/').next().unwrap_or(code).to_string();
        let wanted_suffix = suffix(model_name);
        let info = ImageEmbedding::list_supported_models()
            .into_iter()
            .find(|m| m.model_code.eq_ignore_ascii_case(model_name))
            .or_else(|| {
                ImageEmbedding::list_supported_models()
                    .into_iter()
                    .find(|m| suffix(&m.model_code).eq_ignore_ascii_case(&wanted_suffix))
            })
            .ok_or_else(|| {
                Error::engine(format!("unknown image embedding model: `{model_name}`"))
            })?;

        let dimension = info.dim;
        let model = ImageEmbedding::try_new(ImageInitOptions::new(info.model))
            .map_err(|e| Error::engine(format!("load image model `{model_name}`: {e}")))?;
        Ok(Self {
            model: Arc::new(model),
            model_name: model_name.to_string(),
            dimension,
        })
    }

    /// The model name this embedder was loaded with.
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    /// The embedding dimension of this model.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Embed a single image (raw encoded bytes — PNG/JPEG/…) into an `f32`
    /// vector.
    pub async fn embed(&self, image_bytes: Vec<u8>) -> Result<Vec<f32>> {
        let mut out = self.embed_batch(vec![image_bytes]).await?;
        Ok(out.pop().unwrap_or_default())
    }

    /// Embed a batch of images (each as raw encoded bytes). Embedding runs on a
    /// blocking thread.
    pub async fn embed_batch(&self, images: Vec<Vec<u8>>) -> Result<Vec<Vec<f32>>> {
        if images.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = images.iter().map(Vec::as_slice).collect();
            model.embed_bytes(&refs, None)
        })
        .await
        .map_err(|e| Error::engine(format!("image embedding task panicked: {e}")))?
        .map_err(|e| Error::engine(format!("image embedding failed: {e}")))
    }
}

#[async_trait]
impl VectorSchemaProvider for ImageEmbedder {
    async fn vector_schema(&self) -> Result<VectorSchema> {
        Ok(VectorSchema {
            element_type: VectorElementType::Float32,
            size: self.dimension,
        })
    }
}
