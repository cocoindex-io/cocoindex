//! Local sentence-transformer embeddings via [`fastembed`].
//!
//! Rust-native equivalent of Python's
//! `cocoindex.ops.sentence_transformers.SentenceTransformerEmbedder`. Python
//! loads models through the `sentence-transformers` library; Rust uses
//! `fastembed`, which runs the same ONNX models locally without a Python
//! runtime. The embedder implements [`VectorSchemaProvider`] so a connector
//! column can be defined from the model's dimension.

use std::sync::Arc;

use async_trait::async_trait;
use fastembed::{InitOptions, TextEmbedding};

use crate::error::{Error, Result};
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

/// Wrapper around a locally-loaded `fastembed` text embedding model.
///
/// Cheap to clone: the underlying model is shared behind an [`Arc`].
#[derive(Clone)]
pub struct SentenceTransformerEmbedder {
    model: Arc<TextEmbedding>,
    model_name: String,
    dimension: usize,
}

impl SentenceTransformerEmbedder {
    /// Load a model by name (e.g. `"sentence-transformers/all-MiniLM-L6-v2"`).
    ///
    /// The name is matched against `fastembed`'s supported-model registry,
    /// first by exact (case-insensitive) model code and then by the trailing
    /// model name after the org prefix, so the common cross-org aliases (e.g.
    /// `sentence-transformers/...` vs `Xenova/...`) resolve to the same model.
    ///
    /// Loading downloads and initializes the ONNX model, so it runs on a
    /// blocking thread.
    pub async fn load(model_name: impl Into<String>) -> Result<Self> {
        let model_name = model_name.into();
        tokio::task::spawn_blocking(move || Self::load_blocking(&model_name))
            .await
            .map_err(|e| Error::engine(format!("embedder load task panicked: {e}")))?
    }

    fn load_blocking(model_name: &str) -> Result<Self> {
        let suffix = |code: &str| code.rsplit('/').next().unwrap_or(code).to_string();
        let wanted_suffix = suffix(model_name);
        let info = TextEmbedding::list_supported_models()
            .into_iter()
            .find(|m| m.model_code.eq_ignore_ascii_case(model_name))
            .or_else(|| {
                TextEmbedding::list_supported_models()
                    .into_iter()
                    .find(|m| suffix(&m.model_code).eq_ignore_ascii_case(&wanted_suffix))
            })
            .ok_or_else(|| {
                Error::engine(format!(
                    "unknown sentence-transformer model: `{model_name}`"
                ))
            })?;

        let dimension = info.dim;
        let model = TextEmbedding::try_new(InitOptions::new(info.model))
            .map_err(|e| Error::engine(format!("load embedding model `{model_name}`: {e}")))?;
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

    /// Embed a single text into an `f32` vector.
    pub async fn embed(&self, text: impl Into<String>) -> Result<Vec<f32>> {
        let mut out = self.embed_batch(vec![text.into()]).await?;
        Ok(out.pop().unwrap_or_default())
    }

    /// Embed a batch of texts. Embedding runs on a blocking thread.
    pub async fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || model.embed(texts, None))
            .await
            .map_err(|e| Error::engine(format!("embedding task panicked: {e}")))?
            .map_err(|e| Error::engine(format!("embedding failed: {e}")))
    }
}

#[async_trait]
impl VectorSchemaProvider for SentenceTransformerEmbedder {
    async fn vector_schema(&self) -> Result<VectorSchema> {
        Ok(VectorSchema {
            element_type: VectorElementType::Float32,
            size: self.dimension,
        })
    }
}

#[async_trait]
impl crate::resources::embedder::Embedder for SentenceTransformerEmbedder {
    // Delegate to the inherent methods (method-call resolution prefers inherent,
    // so these don't recurse).
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        self.embed(text).await
    }
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.embed_batch(texts.to_vec()).await
    }
}
