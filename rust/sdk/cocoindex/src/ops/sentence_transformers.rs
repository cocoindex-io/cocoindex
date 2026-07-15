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
    model: Arc<dyn _EmbeddingModel>,
    model_name: String,
    dimension: usize,
}

trait _EmbeddingModel: Send + Sync {
    fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
}

impl _EmbeddingModel for TextEmbedding {
    fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        TextEmbedding::embed(self, texts, None)
            .map_err(|e| Error::engine(format!("embedding failed: {e}")))
    }
}

#[derive(Clone)]
struct _ScheduledEmbedder(SentenceTransformerEmbedder);

impl serde::Serialize for _ScheduledEmbedder {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&(self.0.model_name(), self.0.dimension()), serializer)
    }
}

#[crate::function(memo, batching, max_batch_size = 64)]
async fn _embed_scheduled(
    _ctx: &crate::Ctx,
    texts: Vec<String>,
    embedder: _ScheduledEmbedder,
) -> Result<Vec<Vec<f32>>> {
    embedder.0.embed_batch(texts).await
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

    /// Embed one text with per-text memoization and automatic batching.
    ///
    /// Concurrent cache misses in the same update are coalesced into batches of
    /// up to 64 texts. Repeated texts are served from the engine memo store.
    /// Use [`SentenceTransformerEmbedder::embed_batch`] for an explicit raw
    /// batch outside a CocoIndex update.
    pub async fn embed(&self, ctx: &crate::Ctx, text: impl Into<String>) -> Result<Vec<f32>> {
        _embed_scheduled(ctx, text.into(), _ScheduledEmbedder(self.clone())).await
    }

    /// Embed a batch of texts. Embedding runs on a blocking thread.
    pub async fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || model.embed(texts))
            .await
            .map_err(|e| Error::engine(format!("embedding task panicked: {e}")))?
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
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let mut embeddings = self.embed_batch(vec![text.to_string()]).await?;
        Ok(embeddings.pop().unwrap_or_default())
    }
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.embed_batch(texts.to_vec()).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::future::join_all;

    use super::*;

    #[derive(Default)]
    struct _CountingModel {
        batches: Mutex<Vec<Vec<String>>>,
    }

    impl _EmbeddingModel for _CountingModel {
        fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
            self.batches.lock().unwrap().push(texts.clone());
            Ok(texts
                .into_iter()
                .map(|text| vec![text.len() as f32])
                .collect())
        }
    }

    #[tokio::test]
    async fn single_text_embed_batches_misses_and_memoizes_results() {
        let model = Arc::new(_CountingModel::default());
        let embedder = SentenceTransformerEmbedder {
            model: model.clone(),
            model_name: "counting-model".to_string(),
            dimension: 1,
        };
        let tempdir = tempfile::tempdir().unwrap();
        let app = crate::Environment::builder()
            .db_path(tempdir.path().join("db"))
            .build()
            .await
            .unwrap()
            .app("sentence_transformer_batching")
            .await
            .unwrap();
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
                    .collect::<Result<Vec<_>>>()?;
                assert_eq!(results, expected);
                Ok(())
            })
            .await
            .unwrap();
        }

        let batches = model.batches.lock().unwrap();
        assert_eq!(
            batches.iter().map(Vec::len).sum::<usize>(),
            70,
            "the second update should be served entirely from memo entries"
        );
        assert!(
            batches.iter().any(|batch| batch.len() > 1),
            "concurrent misses should be coalesced into a batch"
        );
        assert!(batches.iter().all(|batch| batch.len() <= 64));
    }
}
