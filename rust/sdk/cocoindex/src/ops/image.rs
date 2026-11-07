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
//! the model's dimension. Each batch runs under the GPU runner, one at a time
//! per GPU, like the text embedder; [`ImageEmbedder::load_with`] picks the
//! ONNX Runtime execution providers.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use fastembed::{ExecutionProviderDispatch, ImageEmbedding, ImageInitOptions};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

/// Wrapper around a locally-loaded `fastembed` image embedding model.
///
/// Cheap to clone: the underlying model is shared behind an [`Arc`].
#[derive(Clone)]
pub struct ImageEmbedder {
    model: Arc<dyn ImageModel>,
    model_name: String,
    dimension: usize,
}

trait ImageModel: Send + Sync {
    fn embed(&self, images: &[&[u8]]) -> Result<Vec<Vec<f32>>>;
}

impl ImageModel for ImageEmbedding {
    fn embed(&self, images: &[&[u8]]) -> Result<Vec<Vec<f32>>> {
        self.embed_bytes(images, None)
            .map_err(|e| Error::engine(format!("image embedding failed: {e}")))
    }
}

#[derive(Clone, crate::MemoInput)]
struct ScheduledImageEmbedder(ImageEmbedder);

impl serde::Serialize for ScheduledImageEmbedder {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&(self.0.model_name(), self.0.dimension()), serializer)
    }
}

#[crate::function(memo, batching, max_batch_size = 64, runner = crate::GPU)]
async fn embed_scheduled(
    _ctx: &Ctx,
    images: Vec<Bytes>,
    embedder: ScheduledImageEmbedder,
) -> Result<Vec<Vec<f32>>> {
    embedder.0.embed_batch(images).await
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
        Self::load_with(model_name, Vec::new()).await
    }

    /// Like [`load`](Self::load), running the model on the first of
    /// `execution_providers` that ONNX Runtime can start; see
    /// [`SentenceTransformerEmbedder::load_with`](crate::ops::sentence_transformers::SentenceTransformerEmbedder::load_with).
    pub async fn load_with(
        model_name: impl Into<String>,
        execution_providers: Vec<ExecutionProviderDispatch>,
    ) -> Result<Self> {
        let model_name = model_name.into();
        tokio::task::spawn_blocking(move || Self::load_blocking(&model_name, execution_providers))
            .await
            .map_err(|e| Error::engine(format!("image embedder load task panicked: {e}")))?
    }

    fn load_blocking(
        model_name: &str,
        execution_providers: Vec<ExecutionProviderDispatch>,
    ) -> Result<Self> {
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
        let options =
            ImageInitOptions::new(info.model).with_execution_providers(execution_providers);
        let model = ImageEmbedding::try_new(options)
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

    /// Embed one image (raw encoded bytes — PNG/JPEG/…) with per-image
    /// memoization and automatic batching.
    ///
    /// The memo key is the image content, so an unchanged image is served from
    /// the engine memo store; concurrent cache misses in the same update are
    /// coalesced into batches of up to 64 images. Use
    /// [`ImageEmbedder::embed_batch`] for an explicit raw batch outside a
    /// CocoIndex update.
    pub async fn embed(&self, ctx: &Ctx, image: impl Into<Bytes>) -> Result<Vec<f32>> {
        embed_scheduled(ctx, image.into(), ScheduledImageEmbedder(self.clone())).await
    }

    /// Embed a batch of images (each as raw encoded bytes) directly, without
    /// memoization or batching. Embedding runs on a blocking thread.
    pub async fn embed_batch<B>(&self, images: Vec<B>) -> Result<Vec<Vec<f32>>>
    where
        B: AsRef<[u8]> + Send + 'static,
    {
        if images.is_empty() {
            return Ok(Vec::new());
        }
        let model = self.model.clone();
        tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = images.iter().map(AsRef::as_ref).collect();
            model.embed(&refs)
        })
        .await
        .map_err(|e| Error::engine(format!("image embedding task panicked: {e}")))?
    }
}

#[async_trait]
impl crate::memo::MemoInput for ImageEmbedder {
    fn write_memo_key(&self, writer: &mut crate::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&(self.model_name(), self.dimension()))
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::future::join_all;

    use super::*;

    #[derive(Default)]
    struct CountingModel {
        batches: Mutex<Vec<usize>>,
    }

    impl ImageModel for CountingModel {
        fn embed(&self, images: &[&[u8]]) -> Result<Vec<Vec<f32>>> {
            self.batches.lock().unwrap().push(images.len());
            Ok(images
                .iter()
                .map(|image| vec![image.len() as f32])
                .collect())
        }
    }

    #[tokio::test]
    async fn single_image_embed_batches_misses_and_memoizes_results() {
        let model = Arc::new(CountingModel::default());
        let embedder = ImageEmbedder {
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
            .app("image_embedding_batching")
            .await
            .unwrap();
        let images: Vec<Vec<u8>> = (0..70).map(|index| vec![0u8; index + 1]).collect();

        for _ in 0..2 {
            let embedder = embedder.clone();
            let images = images.clone();
            app.update(move |ctx| async move {
                let expected: Vec<Vec<f32>> = images
                    .iter()
                    .map(|image| vec![image.len() as f32])
                    .collect();
                let results = join_all(images.into_iter().map(|image| embedder.embed(&ctx, image)))
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
            batches.iter().sum::<usize>(),
            70,
            "the second update should be served entirely from memo entries"
        );
        assert!(batches.iter().any(|batch| *batch > 1));
        assert!(batches.iter().all(|batch| *batch <= 64));
    }
}
