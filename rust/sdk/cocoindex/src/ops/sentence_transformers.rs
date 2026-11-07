//! Local sentence-transformer embeddings via [`fastembed`].
//!
//! Rust-native equivalent of Python's
//! `cocoindex.ops.sentence_transformers.SentenceTransformerEmbedder`. Python
//! loads models through the `sentence-transformers` library; Rust uses
//! `fastembed`, which runs the same ONNX models locally without a Python
//! runtime. The embedder implements [`VectorSchemaProvider`] so a connector
//! column can be defined from the model's dimension, and [`Embedder`] so it
//! plugs into `resolve_entities` directly.
//!
//! As in Python (`runner=coco.GPU`), each batch runs under the GPU runner:
//! batches take turns on the GPU pool, one at a time per GPU, so a burst of
//! calls never starts more inferences than there are GPUs (one, when none is
//! detected).
//!
//! # What `fastembed` cannot do
//!
//! Python's embedder takes `prompt_name`, `normalize_embeddings` and `device`.
//! `fastembed` has no prompt registry (prepend the model's query/document
//! prefix to the text yourself) and always L2-normalizes its output, so those
//! two options have no Rust counterpart. The device is chosen through ONNX
//! Runtime execution providers instead: [`SentenceTransformerEmbedder::load_with`]
//! takes the providers to try, such as CUDA, which the binary must compile in
//! through the `ort` crate's features (`cuda`, `coreml`, …); without any, the
//! model runs on the CPU.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
pub use fastembed::ExecutionProviderDispatch;
use fastembed::{
    InitOptions, InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::resources::embedder::Embedder;
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};

/// Wrapper around a locally-loaded `fastembed` text embedding model.
///
/// Cheap to clone: the underlying model is shared behind an [`Arc`].
#[derive(Clone)]
pub struct SentenceTransformerEmbedder {
    model: Arc<dyn EmbeddingModel>,
    model_name: String,
    dimension: usize,
}

trait EmbeddingModel: Send + Sync {
    fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
}

impl EmbeddingModel for TextEmbedding {
    fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        TextEmbedding::embed(self, texts, None)
            .map_err(|e| Error::engine(format!("embedding failed: {e}")))
    }
}

#[derive(Clone, crate::MemoInput)]
struct ScheduledEmbedder(SentenceTransformerEmbedder);

impl serde::Serialize for ScheduledEmbedder {
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
    texts: Vec<String>,
    embedder: ScheduledEmbedder,
) -> Result<Vec<Vec<f32>>> {
    embedder.0.embed_batch(texts).await
}

/// The files a sentence-transformers export is loaded from, relative to the
/// model directory or Hugging Face repository.
const ONNX_MODEL_FILES: &[&str] = &["onnx/model.onnx", "model.onnx"];
const TOKENIZER_FILE: &str = "tokenizer.json";
const CONFIG_FILE: &str = "config.json";
const SPECIAL_TOKENS_MAP_FILE: &str = "special_tokens_map.json";
const TOKENIZER_CONFIG_FILE: &str = "tokenizer_config.json";
/// sentence-transformers' pooling module config. Optional: an export whose
/// ONNX graph already emits `sentence_embedding` needs no pooling.
const POOLING_CONFIG_FILE: &str = "1_Pooling/config.json";

/// Where a user-defined model's files come from.
trait ModelFiles {
    /// The file's bytes, or `None` when the file does not exist.
    fn read(&self, relative: &str) -> Result<Option<Vec<u8>>>;
}

struct LocalDir(PathBuf);

impl ModelFiles for LocalDir {
    fn read(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        let path = self.0.join(relative);
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(path)?))
    }
}

/// A Hugging Face Hub repository, read through `hf-hub` with the same
/// `HF_HOME` / `HF_ENDPOINT` handling and cache directory as `fastembed`'s
/// own downloads.
struct HfHubRepo {
    name: String,
    repo: hf_hub::api::sync::ApiRepo,
}

impl HfHubRepo {
    fn open(name: &str) -> Result<Self> {
        let cache_dir = std::env::var_os("HF_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(fastembed::get_cache_dir()));
        let endpoint =
            std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string());
        let api = hf_hub::api::sync::ApiBuilder::new()
            .with_cache_dir(cache_dir)
            .with_endpoint(endpoint)
            .with_progress(false)
            .build()
            .map_err(|e| Error::engine(format!("open the Hugging Face Hub client: {e}")))?;
        Ok(Self {
            name: name.to_string(),
            repo: api.model(name.to_string()),
        })
    }
}

impl ModelFiles for HfHubRepo {
    fn read(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        match self.repo.get(relative) {
            Ok(path) => Ok(Some(std::fs::read(path)?)),
            // `hf-hub` reports a missing file as the server's 404 (or 401 on a
            // gated repository) inside its request error, and names no finer
            // variant for it. A required file that is absent fails on its own
            // message in `load_user_defined`; a failed connection is an error.
            Err(hf_hub::api::sync::ApiError::RequestError(err))
                if matches!(*err, ureq::Error::Status(401 | 404, _)) =>
            {
                Ok(None)
            }
            Err(err) => Err(Error::engine(format!(
                "download `{relative}` of `{}` from the Hugging Face Hub: {err}",
                self.name
            ))),
        }
    }
}

impl SentenceTransformerEmbedder {
    /// Load a model by name or local path, like Python's
    /// `SentenceTransformerEmbedder(model_name_or_path)`.
    ///
    /// The name is first matched against `fastembed`'s supported-model
    /// registry, by exact (case-insensitive) model code and then by the
    /// trailing model name after the org prefix, so the common cross-org
    /// aliases (e.g. `sentence-transformers/...` vs `Xenova/...`) resolve to
    /// the same model. A name that is not in the registry is loaded as a
    /// sentence-transformers ONNX export: from the directory at that path when
    /// one exists, otherwise from the Hugging Face Hub repository of that name
    /// (`HF_HOME` / `HF_ENDPOINT` apply, as for every `fastembed` model). The
    /// export needs `onnx/model.onnx` (or `model.onnx`) plus the tokenizer
    /// files; pooling follows `1_Pooling/config.json` when present.
    ///
    /// Loading downloads and initializes the ONNX model, so it runs on a
    /// blocking thread.
    pub async fn load(model_name_or_path: impl Into<String>) -> Result<Self> {
        Self::load_with(model_name_or_path, Vec::new()).await
    }

    /// Like [`load`](Self::load), running the model on the first of
    /// `execution_providers` that ONNX Runtime can start (Python's `device`).
    /// A provider is registered when its `ort` feature is compiled in, e.g.
    /// `ort::execution_providers::CUDAExecutionProvider::default().build()`
    /// with the `cuda` feature; otherwise ONNX Runtime falls back to the CPU.
    pub async fn load_with(
        model_name_or_path: impl Into<String>,
        execution_providers: Vec<ExecutionProviderDispatch>,
    ) -> Result<Self> {
        let model_name = model_name_or_path.into();
        tokio::task::spawn_blocking(move || Self::load_blocking(&model_name, execution_providers))
            .await
            .map_err(|e| Error::engine(format!("embedder load task panicked: {e}")))?
    }

    fn load_blocking(
        model_name: &str,
        execution_providers: Vec<ExecutionProviderDispatch>,
    ) -> Result<Self> {
        let suffix = |code: &str| code.rsplit('/').next().unwrap_or(code).to_string();
        let wanted_suffix = suffix(model_name);
        let registered = TextEmbedding::list_supported_models()
            .into_iter()
            .find(|m| m.model_code.eq_ignore_ascii_case(model_name))
            .or_else(|| {
                TextEmbedding::list_supported_models()
                    .into_iter()
                    .find(|m| suffix(&m.model_code).eq_ignore_ascii_case(&wanted_suffix))
            });

        let (model, dimension) = match registered {
            Some(info) => {
                let options =
                    InitOptions::new(info.model).with_execution_providers(execution_providers);
                let model = TextEmbedding::try_new(options).map_err(|e| {
                    Error::engine(format!("load embedding model `{model_name}`: {e}"))
                })?;
                (model, info.dim)
            }
            None => {
                let path = Path::new(model_name);
                let model = if path.is_dir() {
                    load_user_defined(
                        &LocalDir(path.to_path_buf()),
                        model_name,
                        execution_providers,
                    )?
                } else {
                    load_user_defined(
                        &HfHubRepo::open(model_name)?,
                        model_name,
                        execution_providers,
                    )?
                };
                // The export does not state its output dimension in one agreed
                // place, so measure it.
                let dimension = EmbeddingModel::embed(&model, vec!["hello".to_string()])?
                    .first()
                    .map_or(0, Vec::len);
                (model, dimension)
            }
        };
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
    pub async fn embed(&self, ctx: &Ctx, text: impl Into<String>) -> Result<Vec<f32>> {
        embed_scheduled(ctx, text.into(), ScheduledEmbedder(self.clone())).await
    }

    /// Embed a batch of texts directly, without memoization or batching: the
    /// path for code outside a CocoIndex update, such as embedding a search
    /// query. Embedding runs on a blocking thread.
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

fn load_user_defined(
    files: &dyn ModelFiles,
    model_name: &str,
    execution_providers: Vec<ExecutionProviderDispatch>,
) -> Result<TextEmbedding> {
    let required = |relative: &str| -> Result<Vec<u8>> {
        files.read(relative)?.ok_or_else(|| {
            Error::engine(format!(
                "sentence-transformer model `{model_name}` has no `{relative}`; \
                 expected a sentence-transformers ONNX export"
            ))
        })
    };
    let mut onnx_file = None;
    for relative in ONNX_MODEL_FILES {
        if let Some(bytes) = files.read(relative)? {
            onnx_file = Some(bytes);
            break;
        }
    }
    let onnx_file = onnx_file.ok_or_else(|| {
        Error::engine(format!(
            "sentence-transformer model `{model_name}` has none of {ONNX_MODEL_FILES:?}; \
             `fastembed` loads ONNX exports only"
        ))
    })?;
    let tokenizer_files = TokenizerFiles {
        tokenizer_file: required(TOKENIZER_FILE)?,
        config_file: required(CONFIG_FILE)?,
        special_tokens_map_file: required(SPECIAL_TOKENS_MAP_FILE)?,
        tokenizer_config_file: required(TOKENIZER_CONFIG_FILE)?,
    };
    let mut model = UserDefinedEmbeddingModel::new(onnx_file, tokenizer_files);
    if let Some(pooling_config) = files.read(POOLING_CONFIG_FILE)? {
        model = model.with_pooling(pooling_from_config(&pooling_config, model_name)?);
    }
    let options = InitOptionsUserDefined::default().with_execution_providers(execution_providers);
    TextEmbedding::try_new_from_user_defined(model, options)
        .map_err(|e| Error::engine(format!("load embedding model `{model_name}`: {e}")))
}

/// Read the pooling mode out of sentence-transformers' `1_Pooling/config.json`.
fn pooling_from_config(config: &[u8], model_name: &str) -> Result<Pooling> {
    let config: serde_json::Value = serde_json::from_slice(config).map_err(|e| {
        Error::engine(format!(
            "parse `{POOLING_CONFIG_FILE}` of sentence-transformer model `{model_name}`: {e}"
        ))
    })?;
    let enabled = |key: &str| config.get(key).and_then(|v| v.as_bool()) == Some(true);
    if enabled("pooling_mode_cls_token") {
        Ok(Pooling::Cls)
    } else if enabled("pooling_mode_mean_tokens") {
        Ok(Pooling::Mean)
    } else {
        Err(Error::engine(format!(
            "sentence-transformer model `{model_name}` uses a pooling mode other than \
             CLS or mean, which `fastembed` does not support"
        )))
    }
}

#[async_trait]
impl crate::memo::MemoInput for SentenceTransformerEmbedder {
    fn write_memo_key(&self, writer: &mut crate::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&(self.model_name(), self.dimension()))
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
impl Embedder for SentenceTransformerEmbedder {
    /// Memoized, batched and run on the GPU pool, like the inherent
    /// [`embed`](SentenceTransformerEmbedder::embed).
    async fn embed(&self, ctx: &Ctx, text: &str) -> Result<Vec<f32>> {
        SentenceTransformerEmbedder::embed(self, ctx, text).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::join_all;

    use super::*;
    use crate::entity_resolution::{PairDecision, PairResolver, ResolveOptions, resolve_entities};

    #[derive(Default)]
    struct CountingModel {
        batches: Mutex<Vec<Vec<String>>>,
    }

    impl EmbeddingModel for CountingModel {
        fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
            self.batches.lock().unwrap().push(texts.clone());
            Ok(texts
                .into_iter()
                .map(|text| vec![text.len() as f32])
                .collect())
        }
    }

    fn counting_embedder() -> (Arc<CountingModel>, SentenceTransformerEmbedder) {
        let model = Arc::new(CountingModel::default());
        let embedder = SentenceTransformerEmbedder {
            model: model.clone(),
            model_name: "counting-model".to_string(),
            dimension: 1,
        };
        (model, embedder)
    }

    async fn test_app(name: &str) -> (tempfile::TempDir, crate::App) {
        let tempdir = tempfile::tempdir().unwrap();
        let app = crate::Environment::builder()
            .db_path(tempdir.path().join("db"))
            .build()
            .await
            .unwrap()
            .app(name)
            .await
            .unwrap();
        (tempdir, app)
    }

    #[tokio::test]
    async fn single_text_embed_batches_misses_and_memoizes_results() {
        let (model, embedder) = counting_embedder();
        let (_tempdir, app) = test_app("sentence_transformer_batching").await;
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

    /// Counts the model's batches running at once, as the ONNX session would
    /// see them.
    #[derive(Default)]
    struct SlowModel {
        now: AtomicUsize,
        peak: AtomicUsize,
        batches: AtomicUsize,
    }

    impl EmbeddingModel for SlowModel {
        fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
            let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            self.batches.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            self.now.fetch_sub(1, Ordering::SeqCst);
            Ok(texts
                .into_iter()
                .map(|text| vec![text.len() as f32])
                .collect())
        }
    }

    /// Python runs `_embed` under `runner=coco.GPU`: with one GPU in the pool
    /// (the default when none is detected), batches take turns, so a burst of
    /// calls never runs two inferences at once.
    #[tokio::test]
    async fn batches_run_one_at_a_time_per_gpu() {
        crate::configure_gpu_pool(NonZeroUsize::new(1).unwrap());
        let model = Arc::new(SlowModel::default());
        let embedder = SentenceTransformerEmbedder {
            model: model.clone(),
            model_name: "slow-model".to_string(),
            dimension: 1,
        };
        let (_tempdir, app) = test_app("sentence_transformer_gpu_runner").await;
        let texts: Vec<String> = (0..200).map(|index| format!("text-{index}")).collect();

        app.update(move |ctx| async move {
            join_all(texts.into_iter().map(|text| embedder.embed(&ctx, text)))
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            Ok(())
        })
        .await
        .unwrap();

        assert!(
            model.batches.load(Ordering::SeqCst) >= 4,
            "200 texts in batches of at most 64 take at least four batches"
        );
        assert_eq!(
            model.peak.load(Ordering::SeqCst),
            1,
            "batches run one at a time on one GPU"
        );
    }

    struct NeverMatches;

    #[async_trait]
    impl PairResolver for NeverMatches {
        async fn resolve_pair(
            &self,
            _ctx: &Ctx,
            _entity: &str,
            _candidates: &[String],
        ) -> Result<PairDecision> {
            Ok(PairDecision::no_match())
        }
    }

    /// `resolve_entities` embeds through the `Embedder` impl, so entity names
    /// are batched on the first update and memoized on the second.
    #[tokio::test]
    async fn entity_resolution_embeds_through_the_batched_memoized_path() {
        let (model, embedder) = counting_embedder();
        let (_tempdir, app) = test_app("sentence_transformer_entity_resolution").await;
        let names: Vec<String> = (0..10).map(|index| format!("entity-{index}")).collect();

        for _ in 0..2 {
            let embedder = embedder.clone();
            let names = names.clone();
            app.update(move |ctx| async move {
                let resolved = resolve_entities(
                    &ctx,
                    names.iter(),
                    &embedder,
                    &NeverMatches,
                    None,
                    ResolveOptions::default(),
                )
                .await?;
                assert_eq!(resolved.canonicals().len(), 10);
                Ok(())
            })
            .await
            .unwrap();
        }

        let batches = model.batches.lock().unwrap();
        assert_eq!(
            batches.iter().map(Vec::len).sum::<usize>(),
            10,
            "the second update should be served entirely from memo entries"
        );
        assert!(
            batches.len() < 10,
            "concurrent entity embeddings should be coalesced into batches"
        );
    }
}
