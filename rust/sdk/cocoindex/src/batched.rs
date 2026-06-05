//! `Batched` — call a batch implementation on **single values**, with per-item
//! memoization and automatic coalescing of concurrent cache-misses into one
//! batch call (via the core batcher, `cocoindex_utils::batching`).
//!
//! This is the single batching mechanism, composable with memoization: each
//! `call` memo-probes its own item; only misses execute, and concurrent misses
//! (even across different components) are combined into one invocation of the
//! batch implementation. You never assemble a list yourself.
//!
//! ```ignore
//! #[cocoindex::function]                                   // ctx-free batch impl; emits a logic hash
//! async fn embed_batch(texts: Vec<String>) -> coco::Result<Vec<Vec<f32>>> {
//!     model.encode(&texts)
//! }
//!
//! static EMBED: std::sync::LazyLock<coco::Batched<String, Vec<f32>>> =
//!     std::sync::LazyLock::new(|| coco::Batched::new(embed_batch, __COCO_FN_HASH_EMBED_BATCH));
//!
//! // Call per single item (e.g. inside ctx.map over chunks):
//! let emb = EMBED.call(&ctx, text).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ctx::Ctx;
use crate::error::{Error, Result};

type BatchFuture<Out> =
    Pin<Box<dyn Future<Output = cocoindex_utils::error::Result<Vec<Out>>> + Send>>;
type BatchFn<In, Out> = Box<dyn Fn(Vec<In>) -> BatchFuture<Out> + Send + Sync>;

/// Adapts a user closure `Vec<In> -> Result<Vec<Out>>` to the core batcher's `Runner`.
struct FnRunner<In, Out> {
    f: BatchFn<In, Out>,
}

#[async_trait]
impl<In: Send + 'static, Out: Send + 'static> Runner for FnRunner<In, Out> {
    type Input = In;
    type Output = Out;

    async fn run(
        &self,
        inputs: Vec<In>,
    ) -> cocoindex_utils::error::Result<impl ExactSizeIterator<Item = Out>> {
        let outputs = (self.f)(inputs).await?;
        Ok(outputs.into_iter())
    }
}

/// A batched, memoized function. See the [module docs](self).
pub struct Batched<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    batcher: Arc<Batcher<FnRunner<In, Out>>>,
    code_hash: u64,
}

impl<In, Out> Batched<In, Out>
where
    In: Serialize + Send + 'static,
    Out: Serialize + DeserializeOwned + Send + 'static,
{
    /// Build a `Batched` from a batch implementation `f: Vec<In> -> Result<Vec<Out>>`.
    ///
    /// `code_hash` is the batch function's logic fingerprint — the
    /// `__COCO_FN_HASH_*` constant emitted by `#[cocoindex::function]`. It is
    /// folded into each item's memo key, so editing the batch logic invalidates
    /// cached results.
    pub fn new<F, Fut>(f: F, code_hash: u64) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        Self::with_options(f, code_hash, BatchingOptions::default())
    }

    /// Like [`Batched::new`], but caps how many items are processed per batch.
    pub fn with_max_batch<F, Fut>(f: F, code_hash: u64, max_batch_size: usize) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        Self::with_options(
            f,
            code_hash,
            BatchingOptions {
                max_batch_size: Some(max_batch_size),
            },
        )
    }

    fn with_options<F, Fut>(f: F, code_hash: u64, options: BatchingOptions) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        let wrapped: BatchFn<In, Out> = Box::new(move |inputs| {
            let fut = f(inputs);
            Box::pin(async move {
                fut.await
                    .map_err(|e: Error| cocoindex_utils::error::Error::internal_msg(e.to_string()))
            })
        });
        let runner = FnRunner { f: wrapped };
        let queue = Arc::new(BatchQueue::new());
        let batcher = Arc::new(Batcher::new(runner, queue, options));
        Self { batcher, code_hash }
    }

    /// Process one item. On a memo hit the stored result is returned; on a miss
    /// the item is handed to the core batcher (coalesced with other concurrent
    /// misses) and the result is memoized.
    pub async fn call(&self, ctx: &Ctx, item: In) -> Result<Out> {
        let fp =
            crate::memo::key_fingerprint_result(&("cocoindex_batched", self.code_hash, &item))?;
        let batcher = self.batcher.clone();
        crate::memo::cached_by_fingerprint(ctx, fp, move |_ctx| async move {
            batcher.run(item).await.map_err(Error::from)
        })
        .await
    }
}
