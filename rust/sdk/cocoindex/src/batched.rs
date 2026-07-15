//! `Batched` — call a batch implementation on **single values**, with per-item
//! memoization and automatic coalescing of concurrent cache-misses into one
//! batch call (via the core batcher, `cocoindex_utils::batching`).
//!
//! The `#[cocoindex::function(batching)]` wrapper composes batching with
//! per-item memoization: each call memo-probes its own item; only misses
//! execute, and concurrent misses (even across different components) are
//! combined into one invocation of the batch implementation.
//!
//! ```ignore
//! #[cocoindex::function(memo, batching, max_batch_size = 64)]
//! async fn embed_batch(
//!     _ctx: &cocoindex::Ctx,
//!     texts: Vec<String>,
//! ) -> cocoindex::Result<Vec<Vec<f32>>> {
//!     model.encode(&texts)
//! }
//!
//! // Call per single item (e.g. inside ctx.map over chunks):
//! let emb = embed_batch(&ctx, text).await?;
//! ```
//!
//! A physical-batch error currently fails every item in that batch; the Rust
//! SDK does not automatically retry smaller sub-batches. A batching function
//! must not call itself recursively from its body because it would wait on the
//! batcher that is already executing that body. [`Batched`] remains available
//! as the lower-level explicit adapter when a generated function wrapper is not
//! suitable.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use cocoindex_core::engine::context::FnCallContext;
use cocoindex_core::engine::deadline::DeadlineContext;
use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ctx::Ctx;
use crate::error::{Error, Result};

type BatchFuture<Out> =
    Pin<Box<dyn Future<Output = cocoindex_utils::error::Result<Vec<Out>>> + Send>>;
type BatchFn<In, Out> = Arc<dyn Fn(Ctx, Vec<In>) -> BatchFuture<Out> + Send + Sync>;

struct BatchCall<In> {
    ctx: Ctx,
    item: In,
}

/// Adapts a user closure `Vec<In> -> Result<Vec<Out>>` to the core batcher's `Runner`.
struct FnRunner<In, Out> {
    f: BatchFn<In, Out>,
}

#[async_trait]
impl<In: Send + 'static, Out: Send + 'static> Runner for FnRunner<In, Out> {
    type Input = BatchCall<In>;
    type Output = Out;

    async fn run(
        &self,
        calls: Vec<BatchCall<In>>,
    ) -> cocoindex_utils::error::Result<impl ExactSizeIterator<Item = Out>> {
        let mut contexts = Vec::with_capacity(calls.len());
        let mut inputs = Vec::with_capacity(calls.len());
        for call in calls {
            contexts.push(call.ctx);
            inputs.push(call.item);
        }

        let batch_fn_ctx = Arc::new(FnCallContext::new(true));
        let mut batch_ctx = contexts
            .first()
            .expect("the core batcher never executes an empty batch")
            .with_fn_ctx(batch_fn_ctx.clone());
        // A physical batch belongs to all of its callers, so it must not
        // inherit any one caller's deadline.
        batch_ctx.deadline = DeadlineContext::NONE;
        let result = (self.f)(batch_ctx, inputs).await;

        // The body executes once, but each per-item memo/tracking context must
        // inherit the dependencies it observed (context keys, nested function
        // logic, target states, and child memo entries).
        for ctx in contexts {
            if let Some(parent_fn_ctx) = &ctx.fn_ctx {
                parent_fn_ctx.join_child_shared(&batch_fn_ctx);
            } else if let Some(comp_ctx) = &ctx.comp_ctx {
                comp_ctx.join_fn_call(&batch_fn_ctx);
            }
        }

        let outputs = result?;
        Ok(outputs.into_iter())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ScheduledBatchKey {
    app_id: usize,
    code_hash: u64,
    extra_args: Vec<u8>,
}

type FunctionBatcher<In, Out> = Batcher<FnRunner<In, Out>>;
type ScheduledBatchers<In, Out> = Mutex<HashMap<ScheduledBatchKey, Weak<FunctionBatcher<In, Out>>>>;

fn new_batcher<In, Out>(
    f: BatchFn<In, Out>,
    options: BatchingOptions,
) -> Arc<FunctionBatcher<In, Out>>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    Arc::new(Batcher::new(
        FnRunner { f },
        Arc::new(BatchQueue::new()),
        options,
    ))
}

/// A single-item interface over a batch-shaped async function.
///
/// `#[cocoindex::function(batching)]` is the usual entry point. [`Batched::new`]
/// is the explicit, memoized adapter for code that cannot use the macro.
pub struct Batched<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    batcher: Arc<FunctionBatcher<In, Out>>,
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
        let wrapped: BatchFn<In, Out> = Arc::new(move |_ctx, inputs| {
            let fut = f(inputs);
            Box::pin(async move { fut.await.map_err(Error::into_core) })
        });
        Self {
            batcher: new_batcher(wrapped, BatchingOptions::default()),
            code_hash,
        }
    }

    /// Process one item. On a memo hit the stored result is returned; on a miss
    /// the item is handed to the core batcher (coalesced with other concurrent
    /// misses) and the result is memoized.
    pub async fn call(&self, ctx: &Ctx, item: In) -> Result<Out> {
        let fp =
            crate::memo::key_fingerprint_result(&("cocoindex_batched", self.code_hash, &item))?;
        let batcher = self.batcher.clone();
        // A batch impl is ctx-free, so it makes no tracked child `#[function]`
        // calls; the `propagate_children_fn_logic` flag is therefore inert here.
        // Pass `true` (the default) — only the batch impl's own `code_hash`
        // (folded into the memo key above) tracks its logic.
        crate::memo::cached_by_fingerprint(ctx, fp, true, move |scoped_ctx| async move {
            batcher
                .run(BatchCall {
                    ctx: scoped_ctx,
                    item,
                })
                .await
                .map_err(Error::from)
        })
        .await
    }
}

/// Scheduler backing generated `#[cocoindex::function(batching)]` wrappers.
#[doc(hidden)]
pub struct __ScheduledBatched<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    options: BatchingOptions,
    batchers: ScheduledBatchers<In, Out>,
    code_hash: u64,
}

impl<In, Out> __ScheduledBatched<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    /// Construct the scheduler used by generated batching wrappers.
    #[doc(hidden)]
    pub fn __new(code_hash: u64) -> Self {
        Self::with_options(code_hash, BatchingOptions::default())
    }

    /// Construct the generated scheduler with a maximum batch size.
    #[doc(hidden)]
    pub fn __with_max_batch(code_hash: u64, max_batch_size: usize) -> Self {
        Self::with_options(
            code_hash,
            BatchingOptions {
                max_batch_size: Some(max_batch_size),
            },
        )
    }

    fn with_options(code_hash: u64, options: BatchingOptions) -> Self {
        Self {
            options,
            batchers: Mutex::new(HashMap::new()),
            code_hash,
        }
    }

    /// Schedule one generated function call. `extra_args_key` keeps calls with
    /// different captured arguments in separate batches.
    #[doc(hidden)]
    pub async fn __call_scheduled<F, Fut>(
        &self,
        ctx: Ctx,
        extra_args_key: Vec<u8>,
        item: In,
        f: F,
    ) -> Result<Out>
    where
        F: Fn(Ctx, Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        let key = ScheduledBatchKey {
            // A static generated scheduler may be used by multiple apps in the
            // same process. Never let their context-bound calls share a body.
            app_id: Arc::as_ptr(&ctx.state) as usize,
            code_hash: self.code_hash,
            extra_args: extra_args_key,
        };
        let f: BatchFn<In, Out> = Arc::new(move |ctx, inputs| {
            let fut = f(ctx, inputs);
            Box::pin(async move { fut.await.map_err(Error::into_core) })
        });
        let batcher = {
            let mut batchers = self
                .batchers
                .lock()
                .expect("batch scheduler mutex poisoned");
            batchers.retain(|_, batcher| batcher.strong_count() != 0);
            if let Some(batcher) = batchers.get(&key).and_then(Weak::upgrade) {
                batcher
            } else {
                let batcher = new_batcher(f, self.options.clone());
                batchers.insert(key, Arc::downgrade(&batcher));
                batcher
            }
        };

        batcher
            .run(BatchCall { ctx, item })
            .await
            .map_err(Error::from)
    }
}
