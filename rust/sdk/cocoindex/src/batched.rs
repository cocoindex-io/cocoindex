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
//! A physical-batch error fails every item in that batch, unless the body
//! returns [`Error::retry_with_smaller_batch`]: then the batch is halved and
//! each half re-run, down to single items, so that one bad input fails only
//! its own caller and the rest keep their results. A `runner` (see
//! [`crate::runner`]) bounds how many bodies run at once; each physical batch
//! and each retried half runs under the runner, so it takes the runner's
//! resource for the time it runs.
//! A batching function must not call itself recursively from its body
//! because it would wait on the batcher that is already executing that body.
//! [`Batched`] remains available as the lower-level explicit adapter when a
//! generated function wrapper is not suitable.

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
use crate::runner::__run_under;

type CoreResult<T> = cocoindex_utils::error::Result<T>;

type BatchFuture<Out> = Pin<Box<dyn Future<Output = Result<Vec<Out>>> + Send>>;
type BatchFn<In, Out> = Arc<dyn Fn(Ctx, Vec<In>) -> BatchFuture<Out> + Send + Sync>;

/// One item's outcome inside a physical batch. After a split, a failed
/// sub-batch delivers its error per item through the output list, since the
/// core batcher itself knows only whole-batch errors.
type ItemResult<Out> = CoreResult<Out>;

struct BatchCall<In> {
    ctx: Ctx,
    item: In,
}

/// Adapts a user closure `Vec<In> -> Result<Vec<Out>>` to the core batcher's
/// `Runner`, with split-and-retry on [`Error::RetryWithSmallerBatch`] and an
/// optional [`crate::Runner`] around every body run.
struct FnRunner<In, Out> {
    f: BatchFn<In, Out>,
    runner: Option<Arc<dyn crate::Runner>>,
}

impl<In: Clone + Send + 'static, Out: Send + 'static> FnRunner<In, Out> {
    /// Run the body once over `calls`, under the runner if any, and join the
    /// body's dependencies into each caller's context.
    async fn run_body(&self, contexts: Vec<Ctx>, inputs: Vec<In>) -> Result<Vec<Out>> {
        let num_inputs = inputs.len();

        let batch_fn_ctx = Arc::new(FnCallContext::new(true));
        let mut batch_ctx = contexts
            .first()
            .expect("the core batcher never executes an empty batch")
            .with_fn_ctx(batch_fn_ctx.clone());
        // A physical batch belongs to all of its callers, so it must not
        // inherit any one caller's deadline.
        batch_ctx.deadline = DeadlineContext::NONE;
        let body = (self.f)(batch_ctx, inputs);
        let result = match &self.runner {
            Some(runner) => __run_under(runner.as_ref(), body).await,
            None => body.await,
        };

        // The body executes once, but each per-item memo/tracking context must
        // inherit the dependencies it observed (context keys, nested function
        // logic, target states, and child memo entries).
        for ctx in &contexts {
            if let Some(parent_fn_ctx) = &ctx.fn_ctx {
                parent_fn_ctx.join_child_shared(&batch_fn_ctx);
            } else if let Some(comp_ctx) = &ctx.comp_ctx {
                comp_ctx.join_fn_call(&batch_fn_ctx);
            }
        }

        let outputs = result?;
        if outputs.len() != num_inputs {
            return Err(Error::engine(format!(
                "batch body returned {} outputs for {num_inputs} inputs",
                outputs.len()
            )));
        }
        Ok(outputs)
    }

    /// Run `calls`, halving on [`Error::RetryWithSmallerBatch`] down to single
    /// items. The root batch fails as a whole when it fails without a split;
    /// after a split, a sub-batch's failure is confined to its own items.
    fn run_split<'a>(
        &'a self,
        calls: Vec<BatchCall<In>>,
        is_root: bool,
    ) -> futures::future::BoxFuture<'a, CoreResult<Vec<ItemResult<Out>>>> {
        Box::pin(async move {
            // Items are cloned so that a split can run them again.
            let contexts = calls.iter().map(|call| call.ctx.clone()).collect();
            let inputs = calls.iter().map(|call| call.item.clone()).collect();
            let attempt = self.run_body(contexts, inputs).await;
            let err = match attempt {
                Ok(outputs) => return Ok(outputs.into_iter().map(Ok).collect()),
                Err(err) => err,
            };
            let failure = match err {
                Error::RetryWithSmallerBatch(cause) => {
                    if calls.len() > 1 {
                        let mid = calls.len() / 2;
                        let mut first = calls;
                        let second = first.split_off(mid);
                        let (first, second) = futures::future::join(
                            self.run_split(first, false),
                            self.run_split(second, false),
                        )
                        .await;
                        let mut outputs = first?;
                        outputs.extend(second?);
                        return Ok(outputs);
                    }
                    // Size one: the signal stands for its cause.
                    *cause
                }
                other => other,
            };
            if is_root {
                // No split happened: a whole-batch failure, which the core
                // batcher fans out to every caller.
                return Err(failure.into_core());
            }
            let failure = failure.into_core();
            Ok(calls.iter().map(|_| Err(failure.replica())).collect())
        })
    }
}

#[async_trait]
impl<In: Clone + Send + 'static, Out: Send + 'static> Runner for FnRunner<In, Out> {
    type Input = BatchCall<In>;
    type Output = ItemResult<Out>;

    async fn run(
        &self,
        calls: Vec<BatchCall<In>>,
    ) -> CoreResult<impl ExactSizeIterator<Item = ItemResult<Out>>> {
        let outputs = self.run_split(calls, true).await?;
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
    runner: Option<Arc<dyn crate::Runner>>,
    options: BatchingOptions,
) -> Arc<FunctionBatcher<In, Out>>
where
    In: Clone + Send + 'static,
    Out: Send + 'static,
{
    Arc::new(Batcher::new(
        FnRunner { f, runner },
        Arc::new(BatchQueue::new()),
        options,
    ))
}

/// Await one item's result from a batcher: a whole-batch error, then the
/// item's own error from a split sub-batch.
async fn run_item<In, Out>(batcher: &FunctionBatcher<In, Out>, call: BatchCall<In>) -> Result<Out>
where
    In: Clone + Send + 'static,
    Out: Send + 'static,
{
    let item_result = batcher.run(call).await.map_err(Error::from)?;
    item_result.map_err(Error::from)
}

/// A single-item interface over a batch-shaped async function.
///
/// `#[cocoindex::function(batching)]` is the usual entry point. [`Batched::new`]
/// is the explicit, memoized adapter for code that cannot use the macro.
pub struct Batched<In, Out>
where
    In: Clone + Send + 'static,
    Out: Send + 'static,
{
    batcher: Arc<FunctionBatcher<In, Out>>,
    code_hash: u64,
}

impl<In, Out> Batched<In, Out>
where
    In: Clone + Serialize + Send + 'static,
    Out: Serialize + DeserializeOwned + Send + 'static,
{
    /// Build a `Batched` from a batch implementation `f: Vec<In> -> Result<Vec<Out>>`.
    ///
    /// `code_hash` is the batch function's logic fingerprint — the
    /// `__COCO_FN_HASH_*` constant emitted by `#[cocoindex::function]`. It is
    /// folded into each item's memo key, so editing the batch logic invalidates
    /// cached results.
    ///
    /// `f` may return [`Error::retry_with_smaller_batch`] to have the batch
    /// halved and retried; items are cloned for each retry.
    pub fn new<F, Fut>(f: F, code_hash: u64) -> Self
    where
        F: Fn(Vec<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Out>>> + Send + 'static,
    {
        let wrapped: BatchFn<In, Out> = Arc::new(move |_ctx, inputs| Box::pin(f(inputs)));
        Self {
            batcher: new_batcher(wrapped, None, BatchingOptions::default()),
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
        crate::memo::cached_by_fingerprint(ctx, fp, None, true, move |scoped_ctx| async move {
            run_item(
                &batcher,
                BatchCall {
                    ctx: scoped_ctx,
                    item,
                },
            )
            .await
        })
        .await
    }
}

/// Scheduler backing generated `#[cocoindex::function(batching)]` wrappers.
#[doc(hidden)]
pub struct __ScheduledBatched<In, Out>
where
    In: Clone + Send + 'static,
    Out: Send + 'static,
{
    options: BatchingOptions,
    runner: Option<Arc<dyn crate::Runner>>,
    batchers: ScheduledBatchers<In, Out>,
    code_hash: u64,
}

impl<In, Out> __ScheduledBatched<In, Out>
where
    In: Clone + Send + 'static,
    Out: Send + 'static,
{
    /// Construct the scheduler used by generated batching wrappers.
    #[doc(hidden)]
    pub fn __new(
        code_hash: u64,
        max_batch_size: Option<usize>,
        runner: Option<Arc<dyn crate::Runner>>,
    ) -> Self {
        Self {
            options: BatchingOptions {
                max_batch_size,
                ..BatchingOptions::default()
            },
            runner,
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
        let f: BatchFn<In, Out> = Arc::new(move |ctx, inputs| Box::pin(f(ctx, inputs)));
        let batcher = {
            let mut batchers = self
                .batchers
                .lock()
                .expect("batch scheduler mutex poisoned");
            batchers.retain(|_, batcher| batcher.strong_count() != 0);
            if let Some(batcher) = batchers.get(&key).and_then(Weak::upgrade) {
                batcher
            } else {
                let batcher = new_batcher(f, self.runner.clone(), self.options.clone());
                batchers.insert(key, Arc::downgrade(&batcher));
                batcher
            }
        };

        run_item(&batcher, BatchCall { ctx, item }).await
    }
}
