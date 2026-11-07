//! Runners: bound how many calls of a function run at once on a scarce
//! resource, the Rust form of Python's `@coco.fn.as_async(runner=coco.GPU)`.
//!
//! A [`Runner`] wraps the body of every call of a function (or, for a
//! `batching` function, of every physical batch). The built-in runner is
//! [`GPURunner`]: each body acquires a share of one GPU from the process-wide
//! GPU pool before it runs and releases it afterwards, so a body that holds a
//! whole GPU never runs twice at once on the same GPU, and two halves share
//! one. Inside the body, [`current_gpu`], [`current_gpus`] and
//! [`current_gpu_fraction`] tell which GPU was assigned.
//!
//! ```ignore
//! #[cocoindex::function(memo, batching, max_batch_size = 16, runner = cocoindex::GPU)]
//! async fn embed(_ctx: &Ctx, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
//!     let device = cocoindex::current_gpu().unwrap_or(0);
//!     model.encode_on(device, &texts)
//! }
//! ```
//!
//! A user-defined runner implements [`Runner`], Python's `Runner` base class:
//! it receives the body as a future and decides when and how to run it.
//!
//! ```ignore
//! struct AtMostTwo(tokio::sync::Semaphore);
//!
//! #[async_trait::async_trait]
//! impl cocoindex::Runner for AtMostTwo {
//!     async fn run(&self, body: cocoindex::RunnerBody<'_>) -> Result<cocoindex::RunnerOutput> {
//!         let _permit = self.0.acquire().await;
//!         body.await
//!     }
//! }
//!
//! #[cocoindex::function(runner = AtMostTwo(tokio::sync::Semaphore::new(2)))]
//! async fn summarize(_ctx: &Ctx, text: String) -> Result<String> { /* ... */ }
//! ```
//!
//! The pool's size is detected from `COCOINDEX_NUM_GPUS`, then
//! `CUDA_VISIBLE_DEVICES`, then `nvidia-smi`, and falls back to one;
//! [`configure_gpu_pool`] overrides it. Python's subprocess mode
//! (`COCOINDEX_RUN_GPU_IN_SUBPROCESS`) and its dedicated GPU thread exist
//! for the GIL and CUDA's fork rules; a Rust body runs on the Tokio runtime
//! and has neither problem, so there is no equivalent here.

use std::any::Any;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use cocoindex_utils::gpu_pool::GPUPool;
use cocoindex_utils::gpu_pool::gpu_capacity::GPUCapacity;
use futures::future::BoxFuture;

use crate::error::{Error, Result};

/// The value a [`RunnerBody`] produces. It is the function's own result,
/// opaque to the runner, which hands it back unchanged.
pub struct RunnerOutput(Box<dyn Any + Send>);

/// The body of one call (or one physical batch) that a [`Runner`] runs: a
/// future the runner awaits once, when its resource is available.
pub struct RunnerBody<'a>(BoxFuture<'a, Result<RunnerOutput>>);

impl Future for RunnerBody<'_> {
    type Output = Result<RunnerOutput>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// Runs the bodies of a function's calls, Python's `Runner` base class. The
/// value of `#[cocoindex::function(runner = ...)]` is any type implementing
/// it; [`GPURunner`] is the built-in one.
///
/// An implementation awaits `body` exactly once and returns its output. What
/// it does around that await is its policy: wait for a permit, hold a
/// resource, set a task-local for the body to read.
#[async_trait]
pub trait Runner: Send + Sync + 'static {
    async fn run(&self, body: RunnerBody<'_>) -> Result<RunnerOutput>;
}

/// Run `f` under `runner` and give back its typed output. The macro-generated
/// wrappers and the batch scheduler go through here.
#[doc(hidden)]
pub async fn __run_under<T, F>(runner: &dyn Runner, f: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send,
{
    let body = RunnerBody(Box::pin(async move {
        let output = f.await?;
        Ok(RunnerOutput(Box::new(output)))
    }));
    let output = runner.run(body).await?;
    output
        .0
        .downcast::<T>()
        .map(|output| *output)
        .map_err(|_| Error::engine("the runner returned another body's output"))
}

/// A runner that serializes calls over the GPUs in the process-wide pool.
///
/// [`GPU`] takes a whole GPU per call. [`GPURunner::fraction`] takes a share
/// of one, so that several calls run at once on the same GPU.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GPURunner {
    fraction: f32,
}

/// The runner that takes one whole GPU per call, Python's `coco.GPU`.
pub const GPU: GPURunner = GPURunner { fraction: 1.0 };

impl GPURunner {
    /// A runner that takes `fraction` of one GPU per call, Python's
    /// `coco.GPU(fraction)`. Calls whose fractions fit together run at once on
    /// the same GPU: two `fraction(0.5)` calls share one.
    ///
    /// `fraction` must be in `(0, 1]`: the function panics otherwise. It is
    /// `const`, so a `const` holding a bad literal fails to compile; in
    /// `#[cocoindex::function(runner = ...)]` the value is built on the first
    /// call, where a bad value panics.
    pub const fn fraction(fraction: f32) -> Self {
        assert!(
            fraction > 0.0 && fraction <= 1.0,
            "GPURunner fraction must be in (0, 1]"
        );
        Self { fraction }
    }
}

#[async_trait]
impl Runner for GPURunner {
    /// Run `body` while holding this runner's share of one GPU: wait until a
    /// GPU has that much capacity free, run `body` with [`current_gpu`] set
    /// to it, and give the capacity back when `body` finishes or is cancelled.
    async fn run(&self, body: RunnerBody<'_>) -> Result<RunnerOutput> {
        let pool = default_gpu_pool()?;
        let fraction = GPUCapacity::try_from(self.fraction)?;
        let gpu_id = pool.acquire(fraction).await?;
        let _held = HeldCapacity {
            pool,
            gpu_id,
            fraction,
        };
        let assignment = GpuAssignment {
            gpus: vec![gpu_id],
            fraction: self.fraction,
        };
        CURRENT_GPU.scope(assignment, body).await
    }
}

/// Capacity taken from the pool for one call; returned on drop, so a
/// cancelled call never leaks it.
struct HeldCapacity {
    pool: Arc<GPUPool>,
    gpu_id: usize,
    fraction: GPUCapacity,
}

impl Drop for HeldCapacity {
    fn drop(&mut self) {
        if let Err(err) = self.pool.release(self.gpu_id, self.fraction) {
            // Only a pool invariant violation reaches here; there is no caller
            // left to return it to.
            tracing::error!("failed to release GPU {} to the pool: {err}", self.gpu_id);
        }
    }
}

#[derive(Debug, Clone)]
struct GpuAssignment {
    gpus: Vec<usize>,
    fraction: f32,
}

tokio::task_local! {
    static CURRENT_GPU: GpuAssignment;
}

/// The first GPU assigned to the current call, or `None` outside a
/// [`GPURunner`] body. Like Python's `coco.current_gpu()`.
///
/// The assignment is task-local: a task the body spawns does not see it.
pub fn current_gpu() -> Option<usize> {
    CURRENT_GPU
        .try_with(|assignment| assignment.gpus.first().copied())
        .ok()
        .flatten()
}

/// The GPUs assigned to the current call; empty outside a [`GPURunner`]
/// body. Like Python's `coco.current_gpus()`.
pub fn current_gpus() -> Vec<usize> {
    CURRENT_GPU
        .try_with(|assignment| assignment.gpus.clone())
        .unwrap_or_default()
}

/// The share of a GPU the current call holds, or `None` outside a
/// [`GPURunner`] body. Like Python's `coco.current_gpu_fraction()`.
pub fn current_gpu_fraction() -> Option<f32> {
    CURRENT_GPU.try_with(|assignment| assignment.fraction).ok()
}

static DEFAULT_GPU_POOL: Mutex<Option<Arc<GPUPool>>> = Mutex::new(None);

/// Replace the process-wide GPU pool with one of `num_gpus` GPUs, like
/// Python's `coco.configure_gpu_pool`. Call it before any function with a
/// runner runs: a call already holding capacity in the old pool returns it
/// there, not to the new one.
pub fn configure_gpu_pool(num_gpus: NonZeroUsize) {
    let mut pool = DEFAULT_GPU_POOL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *pool = Some(Arc::new(GPUPool::new(num_gpus)));
}

/// The process-wide pool, detected on first use unless configured.
fn default_gpu_pool() -> Result<Arc<GPUPool>> {
    let mut pool = DEFAULT_GPU_POOL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(pool) = pool.as_ref() {
        return Ok(pool.clone());
    }
    let detected = Arc::new(GPUPool::detected()?);
    *pool = Some(detected.clone());
    Ok(detected)
}
