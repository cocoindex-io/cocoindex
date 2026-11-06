//! Runners: bound how many calls of a function run at once on a scarce
//! resource, the Rust form of Python's `@coco.fn.as_async(runner=coco.GPU)`.
//!
//! The one runner is [`GPURunner`]. Each call (or, for a `batching` function,
//! each physical batch) acquires a share of one GPU from the process-wide
//! GPU pool before its body runs and releases it afterwards, so a body that
//! holds a whole GPU never runs twice at once on the same GPU, and two
//! halves share one. Inside the body, [`current_gpu`], [`current_gpus`] and
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
//! The pool's size is detected from `COCOINDEX_NUM_GPUS`, then
//! `CUDA_VISIBLE_DEVICES`, then `nvidia-smi`, and falls back to one;
//! [`configure_gpu_pool`] overrides it. Python's subprocess mode
//! (`COCOINDEX_RUN_GPU_IN_SUBPROCESS`) and its dedicated GPU thread exist
//! for the GIL and CUDA's fork rules; a Rust body runs on the Tokio runtime
//! and has neither problem, so there is no equivalent here.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use cocoindex_utils::gpu_pool::GPUPool;
use cocoindex_utils::gpu_pool::gpu_capacity::GPUCapacity;

use crate::error::Result;

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
    /// `fraction` must be in `(0, 1]`. The function is `const`, so with a
    /// literal in `#[cocoindex::function(runner = ...)]` a bad value is a
    /// compile error; called at runtime with a bad value, it panics.
    pub const fn fraction(fraction: f32) -> Self {
        assert!(
            fraction > 0.0 && fraction <= 1.0,
            "GPURunner fraction must be in (0, 1]"
        );
        Self { fraction }
    }

    /// Run `f` while holding this runner's share of one GPU: wait until a GPU
    /// has that much capacity free, run `f` with [`current_gpu`] set to it,
    /// and give the capacity back when `f` finishes or is cancelled.
    pub async fn run<T, F>(&self, f: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
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
        CURRENT_GPU.scope(assignment, f).await
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
