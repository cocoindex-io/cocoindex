//! Python bindings for batching infrastructure.
//!
//! Exposes BatchQueue and Batcher to Python for implementing batched function execution.
//!
//! Design: Multiple batchers can share the same queue (e.g., for GPU serialization),
//! and each batcher has its own runner function. When a batcher creates a batch,
//! that batch carries the batcher's runner function.

use crate::prelude::*;
use cocoindex_utils::gpu_pool::GPUPool;
use pyo3::exceptions::PyValueError;
use pyo3_async_runtimes::tokio::future_into_py;
use std::num::NonZeroUsize;

#[pyclass(name = "GPUPool")]
#[derive(Clone)]
pub struct PyGPUPool {
    inner: Arc<GPUPool>,
}

#[pymethods]
impl PyGPUPool {
    #[new]
    pub fn new(num_gpus: usize) -> Self {
        Self {
            inner: Arc::new(GPUPool::new(num_gpus)),
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        Self {
            inner: Arc::new(GPUPool::default()),
        }
    }

    #[getter]
    pub fn num_gpus(&self) -> usize {
        self.inner.num_gpus()
    }

    pub fn acquire<'py>(&self, py: Python<'py>, fraction: f32) -> PyResult<Bound<'py, PyAny>> {
        let gpu_pool = self.inner.clone();
        future_into_py(py, async move { Ok(gpu_pool.acquire(fraction).await) })
    }

    pub fn acquire_full<'py>(
        &self,
        py: Python<'py>,
        gpu_count: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        if gpu_count <= 0 {
            return Err(PyValueError::new_err(format!(
                "gpu_count must be > 0, got {gpu_count}"
            )));
        };
        let gpu_pool = self.inner.clone();
        future_into_py(py, async move {
            gpu_pool
                .acquire_full(NonZeroUsize::new(gpu_count).unwrap())
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }

    pub fn release<'py>(
        &self,
        py: Python<'py>,
        gpu_id: usize,
        fraction: f32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gpu_pool = self.inner.clone();
        future_into_py(
            py,
            async move { Ok(gpu_pool.release(gpu_id, fraction).await) },
        )
    }
}
