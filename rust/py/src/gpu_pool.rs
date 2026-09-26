//! Python bindings for GPU pool management.
//!
//! Exposes `GPUPool` to Python for coordinating GPU capacity across tasks.
//!
//! Supports asynchronous acquisition of fractional GPU capacity (`acquire`),
//! acquisition of multiple GPUs (`acquire_full`), and returning capacity back to
//! the pool (`release`).

use crate::prelude::*;
use cocoindex_utils::gpu_pool::{GPUPool, gpu_capacity::GPUCapacity};
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
    pub fn new(num_gpus: usize) -> PyResult<Self> {
        NonZeroUsize::new(num_gpus)
            .ok_or_else(|| PyValueError::new_err("num_gpus must be > 0"))
            .map(GPUPool::new)
            .map(Arc::new)
            .map(|gpu_pool| Self { inner: gpu_pool })
    }

    #[staticmethod]
    pub fn default(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            // Releases the GIL during the probe so other Python threads are not stalled.
            inner: Arc::new(py.detach(|| GPUPool::detected().into_py_result())?),
        })
    }

    #[getter]
    pub fn num_gpus(&self) -> usize {
        self.inner.num_gpus()
    }

    pub fn acquire<'py>(&self, py: Python<'py>, fraction: f32) -> PyResult<Bound<'py, PyAny>> {
        let fraction =
            GPUCapacity::try_from(fraction).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gpu_pool = self.inner.clone();
        future_into_py(py, async move {
            gpu_pool.acquire(fraction).await.into_py_result()
        })
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
                .into_py_result()
        })
    }

    pub fn release<'py>(&self, gpu_id: usize, fraction: f32) -> PyResult<()> {
        let fraction = GPUCapacity::try_from(fraction).into_py_result()?;
        self.inner.release(gpu_id, fraction).into_py_result()
    }
}
