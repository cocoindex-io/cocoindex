//! Python bindings for GPU pool management.
//!
//! Exposes `GPUPool` to Python for coordinating GPU capacity across tasks.
//!
//! Supports asynchronous acquisition of fractional GPU capacity (`acquire`),
//! acquisition of multiple GPUs (`acquire_full`), and returning capacity back to
//! the pool (`release`).

use crate::prelude::*;
use cocoindex_core::engine::runtime::{get_runtime, is_runtime_shutdown};
use cocoindex_utils::gpu_pool::{GPUPool, gpu_capacity::GPUCapacity};
use futures::future::BoxFuture;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyValueError};
use std::num::NonZeroUsize;
use std::sync::PoisonError;
use std::task::{Context, Poll, Wake, Waker};

#[pyclass(name = "GPUPool", skip_from_py_object)]
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

    pub fn acquire(&self, fraction: f32) -> PyResult<PyGPUAcquisition> {
        let fraction =
            GPUCapacity::try_from(fraction).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gpu_pool = self.inner.clone();
        Ok(PyGPUAcquisition::new(async move {
            gpu_pool.acquire(fraction).await.map(AcquiredGpus::One)
        }))
    }

    pub fn acquire_full(&self, gpu_count: usize) -> PyResult<PyGPUAcquisition> {
        let Some(gpu_count) = NonZeroUsize::new(gpu_count) else {
            return Err(PyValueError::new_err(format!(
                "gpu_count must be > 0, got {gpu_count}"
            )));
        };
        let gpu_pool = self.inner.clone();
        Ok(PyGPUAcquisition::new(async move {
            gpu_pool
                .acquire_full(gpu_count)
                .await
                .map(AcquiredGpus::Many)
        }))
    }

    pub fn release(&self, gpu_id: usize, fraction: f32) -> PyResult<()> {
        let fraction = GPUCapacity::try_from(fraction).into_py_result()?;
        self.inner.release(gpu_id, fraction).into_py_result()
    }
}

/// The GPU ids an acquisition hands to Python: one for `acquire`, a list for `acquire_full`.
#[derive(IntoPyObject)]
enum AcquiredGpus {
    One(usize),
    Many(Vec<usize>),
}

/// The awaitable that `acquire` and `acquire_full` return.
///
/// It polls the pool's future in the awaiting task's own steps, so the GPU ids reach the
/// task in the step that completes the future. If the task is cancelled, or the awaitable
/// is closed or dropped, before that step, the future is dropped instead, and dropping it
/// returns any capacity the pool already assigned to it.
///
/// `future_into_py` cannot give this: it completes an `asyncio.Future` from another thread
/// and drops the result if that future was cancelled in the meantime, so the GPU leaks.
#[pyclass(name = "GPUAcquisition", frozen)]
pub struct PyGPUAcquisition {
    state: Arc<AcquisitionState>,
}

struct AcquisitionState {
    /// `None` once the acquisition has finished, or has been closed.
    future: Mutex<Option<BoxFuture<'static, Result<AcquiredGpus>>>>,
    /// The `asyncio.Future` the awaiting task waits on until the next step.
    wakeup: Mutex<Option<Py<PyAny>>>,
}

impl PyGPUAcquisition {
    fn new(future: impl Future<Output = Result<AcquiredGpus>> + Send + 'static) -> Self {
        Self {
            state: Arc::new(AcquisitionState {
                future: Mutex::new(Some(Box::pin(future))),
                wakeup: Mutex::new(None),
            }),
        }
    }

    /// Polls the acquisition. Returns the GPU ids by raising `StopIteration`, or the
    /// future the task waits on before the next step.
    fn step(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let wakeup = py
            .import(pyo3::intern!(py, "asyncio"))?
            .call_method0(pyo3::intern!(py, "get_running_loop"))?
            .call_method0(pyo3::intern!(py, "create_future"))?;
        // Stored before polling, so a wake during or after the poll completes this future.
        let previous_wakeup = self
            .state
            .wakeup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(wakeup.clone().unbind());
        drop(previous_wakeup);

        let polled = {
            let mut future = self
                .state
                .future
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(pending) = future.as_mut() else {
                return Err(PyRuntimeError::new_err(
                    "cannot reuse an already awaited GPU acquisition",
                ));
            };
            let waker = Waker::from(self.state.clone());
            let polled = pending.as_mut().poll(&mut Context::from_waker(&waker));
            if polled.is_ready() {
                *future = None;
            }
            polled
        };
        match polled {
            Poll::Ready(gpus) => Err(PyStopIteration::new_err((gpus.into_py_result()?,))),
            // A task waits only on a future yielded by that future's own `__await__`.
            Poll::Pending => Ok(wakeup
                .call_method0(pyo3::intern!(py, "__await__"))?
                .call_method0(pyo3::intern!(py, "__next__"))?
                .unbind()),
        }
    }
}

#[pymethods]
impl PyGPUAcquisition {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.step(py)
    }

    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.step(py)
    }

    fn throw(&self, exc: Bound<'_, PyAny>) -> PyResult<()> {
        self.state.close();
        Err(PyErr::from_value(exc))
    }

    fn close(&self) {
        self.state.close();
    }
}

impl Drop for PyGPUAcquisition {
    fn drop(&mut self) {
        self.state.close();
    }
}

impl AcquisitionState {
    /// Drops the pool's future, which returns any capacity already assigned to it.
    fn close(&self) {
        let future = self
            .future
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(future);
    }

    fn wake_task(&self) {
        let scheduled = Python::attach(|py| -> PyResult<()> {
            // Left in place rather than taken: the pool's future also wakes itself after a
            // poll (`FuturesUnordered` does), and a later wake must still find this future
            // to notice a closed loop.
            let Some(wakeup) = self
                .wakeup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .map(|wakeup| wakeup.clone_ref(py))
            else {
                return Ok(());
            };
            let wakeup = wakeup.bind(py);
            wakeup
                .call_method0(pyo3::intern!(py, "get_loop"))?
                .call_method1(
                    pyo3::intern!(py, "call_soon_threadsafe"),
                    (wrap_pyfunction!(wake_up, py)?, wakeup),
                )?;
            Ok(())
        });
        if scheduled.is_err() {
            // The task's event loop is closed, so the task never steps again.
            self.close();
        }
    }
}

impl Wake for AcquisitionState {
    fn wake(self: Arc<Self>) {
        // Past `shutdown_runtime` (interpreter exit), no task will step again.
        if is_runtime_shutdown() {
            return;
        }
        // The pool wakes waiters while it holds its lock. A Python call there can pass the
        // GIL to a thread that then blocks on that lock, so wake the task from another thread.
        get_runtime().spawn_blocking(move || self.wake_task());
    }
}

/// Completes a task's wakeup future, unless the task was cancelled out of it.
#[pyfunction]
fn wake_up(wakeup: &Bound<'_, PyAny>) -> PyResult<()> {
    if !wakeup
        .call_method0(pyo3::intern!(wakeup.py(), "done"))?
        .is_truthy()?
    {
        wakeup.call_method1(
            pyo3::intern!(wakeup.py(), "set_result"),
            (wakeup.py().None(),),
        )?;
    }
    Ok(())
}
