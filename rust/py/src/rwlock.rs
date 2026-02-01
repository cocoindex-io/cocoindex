//! Fair Read-Write Lock exposed to Python.
//!
//! Uses `tokio::sync::RwLock` which provides a fair (FIFO) lock.

use cocoindex_core::engine::runtime::get_runtime;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// A fair read-write lock.
///
/// Multiple readers can hold the lock concurrently, but writers have exclusive access.
/// The lock is fair (FIFO) - requests are served in arrival order.
#[pyclass]
pub struct RWLock {
    inner: Arc<RwLock<()>>,
}

#[pymethods]
impl RWLock {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(())),
        }
    }

    /// Create a read guard. Use with `with` or `async with` to acquire the lock.
    fn read(&self) -> RWLockReadGuard {
        RWLockReadGuard {
            lock: self.inner.clone(),
            guard: None,
        }
    }

    /// Create a write guard. Use with `with` or `async with` to acquire the lock.
    fn write(&self) -> RWLockWriteGuard {
        RWLockWriteGuard {
            lock: self.inner.clone(),
            guard: None,
        }
    }
}

/// Guard for a read lock. Releases the lock when exiting the context.
#[pyclass]
pub struct RWLockReadGuard {
    lock: Arc<RwLock<()>>,
    guard: Option<OwnedRwLockReadGuard<()>>,
}

#[pymethods]
impl RWLockReadGuard {
    /// Release the read lock explicitly.
    fn release(&mut self) {
        self.guard.take();
    }

    fn __enter__<'py>(mut slf: PyRefMut<'py, Self>, py: Python<'py>) -> PyRefMut<'py, Self> {
        let lock = slf.lock.clone();
        let guard = py.detach(|| get_runtime().block_on(async move { lock.read_owned().await }));
        slf.guard = Some(guard);
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) {
        self.release();
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lock = slf.borrow(py).lock.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = lock.read_owned().await;
            Python::attach(|py| {
                slf.borrow_mut(py).guard = Some(guard);
            });
            Ok(slf)
        })
    }

    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.release();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) })
    }
}

/// Guard for a write lock. Releases the lock when exiting the context.
#[pyclass]
pub struct RWLockWriteGuard {
    lock: Arc<RwLock<()>>,
    guard: Option<OwnedRwLockWriteGuard<()>>,
}

#[pymethods]
impl RWLockWriteGuard {
    /// Release the write lock explicitly.
    fn release(&mut self) {
        self.guard.take();
    }

    fn __enter__<'py>(mut slf: PyRefMut<'py, Self>, py: Python<'py>) -> PyRefMut<'py, Self> {
        let lock = slf.lock.clone();
        let guard = py.detach(|| get_runtime().block_on(async move { lock.write_owned().await }));
        slf.guard = Some(guard);
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) {
        self.release();
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lock = slf.borrow(py).lock.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = lock.write_owned().await;
            Python::attach(|py| {
                slf.borrow_mut(py).guard = Some(guard);
            });
            Ok(slf)
        })
    }

    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.release();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) })
    }
}
