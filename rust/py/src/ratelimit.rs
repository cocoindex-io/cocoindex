//! Token-bucket rate limiter exposed to Python.
//!
//! Thin wrapper over the `governor`-based limiter in `cocoindex_utils`.
//! Lets a pipeline throttle outbound work (e.g. API calls in a source /
//! target connector) to stay within an external service's rate limit.

use crate::prelude::*;
use pyo3::exceptions::{PyException, PyValueError};
use std::time::Duration;
use utils::ratelimit::{RateLimit, RateLimiter};

/// A token-bucket rate limiter.
///
/// `acquire(n)` asynchronously waits until `n` tokens are available.
/// Concurrent callers are served in FIFO order.
#[pyclass(name = "RateLimiter")]
pub struct PyRateLimiter {
    inner: Arc<RateLimiter>,
}

#[pymethods]
impl PyRateLimiter {
    #[new]
    #[pyo3(signature = (max_rows_per_second, burst_window_secs=1.0))]
    fn new(max_rows_per_second: f64, burst_window_secs: f64) -> PyResult<Self> {
        let rate_limit = RateLimit {
            max_rows_per_second,
            burst_window: Duration::from_secs_f64(burst_window_secs.max(0.0)),
        };
        let limiter =
            RateLimiter::new(&rate_limit).map_err(|e| PyValueError::new_err(format!("{e}")))?;
        Ok(Self {
            inner: Arc::new(limiter),
        })
    }

    /// Wait until `n` tokens are available (default 1). Returns an awaitable.
    #[pyo3(signature = (n=1))]
    fn acquire<'py>(&self, py: Python<'py>, n: u32) -> PyResult<Bound<'py, PyAny>> {
        let limiter = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            limiter
                .until_ready_n(n)
                .await
                .map_err(|e| PyException::new_err(format!("{e}")))?;
            Ok(())
        })
    }
}
