use cocoindex_core::engine::function::FnCallMemoGuard;
use cocoindex_core::engine::runtime::get_runtime;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::fingerprint::PyFingerprint;
use crate::prelude::*;
use crate::value::PyValue;

/// Python-facing guard for a function-call memo entry.
///
/// Always holds a write lock on the underlying memo entry. On cache hits,
/// `cached_value` and `cached_memo_states` are pre-extracted owned copies.
/// Call `resolve()` after (re-)execution, or `close()` / drop to release.
#[pyclass(name = "FnCallMemoGuard")]
pub struct PyFnCallMemoGuard {
    guard: Option<FnCallMemoGuard<PyEngineProfile>>,
    is_cached: bool,
    cached_value: Option<Py<PyAny>>,
    cached_memo_states: Option<Py<PyAny>>, // Python list
}

#[pymethods]
impl PyFnCallMemoGuard {
    /// Whether this guard represents a cache hit (true) or miss (false).
    ///
    /// When true, `cached_value` and `cached_memo_states` hold the stored results.
    /// Note: `cached_value` itself may be Python `None` (e.g. for functions returning
    /// `None`), so use `is_cached` rather than `cached_value is not None` to distinguish
    /// cache hits from misses.
    #[getter]
    pub fn is_cached(&self) -> bool {
        self.is_cached
    }

    #[getter]
    pub fn cached_value(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.cached_value.as_ref().map(|v| v.clone_ref(py))
    }

    #[getter]
    pub fn cached_memo_states(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.cached_memo_states.as_ref().map(|s| s.clone_ref(py))
    }

    /// Update memo states on a cache hit without re-execution.
    ///
    /// Used when the state function says `can_reuse=True` but the state value has changed.
    pub fn update_memo_states(&mut self, memo_states: Vec<Py<PyAny>>) -> PyResult<()> {
        let states: Vec<PyValue> = memo_states.into_iter().map(PyValue::new).collect();
        if let Some(ref mut guard) = self.guard {
            guard.update_memo_states(states);
        }
        Ok(())
    }

    #[pyo3(signature = (fn_ctx, ret, memo_states=None))]
    pub fn resolve(
        &mut self,
        fn_ctx: &PyFnCallContext,
        ret: Py<PyAny>,
        memo_states: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<bool> {
        let states: Vec<PyValue> = memo_states
            .unwrap_or_default()
            .into_iter()
            .map(PyValue::new)
            .collect();
        let resolved = if let Some(guard) = self.guard.take() {
            guard
                .resolve(&fn_ctx.0, || PyValue::new(ret), states)
                .into_py_result()?
        } else {
            false
        };
        Ok(resolved)
    }

    /// Release the underlying Rust write lock without resolving the memo entry.
    ///
    /// Users should call this in a `finally` block if they don't end up calling `resolve(...)`.
    pub fn close(&mut self) {
        self.guard = None;
    }
}

async fn reserve_memoization_inner(
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> Result<Py<PyAny>> {
    let guard =
        cocoindex_core::engine::function::reserve_memoization(&comp_ctx.0, memo_fp.0).await?;

    Python::attach(|py| {
        // Extract cached data (if cache hit) into owned Python objects before moving the guard.
        let (is_cached, cached_value, cached_memo_states) = match guard.cached() {
            Some((ret, states)) => {
                let value = ret.value().clone_ref(py);
                let states_list = PyList::new(py, states.iter().map(|s| s.value()))?;
                (true, Some(value), Some(states_list.unbind().into_any()))
            }
            None => (false, None, None),
        };

        let py_guard = PyFnCallMemoGuard {
            guard: Some(guard),
            is_cached,
            cached_value,
            cached_memo_states,
        };

        Ok(Py::new(py, py_guard)?.into_any())
    })
}

#[pyfunction]
pub fn reserve_memoization(
    py: Python<'_>,
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> PyResult<Py<PyAny>> {
    py.detach(|| {
        get_runtime()
            .block_on(async move { reserve_memoization_inner(comp_ctx, memo_fp).await })
            .into_py_result()
    })
}

#[pyfunction]
pub fn reserve_memoization_async<'py>(
    py: Python<'py>,
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        reserve_memoization_inner(comp_ctx, memo_fp)
            .await
            .into_py_result()
    })
}
