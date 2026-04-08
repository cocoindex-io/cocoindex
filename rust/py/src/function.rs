use std::collections::HashMap;

use cocoindex_core::engine::context::MemoStatesPayload;
use cocoindex_core::engine::function::FnCallMemoGuard;
use cocoindex_core::engine::runtime::get_runtime;
use cocoindex_utils::fingerprint::Fingerprint;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::fingerprint::PyFingerprint;
use crate::prelude::*;
use crate::value::PyStoredValue;

/// Build a `MemoStatesPayload<PyEngineProfile>` from the Python-side inputs.
///
/// `context_entries` is a Python `dict[Fingerprint, list[Any]]` extracted by
/// pyo3 directly into a `HashMap` — no intermediate list-of-tuples.
pub(crate) fn build_memo_states_payload(
    positional: Option<Vec<Py<PyAny>>>,
    context_entries: Option<HashMap<PyFingerprint, Vec<Py<PyAny>>>>,
) -> MemoStatesPayload<PyEngineProfile> {
    let positional = positional
        .unwrap_or_default()
        .into_iter()
        .map(PyStoredValue::new)
        .collect();
    let by_context_fp = context_entries
        .unwrap_or_default()
        .into_iter()
        .map(|(fp, values)| {
            (
                fp.0,
                values
                    .into_iter()
                    .map(PyStoredValue::new)
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    MemoStatesPayload {
        positional,
        by_context_fp,
    }
}

/// Convert the stored context-borne memo states into a Python `dict`
/// `{Fingerprint: list[PyStoredValue]}` keyed by fingerprint for O(1) lookup
/// on the Python side.
///
/// Used by the cache-hit validation path, which calls `.get(deserializer)`
/// on each value to extract a typed Python object from stored bytes or a
/// cached Python object reference.
pub(crate) fn context_memo_states_to_pydict<'py>(
    py: Python<'py>,
    entries: &[(Fingerprint, Vec<PyStoredValue>)],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (fp, values) in entries {
        let values_list = PyList::new(py, values.iter().map(|v| Py::new(py, v.clone()).unwrap()))?;
        let fp_obj = Py::new(py, PyFingerprint(*fp))?;
        dict.set_item(fp_obj, values_list)?;
    }
    Ok(dict)
}

/// Convert the eager initial memo states into a Python `dict`
/// `{Fingerprint: list[Any]}` — each inner value is the *raw* Python object
/// that was originally stored, not a `StoredValue` wrapper.
///
/// Used by the cache-miss path to populate `guard.resolve(..., context_memo_states=...)`
/// which wraps the raw Python objects into `PyStoredValue` for storage. The
/// intermediate `PyStoredValue` instances that the registry holds are
/// transparently unwrapped here so the downstream path doesn't double-wrap
/// (which would leave the outer wrapper's `object` field pointing at a
/// Python `StoredValue` that msgspec can't serialize).
///
/// # Invariant
///
/// Every value stored in the registry was constructed via
/// `PyStoredValue::new(raw_py_obj)` inside `PyEnvironment::register_context_initial_states`,
/// which sets `object: Some(..)`. Nothing downstream ever calls `from_bytes`
/// on these values (which is the only way to construct a bytes-only
/// `PyStoredValue`), and `to_bytes` merely *caches* bytes alongside the
/// existing object reference — it never clears `object`. Therefore
/// `object_ref()` always returns `Some` here.
pub(crate) fn context_initial_states_to_pydict<'py>(
    py: Python<'py>,
    entries: &[(Fingerprint, Vec<PyStoredValue>)],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (fp, values) in entries {
        let unwrapped: Vec<Py<PyAny>> = values
            .iter()
            .map(|v| match v.object_ref(py) {
                Some(obj) => obj,
                None => unreachable!(
                    "context_initial_states registry holds a PyStoredValue \
                     with no Python object reference — see invariant in \
                     context_initial_states_to_pydict doc comment"
                ),
            })
            .collect();
        let values_list = PyList::new(py, unwrapped)?;
        let fp_obj = Py::new(py, PyFingerprint(*fp))?;
        dict.set_item(fp_obj, values_list)?;
    }
    Ok(dict)
}

/// Python-facing guard for a function-call memo entry.
///
/// Always holds a write lock on the underlying memo entry. On cache hits,
/// `cached_value`, `cached_memo_states`, and `cached_context_memo_states` are
/// pre-extracted owned copies. Call `resolve()` after (re-)execution, or
/// `close()` / drop to release.
#[pyclass(name = "FnCallMemoGuard")]
pub struct PyFnCallMemoGuard {
    guard: Option<FnCallMemoGuard<PyEngineProfile>>,
    is_cached: bool,
    cached_value: Option<Py<PyAny>>,
    cached_memo_states: Option<Py<PyAny>>, // Python list
    cached_context_memo_states: Option<Py<PyAny>>, // Python dict[Fingerprint, list]
}

#[pymethods]
impl PyFnCallMemoGuard {
    /// Whether this guard represents a cache hit (true) or miss (false).
    ///
    /// When true, `cached_value`, `cached_memo_states`, and
    /// `cached_context_memo_states` hold the stored results. Note: `cached_value`
    /// itself may be Python `None` (e.g. for functions returning `None`), so use
    /// `is_cached` rather than `cached_value is not None` to distinguish cache
    /// hits from misses.
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

    #[getter]
    pub fn cached_context_memo_states(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.cached_context_memo_states
            .as_ref()
            .map(|s| s.clone_ref(py))
    }

    /// Update memo states on a cache hit without re-execution.
    ///
    /// Used when the state function says `can_reuse=True` but the state value has changed.
    #[pyo3(signature = (memo_states=None, context_memo_states=None))]
    pub fn update_memo_states(
        &mut self,
        memo_states: Option<Vec<Py<PyAny>>>,
        context_memo_states: Option<HashMap<PyFingerprint, Vec<Py<PyAny>>>>,
    ) -> PyResult<()> {
        let payload = build_memo_states_payload(memo_states, context_memo_states);
        if let Some(ref mut guard) = self.guard {
            guard.update_memo_states(payload);
        }
        Ok(())
    }

    #[pyo3(signature = (fn_ctx, ret, memo_states=None, context_memo_states=None))]
    pub fn resolve(
        &mut self,
        fn_ctx: &PyFnCallContext,
        ret: Py<PyAny>,
        memo_states: Option<Vec<Py<PyAny>>>,
        context_memo_states: Option<HashMap<PyFingerprint, Vec<Py<PyAny>>>>,
    ) -> PyResult<bool> {
        let payload = build_memo_states_payload(memo_states, context_memo_states);
        let resolved = if let Some(guard) = self.guard.take() {
            guard
                .resolve(&fn_ctx.0, || PyStoredValue::new(ret), payload)
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
        // Extract cached data (if cache hit) as PyStoredValue objects (not inner Python objects).
        let (is_cached, cached_value, cached_memo_states, cached_context_memo_states) = match guard
            .cached()
        {
            Some(cached) => {
                let value = Py::new(py, cached.ret.clone())?.into_any();
                let states_list = PyList::new(
                    py,
                    cached
                        .memo_states
                        .iter()
                        .map(|s| Py::new(py, s.clone()).unwrap()),
                )?;
                let context_dict = context_memo_states_to_pydict(py, cached.context_memo_states)?;
                (
                    true,
                    Some(value),
                    Some(states_list.unbind().into_any()),
                    Some(context_dict.unbind().into_any()),
                )
            }
            None => (false, None, None, None),
        };

        let py_guard = PyFnCallMemoGuard {
            guard: Some(guard),
            is_cached,
            cached_value,
            cached_memo_states,
            cached_context_memo_states,
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
