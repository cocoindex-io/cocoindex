use crate::fingerprint::PyFingerprint;
use crate::function::context_initial_states_to_pydict;
use crate::prelude::*;
use crate::stable_path::PyStableKey;

use crate::{environment::PyEnvironment, stable_path::PyStablePath};
use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass(name = "ComponentProcessorContext")]
#[derive(Clone)]
pub struct PyComponentProcessorContext(pub ComponentProcessorContext<PyEngineProfile>);

#[pymethods]
impl PyComponentProcessorContext {
    #[getter]
    fn environment(&self) -> PyEnvironment {
        PyEnvironment(self.0.app_ctx().env().clone())
    }

    #[getter]
    fn stable_path(&self) -> PyStablePath {
        PyStablePath(self.0.stable_path().clone())
    }

    #[getter]
    fn live(&self) -> bool {
        self.0.live()
    }

    fn join_fn_call(&self, fn_ctx: &PyFnCallContext) -> PyResult<()> {
        self.0.join_fn_call(&fn_ctx.0);
        Ok(())
    }

    /// Collect eager initial memo states for the tracked-context fingerprints
    /// observed so far on this component (from `logic_deps`). Returns a
    /// `dict[Fingerprint, list[Any]]` directly — fingerprints with no
    /// registered state functions are skipped. Values are the *raw* Python
    /// state objects (not `StoredValue` wrappers), ready to be passed into
    /// `guard.resolve(..., context_memo_states=...)` without double wrapping.
    ///
    /// Used on cache miss to populate the new entry's `context_memo_states`
    /// without snapshotting `logic_deps` to Python.
    fn initial_context_memo_states<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let entries = self.0.collect_context_initial_states();
        context_initial_states_to_pydict(py, &entries)
    }

    /// Get the next ID for the given key.
    ///
    /// Args:
    ///     key: Optional stable key for the ID sequencer. If None, uses a default sequencer.
    ///
    /// Returns:
    ///     A coroutine that resolves to the next unique ID as an integer.
    #[pyo3(signature = (key=None))]
    fn next_id<'py>(
        &self,
        py: Python<'py>,
        key: Option<PyStableKey>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let app_ctx = self.0.app_ctx().clone();
        future_into_py(py, async move {
            let id = app_ctx
                .next_id(key.as_ref().map(|k| &k.0))
                .await
                .into_py_result()?;
            Ok(id)
        })
    }
}

#[pyclass(name = "FnCallContext")]
pub struct PyFnCallContext(pub FnCallContext);

#[pymethods]
impl PyFnCallContext {
    #[new]
    #[pyo3(signature = (*, propagate_children_fn_logic=true))]
    pub fn new(propagate_children_fn_logic: bool) -> Self {
        Self(FnCallContext::new(propagate_children_fn_logic))
    }

    pub fn join_child(&self, child_fn_ctx: &PyFnCallContext) -> PyResult<()> {
        self.0.join_child(&child_fn_ctx.0);
        Ok(())
    }

    pub fn join_child_memo(&self, memo_fp: PyFingerprint) -> PyResult<()> {
        self.0.update(|inner| {
            inner.dependency_memo_entries.insert(memo_fp.0);
        });
        Ok(())
    }

    pub fn add_fn_logic_dep(&self, fp: PyFingerprint) -> PyResult<()> {
        self.0.add_fn_logic_dep(fp.0);
        Ok(())
    }

    pub fn add_context_tracked_dep(&self, fp: PyFingerprint) -> PyResult<()> {
        self.0.add_context_tracked_dep(fp.0);
        Ok(())
    }

    /// Collect eager initial memo states for the tracked-context fingerprints
    /// captured in this fn call context, by looking them up in the given
    /// environment's registry. Returns a `dict[Fingerprint, list[Any]]`
    /// directly — fingerprints with no registered state functions are
    /// skipped. Values are the *raw* Python state objects (not `StoredValue`
    /// wrappers), ready to be passed into
    /// `guard.resolve(..., context_memo_states=...)` without double wrapping.
    ///
    /// Used on cache miss in the function-level memoization path to populate
    /// a new memo entry's `context_memo_states`.
    pub fn initial_context_memo_states<'py>(
        &self,
        py: Python<'py>,
        env: &PyEnvironment,
    ) -> PyResult<Bound<'py, PyDict>> {
        let entries = self.0.collect_context_initial_states(&env.0);
        context_initial_states_to_pydict(py, &entries)
    }
}
