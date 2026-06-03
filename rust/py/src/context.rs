use crate::fingerprint::PyFingerprint;
use crate::function::context_initial_states_to_pydict;
use crate::prelude::*;
use crate::stable_path::PyStableKey;

use crate::app::PyStatsGroupHandle;
use crate::{environment::PyEnvironment, stable_path::PyStablePath};
use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};
use cocoindex_core::state::stable_path::StableKey;
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

    /// Open a stats group rooted at this context. Returns the derived context
    /// (whose mounts aggregate into the group, split out of the enclosing
    /// scope) and a `StatsGroupHandle` for `stats()`/`watch()`.
    #[pyo3(signature = (title, report_to_stdout, refresh_interval_secs=None))]
    fn begin_stats_group(
        &self,
        title: String,
        report_to_stdout: bool,
        refresh_interval_secs: Option<f64>,
    ) -> (PyComponentProcessorContext, PyStatsGroupHandle) {
        let (derived, stats) =
            self.0
                .begin_stats_group(title, report_to_stdout, refresh_interval_secs);
        (
            PyComponentProcessorContext(derived),
            PyStatsGroupHandle::new(stats),
        )
    }

    /// Close the group (stop registering members). Non-blocking — readiness
    /// resolves asynchronously once the members finish.
    fn end_stats_group(&self) {
        self.0.end_stats_group();
    }

    /// Collect eager initial memo states for the change-detection context fingerprints
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

    /// Declare a persistent state key for this component build.
    ///
    /// Returns the previously stored bytes for `key` if available, otherwise
    /// `initial_value`. Raises if the same key is declared more than once
    /// within the same component run.
    fn use_state(&self, key: String, initial_value: Vec<u8>) -> PyResult<Vec<u8>> {
        let stable_key = StableKey::Symbol(key.into());
        self.0.use_state(stable_key, initial_value).into_py_result()
    }

    /// Update the value for an already-declared state key.
    ///
    /// Raises if `key` was not declared via `use_state` in this component run.
    fn update_user_state(&self, key: String, value: Vec<u8>) -> PyResult<()> {
        let stable_key = StableKey::Symbol(key.into());
        self.0
            .update_user_state(&stable_key, value)
            .into_py_result()
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

    pub fn add_context_change_dep(&self, fp: PyFingerprint) -> PyResult<()> {
        self.0.add_context_change_dep(fp.0);
        Ok(())
    }

    /// Collect eager initial memo states for the change-detection context fingerprints
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
