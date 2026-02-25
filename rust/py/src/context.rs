use crate::fingerprint::PyFingerprint;
use crate::prelude::*;
use crate::stable_path::PyStableKey;

use crate::{environment::PyEnvironment, stable_path::PyStablePath};
use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};
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

    fn join_fn_call(&self, fn_ctx: &PyFnCallContext) -> PyResult<()> {
        self.0.join_fn_call(&fn_ctx.0);
        Ok(())
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
    pub fn new() -> Self {
        Self(FnCallContext::default())
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

    pub fn add_logic_dep(&self, fp: PyFingerprint) -> PyResult<()> {
        self.0.add_logic_dep(fp.0);
        Ok(())
    }
}
