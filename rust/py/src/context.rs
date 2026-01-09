use crate::fingerprint::PyFingerprint;
use crate::prelude::*;

use crate::{environment::PyEnvironment, stable_path::PyStablePath};
use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};

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
}
