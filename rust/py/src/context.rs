use crate::prelude::*;

use crate::{environment::PyEnvironment, state_path::PyStatePath};
use cocoindex_core::engine::context::ComponentProcessorContext;

#[pyclass(name = "ComponentProcessorContext")]
pub struct PyComponentProcessorContext(pub ComponentProcessorContext<PyEngineProfile>);

#[pymethods]
impl PyComponentProcessorContext {
    #[getter]
    fn environment(&self) -> PyEnvironment {
        PyEnvironment(self.0.app_ctx().env.clone())
    }

    #[getter]
    fn state_path(&self) -> PyStatePath {
        PyStatePath(self.0.state_path().clone())
    }
}
