use crate::prelude::*;

use crate::{environment::PyEnvironment, state_path::PyStatePath};
use cocoindex_core::engine::context::ComponentBuilderContext;

#[pyclass(name = "ComponentBuilderContext")]
pub struct PyComponentBuilderContext(pub Arc<ComponentBuilderContext>);

#[pymethods]
impl PyComponentBuilderContext {
    #[getter]
    fn environment(&self) -> PyEnvironment {
        PyEnvironment(self.0.app_ctx.env.clone())
    }

    #[getter]
    fn state_path(&self) -> PyStatePath {
        PyStatePath(self.0.state_path.clone())
    }
}
