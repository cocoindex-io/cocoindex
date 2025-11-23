use crate::prelude::*;

use cocoindex_core::engine::{
    component::ComponentBuilder, context::ComponentBuilderContext, runtime::get_runtime,
};
use tokio::task::JoinHandle;

use crate::context::PyComponentBuilderContext;

#[pyclass(name = "ComponentBuilder")]
#[derive(Clone)]
pub struct PyComponentBuilder {
    builder_fn: PyComponentBuilderFn,
}

#[derive(Clone)]
enum PyComponentBuilderFn {
    Sync(Arc<Py<PyAny>>),
}

#[pymethods]
impl PyComponentBuilder {
    #[staticmethod]
    pub fn new_sync(builder_fn: Py<PyAny>) -> Self {
        Self {
            builder_fn: PyComponentBuilderFn::Sync(Arc::new(builder_fn)),
        }
    }
}

impl ComponentBuilder for PyComponentBuilder {
    type HostStateCtx = Arc<Py<PyAny>>;
    type BuildRet = Py<PyAny>;
    type BuildErr = PyErr;

    fn build(
        &self,
        context: &Arc<ComponentBuilderContext>,
    ) -> Result<JoinHandle<Result<Self::BuildRet, Self::BuildErr>>> {
        let py_context = PyComponentBuilderContext(context.clone());
        let handle = match &self.builder_fn {
            PyComponentBuilderFn::Sync(builder_fn) => {
                let builder_fn = builder_fn.clone();
                get_runtime().spawn_blocking(move || {
                    Python::with_gil(|py| builder_fn.call(py, (py_context,), None))
                })
            }
        };
        Ok(handle)
    }
}
