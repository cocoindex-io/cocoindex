use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
};

use crate::context::PyComponentBuilderContext;
use cocoindex_core::engine::{component::ComponentBuilder, context::ComponentBuilderContext};

#[pyclass(name = "ComponentBuilder")]
#[derive(Clone)]
pub struct PyComponentBuilder {
    builder_fn: PyCallback,
}

#[pymethods]
impl PyComponentBuilder {
    #[staticmethod]
    pub fn new_sync(builder_fn: Py<PyAny>) -> Self {
        Self {
            builder_fn: PyCallback::Sync(Arc::new(builder_fn)),
        }
    }

    #[staticmethod]
    pub fn new_async<'py>(builder_fn: Py<PyAny>, async_context: PyAsyncContext) -> Self {
        Self {
            builder_fn: PyCallback::Async {
                async_fn: Arc::new(builder_fn),
                async_context,
            },
        }
    }
}

impl ComponentBuilder<PyEngineProfile> for PyComponentBuilder {
    async fn build(
        &self,
        context: &ComponentBuilderContext<PyEngineProfile>,
    ) -> Result<PyResult<Py<PyAny>>> {
        let py_context = PyComponentBuilderContext(context.clone());
        self.builder_fn.call((py_context,)).await
    }
}
