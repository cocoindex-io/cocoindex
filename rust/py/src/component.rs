use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
};

use crate::context::PyComponentProcessorContext;
use cocoindex_core::engine::{component::ComponentProcessor, context::ComponentProcessorContext};

#[pyclass(name = "ComponentProcessor")]
#[derive(Clone)]
pub struct PyComponentProcessor {
    builder_fn: PyCallback,
}

#[pymethods]
impl PyComponentProcessor {
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

impl ComponentProcessor<PyEngineProfile> for PyComponentProcessor {
    async fn build(
        &self,
        context: &ComponentProcessorContext<PyEngineProfile>,
    ) -> Result<PyResult<Py<PyAny>>> {
        let py_context = PyComponentProcessorContext(context.clone());
        self.builder_fn.call((py_context,)).await
    }
}
