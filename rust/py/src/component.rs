use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
};

use crate::context::PyComponentProcessorContext;
use cocoindex_core::engine::{component::ComponentProcessor, context::ComponentProcessorContext};

#[pyclass(name = "ComponentProcessor")]
#[derive(Clone)]
pub struct PyComponentProcessor {
    processor_fn: PyCallback,
}

#[pymethods]
impl PyComponentProcessor {
    #[staticmethod]
    pub fn new_sync(processor_fn: Py<PyAny>) -> Self {
        Self {
            processor_fn: PyCallback::Sync(Arc::new(processor_fn)),
        }
    }

    #[staticmethod]
    pub fn new_async<'py>(processor_fn: Py<PyAny>, async_context: PyAsyncContext) -> Self {
        Self {
            processor_fn: PyCallback::Async {
                async_fn: Arc::new(processor_fn),
                async_context,
            },
        }
    }
}

impl ComponentProcessor<PyEngineProfile> for PyComponentProcessor {
    fn process(
        &self,
        context: &ComponentProcessorContext<PyEngineProfile>,
    ) -> Result<impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static> {
        let py_context = PyComponentProcessorContext(context.clone());
        self.processor_fn.call((py_context,))
    }
}
