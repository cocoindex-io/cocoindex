use crate::prelude::*;

use cocoindex_core::engine::runtime::get_runtime;
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::TaskLocals;

#[pyfunction]
pub fn init_runtime() -> PyResult<()> {
    if let Err(_) = pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()) {
        return Err(PyException::new_err(
            "Failed to initialize Tokio runtime: already initialized",
        ));
    }
    Ok(())
}

#[pyclass(name = "AsyncContext")]
#[derive(Clone)]
pub struct PyAsyncContext(pub Arc<TaskLocals>);

#[pymethods]
impl PyAsyncContext {
    #[new]
    pub fn new(event_loop: Bound<PyAny>) -> Self {
        Self(Arc::new(pyo3_async_runtimes::TaskLocals::new(event_loop)))
    }
}
