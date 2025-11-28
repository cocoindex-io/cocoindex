use crate::prelude::*;

use cocoindex_core::engine::runtime::get_runtime;
use cocoindex_py_utils::from_py_future;
use pyo3::{exceptions::PyException, types::PyTuple};
use pyo3_async_runtimes::TaskLocals;
use tokio_util::task::AbortOnDropHandle;

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

#[derive(Clone)]
pub enum PyCallback {
    Sync(Arc<Py<PyAny>>),
    Async {
        async_fn: Arc<Py<PyAny>>,
        async_context: PyAsyncContext,
    },
}

impl PyCallback {
    pub async fn call<A>(&self, args: A) -> Result<PyResult<Py<PyAny>>>
    where
        A: for<'py> IntoPyObject<'py, Target = PyTuple> + Send + 'static,
    {
        let ret = match self {
            PyCallback::Sync(sync_fn) => {
                let sync_fn = sync_fn.clone();
                let result_fut =
                    AbortOnDropHandle::new(get_runtime().spawn_blocking(move || {
                        Python::with_gil(|py| sync_fn.call(py, args, None))
                    }));
                result_fut.await?
            }
            PyCallback::Async {
                async_fn,
                async_context,
            } => {
                let result_fut = Python::with_gil(|py| {
                    let result_coro = async_fn.call(py, args, None)?;
                    from_py_future(py, &async_context.0, result_coro.into_bound(py))
                })?;
                result_fut.await
            }
        };
        Ok(ret)
    }
}
