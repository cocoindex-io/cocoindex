use std::sync::OnceLock;

use crate::prelude::*;

use cocoindex_core::engine::runtime::get_runtime;
use cocoindex_py_utils::from_py_future;
use futures::FutureExt;
use pyo3::{call::PyCallArgs, exceptions::PyException};
use pyo3_async_runtimes::TaskLocals;
use tokio_util::task::AbortOnDropHandle;

pub struct PythonFunctions {
    pub serialize_fn: Py<PyAny>,
    pub deserialize_fn: Py<PyAny>,
}

impl PythonFunctions {
    pub fn serialize<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<bytes::Bytes> {
        self.serialize_fn
            .call(py, (value,), None)?
            .extract::<bytes::Bytes>(py)
            .into_py_result()
    }
    pub fn deserialize<'py>(&self, py: Python<'py>, value: &[u8]) -> PyResult<Py<PyAny>> {
        self.deserialize_fn.call(py, (value,), None)
    }
}

static PY_FUNCTIONS: OnceLock<PythonFunctions> = OnceLock::new();

#[pyfunction]
pub fn init_runtime(serialize_fn: Py<PyAny>, deserialize_fn: Py<PyAny>) -> PyResult<()> {
    if let Err(_) = pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()) {
        return Err(PyException::new_err(
            "Failed to initialize Tokio runtime: already initialized",
        ));
    }
    PY_FUNCTIONS
        .set(PythonFunctions {
            serialize_fn,
            deserialize_fn,
        })
        .map_err(|_| PyException::new_err("Failed to set Python functions: already initialized"))?;
    Ok(())
}

pub fn python_functions() -> &'static PythonFunctions {
    PY_FUNCTIONS
        .get()
        .expect("Python functions not initialized")
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
    pub fn call<A>(
        &self,
        args: A,
    ) -> Result<impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static>
    where
        A: for<'py> PyCallArgs<'py> + Send + 'static,
    {
        let boxed_fut = match self {
            PyCallback::Sync(sync_fn) => {
                let sync_fn = sync_fn.clone();
                let result_fut = AbortOnDropHandle::new(
                    get_runtime()
                        .spawn_blocking(move || Python::attach(|py| sync_fn.call(py, args, None))),
                );
                async move {
                    result_fut.await.map_err(|err| {
                        PyException::new_err(format!("Failed to call Python function: {err:?}"))
                    })?
                }
                .boxed()
            }
            PyCallback::Async {
                async_fn,
                async_context,
            } => Python::attach(|py| {
                let result_coro = async_fn.call(py, args, None)?;
                from_py_future(py, &async_context.0, result_coro.into_bound(py))
            })?
            .boxed(),
        };
        Ok(boxed_fut)
    }
}
