use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
    stable_path::PyStablePath,
};

use crate::context::PyComponentProcessorContext;
use crate::fingerprint::PyFingerprint;
use cocoindex_core::engine::{
    component::{ComponentMountHandle, ComponentMountRunHandle, ComponentProcessor},
    context::ComponentProcessorContext,
    runtime::get_runtime,
};
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass(name = "ComponentProcessor")]
#[derive(Clone)]
pub struct PyComponentProcessor {
    processor_fn: PyCallback,
    memo_key_fingerprint: Option<utils::fingerprint::Fingerprint>,
}

#[pymethods]
impl PyComponentProcessor {
    #[staticmethod]
    #[pyo3(signature = (processor_fn, memo_key_fingerprint=None))]
    pub fn new_sync(processor_fn: Py<PyAny>, memo_key_fingerprint: Option<PyFingerprint>) -> Self {
        Self {
            processor_fn: PyCallback::Sync(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (processor_fn, memo_key_fingerprint=None))]
    pub fn new_async(processor_fn: Py<PyAny>, memo_key_fingerprint: Option<PyFingerprint>) -> Self {
        Self {
            processor_fn: PyCallback::Async(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
        }
    }
}

impl ComponentProcessor<PyEngineProfile> for PyComponentProcessor {
    fn process(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        context: &ComponentProcessorContext<PyEngineProfile>,
    ) -> Result<impl Future<Output = Result<crate::value::PyValue>> + Send + 'static> {
        let py_context = PyComponentProcessorContext(context.clone());
        let fut = self.processor_fn.call(host_runtime_ctx, (py_context,))?;
        Ok(async move {
            let value = fut.await?;
            Ok(crate::value::PyValue::new(value))
        })
    }

    fn memo_key_fingerprint(&self) -> Option<utils::fingerprint::Fingerprint> {
        self.memo_key_fingerprint
    }
}

#[pyfunction]
pub fn mount_run(
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    parent_ctx: PyComponentProcessorContext,
) -> PyResult<PyComponentMountRunHandle> {
    let component = parent_ctx.0.component().get_child(stable_path.0);
    let handle = component
        .run(processor, Some(parent_ctx.0))
        .into_py_result()?;
    Ok(PyComponentMountRunHandle(Some(handle)))
}

#[pyfunction]
pub fn mount(
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    parent_ctx: PyComponentProcessorContext,
) -> PyResult<PyComponentMountHandle> {
    let component = parent_ctx.0.component().get_child(stable_path.0);
    let handle = component
        .run_in_background(processor, Some(parent_ctx.0))
        .into_py_result()?;
    Ok(PyComponentMountHandle(Some(handle)))
}

#[pyclass(name = "ComponentMountRunHandle")]
pub struct PyComponentMountRunHandle(Option<ComponentMountRunHandle<PyEngineProfile>>);

impl PyComponentMountRunHandle {
    fn take_handle(&mut self) -> PyResult<ComponentMountRunHandle<PyEngineProfile>> {
        self.0.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Handle has already been consumed")
        })
    }
}

#[pymethods]
impl PyComponentMountRunHandle {
    pub fn result_async<'py>(
        slf: Bound<'py, Self>,
        parent_ctx: PyComponentProcessorContext,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let handle = slf.borrow_mut().take_handle()?;
        future_into_py(py, async move {
            let ret = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
            Ok(ret.into_inner())
        })
    }

    pub fn result<'py>(
        mut slf: PyRefMut<'py, Self>,
        parent_ctx: PyComponentProcessorContext,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let handle = slf.take_handle()?;
        py.detach(|| {
            get_runtime().block_on(async move {
                let ret = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
                Ok(ret.into_inner())
            })
        })
    }
}

#[pyclass(name = "ComponentMountHandle")]
pub struct PyComponentMountHandle(Option<ComponentMountHandle>);

impl PyComponentMountHandle {
    fn take_handle(&mut self) -> PyResult<ComponentMountHandle> {
        self.0.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Handle has already been consumed")
        })
    }
}

#[pymethods]
impl PyComponentMountHandle {
    pub fn ready_async<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let handle = slf.borrow_mut().take_handle()?;
        future_into_py(py, async move { handle.ready().await.into_py_result() })
    }

    pub fn ready<'py>(mut slf: PyRefMut<'py, Self>) -> PyResult<()> {
        let py = slf.py();
        let handle = slf.take_handle()?;
        py.detach(|| get_runtime().block_on(async move { handle.ready().await.into_py_result() }))
    }
}
