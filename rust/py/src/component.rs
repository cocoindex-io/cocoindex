use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
    stable_path::PyStablePath,
};

use crate::context::PyComponentProcessorContext;
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
            let result = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
            result.into_py_result()
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
                let result = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
                result.into_py_result()
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
