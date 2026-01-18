use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
    stable_path::PyStablePath,
};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::fingerprint::PyFingerprint;
use cocoindex_core::engine::{
    component::{
        ComponentExecutionHandle, ComponentMountRunHandle, ComponentProcessor,
        ComponentProcessorInfo,
    },
    context::ComponentProcessorContext,
    runtime::get_runtime,
};
use pyo3_async_runtimes::tokio::future_into_py;

/// Python wrapper for ComponentProcessorInfo that shares the same Arc instance.
#[pyclass(name = "ComponentProcessorInfo")]
#[derive(Clone)]
pub struct PyComponentProcessorInfo(pub Arc<ComponentProcessorInfo>);

#[pymethods]
impl PyComponentProcessorInfo {
    #[new]
    pub fn new(name: String) -> Self {
        Self(Arc::new(ComponentProcessorInfo::new(name)))
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.0.name
    }
}

#[pyclass(name = "ComponentProcessor")]
#[derive(Clone)]
pub struct PyComponentProcessor {
    processor_fn: PyCallback,
    memo_key_fingerprint: Option<utils::fingerprint::Fingerprint>,
    processor_info: Arc<ComponentProcessorInfo>,
}

#[pymethods]
impl PyComponentProcessor {
    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None))]
    pub fn new_sync(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Sync(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None))]
    pub fn new_async(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Async(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
        }
    }
}

impl ComponentProcessor<PyEngineProfile> for PyComponentProcessor {
    fn process(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        comp_ctx: &ComponentProcessorContext<PyEngineProfile>,
    ) -> Result<impl Future<Output = Result<crate::value::PyValue>> + Send + 'static> {
        let py_context = PyComponentProcessorContext(comp_ctx.clone());
        let fut = self.processor_fn.call(host_runtime_ctx, (py_context,))?;
        Ok(async move {
            let value = fut.await?;
            Ok(crate::value::PyValue::new(value))
        })
    }

    fn memo_key_fingerprint(&self) -> Option<utils::fingerprint::Fingerprint> {
        self.memo_key_fingerprint
    }

    fn processor_info(&self) -> &ComponentProcessorInfo {
        &self.processor_info
    }
}

#[pyfunction]
pub fn mount_run(
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
) -> PyResult<PyComponentMountRunHandle> {
    let component = comp_ctx
        .0
        .component()
        .mount_child(&fn_ctx.0, stable_path.0)
        .into_py_result()?;
    let child_ctx = component
        .new_processor_context_for_build(Some(&comp_ctx.0), comp_ctx.0.processing_stats().clone())
        .into_py_result()?;
    let handle = component.run(processor, child_ctx).into_py_result()?;
    Ok(PyComponentMountRunHandle(Some(handle)))
}

#[pyfunction]
pub fn mount(
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
) -> PyResult<PyComponentMountHandle> {
    let component = comp_ctx
        .0
        .component()
        .mount_child(&fn_ctx.0, stable_path.0)
        .into_py_result()?;
    let child_ctx = component
        .new_processor_context_for_build(Some(&comp_ctx.0), comp_ctx.0.processing_stats().clone())
        .into_py_result()?;
    let handle = component
        .run_in_background(processor, child_ctx)
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
        &mut self,
        py: Python<'py>,
        parent_ctx: PyComponentProcessorContext,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.take_handle()?;
        future_into_py(py, async move {
            let ret = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
            Ok(ret.into_inner())
        })
    }

    pub fn result<'py>(
        &mut self,
        py: Python<'py>,
        parent_ctx: PyComponentProcessorContext,
    ) -> PyResult<Py<PyAny>> {
        let handle = self.take_handle()?;
        py.detach(|| {
            get_runtime().block_on(async move {
                let ret = handle.result(Some(&parent_ctx.0)).await.into_py_result()?;
                Ok(ret.into_inner())
            })
        })
    }
}

#[pyclass(name = "ComponentMountHandle")]
pub struct PyComponentMountHandle(Option<ComponentExecutionHandle>);

impl PyComponentMountHandle {
    fn take_handle(&mut self) -> PyResult<ComponentExecutionHandle> {
        self.0.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Handle has already been consumed")
        })
    }
}

#[pymethods]
impl PyComponentMountHandle {
    pub fn ready_async<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.take_handle()?;
        future_into_py(py, async move { handle.ready().await.into_py_result() })
    }

    pub fn wait_until_ready<'py>(&mut self, py: Python<'py>) -> PyResult<()> {
        let handle = self.take_handle()?;
        py.detach(|| get_runtime().block_on(async move { handle.ready().await.into_py_result() }))
    }
}
