use crate::{
    component::{PyComponentMountHandle, PyComponentProcessor},
    context::{PyComponentProcessorContext, PyFnCallContext},
    prelude::*,
    stable_path::PyStablePath,
};

use cocoindex_core::engine::live_component::LiveComponentController;
use cocoindex_py_utils::from_py_future;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass(name = "LiveComponentController")]
#[derive(Clone)]
pub struct PyLiveComponentController(pub Arc<LiveComponentController<PyEngineProfile>>);

#[pymethods]
impl PyLiveComponentController {
    pub fn update_full_async<'py>(
        &self,
        py: Python<'py>,
        processor: PyComponentProcessor,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctrl = self.0.clone();
        future_into_py(py, async move {
            ctrl.update_full(processor).await.into_py_result()?;
            Ok(())
        })
    }

    pub fn update_async<'py>(
        &self,
        py: Python<'py>,
        stable_path: PyStablePath,
        processor: PyComponentProcessor,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctrl = self.0.clone();
        future_into_py(py, async move {
            let handle = ctrl
                .update(stable_path.0, processor)
                .await
                .into_py_result()?;
            Ok(PyComponentMountHandle::from_handle(handle))
        })
    }

    pub fn delete_async<'py>(
        &self,
        py: Python<'py>,
        stable_path: PyStablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctrl = self.0.clone();
        future_into_py(py, async move {
            let handle = ctrl.delete(stable_path.0).await.into_py_result()?;
            Ok(PyComponentMountHandle::from_handle(handle))
        })
    }

    pub fn mark_ready_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ctrl = self.0.clone();
        future_into_py(py, async move {
            ctrl.mark_ready().await;
            Ok(())
        })
    }

    pub fn start(&self, py: Python<'_>, process_live_fut: Py<PyAny>) -> PyResult<()> {
        // Convert the Python coroutine into a Rust future using from_py_future.
        let host_runtime_ctx = self.0.component().app_ctx().env().host_runtime_ctx();
        let fut = from_py_future(py, &host_runtime_ctx.0, process_live_fut.into_bound(py))?;
        // Wrap to convert PyResult<Py<PyAny>> → Result<()>
        let rust_fut = async move {
            fut.await.from_py_result()?;
            Ok(())
        };
        self.0.start(rust_fut);
        Ok(())
    }

    #[getter]
    pub fn is_live(&self) -> bool {
        self.0.is_live()
    }
}

#[pyfunction]
pub fn mount_live_async<'py>(
    py: Python<'py>,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
    live: bool,
) -> PyResult<Bound<'py, PyAny>> {
    // Sync phase: borrows fn_ctx (only valid for this call).
    let pending = cocoindex_core::engine::live_component::mount_live_prepare(
        &comp_ctx.0,
        &fn_ctx.0,
        stable_path.0,
        live,
    )
    .into_py_result()?;

    // Async phase: no borrows needed, all data is owned.
    future_into_py(py, async move {
        let result = pending.complete().await.into_py_result()?;
        let py_controller = PyLiveComponentController(Arc::new(result.controller));
        let py_handle = PyComponentMountHandle::from_handle(result.readiness_handle);
        Ok((py_controller, py_handle))
    })
}
