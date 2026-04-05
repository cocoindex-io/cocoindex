use crate::{
    prelude::*,
    runtime::{PyAsyncContext, PyCallback},
    stable_path::PyStablePath,
    value::PyStoredValue,
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
    state_handler: Option<PyCallback>,
}

#[pymethods]
impl PyComponentProcessor {
    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None, state_handler=None))]
    pub fn new_sync(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
        state_handler: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Sync(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
            state_handler: state_handler.map(|h| PyCallback::Async(Arc::new(h))),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None, state_handler=None))]
    pub fn new_async(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
        state_handler: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Async(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
            state_handler: state_handler.map(|h| PyCallback::Async(Arc::new(h))),
        }
    }
}

impl ComponentProcessor<PyEngineProfile> for PyComponentProcessor {
    fn process(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        comp_ctx: &ComponentProcessorContext<PyEngineProfile>,
    ) -> Result<impl Future<Output = Result<crate::value::PyStoredValue>> + Send + 'static> {
        let py_context = PyComponentProcessorContext(comp_ctx.clone());
        let fut = self.processor_fn.call(host_runtime_ctx, (py_context,))?;
        Ok(async move {
            let value = fut.await?;
            Ok(crate::value::PyStoredValue::new(value))
        })
    }

    fn memo_key_fingerprint(&self) -> Option<utils::fingerprint::Fingerprint> {
        self.memo_key_fingerprint
    }

    fn processor_info(&self) -> &ComponentProcessorInfo {
        &self.processor_info
    }

    fn has_memo_state_handler(&self) -> bool {
        self.state_handler.is_some()
    }

    fn handle_memo_states(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        comp_ctx: &ComponentProcessorContext<PyEngineProfile>,
        stored_states: Option<Vec<crate::value::PyStoredValue>>,
    ) -> Result<
        impl Future<Output = Result<(Vec<crate::value::PyStoredValue>, bool, bool)>> + Send + 'static,
    > {
        let Some(state_handler) = &self.state_handler else {
            return Ok(futures::future::Either::Left(async {
                Ok((vec![], true, false))
            }));
        };

        // Convert Option<Vec<PyStoredValue>> → Python (list[PyStoredValue] | None)
        let py_comp_ctx = PyComponentProcessorContext(comp_ctx.clone());
        let py_arg: Py<PyAny> = Python::attach(|py| -> Result<Py<PyAny>> {
            match stored_states {
                Some(states) => {
                    let list = pyo3::types::PyList::new(
                        py,
                        states.iter().map(|s| pyo3::Py::new(py, s.clone()).unwrap()),
                    )
                    .from_py_result()?;
                    Ok(list.unbind().into_any())
                }
                None => Ok(py.None()),
            }
        })?;

        let fut = state_handler.call(host_runtime_ctx, (py_comp_ctx, py_arg))?;

        Ok(futures::future::Either::Right(async move {
            let result = fut.await?;
            Python::attach(|py| -> PyResult<_> {
                let result = result.bind(py);
                let tuple = result.cast::<pyo3::types::PyTuple>()?;
                let states_list = tuple.get_item(0)?;
                let can_reuse: bool = tuple.get_item(1)?.extract()?;
                let states_changed: bool = tuple.get_item(2)?.extract()?;

                let new_states: Vec<crate::value::PyStoredValue> = states_list
                    .cast::<pyo3::types::PyList>()?
                    .iter()
                    .map(|item| crate::value::PyStoredValue::new(item.unbind()))
                    .collect();

                Ok((new_states, can_reuse, states_changed))
            })
            .from_py_result()
        }))
    }
}

#[pyfunction]
pub fn use_mount_async<'py>(
    py: Python<'py>,
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
) -> PyResult<Bound<'py, PyAny>> {
    let child = comp_ctx
        .0
        .component()
        .mount_child(&fn_ctx.0, stable_path.0)
        .into_py_result()?;
    future_into_py(py, async move {
        let handle = child
            .use_mount(&comp_ctx.0, processor)
            .await
            .into_py_result()?;
        Ok(PyComponentMountRunHandle(Some(handle)))
    })
}

#[pyfunction]
#[pyo3(signature = (processor, stable_path, comp_ctx, fn_ctx, handler_callback=None))]
pub fn mount_async<'py>(
    py: Python<'py>,
    processor: PyComponentProcessor,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
    handler_callback: Option<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let child = comp_ctx
        .0
        .component()
        .mount_child(&fn_ctx.0, stable_path.0)
        .into_py_result()?;

    let on_error = handler_callback.map(|handler_callback| {
        // Capture host runtime context so we can run an async Python callback.
        let host_runtime_ctx = comp_ctx.0.app_ctx().env().host_runtime_ctx().clone();
        let cb = PyCallback::Async(Arc::new(handler_callback));

        let on_error: Arc<
            dyn Fn(
                    Error,
                )
                    -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>
                + Send
                + Sync
                + 'static,
        > = Arc::new(move |err: Error| {
            let cb = cb.clone();
            let host_runtime_ctx = host_runtime_ctx.clone();
            Box::pin(async move {
                let err_str = format!("{err:?}");
                let fut = match cb.call(&host_runtime_ctx, (err_str,)) {
                    Ok(fut) => fut,
                    Err(e) => {
                        error!("exception handler dispatch failed:\n{e:?}");
                        return;
                    }
                };
                if let Err(e) = fut.await {
                    error!("exception handler failed:\n{e:?}");
                };
            })
        });
        on_error
    });

    future_into_py(py, async move {
        let handle = child
            .mount(&comp_ctx.0, processor, on_error, None)
            .await
            .into_py_result()?;
        Ok(PyComponentMountHandle(Some(handle)))
    })
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
            handle.result(Some(&parent_ctx.0)).await.into_py_result()
        })
    }

    pub fn result(
        &mut self,
        py: Python<'_>,
        parent_ctx: PyComponentProcessorContext,
    ) -> PyResult<PyStoredValue> {
        let handle = self.take_handle()?;
        py.detach(|| {
            get_runtime()
                .block_on(async move { handle.result(Some(&parent_ctx.0)).await.into_py_result() })
        })
    }
}

#[pyclass(name = "ComponentMountHandle")]
pub struct PyComponentMountHandle(Option<ComponentExecutionHandle>);

impl PyComponentMountHandle {
    pub fn from_handle(handle: ComponentExecutionHandle) -> Self {
        Self(Some(handle))
    }

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
