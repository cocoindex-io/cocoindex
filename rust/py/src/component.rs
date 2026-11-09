use std::collections::HashMap;

use crate::{
    deadline::PyDeadlineContext,
    function::{
        PyRefreshedDataDep, build_memo_states_payload, context_memo_states_to_pydict,
        data_deps_to_pylist,
    },
    prelude::*,
    runtime::{PyAsyncContext, PyCallback, build_on_error},
    stable_path::{PyStableKey, PyStablePath},
    value::PyStoredValue,
};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::fingerprint::PyFingerprint;
use cocoindex_core::engine::{
    component::{
        ComponentExecutionHandle, ComponentMountRunHandle, ComponentProcessor,
        ComponentProcessorInfo,
    },
    context::{ComponentProcessorContext, MemoStatesPayload},
    runtime::get_runtime,
};
use pyo3_async_runtimes::tokio::future_into_py;

/// Python wrapper for ComponentProcessorInfo that shares the same Arc instance.
#[pyclass(name = "ComponentProcessorInfo", from_py_object)]
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

#[pyclass(name = "ComponentProcessor", from_py_object)]
#[derive(Clone)]
pub struct PyComponentProcessor {
    processor_fn: PyCallback,
    memo_key_fingerprint: Option<utils::fingerprint::Fingerprint>,
    processor_info: Arc<ComponentProcessorInfo>,
    state_handler: Option<PyCallback>,
    /// See `ComponentProcessor::collects_initial_states`.
    collects_initial_states: bool,
}

#[pymethods]
impl PyComponentProcessor {
    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None, state_handler=None, collects_initial_states=true))]
    pub fn new_sync(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
        state_handler: Option<Py<PyAny>>,
        collects_initial_states: bool,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Sync(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
            state_handler: state_handler.map(|h| PyCallback::Async(Arc::new(h))),
            collects_initial_states,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (processor_fn, processor_info, memo_key_fingerprint=None, state_handler=None, collects_initial_states=true))]
    pub fn new_async(
        processor_fn: Py<PyAny>,
        processor_info: PyComponentProcessorInfo,
        memo_key_fingerprint: Option<PyFingerprint>,
        state_handler: Option<Py<PyAny>>,
        collects_initial_states: bool,
    ) -> Self {
        Self {
            processor_fn: PyCallback::Async(Arc::new(processor_fn)),
            memo_key_fingerprint: memo_key_fingerprint.map(|f| f.0),
            processor_info: processor_info.0,
            state_handler: state_handler.map(|h| PyCallback::Async(Arc::new(h))),
            collects_initial_states,
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

    fn collects_initial_states(&self) -> bool {
        self.state_handler.is_some() && self.collects_initial_states
    }

    fn handle_memo_states(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        comp_ctx: &ComponentProcessorContext<PyEngineProfile>,
        stored_states: Option<MemoStatesPayload<PyEngineProfile>>,
    ) -> Result<
        impl Future<Output = Result<(MemoStatesPayload<PyEngineProfile>, bool, bool)>> + Send + 'static,
    > {
        let Some(state_handler) = &self.state_handler else {
            return Ok(futures::future::Either::Left(async {
                Ok((MemoStatesPayload::default(), true, false))
            }));
        };

        // Marshal the stored payload to Python as three separate args:
        // `positional_stored: list[PyStoredValue] | None`,
        // `context_stored: dict[Fingerprint, list[PyStoredValue]] | None` and
        // `data_deps_stored: list[tuple[...]] | None` (see `data_deps_to_pylist`).
        // All are `None` on cache miss, all non-None on cache hit.
        let py_comp_ctx = PyComponentProcessorContext(comp_ctx.clone());
        let (py_positional, py_context, py_data_deps): (Py<PyAny>, Py<PyAny>, Py<PyAny>) =
            Python::attach(|py| -> Result<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
                match stored_states {
                    Some(payload) => {
                        let positional_list = pyo3::types::PyList::new(
                            py,
                            payload
                                .positional
                                .iter()
                                .map(|s| pyo3::Py::new(py, s.clone()).unwrap()),
                        )
                        .from_py_result()?;
                        let context_dict =
                            context_memo_states_to_pydict(py, &payload.by_context_fp)
                                .from_py_result()?;
                        let data_deps_list =
                            data_deps_to_pylist(py, &payload.data_deps).from_py_result()?;
                        Ok((
                            positional_list.unbind().into_any(),
                            context_dict.unbind().into_any(),
                            data_deps_list.unbind().into_any(),
                        ))
                    }
                    None => Ok((py.None(), py.None(), py.None())),
                }
            })?;

        let fut = state_handler.call(
            host_runtime_ctx,
            (py_comp_ctx, py_positional, py_context, py_data_deps),
        )?;

        Ok(futures::future::Either::Right(async move {
            let result = fut.await?;
            Python::attach(|py| -> PyResult<_> {
                // Handler returns a flat tuple starting with
                // (new_positional, new_context_dict, can_reuse, states_changed,
                // new_data_deps); see `StateMethodsResult` in function.py.
                let result = result.bind(py);
                let tuple = result.cast::<pyo3::types::PyTuple>()?;
                let positional: Vec<Py<PyAny>> = tuple.get_item(0)?.extract()?;
                let context_entries: HashMap<PyFingerprint, Vec<Py<PyAny>>> =
                    tuple.get_item(1)?.extract()?;
                let can_reuse: bool = tuple.get_item(2)?.extract()?;
                let states_changed: bool = tuple.get_item(3)?.extract()?;
                let data_deps: Vec<PyRefreshedDataDep> = tuple.get_item(4)?.extract()?;

                let new_payload = build_memo_states_payload(
                    py,
                    Some(positional),
                    Some(context_entries),
                    Some(data_deps),
                )?;

                Ok((new_payload, can_reuse, states_changed))
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
    deadline: PyDeadlineContext,
) -> PyResult<Bound<'py, PyAny>> {
    let child = comp_ctx
        .0
        .component()
        .mount_child(&fn_ctx.0, stable_path.0)
        .into_py_result()?;
    future_into_py(py, async move {
        let handle = child
            .use_mount(&comp_ctx.0, processor, deadline.0)
            .await
            .into_py_result()?;
        Ok(PyComponentMountRunHandle(Some(handle)))
    })
}

/// Whether the update's selector leaves out a mount at `stable_path`, so that
/// the caller prepares nothing for it. See `ComponentProcessorContext::skips_mount`.
#[pyfunction]
pub fn skips_mount(
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
) -> PyResult<bool> {
    comp_ctx
        .0
        .skips_mount(&fn_ctx.0, &stable_path.0)
        .into_py_result()
}

/// The item keys a `mount_each` at `stable_path` can run: `None` for every
/// key. See `ComponentProcessorContext::mount_each_keys`.
#[pyfunction]
pub fn mount_each_keys(
    py: Python<'_>,
    stable_path: PyStablePath,
    comp_ctx: PyComponentProcessorContext,
    fn_ctx: &PyFnCallContext,
) -> PyResult<Option<Vec<Py<PyAny>>>> {
    comp_ctx
        .0
        .mount_each_keys(&fn_ctx.0, &stable_path.0)
        .map(|keys| {
            keys.into_iter()
                .map(|key| PyStableKey(key).into_pyobject(py).map(|b| b.unbind()))
                .collect()
        })
        .transpose()
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

    let host_runtime_ctx = comp_ctx.0.app_ctx().env().host_runtime_ctx().clone();
    let on_error = build_on_error(host_runtime_ctx, handler_callback);

    future_into_py(py, async move {
        let handle = child
            .mount(&comp_ctx.0, processor, on_error)
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
