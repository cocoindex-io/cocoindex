use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::sync::{LazyLock, Mutex};

use cocoindex_core::engine::target_state::{
    ChildInvalidation, ChildTargetDef, TargetActionSink, TargetHandler, TargetReconcileOutput,
    TargetStateProvider, TargetStateProviderRegistry,
};
use pyo3::types::{PyList, PySequence, PyTuple};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::prelude::*;

use crate::stable_path::PyStableKey;

use crate::runtime::{PyAsyncContext, PyCallback, python_objects};
use crate::value::PyValue;

#[pyclass(name = "TargetActionSink")]
#[derive(Clone)]
pub struct PyTargetActionSink {
    key: usize,
    callback: PyCallback,
}

#[pymethods]
impl PyTargetActionSink {
    #[staticmethod]
    pub fn new_sync(callback: Py<PyAny>) -> Self {
        Self {
            key: callback.as_ptr() as usize,
            callback: PyCallback::Sync(Arc::new(callback)),
        }
    }

    #[staticmethod]
    pub fn new_async(callback: Py<PyAny>) -> Self {
        Self {
            key: callback.as_ptr() as usize,
            callback: PyCallback::Async(Arc::new(callback)),
        }
    }
}

impl PartialEq for PyTargetActionSink {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PyTargetActionSink {}

impl Hash for PyTargetActionSink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

fn get_core_field(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let core_obj = obj.getattr(py, "_core")?;
    let core_py = core_obj.extract::<Py<PyAny>>(py)?;
    Ok(core_py)
}

#[async_trait]
impl TargetActionSink<PyEngineProfile> for PyTargetActionSink {
    async fn apply(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        host_ctx: Arc<Py<PyAny>>,
        actions: Vec<Py<PyAny>>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<PyEngineProfile>>>>> {
        let context_provider = Python::attach(|py| host_ctx.as_ref().clone_ref(py));
        let ret = self
            .callback
            .call(host_runtime_ctx, (context_provider, actions))?
            .await?;
        Python::attach(|py| -> PyResult<_> {
            if ret.is_none(py) {
                return Ok(None);
            }
            let seq = ret.bind(py).cast::<PySequence>()?;
            let len = seq.len()? as usize;
            let mut results: Vec<Option<ChildTargetDef<PyEngineProfile>>> = Vec::with_capacity(len);
            for i in 0..len {
                let obj = seq.get_item(i)?;
                if obj.is_none() {
                    results.push(None);
                } else {
                    // Extract handler from ChildTargetDef NamedTuple
                    let (handler,) = obj.extract::<(Py<PyAny>,)>()?;
                    results.push(Some(ChildTargetDef {
                        handler: PyTargetHandler(handler),
                    }));
                }
            }
            Ok(Some(results))
        })
        .from_py_result()
    }
}

#[pyclass(name = "TargetHandler")]
pub struct PyTargetHandler(Py<PyAny>);

impl TargetHandler<PyEngineProfile> for PyTargetHandler {
    fn reconcile(
        &self,
        key: cocoindex_core::state::stable_path::StableKey,
        desired_effect: Option<Py<PyAny>>,
        prev_possible_states: &[PyValue],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<PyEngineProfile>>> {
        Python::attach(|py| -> PyResult<_> {
            let prev_possible_states =
                PyList::new(py, prev_possible_states.iter().map(|s| s.value().bind(py)))?;
            let non_existence = &python_objects().non_existence;
            let py_output = self.0.call_method(
                py,
                "reconcile",
                (
                    PyStableKey(key),
                    desired_effect.as_ref().unwrap_or(non_existence).bind(py),
                    prev_possible_states,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let output = if py_output.is_none(py) {
                None
            } else {
                let (action, sink, state, py_child_invalidation) =
                    py_output.extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)>(py)?;
                let child_invalidation = if py_child_invalidation.is_none(py) {
                    None
                } else {
                    let s = py_child_invalidation.extract::<String>(py)?;
                    match s.as_str() {
                        "destructive" => Some(ChildInvalidation::Destructive),
                        "lossy" => Some(ChildInvalidation::Lossy),
                        other => {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Invalid child_invalidation value: {other:?}"
                            )));
                        }
                    }
                };
                Some(TargetReconcileOutput {
                    action,
                    sink: get_core_field(py, sink)?.extract::<PyTargetActionSink>(py)?,
                    tracking_record: if non_existence.is(&state) {
                        None
                    } else {
                        Some(PyValue::new(state))
                    },
                    child_invalidation,
                })
            };
            Ok(output)
        })
        .from_py_result()
    }

    fn attachment(&self, att_type: &str) -> Result<Option<PyTargetHandler>> {
        Python::attach(|py| -> PyResult<_> {
            let obj = self.0.bind(py);
            if !obj.hasattr("attachment")? {
                return Ok(None);
            }
            let result = obj.call_method1("attachment", (att_type,))?;
            if result.is_none() {
                return Ok(None);
            }
            Ok(Some(PyTargetHandler(result.unbind())))
        })
        .from_py_result()
    }
}

#[pyclass(name = "TargetStateProvider")]
pub struct PyTargetStateProvider(TargetStateProvider<PyEngineProfile>);

#[pymethods]
impl PyTargetStateProvider {
    pub fn coco_memo_key(&self) -> String {
        let path = self.0.target_state_path().to_string();
        match self.0.provider_generation() {
            Some(g) => format!("{}[{},{}]", path, g.provider_id, g.provider_schema_version),
            None => path,
        }
    }

    pub fn stable_key_chain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let chain = self.0.stable_key_chain();
        let py_keys: Vec<Bound<'py, PyAny>> = chain
            .into_iter()
            .map(|k| PyStableKey(k).into_pyobject(py))
            .collect::<Result<_, _>>()?;
        PyTuple::new(py, py_keys)
    }

    pub fn register_attachment_provider(
        &self,
        comp_ctx: &PyComponentProcessorContext,
        att_type: &str,
    ) -> PyResult<PyTargetStateProvider> {
        let provider = self
            .0
            .register_attachment_provider(&comp_ctx.0, att_type)
            .into_py_result()?;
        Ok(PyTargetStateProvider(provider))
    }
}

#[pyfunction]
pub fn declare_target_state<'py>(
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<()> {
    cocoindex_core::engine::execution::declare_target_state(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        value,
    )
    .into_py_result()?;
    Ok(())
}

#[pyfunction]
pub fn declare_target_state_with_child<'py>(
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let output = cocoindex_core::engine::execution::declare_target_state_with_child(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        value,
    )
    .into_py_result()?;
    Ok(PyTargetStateProvider(output))
}

static ROOT_TARGET_STATE_PROVIDER_REGISTRY: LazyLock<
    ManuallyDrop<Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>>>,
> = LazyLock::new(|| ManuallyDrop::new(Default::default()));

pub fn root_target_states_provider_registry()
-> &'static Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>> {
    &**ROOT_TARGET_STATE_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_target_states_provider(
    name: String,
    handler: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let provider = root_target_states_provider_registry()
        .lock()
        .unwrap()
        .register_root(name, PyTargetHandler(handler))
        .into_py_result()?;
    Ok(PyTargetStateProvider(provider))
}
