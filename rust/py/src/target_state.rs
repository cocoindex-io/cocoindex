use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex};

use cocoindex_core::engine::target_state::{
    ChildTargetDef, TargetActionSink, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    TargetStateProviderRegistry,
};
use cocoindex_core::state::target_state_path::TargetStatePath;
use pyo3::types::{PyList, PySequence};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::prelude::*;

use crate::runtime::{PyAsyncContext, PyCallback, python_objects};
use crate::value::{PyKey, PyValue};

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
        actions: Vec<Py<PyAny>>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<PyEngineProfile>>>>> {
        let ret = self.callback.call(host_runtime_ctx, (actions,))?.await?;
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
        key: Arc<PyKey>,
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
                    key.value().bind(py),
                    desired_effect.as_ref().unwrap_or(non_existence).bind(py),
                    prev_possible_states,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let output = if py_output.is_none(py) {
                None
            } else {
                let (action, sink, state) =
                    py_output.extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>)>(py)?;
                Some(TargetReconcileOutput {
                    action,
                    sink: get_core_field(py, sink)?.extract::<PyTargetActionSink>(py)?,
                    tracking_record: if non_existence.is(&state) {
                        None
                    } else {
                        Some(PyValue::new(state))
                    },
                })
            };
            Ok(output)
        })
        .from_py_result()
    }
}

#[pyclass(name = "TargetStateProvider")]
pub struct PyTargetStateProvider(TargetStateProvider<PyEngineProfile>);

#[pymethods]
impl PyTargetStateProvider {
    pub fn coco_memo_key(&self) -> String {
        self.0.target_state_path().to_string()
    }
}

#[pyfunction]
pub fn declare_target_state<'py>(
    py: Python<'py>,
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: Py<PyAny>,
    value: Py<PyAny>,
) -> PyResult<()> {
    let py_key = PyKey::new(py, key).into_py_result()?;
    cocoindex_core::engine::execution::declare_target_state(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        Arc::new(py_key),
        value,
    )
    .into_py_result()?;
    Ok(())
}

#[pyfunction]
pub fn declare_target_state_with_child<'py>(
    py: Python<'py>,
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: Py<PyAny>,
    value: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let py_key = PyKey::new(py, key).into_py_result()?;
    let output = cocoindex_core::engine::execution::declare_target_state_with_child(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        Arc::new(py_key),
        value,
    )
    .into_py_result()?;
    Ok(PyTargetStateProvider(output))
}

static ROOT_TARGET_STATE_PROVIDER_REGISTRY: LazyLock<
    Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>>,
> = LazyLock::new(Default::default);

pub fn root_target_states_provider_registry()
-> &'static Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>> {
    &ROOT_TARGET_STATE_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_target_states_provider(
    name: String,
    handler: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let provider = root_target_states_provider_registry()
        .lock()
        .unwrap()
        .register(
            TargetStatePath::new(
                utils::fingerprint::Fingerprint::from(&name).into_py_result()?,
                None,
            ),
            PyTargetHandler(handler),
        )
        .into_py_result()?;
    Ok(PyTargetStateProvider(provider))
}
