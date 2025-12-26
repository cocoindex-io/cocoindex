use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex, OnceLock};

use cocoindex_core::engine::effect::{
    ChildEffectDef, EffectHandler, EffectProvider, EffectProviderRegistry, EffectReconcileOutput,
    EffectSink,
};
use cocoindex_core::state::effect_path::EffectPath;
use pyo3::exceptions::PyException;
use pyo3::types::{PyList, PySequence};

use crate::context::PyComponentProcessorContext;
use crate::prelude::*;

use crate::runtime::{PyAsyncContext, PyCallback};
use crate::value::{PyKey, PyValue};

static NON_EXISTENCE: OnceLock<Py<PyAny>> = OnceLock::new();

#[pyfunction]
pub fn init_effect_module(non_existence: Py<PyAny>) -> PyResult<()> {
    NON_EXISTENCE.set(non_existence).map_err(|_| {
        PyException::new_err("Failed to initialize effect module: already initialized")
    })?;
    Ok(())
}

#[pyclass(name = "EffectSink")]
#[derive(Clone)]
pub struct PyEffectSink {
    key: usize,
    callback: PyCallback,
}

#[pymethods]
impl PyEffectSink {
    #[staticmethod]
    pub fn new_sync(callback: Py<PyAny>) -> Self {
        Self {
            key: callback.as_ptr() as usize,
            callback: PyCallback::Sync(Arc::new(callback)),
        }
    }

    #[staticmethod]
    pub fn new_async(callback: Py<PyAny>, async_context: PyAsyncContext) -> Self {
        Self {
            key: callback.as_ptr() as usize,
            callback: PyCallback::Async {
                async_fn: Arc::new(callback),
                async_context,
            },
        }
    }
}

impl PartialEq for PyEffectSink {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for PyEffectSink {}

impl Hash for PyEffectSink {
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
impl EffectSink<PyEngineProfile> for PyEffectSink {
    async fn apply(
        &self,
        actions: Vec<Py<PyAny>>,
    ) -> Result<Option<Vec<Option<ChildEffectDef<PyEngineProfile>>>>> {
        let ret = self.callback.call((actions,))?.await?;
        Python::attach(|py| -> PyResult<_> {
            if ret.is_none(py) {
                return Ok(None);
            }
            let seq = ret.bind(py).cast::<PySequence>()?;
            let len = seq.len()? as usize;
            let mut results: Vec<Option<ChildEffectDef<PyEngineProfile>>> = Vec::with_capacity(len);
            for i in 0..len {
                let obj = seq.get_item(i)?;
                if obj.is_none() {
                    results.push(None);
                } else {
                    // Extract handler from ChildEffectDef NamedTuple
                    let (handler,) = obj.extract::<(Py<PyAny>,)>()?;
                    results.push(Some(ChildEffectDef {
                        handler: PyEffectHandler(handler),
                    }));
                }
            }
            Ok(Some(results))
        })
        .from_py_result()
    }
}

#[pyclass(name = "EffectHandler")]
pub struct PyEffectHandler(Py<PyAny>);

impl EffectHandler<PyEngineProfile> for PyEffectHandler {
    fn reconcile(
        &self,
        key: PyKey,
        desired_effect: Option<Py<PyAny>>,
        prev_possible_states: &[PyValue],
        prev_may_be_missing: bool,
    ) -> Result<Option<EffectReconcileOutput<PyEngineProfile>>> {
        Python::attach(|py| -> PyResult<_> {
            let prev_possible_states =
                PyList::new(py, prev_possible_states.iter().map(|s| s.value().bind(py)))?;
            let non_existence = NON_EXISTENCE
                .get()
                .ok_or_else(|| PyException::new_err("Effect module not initialized"))?;
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
                Some(EffectReconcileOutput {
                    action,
                    sink: get_core_field(py, sink)?.extract::<PyEffectSink>(py)?,
                    state: if non_existence.is(&state) {
                        None
                    } else {
                        Some(PyValue::new(Arc::new(state)))
                    },
                })
            };
            Ok(output)
        })
        .from_py_result()
    }
}

#[pyclass(name = "EffectProvider")]
pub struct PyEffectProvider(EffectProvider<PyEngineProfile>);

#[pyfunction]
pub fn declare_effect<'py>(
    py: Python<'py>,
    context: &'py PyComponentProcessorContext,
    provider: &PyEffectProvider,
    key: Py<PyAny>,
    value: Py<PyAny>,
) -> PyResult<()> {
    let py_key = PyKey::new(py, key).into_py_result()?;
    cocoindex_core::engine::execution::declare_effect(
        &context.0,
        provider.0.clone(),
        py_key,
        value,
    )
    .into_py_result()?;
    Ok(())
}

#[pyfunction]
pub fn declare_effect_with_child<'py>(
    py: Python<'py>,
    context: &'py PyComponentProcessorContext,
    provider: &PyEffectProvider,
    key: Py<PyAny>,
    value: Py<PyAny>,
) -> PyResult<PyEffectProvider> {
    let py_key = PyKey::new(py, key).into_py_result()?;
    let output = cocoindex_core::engine::execution::declare_effect_with_child(
        &context.0,
        provider.0.clone(),
        py_key,
        value,
    )
    .into_py_result()?;
    Ok(PyEffectProvider(output))
}

static ROOT_EFFECT_PROVIDER_REGISTRY: LazyLock<
    Arc<Mutex<EffectProviderRegistry<PyEngineProfile>>>,
> = LazyLock::new(Default::default);

pub fn root_effect_provider_registry()
-> &'static Arc<Mutex<EffectProviderRegistry<PyEngineProfile>>> {
    &ROOT_EFFECT_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_effect_provider(
    name: String,
    handler: Py<PyAny>,
) -> PyResult<PyEffectProvider> {
    let provider = root_effect_provider_registry()
        .lock()
        .unwrap()
        .register(
            EffectPath::new(
                utils::fingerprint::Fingerprint::from(&name).into_py_result()?,
                None,
            ),
            PyEffectHandler(handler),
        )
        .into_py_result()?;
    Ok(PyEffectProvider(provider))
}
