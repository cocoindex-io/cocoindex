use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, OnceLock};

use cocoindex_core::engine::context::DeclaredEffect;
use cocoindex_core::engine::effect::{
    EffectProvider, EffectReconcileOutput, EffectReconciler, EffectSink, RootEffectProviderRegistry,
};
use pyo3::exceptions::PyException;
use pyo3::types::PyList;

use crate::context::PyComponentBuilderContext;
use crate::prelude::*;

use crate::runtime::{PyAsyncContext, PyCallback};
use crate::state_path::PyStatePath;
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

impl EffectSink<PyEngineProfile> for PyEffectSink {
    async fn apply(&self, actions: Vec<Py<PyAny>>) -> PyResult<()> {
        let ret = self.callback.call((actions,)).await;
        match ret {
            Ok(ret) => ret.map(|_| ()),
            Err(e) => Err(PyException::new_err(format!("{e:?}"))),
        }
    }
}

#[pyclass(name = "EffectReconciler")]
pub struct PyEffectReconciler {
    sync_fn: Py<PyAny>,
}

#[pymethods]
impl PyEffectReconciler {
    #[staticmethod]
    pub fn new_sync(sync_fn: Py<PyAny>) -> Self {
        Self { sync_fn }
    }
}

impl PyEffectReconciler {
    fn clone_ref(&self, py: Python<'_>) -> PyEffectReconciler {
        Self {
            sync_fn: self.sync_fn.clone_ref(py),
        }
    }
}

impl EffectReconciler<PyEngineProfile> for PyEffectReconciler {
    fn reconcile(
        &self,
        key: PyKey,
        desired_effect: Option<Py<PyAny>>,
        prev_possible_states: &[PyValue],
        prev_may_be_missing: bool,
    ) -> PyResult<EffectReconcileOutput<PyEngineProfile>> {
        let output = Python::attach(|py| -> PyResult<_> {
            let prev_possible_states =
                PyList::new(py, prev_possible_states.iter().map(|s| s.value().bind(py)))?;
            let non_existence = NON_EXISTENCE
                .get()
                .ok_or_else(|| PyException::new_err("Effect module not initialized"))?;
            let output = self.sync_fn.call(
                py,
                (
                    key.value().bind(py),
                    desired_effect.as_ref().unwrap_or(non_existence).bind(py),
                    prev_possible_states,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let (action, sink, state) =
                output.extract::<(Py<PyAny>, PyEffectSink, Py<PyAny>)>(py)?;
            Ok(EffectReconcileOutput {
                action,
                sink,
                state: if non_existence.is(&state) {
                    None
                } else {
                    Some(PyValue::new(Arc::new(state)))
                },
            })
        })?;
        Ok(output)
    }
}

#[pyclass(name = "EffectProvider")]
pub struct PyEffectProvider(EffectProvider<PyEngineProfile>);

#[pyfunction]
pub fn declare_effect<'py>(
    py: Python<'py>,
    state_path: &'py PyStatePath,
    context: &'py PyComponentBuilderContext,
    provider: &PyEffectProvider,
    key: Py<PyAny>,
    decl: Py<PyAny>,
    child_reconciler: Option<&'py PyEffectReconciler>,
) -> PyResult<Option<PyEffectProvider>> {
    let py_key = PyKey::new(py, key)?;
    let output = cocoindex_core::engine::effect_exec::declare_effect(
        &context.0,
        DeclaredEffect {
            mounted_state_path: state_path.0.clone(),
            provider: provider.0.clone(),
            key: py_key,
            decl,
        },
        child_reconciler.map(|r| r.clone_ref(py)),
    )
    .into_py_result()?;
    Ok(output.map(|p| PyEffectProvider(p)))
}

static ROOT_EFFECT_PROVIDER_REGISTRY: LazyLock<RootEffectProviderRegistry<PyEngineProfile>> =
    LazyLock::new(|| RootEffectProviderRegistry::new());

pub fn root_effect_provider_registry() -> &'static RootEffectProviderRegistry<PyEngineProfile> {
    &ROOT_EFFECT_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_effect_provider(
    py: Python<'_>,
    name: String,
    reconciler: &PyEffectReconciler,
) -> PyResult<PyEffectProvider> {
    let provider = root_effect_provider_registry()
        .register(name, reconciler.clone_ref(py))
        .into_py_result()?;
    Ok(PyEffectProvider(provider))
}
