use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

use cocoindex_core::engine::effect::{
    EffectProvider, EffectReconcileOutput, EffectReconciler, EffectSink,
};
use pyo3::exceptions::PyException;
use pyo3::types::PyList;

use crate::context::PyComponentBuilderContext;
use crate::prelude::*;

use crate::runtime::{PyAsyncContext, PyCallback};
use crate::state_path::PyStatePath;
use crate::value::PyKey;

static NON_EXISTENCE: OnceLock<Py<PyAny>> = OnceLock::new();

#[pyfunction]
pub fn init_module(non_existence: Py<PyAny>) -> PyResult<()> {
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
    #[new]
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

impl EffectSink<Py<PyAny>> for PyEffectSink {
    async fn apply(&self, actions: Vec<Py<PyAny>>) -> Result<()> {
        self.callback.call((actions,)).await??;
        Ok(())
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

impl EffectReconciler for PyEffectReconciler {
    type Key = crate::value::PyKey;
    type State = Arc<Py<PyAny>>;
    type Action = Py<PyAny>;
    type Sink = PyEffectSink;
    type Decl = Py<PyAny>;

    fn reconcile(
        &self,
        key: Self::Key,
        desired_effect: Option<Self::Decl>,
        prev_possible_states: &[Self::State],
        prev_may_be_missing: bool,
    ) -> Result<EffectReconcileOutput<Self>> {
        let output = Python::with_gil(|py| -> PyResult<_> {
            let prev_possible_states =
                PyList::new(py, prev_possible_states.iter().map(|s| s.bind(py)))?;
            let desired_effect = match &desired_effect {
                Some(d) => d,
                None => NON_EXISTENCE
                    .get()
                    .ok_or_else(|| PyException::new_err("Effect module not initialized"))?,
            };
            let output = self.sync_fn.call(
                py,
                (
                    key.value().bind(py),
                    desired_effect,
                    prev_possible_states,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let (state, action, sink) = output.extract::<(Py<PyAny>, Py<PyAny>, Self::Sink)>(py)?;
            Ok(EffectReconcileOutput {
                state: Arc::new(state),
                action,
                sink,
            })
        })?;
        Ok(output)
    }
}

#[pyclass(name = "EffectProvider")]
pub struct PyEffectProvider(EffectProvider<PyEffectReconciler>);

#[pyfunction]
pub fn declare_effect<'py>(
    py: Python<'py>,
    state_path: &'py PyStatePath,
    context: &'py PyComponentBuilderContext,
    provider: &PyEffectProvider,
    decl: Py<PyAny>,
    key: Py<PyAny>,
    child_reconciler: Option<&'py PyEffectReconciler>,
) -> PyResult<Option<PyEffectProvider>> {
    let py_key = PyKey::new(py, Arc::new(key))?;
    let output = cocoindex_core::engine::effect::declare_effect(
        &state_path.0,
        &context.0,
        &provider.0,
        decl,
        py_key,
        child_reconciler.map(|r| r.clone_ref(py)),
    )
    .into_py_result()?;
    Ok(output.map(|p| PyEffectProvider(p)))
}
