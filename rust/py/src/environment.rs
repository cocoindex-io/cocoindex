use crate::fingerprint::PyFingerprint;
use crate::value::PyStoredValue;
use crate::{prelude::*, target_state::root_target_states_provider_registry};

use crate::runtime::PyAsyncContext;
use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_py_utils::Pythonized;

#[pyclass(name = "Environment")]
pub struct PyEnvironment(pub Environment<PyEngineProfile>);

#[pymethods]
impl PyEnvironment {
    #[new]
    pub fn new(
        settings: Pythonized<EnvironmentSettings>,
        async_context: PyAsyncContext,
    ) -> PyResult<Self> {
        let settings = settings.into_inner();
        let environment = Environment::<PyEngineProfile>::new(
            settings,
            root_target_states_provider_registry().clone(),
            async_context,
        )
        .into_py_result()?;
        Ok(Self(environment))
    }

    pub fn register_logic(&self, fp: PyFingerprint) {
        self.0.register_logic(fp.0);
    }

    pub fn unregister_logic(&self, fp: PyFingerprint) {
        self.0.unregister_logic(&fp.0);
    }

    /// Register the eager initial memo states for a tracked context value.
    /// Called from `ContextProvider.provide()` after the value's state
    /// functions have been evaluated with `NON_EXISTENCE`.
    pub fn register_context_initial_states(&self, fp: PyFingerprint, states: Vec<Py<PyAny>>) {
        let wrapped: Vec<PyStoredValue> = states.into_iter().map(PyStoredValue::new).collect();
        self.0.register_context_initial_states(fp.0, wrapped);
    }

    /// Remove the initial states for a tracked context fingerprint.
    /// Called on re-provide (when a context key is provided with a new
    /// value whose fingerprint differs).
    pub fn unregister_context_initial_states(&self, fp: PyFingerprint) {
        self.0.unregister_context_initial_states(&fp.0);
    }
}
