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
}
