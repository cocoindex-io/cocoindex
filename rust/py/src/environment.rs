use crate::{effect::root_effect_provider_registry, prelude::*};

use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_py_utils::Pythonized;

#[pyclass(name = "Environment")]
pub struct PyEnvironment(pub Environment<PyEngineProfile>);

#[pymethods]
impl PyEnvironment {
    #[new]
    pub fn new(settings: Pythonized<EnvironmentSettings>) -> PyResult<Self> {
        let settings = settings.into_inner();
        let environment =
            Environment::<PyEngineProfile>::new(settings, root_effect_provider_registry().clone())
                .into_py_result()?;
        Ok(Self(environment))
    }
}
