use crate::prelude::*;

use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_py_utils::{AnyhowIntoPyResult, Pythonized};

#[pyclass(name = "Environment")]
pub struct PyEnvironment(pub Environment);

#[pymethods]
impl PyEnvironment {
    #[new]
    pub fn new(settings: Pythonized<EnvironmentSettings>) -> PyResult<Self> {
        let settings = settings.into_inner();
        let environment = Environment::new(settings).into_py_result()?;
        Ok(Self(environment))
    }
}
