use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};

use cocoindex_py_utils::{AnyhowIntoPyResult, Pythonized};
use pyo3::{prelude::*, pyclass};

#[pyclass(name = "Environment")]
struct PyEnvironment(Environment);

#[pymethods]
impl PyEnvironment {
    #[new]
    pub fn new(settings: Pythonized<EnvironmentSettings>) -> PyResult<Self> {
        let settings = settings.into_inner();
        let environment = Environment::new(settings).into_py_result()?;
        Ok(Self(environment))
    }
}

#[pymodule]
#[pyo3(name = "_core")]
fn core_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEnvironment>()?;
    Ok(())
}
