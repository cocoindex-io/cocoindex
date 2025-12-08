use crate::prelude::*;

use cocoindex_core::engine::{app::App, runtime::get_runtime};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{component::PyComponentProcessor, environment::PyEnvironment};

#[pyclass(name = "App")]
pub struct PyApp(Arc<App<PyEngineProfile>>);

#[pymethods]
impl PyApp {
    #[new]
    pub fn new(
        name: &str,
        env: &PyEnvironment,
        root_component_builder: PyComponentProcessor,
    ) -> PyResult<Self> {
        let app = App::new(name, env.0.clone(), root_component_builder).into_py_result()?;
        Ok(Self(Arc::new(app)))
    }

    pub fn update_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let app = self.0.clone();
        let fut = future_into_py(py, async move { app.update().await.into_py_result()? })?;
        Ok(fut)
    }

    pub fn update<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let app = self.0.clone();
        py.detach(|| get_runtime().block_on(async move { app.update().await.into_py_result()? }))
    }
}
