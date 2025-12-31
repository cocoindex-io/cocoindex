use crate::prelude::*;

use cocoindex_core::engine::{app::App, runtime::get_runtime};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{component::PyComponentProcessor, environment::PyEnvironment};

#[pyclass(name = "App")]
pub struct PyApp(pub Arc<App<PyEngineProfile>>);

#[pymethods]
impl PyApp {
    #[new]
    pub fn new(name: &str, env: &PyEnvironment) -> PyResult<Self> {
        let app = App::new(name, env.0.clone()).into_py_result()?;
        Ok(Self(Arc::new(app)))
    }

    pub fn run_async<'py>(
        &self,
        py: Python<'py>,
        root_processor: PyComponentProcessor,
    ) -> PyResult<Bound<'py, PyAny>> {
        let app = self.0.clone();
        let fut = future_into_py(py, async move {
            let ret = app.run(root_processor).await.into_py_result()?;
            Ok(ret.into_inner())
        })?;
        Ok(fut)
    }

    pub fn run<'py>(
        &self,
        py: Python<'py>,
        root_processor: PyComponentProcessor,
    ) -> PyResult<Py<PyAny>> {
        let app = self.0.clone();
        py.detach(|| {
            get_runtime().block_on(async move {
                let ret = app.run(root_processor).await.into_py_result()?;
                Ok(ret.into_inner())
            })
        })
    }
}
