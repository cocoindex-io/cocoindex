use crate::prelude::*;

use cocoindex_core::engine::{
    app::{App, AppDropOptions, AppUpdateOptions},
    runtime::get_runtime,
};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{component::PyComponentProcessor, environment::PyEnvironment};

#[pyclass(name = "App")]
pub struct PyApp(pub Arc<App<PyEngineProfile>>);

#[pymethods]
impl PyApp {
    #[new]
    #[pyo3(signature = (name, env, max_inflight_components=None))]
    pub fn new(
        name: &str,
        env: &PyEnvironment,
        max_inflight_components: Option<usize>,
    ) -> PyResult<Self> {
        let app = App::new(name, env.0.clone(), max_inflight_components).into_py_result()?;
        Ok(Self(Arc::new(app)))
    }

    pub fn update_async<'py>(
        &self,
        py: Python<'py>,
        root_processor: PyComponentProcessor,
        report_to_stdout: bool,
        full_reprocess: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            report_to_stdout,
            full_reprocess,
        };
        let fut = future_into_py(py, async move {
            let ret = app.update(root_processor, options).await.into_py_result()?;
            Ok(ret.into_inner())
        })?;
        Ok(fut)
    }

    pub fn update<'py>(
        &self,
        py: Python<'py>,
        root_processor: PyComponentProcessor,
        report_to_stdout: bool,
        full_reprocess: bool,
    ) -> PyResult<Py<PyAny>> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            report_to_stdout,
            full_reprocess,
        };
        py.detach(|| {
            get_runtime().block_on(async move {
                let ret = app.update(root_processor, options).await.into_py_result()?;
                Ok(ret.into_inner())
            })
        })
    }

    pub fn drop_async<'py>(
        &self,
        py: Python<'py>,
        report_to_stdout: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let app = self.0.clone();
        let options = AppDropOptions { report_to_stdout };
        let fut = future_into_py(py, async move {
            app.drop_app(options).await.into_py_result()?;
            Ok(())
        })?;
        Ok(fut)
    }

    pub fn drop<'py>(&self, py: Python<'py>, report_to_stdout: bool) -> PyResult<()> {
        let app = self.0.clone();
        let options = AppDropOptions { report_to_stdout };
        py.detach(|| {
            get_runtime().block_on(async move { app.drop_app(options).await.into_py_result() })
        })
    }
}
