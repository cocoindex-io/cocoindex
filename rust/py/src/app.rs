use crate::prelude::*;

use cocoindex_core::engine::{
    app::{App, AppDropOptions, AppUpdateOptions, UpdateHandle},
    runtime::get_runtime,
    stats::{ProcessingStats, VersionedProcessingStats},
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::watch;

use crate::{component::PyComponentProcessor, environment::PyEnvironment, value::PyStoredValue};

fn snapshot_to_py<'py>(
    py: Python<'py>,
    versioned: &VersionedProcessingStats,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, group) in &versioned.stats {
        let group_dict = PyDict::new(py);
        group_dict.set_item("num_execution_starts", group.num_execution_starts)?;
        group_dict.set_item("num_unchanged", group.num_unchanged)?;
        group_dict.set_item("num_adds", group.num_adds)?;
        group_dict.set_item("num_deletes", group.num_deletes)?;
        group_dict.set_item("num_reprocesses", group.num_reprocesses)?;
        group_dict.set_item("num_errors", group.num_errors)?;
        dict.set_item(name, group_dict)?;
    }
    Ok(dict)
}

#[pyclass(name = "UpdateHandle")]
pub struct PyUpdateHandle {
    handle: Mutex<Option<UpdateHandle<PyEngineProfile>>>,
    stats: ProcessingStats,
    /// Persistent receiver shared across `changed()` calls via Arc<tokio::Mutex>.
    /// Using tokio::Mutex so it can be held across .await points.
    version_rx: Arc<tokio::sync::Mutex<watch::Receiver<u64>>>,
}

#[pymethods]
impl PyUpdateHandle {
    /// Returns (version, ready, {processor_name: {field: value}}) — atomic snapshot.
    pub fn stats_snapshot<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(u64, bool, Bound<'py, PyDict>)> {
        let snapshot = self.stats.snapshot();
        let dict = snapshot_to_py(py, &snapshot)?;
        Ok((snapshot.version, snapshot.ready, dict))
    }

    /// Awaits a version change notification. Returns the new version.
    /// Returns u64::MAX when the task terminates.
    ///
    /// Uses a persistent receiver: each call waits for the next change
    /// relative to what previous calls already saw.
    pub fn changed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.version_rx.clone();
        future_into_py(py, async move {
            let mut guard = rx.lock().await;
            guard
                .changed()
                .await
                .map_err(|_| PyRuntimeError::new_err("update task dropped"))?;
            Ok(*guard.borrow())
        })
    }

    /// Awaits the task completion and returns the result. Consumes the handle.
    pub fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("result already consumed"))?;
        future_into_py(py, async move {
            let ret = handle.result().await.into_py_result()?;
            Ok(ret)
        })
    }
}

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

    #[pyo3(signature = (root_processor, report_to_stdout, full_reprocess, host_ctx, live=false))]
    pub fn update_async(
        &self,
        root_processor: PyComponentProcessor,
        report_to_stdout: bool,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        live: bool,
    ) -> PyResult<PyUpdateHandle> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            report_to_stdout,
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        let handle = app
            .update(root_processor, options, host_ctx)
            .into_py_result()?;
        let stats = handle.stats().clone();
        let version_rx = Arc::new(tokio::sync::Mutex::new(stats.subscribe()));
        Ok(PyUpdateHandle {
            handle: Mutex::new(Some(handle)),
            stats,
            version_rx,
        })
    }

    #[pyo3(signature = (root_processor, report_to_stdout, full_reprocess, host_ctx, live=false))]
    pub fn update(
        &self,
        py: Python<'_>,
        root_processor: PyComponentProcessor,
        report_to_stdout: bool,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        live: bool,
    ) -> PyResult<PyStoredValue> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            report_to_stdout,
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        py.detach(|| {
            get_runtime().block_on(async move {
                let handle = app
                    .update(root_processor, options, host_ctx)
                    .into_py_result()?;
                handle.result().await.into_py_result()
            })
        })
    }

    pub fn drop_async<'py>(
        &self,
        py: Python<'py>,
        report_to_stdout: bool,
        host_ctx: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let app = self.0.clone();
        let options = AppDropOptions { report_to_stdout };
        let host_ctx = Arc::new(host_ctx);
        let fut = future_into_py(py, async move {
            app.drop_app(options, host_ctx).await.into_py_result()?;
            Ok(())
        })?;
        Ok(fut)
    }

    pub fn drop<'py>(
        &self,
        py: Python<'py>,
        report_to_stdout: bool,
        host_ctx: Py<PyAny>,
    ) -> PyResult<()> {
        let app = self.0.clone();
        let options = AppDropOptions { report_to_stdout };
        let host_ctx = Arc::new(host_ctx);
        py.detach(|| {
            get_runtime()
                .block_on(async move { app.drop_app(options, host_ctx).await.into_py_result() })
        })
    }
}
