use crate::prelude::*;

use cocoindex_core::engine::{
    app::{App, AppOpHandle, AppUpdateOptions},
    progress_display::{ProgressDisplayOptions, show_progress as rust_show_progress},
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
    handle: Mutex<Option<AppOpHandle<PyStoredValue>>>,
    stats: ProcessingStats,
    /// Persistent receiver shared across `changed()` calls via Arc<tokio::Mutex>.
    /// Using tokio::Mutex so it can be held across .await points.
    version_rx: Arc<tokio::sync::Mutex<watch::Receiver<u64>>>,
}

impl PyUpdateHandle {
    fn new(handle: AppOpHandle<PyStoredValue>) -> Self {
        let stats = handle.stats().clone();
        let version_rx = Arc::new(tokio::sync::Mutex::new(stats.subscribe()));
        Self {
            handle: Mutex::new(Some(handle)),
            stats,
            version_rx,
        }
    }
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

#[pyclass(name = "DropHandle")]
pub struct PyDropHandle {
    handle: Mutex<Option<AppOpHandle<()>>>,
    stats: ProcessingStats,
    /// Persistent receiver shared across `changed()` calls via Arc<tokio::Mutex>.
    /// Using tokio::Mutex so it can be held across .await points.
    version_rx: Arc<tokio::sync::Mutex<watch::Receiver<u64>>>,
}

impl PyDropHandle {
    fn new(handle: AppOpHandle<()>) -> Self {
        let stats = handle.stats().clone();
        let version_rx = Arc::new(tokio::sync::Mutex::new(stats.subscribe()));
        Self {
            handle: Mutex::new(Some(handle)),
            stats,
            version_rx,
        }
    }
}

#[pymethods]
impl PyDropHandle {
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
    pub fn changed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.version_rx.clone();
        future_into_py(py, async move {
            let mut guard = rx.lock().await;
            guard
                .changed()
                .await
                .map_err(|_| PyRuntimeError::new_err("drop task dropped"))?;
            Ok(*guard.borrow())
        })
    }

    /// Awaits the task completion and returns the result (None). Consumes the handle.
    pub fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("result already consumed"))?;
        future_into_py(py, async move {
            handle.result().await.into_py_result()?;
            Ok(())
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

    #[pyo3(signature = (root_processor, full_reprocess, host_ctx, live=false))]
    pub fn update_async(
        &self,
        root_processor: PyComponentProcessor,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        live: bool,
    ) -> PyResult<PyUpdateHandle> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        let handle = app
            .update(root_processor, options, host_ctx)
            .into_py_result()?;
        Ok(PyUpdateHandle::new(handle))
    }

    #[pyo3(signature = (root_processor, full_reprocess, host_ctx, report_to_stdout=false, live=false))]
    pub fn update(
        &self,
        py: Python<'_>,
        root_processor: PyComponentProcessor,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        report_to_stdout: bool,
        live: bool,
    ) -> PyResult<PyStoredValue> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        py.detach(|| {
            get_runtime().block_on(async move {
                let handle = app
                    .update(root_processor, options, host_ctx)
                    .into_py_result()?;
                if report_to_stdout {
                    rust_show_progress(handle, ProgressDisplayOptions::default())
                        .await
                        .into_py_result()
                } else {
                    handle.result().await.into_py_result()
                }
            })
        })
    }

    pub fn drop_async(&self, host_ctx: Py<PyAny>) -> PyResult<PyDropHandle> {
        let app = self.0.clone();
        let host_ctx = Arc::new(host_ctx);
        let handle = app.drop_app(host_ctx).into_py_result()?;
        Ok(PyDropHandle::new(handle))
    }

    #[pyo3(signature = (host_ctx, report_to_stdout=false))]
    pub fn drop(
        &self,
        py: Python<'_>,
        host_ctx: Py<PyAny>,
        report_to_stdout: bool,
    ) -> PyResult<()> {
        let app = self.0.clone();
        let host_ctx = Arc::new(host_ctx);
        py.detach(|| {
            get_runtime().block_on(async move {
                let handle = app.drop_app(host_ctx).into_py_result()?;
                if report_to_stdout {
                    rust_show_progress(handle, ProgressDisplayOptions::default())
                        .await
                        .into_py_result()
                } else {
                    handle.result().await.into_py_result()
                }
            })
        })
    }
}

/// Awaits the update handle with progress display. Returns the result.
/// Consumes the handle.
#[pyfunction]
pub fn show_progress<'py>(py: Python<'py>, handle: &PyUpdateHandle) -> PyResult<Bound<'py, PyAny>> {
    let op_handle = handle
        .handle
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| PyRuntimeError::new_err("handle already consumed"))?;
    future_into_py(py, async move {
        let ret = rust_show_progress(op_handle, ProgressDisplayOptions::default())
            .await
            .into_py_result()?;
        Ok(ret)
    })
}
