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

type PyPreviewCollector = Arc<std::sync::Mutex<Vec<Py<PyAny>>>>;

#[pyclass(name = "UpdateHandle")]
pub struct PyUpdateHandle {
    handle: Mutex<Option<AppOpHandle<PyStoredValue>>>,
    stats: ProcessingStats,
    /// Persistent receiver shared across `changed()` calls via Arc<tokio::Mutex>.
    /// Using tokio::Mutex so it can be held across .await points.
    version_rx: Arc<tokio::sync::Mutex<watch::Receiver<u64>>>,
    preview_collector: Option<PyPreviewCollector>,
}

impl PyUpdateHandle {
    fn new(handle: AppOpHandle<PyStoredValue>) -> Self {
        let stats = handle.stats().clone();
        let version_rx = Arc::new(tokio::sync::Mutex::new(stats.subscribe()));
        Self {
            handle: Mutex::new(Some(handle)),
            stats,
            version_rx,
            preview_collector: None,
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

    /// Returns collected preview actions as a Python list. Call after result().
    pub fn take_preview_actions<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        let actions = self
            .preview_collector
            .as_ref()
            .map(|c| std::mem::take(&mut *c.lock().unwrap()))
            .unwrap_or_default();
        pyo3::types::PyList::new(py, actions.iter().map(|a| a.bind(py))).map_err(|e| e.into())
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

/// Read handle for a `coco.stats_group(...)` scope: the same stats/watch
/// surface as `UpdateHandle`, over the group's `ProcessingStats`. No `result()` —
/// a group has no return value.
#[pyclass(name = "StatsGroupHandle")]
pub struct PyStatsGroupHandle {
    stats: ProcessingStats,
    /// Persistent receiver shared across `changed()` calls via Arc<tokio::Mutex>.
    version_rx: Arc<tokio::sync::Mutex<watch::Receiver<u64>>>,
}

impl PyStatsGroupHandle {
    pub fn new(stats: ProcessingStats) -> Self {
        let version_rx = Arc::new(tokio::sync::Mutex::new(stats.subscribe()));
        Self { stats, version_rx }
    }
}

#[pymethods]
impl PyStatsGroupHandle {
    /// Returns (version, ready, {processor_name: {field: value}}) — atomic snapshot.
    pub fn stats_snapshot<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(u64, bool, Bound<'py, PyDict>)> {
        let snapshot = self.stats.snapshot();
        let dict = snapshot_to_py(py, &snapshot)?;
        Ok((snapshot.version, snapshot.ready, dict))
    }

    /// Awaits a version change notification. Returns the new version, or
    /// u64::MAX when the group terminates.
    pub fn changed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.version_rx.clone();
        future_into_py(py, async move {
            let mut guard = rx.lock().await;
            guard
                .changed()
                .await
                .map_err(|_| PyRuntimeError::new_err("stats group dropped"))?;
            Ok(*guard.borrow())
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
        py: Python<'_>,
        name: &str,
        env: &PyEnvironment,
        max_inflight_components: Option<usize>,
    ) -> PyResult<Self> {
        let name_owned = name.to_string();
        let env = env.0.clone();
        let app = py
            .detach(|| {
                get_runtime().block_on(async move {
                    App::new(&name_owned, env, max_inflight_components).await
                })
            })
            .context(format!("failed to initialize app '{name}'"))
            .into_py_result()?;
        Ok(Self(Arc::new(app)))
    }

    #[pyo3(signature = (root_processor, full_reprocess, host_ctx, live=false, preview=false))]
    pub fn update_async(
        &self,
        root_processor: PyComponentProcessor,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        live: bool,
        preview: bool,
    ) -> PyResult<PyUpdateHandle> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        let preview_collector = if preview {
            Some(Arc::new(std::sync::Mutex::new(Vec::new())))
        } else {
            None
        };
        let (handle, preview_collector) = app
            .update(root_processor, options, host_ctx, preview_collector)
            .context("failed to start app update")
            .into_py_result()?;
        let mut uh = PyUpdateHandle::new(handle);
        uh.preview_collector = preview_collector;
        Ok(uh)
    }

    #[pyo3(signature = (root_processor, full_reprocess, host_ctx, report_to_stdout=false, refresh_interval_secs=None, live=false, preview=false))]
    pub fn update(
        &self,
        py: Python<'_>,
        root_processor: PyComponentProcessor,
        full_reprocess: bool,
        host_ctx: Py<PyAny>,
        report_to_stdout: bool,
        refresh_interval_secs: Option<f64>,
        live: bool,
        preview: bool,
    ) -> PyResult<Py<PyAny>> {
        let app = self.0.clone();
        let options = AppUpdateOptions {
            full_reprocess,
            live,
        };
        let host_ctx = Arc::new(host_ctx);
        let preview_collector = if preview {
            Some(Arc::new(std::sync::Mutex::new(Vec::new())))
        } else {
            None
        };
        py.detach(|| {
            get_runtime().block_on(async move {
                let (handle, preview_collector) = app
                    .update(root_processor, options, host_ctx, preview_collector)
                    .context("failed to start app update")
                    .into_py_result()?;
                if preview {
                    handle.result().await.into_py_result()?;
                    let actions = preview_collector
                        .map(|c| std::mem::take(&mut *c.lock().unwrap()))
                        .unwrap_or_default();
                    Python::attach(|py| {
                        let list =
                            pyo3::types::PyList::new(py, actions.iter().map(|a| a.bind(py)))?;
                        Ok(list.unbind().into_any())
                    })
                } else if report_to_stdout {
                    let ret: PyStoredValue = rust_show_progress(
                        handle,
                        ProgressDisplayOptions::from_refresh_secs(refresh_interval_secs),
                    )
                    .await
                    .into_py_result()?;
                    Python::attach(|py| Ok(Py::new(py, ret)?.into_any()))
                } else {
                    let ret: PyStoredValue = handle.result().await.into_py_result()?;
                    Python::attach(|py| Ok(Py::new(py, ret)?.into_any()))
                }
            })
        })
    }

    pub fn drop_async(&self, host_ctx: Py<PyAny>) -> PyResult<PyDropHandle> {
        let app = self.0.clone();
        let host_ctx = Arc::new(host_ctx);
        let handle = app
            .drop_app(host_ctx)
            .context("failed to start app drop")
            .into_py_result()?;
        Ok(PyDropHandle::new(handle))
    }

    #[pyo3(signature = (host_ctx, report_to_stdout=false, refresh_interval_secs=None))]
    pub fn drop(
        &self,
        py: Python<'_>,
        host_ctx: Py<PyAny>,
        report_to_stdout: bool,
        refresh_interval_secs: Option<f64>,
    ) -> PyResult<()> {
        let app = self.0.clone();
        let host_ctx = Arc::new(host_ctx);
        py.detach(|| {
            get_runtime().block_on(async move {
                let handle = app
                    .drop_app(host_ctx)
                    .context("failed to start app drop")
                    .into_py_result()?;
                if report_to_stdout {
                    rust_show_progress(
                        handle,
                        ProgressDisplayOptions::from_refresh_secs(refresh_interval_secs),
                    )
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
#[pyo3(signature = (handle, refresh_interval_secs=None))]
pub fn show_progress<'py>(
    py: Python<'py>,
    handle: &PyUpdateHandle,
    refresh_interval_secs: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let op_handle = handle
        .handle
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| PyRuntimeError::new_err("handle already consumed"))?;
    future_into_py(py, async move {
        let ret = rust_show_progress(
            op_handle,
            ProgressDisplayOptions::from_refresh_secs(refresh_interval_secs),
        )
        .await
        .into_py_result()?;
        Ok(ret)
    })
}
