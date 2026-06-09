use std::pin::Pin;
use std::sync::Arc;

use crate::{app::PyApp, environment::PyEnvironment, prelude::*, stable_path::PyStablePath};

use cocoindex_core::engine::runtime::get_runtime;
use cocoindex_core::inspect::db_inspect;
use cocoindex_core::inspect::db_inspect::StablePathNodeType;
use futures::stream::Stream;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyfunction]
pub fn list_stable_paths(py: Python<'_>, app: &PyApp) -> PyResult<Vec<PyStablePath>> {
    let app = app.0.clone();
    let stable_paths = py
        .detach(|| get_runtime().block_on(async move { db_inspect::list_stable_paths(&app).await }))
        .into_py_result()?;
    let py_stable_paths = stable_paths
        .into_iter()
        .map(|path| PyStablePath(path))
        .collect();
    Ok(py_stable_paths)
}

#[pyclass(name = "StablePathNodeType")]
#[derive(Clone, Copy, Debug)]
pub struct PyStablePathNodeType(pub StablePathNodeType);

#[pymethods]
impl PyStablePathNodeType {
    #[staticmethod]
    pub fn directory() -> Self {
        Self(StablePathNodeType::Directory)
    }

    #[staticmethod]
    pub fn component() -> Self {
        Self(StablePathNodeType::Component)
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub fn __str__(&self) -> String {
        match self.0 {
            StablePathNodeType::Directory => "Directory".to_string(),
            StablePathNodeType::Component => "Component".to_string(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("StablePathNodeType.{}", self.__str__())
    }
}

#[pyclass(name = "StablePathInfo")]
#[derive(Clone)]
pub struct PyStablePathInfo {
    #[pyo3(get)]
    pub path: PyStablePath,
    #[pyo3(get)]
    pub node_type: PyStablePathNodeType,
}

/// Python async iterator that yields `StablePathInfo` items one-by-one (no blocking calls, no forwarder).
#[pyclass(name = "StablePathInfoAsyncIterator")]
pub struct PyStablePathInfoAsyncIterator {
    /// Stream wrapped in async Mutex to allow &self access without blocking Python thread.
    /// Pin<Box<...>> is needed because streams are not Unpin.
    stream: Arc<
        tokio::sync::Mutex<Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathInfo>> + Send>>>,
    >,
}

#[pymethods]
impl PyStablePathInfoAsyncIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use futures::StreamExt;

        let stream = Arc::clone(&self.stream);
        future_into_py(py, async move {
            let mut guard = stream.lock().await;
            match StreamExt::next(&mut *guard).await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(result) => {
                    let item = result.into_py_result()?;
                    Python::attach(|py| {
                        Py::new(
                            py,
                            PyStablePathInfo {
                                path: PyStablePath(item.path),
                                node_type: PyStablePathNodeType(item.node_type),
                            },
                        )
                        .map(|p| p.into_any())
                    })
                    .map_err(|e| e.into())
                }
            }
        })
    }
}

#[pyfunction]
pub fn iter_stable_paths<'py>(app: &PyApp, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let app_clone = app.0.clone();
    let stream = py.detach(|| {
        get_runtime().block_on(async move { db_inspect::iter_stable_paths(&app_clone).await })
    });
    wrap_stream_as_async_iterator(stream, py)
}

#[pyfunction]
pub fn iter_stable_paths_by_name<'py>(
    env: &PyEnvironment,
    app_name: &str,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let env_clone = env.0.clone();
    let app_name = app_name.to_string();
    let stream = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::iter_stable_paths_by_name(&env_clone, &app_name).await
            })
        })
        .into_py_result()?;
    wrap_stream_as_async_iterator(stream, py)
}

fn wrap_stream_as_async_iterator<'py>(
    stream: impl Stream<Item = Result<db_inspect::StablePathInfo>> + Send + 'static,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    // Box and pin the stream to store it in the iterator.
    // No forwarder task needed - we poll the stream directly.
    let stream: Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathInfo>> + Send>> =
        Box::pin(stream);

    let iterator = PyStablePathInfoAsyncIterator {
        stream: Arc::new(tokio::sync::Mutex::new(stream)),
    };
    Ok(Py::new(py, iterator)?.into_any().into_bound(py))
}

#[pyfunction]
pub fn list_app_names(py: Python<'_>, env: &PyEnvironment) -> PyResult<Vec<String>> {
    let env_clone = env.0.clone();
    py.detach(|| {
        get_runtime().block_on(async move { db_inspect::list_app_names(&env_clone).await })
    })
    .into_py_result()
}

#[pyclass(name = "TargetStateVersion")]
#[derive(Clone)]
pub struct PyTargetStateVersion {
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub state: String,
}

#[pyclass(name = "ProviderGeneration")]
#[derive(Clone)]
pub struct PyProviderGeneration {
    #[pyo3(get)]
    pub provider_id: u64,
    #[pyo3(get)]
    pub provider_schema_version: u64,
}

#[pyclass(name = "TargetStateInfoItemSummary")]
#[derive(Clone)]
pub struct PyTargetStateInfoItemSummary {
    #[pyo3(get)]
    pub target_state_path: String,
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub states: Vec<PyTargetStateVersion>,
    #[pyo3(get)]
    pub provider_schema_version: u64,
    #[pyo3(get)]
    pub provider_generation: Option<PyProviderGeneration>,
}

#[pyclass(name = "StablePathDetail")]
#[derive(Clone)]
pub struct PyStablePathDetail {
    #[pyo3(get)]
    pub path: PyStablePath,
    #[pyo3(get)]
    pub node_type: PyStablePathNodeType,
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub processor_name: String,
    #[pyo3(get)]
    pub target_state_count: usize,
    #[pyo3(get)]
    pub has_memoization: bool,
    #[pyo3(get)]
    pub target_state_items: Vec<PyTargetStateInfoItemSummary>,
}

fn convert_detail(py: Python<'_>, d: db_inspect::StablePathDetail) -> PyResult<PyStablePathDetail> {
    Ok(PyStablePathDetail {
        path: PyStablePath(d.path),
        node_type: PyStablePathNodeType(d.node_type),
        version: d.version,
        processor_name: d.processor_name,
        target_state_count: d.target_state_count,
        has_memoization: d.has_memoization,
        target_state_items: d
            .target_state_items
            .into_iter()
            .map(|item| -> PyResult<PyTargetStateInfoItemSummary> {
                Ok(PyTargetStateInfoItemSummary {
                    target_state_path: item.target_state_path,
                    key: item.key.to_string(),
                    states: item
                        .states
                        .into_iter()
                        .map(|s| PyTargetStateVersion {
                            version: s.version,
                            state: s.state,
                        })
                        .collect(),
                    provider_schema_version: item.provider_schema_version,
                    provider_generation: item.provider_generation.map(|g| PyProviderGeneration {
                        provider_id: g.provider_id,
                        provider_schema_version: g.provider_schema_version,
                    }),
                })
            })
            .collect::<PyResult<Vec<_>>>()?,
    })
}

#[pyfunction]
pub fn get_stable_path_detail(
    py: Python<'_>,
    app: &PyApp,
    path: &PyStablePath,
) -> PyResult<Option<PyStablePathDetail>> {
    let app = app.0.clone();
    let path_owned = path.0.clone();
    let detail = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::get_stable_path_detail(&app, &path_owned).await
            })
        })
        .into_py_result()?;
    detail.map(|d| convert_detail(py, d)).transpose()
}

#[pyfunction]
pub fn get_stable_path_detail_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
    path: &PyStablePath,
) -> PyResult<Option<PyStablePathDetail>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let path_owned = path.0.clone();
    let detail = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::get_stable_path_detail_by_name(&env, &app_name, &path_owned).await
            })
        })
        .into_py_result()?;
    detail.map(|d| convert_detail(py, d)).transpose()
}

#[pyfunction]
pub fn query_stable_path_details(
    py: Python<'_>,
    app: &PyApp,
    path: &PyStablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> PyResult<Vec<PyStablePathDetail>> {
    let app = app.0.clone();
    let path_owned = path.0.clone();
    let details = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::query_stable_path_details(
                    &app,
                    &path_owned,
                    include_children,
                    recursive,
                    include_parents,
                )
                .await
            })
        })
        .into_py_result()?;
    details.into_iter().map(|d| convert_detail(py, d)).collect()
}

#[pyfunction]
pub fn query_stable_path_details_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
    path: &PyStablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> PyResult<Vec<PyStablePathDetail>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let path_owned = path.0.clone();
    let details = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::query_stable_path_details_by_name(
                    &env,
                    &app_name,
                    &path_owned,
                    include_children,
                    recursive,
                    include_parents,
                )
                .await
            })
        })
        .into_py_result()?;
    details.into_iter().map(|d| convert_detail(py, d)).collect()
}
