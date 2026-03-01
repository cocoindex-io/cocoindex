use std::pin::Pin;
use std::sync::Arc;

use crate::{app::PyApp, environment::PyEnvironment, prelude::*, stable_path::PyStablePath};

use cocoindex_core::inspect::db_inspect;
use cocoindex_core::inspect::db_inspect::StablePathNodeType;
use futures::stream::Stream;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyfunction]
pub fn list_stable_paths(app: &PyApp) -> PyResult<Vec<PyStablePath>> {
    let stable_paths = db_inspect::list_stable_paths(&app.0).into_py_result()?;
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
    let stream = db_inspect::iter_stable_paths(&app_clone);

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
pub fn list_app_names(env: &PyEnvironment) -> PyResult<Vec<String>> {
    db_inspect::list_app_names(&env.0).into_py_result()
}
