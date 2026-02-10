use crate::{app::PyApp, environment::PyEnvironment, prelude::*, stable_path::PyStablePath};

use cocoindex_core::inspect::db_inspect;
use cocoindex_core::inspect::db_inspect::StablePathNodeType;

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

#[pyclass(name = "StablePathWithType")]
#[derive(Clone)]
pub struct PyStablePathWithType {
    #[pyo3(get)]
    pub path: PyStablePath,
    #[pyo3(get)]
    pub node_type: PyStablePathNodeType,
}

#[pyfunction]
pub fn list_stable_paths_with_types(app: &PyApp) -> PyResult<Vec<PyStablePathWithType>> {
    let items = db_inspect::list_stable_paths_with_types_collect(&app.0).into_py_result()?;
    Ok(items
        .into_iter()
        .map(|item| PyStablePathWithType {
            path: PyStablePath(item.path),
            node_type: PyStablePathNodeType(item.node_type),
        })
        .collect())
}

#[pyfunction]
pub fn list_app_names(env: &PyEnvironment) -> PyResult<Vec<String>> {
    db_inspect::list_app_names(&env.0).into_py_result()
}
