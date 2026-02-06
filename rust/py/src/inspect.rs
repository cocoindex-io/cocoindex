use crate::{app::PyApp, environment::PyEnvironment, prelude::*, stable_path::PyStablePath};

use cocoindex_core::inspect::db_inspect;

#[pyfunction]
pub fn list_stable_paths(app: &PyApp) -> PyResult<Vec<PyStablePath>> {
    let stable_paths = db_inspect::list_stable_paths(&app.0).into_py_result()?;
    let py_stable_paths = stable_paths
        .into_iter()
        .map(|path| PyStablePath(path))
        .collect();
    Ok(py_stable_paths)
}

#[pyfunction]
pub fn list_stable_paths_with_types(app: &PyApp) -> PyResult<Vec<(PyStablePath, bool)>> {
    let items = db_inspect::list_stable_paths_with_types(&app.0).into_py_result()?;
    let out = items
        .into_iter()
        .map(|(path, is_component)| (PyStablePath(path), is_component))
        .collect();
    Ok(out)
}

#[pyfunction]
pub fn list_app_names(env: &PyEnvironment) -> PyResult<Vec<String>> {
    db_inspect::list_app_names(&env.0).into_py_result()
}
