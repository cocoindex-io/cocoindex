use crate::prelude::*;

use cocoindex_core::engine::runtime::get_runtime;
use pyo3::exceptions::PyException;

#[pyfunction]
pub fn init_runtime() -> PyResult<()> {
    if let Err(_) = pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()) {
        return Err(PyException::new_err(
            "Failed to initialize Tokio runtime: already initialized",
        ));
    }
    Ok(())
}
