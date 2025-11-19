use cocoindex_core::engine::planet::PlanetSettings;

use cocoindex_py_utils::{AnyhowIntoPyResult, Pythonized};
use pyo3::prelude::*;

#[pyfunction]
fn init_planet(options: Pythonized<PlanetSettings>) -> PyResult<()> {
    cocoindex_core::engine::planet::init_planet(options.into_inner()).into_py_result()?;
    Ok(())
}

#[pyfunction]
fn close_planet() -> PyResult<()> {
    cocoindex_core::engine::planet::close_planet().into_py_result()?;
    Ok(())
}

#[pymodule]
#[pyo3(name = "_core")]
fn core_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_planet, m)?)?;
    m.add_function(wrap_pyfunction!(close_planet, m)?)?;
    Ok(())
}
