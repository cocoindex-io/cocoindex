use std::time::Duration;

use crate::prelude::*;
use cocoindex_core::engine::deadline::{
    DeadlineContext, testing_advance_deadline_clock as rust_testing_advance_deadline_clock,
    testing_disable_deadline_clock as rust_testing_disable_deadline_clock,
    testing_reset_deadline_clock as rust_testing_reset_deadline_clock,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};

const TESTING_ENV_VAR: &str = "COCOINDEX_TESTING";

#[pyclass(name = "DeadlineContext")]
#[derive(Clone, Copy)]
pub struct PyDeadlineContext(pub DeadlineContext);

#[pymethods]
impl PyDeadlineContext {
    fn with_timeout(&self, seconds: f64) -> PyResult<Self> {
        let timeout = Duration::try_from_secs_f64(seconds).map_err(|_| {
            PyValueError::new_err(
                "timeout duration must be a non-negative finite number of seconds within the supported range",
            )
        })?;
        Ok(Self(self.0.with_timeout(timeout)))
    }

    fn check(&self) -> PyResult<()> {
        self.0.check().into_py_result()
    }

    fn remaining_secs(&self) -> Option<f64> {
        self.0.remaining().map(|d| d.as_secs_f64())
    }

    fn has_deadline(&self) -> bool {
        self.0.has_deadline()
    }
}

#[pyfunction]
pub fn deadline_none() -> PyDeadlineContext {
    PyDeadlineContext(DeadlineContext::NONE)
}

fn ensure_testing_clock_enabled() -> PyResult<()> {
    if std::env::var_os(TESTING_ENV_VAR).as_deref() == Some(std::ffi::OsStr::new("1")) {
        return Ok(());
    }
    Err(PyRuntimeError::new_err(format!(
        "deadline test-clock APIs require {TESTING_ENV_VAR}=1"
    )))
}

#[pyfunction]
pub fn testing_reset_deadline_clock() -> PyResult<()> {
    ensure_testing_clock_enabled()?;
    rust_testing_reset_deadline_clock();
    Ok(())
}

#[pyfunction]
pub fn testing_disable_deadline_clock() -> PyResult<()> {
    ensure_testing_clock_enabled()?;
    rust_testing_disable_deadline_clock();
    Ok(())
}

#[pyfunction]
pub fn testing_advance_deadline_clock(ms: u64) -> PyResult<()> {
    ensure_testing_clock_enabled()?;
    rust_testing_advance_deadline_clock(Duration::from_millis(ms));
    Ok(())
}
