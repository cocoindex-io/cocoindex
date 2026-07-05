use std::time::Duration;

use crate::prelude::*;
use cocoindex_core::engine::deadline::{
    DeadlineContext, testing_advance_deadline_clock as rust_testing_advance_deadline_clock,
    testing_disable_deadline_clock as rust_testing_disable_deadline_clock,
    testing_reset_deadline_clock as rust_testing_reset_deadline_clock,
};
use pyo3::exceptions::PyValueError;

#[pyclass(name = "DeadlineContext")]
#[derive(Clone, Copy)]
pub struct PyDeadlineContext(pub DeadlineContext);

#[pymethods]
impl PyDeadlineContext {
    #[staticmethod]
    fn none() -> Self {
        Self(DeadlineContext::NONE)
    }

    fn with_timeout(&self, seconds: f64) -> PyResult<Self> {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(PyValueError::new_err(
                "timeout duration must be a non-negative finite number of seconds",
            ));
        }
        Ok(Self(self.0.with_timeout(Duration::from_secs_f64(seconds))))
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

    fn raw_ns(&self) -> u64 {
        self.0.raw_ns()
    }
}

#[pyfunction]
pub fn deadline_none() -> PyDeadlineContext {
    PyDeadlineContext(DeadlineContext::NONE)
}

#[pyfunction]
pub fn testing_reset_deadline_clock() {
    rust_testing_reset_deadline_clock();
}

#[pyfunction]
pub fn testing_disable_deadline_clock() {
    rust_testing_disable_deadline_clock();
}

#[pyfunction]
pub fn testing_advance_deadline_clock(ms: u64) {
    rust_testing_advance_deadline_clock(Duration::from_millis(ms));
}
