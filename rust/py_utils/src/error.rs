use pyo3::types::{PyDict, PyModule, PyString};
use pyo3::{exceptions::PyException, prelude::*};
use std::fmt::Write;
pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

pub trait ToResultWithPyTrace<T> {
    fn to_result_with_py_trace(self, py: Python<'_>) -> anyhow::Result<T>;
}

impl<T> ToResultWithPyTrace<T> for Result<T, PyErr> {
    fn to_result_with_py_trace(self, py: Python<'_>) -> anyhow::Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                // Attempt to render a full Python-style traceback including cause/context chain
                let full_trace: PyResult<String> = (|| {
                    let exc = err.value(py);
                    let traceback = PyModule::import(py, "traceback")?;
                    let tbe_class = traceback.getattr("TracebackException")?;
                    let tbe = tbe_class.call_method1("from_exception", (exc,))?;
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("chain", true)?;
                    let lines = tbe.call_method("format", (), Some(&kwargs))?;
                    let joined = PyString::new(py, "").call_method1("join", (lines,))?;
                    joined.extract::<String>()
                })();

                let err_str = match full_trace {
                    Ok(trace) => format!("Error calling Python function:\n{trace}"),
                    Err(_) => {
                        // Fallback: include the PyErr display and available traceback formatting
                        let mut s = format!("Error calling Python function: {err}");
                        if let Some(tb) = err.traceback(py) {
                            write!(&mut s, "\n{}", tb.format()?).ok();
                        }
                        s
                    }
                };

                Err(anyhow::anyhow!(err_str))
            }
        }
    }
}
pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T, E: std::error::Error> IntoPyResult<T> for Result<T, E> {
    fn into_py_result(self) -> PyResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(PyException::new_err(format!("{err:?}"))),
        }
    }
}

pub trait AnyhowIntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> AnyhowIntoPyResult<T> for anyhow::Result<T> {
    fn into_py_result(self) -> PyResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(PyException::new_err(format!("{err:?}"))),
        }
    }
}
