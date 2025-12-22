use cocoindex_utils::error::{CError, CResult};
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyString};
use std::any::Any;
use std::fmt::Write;

pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

pub fn cerror_to_pyerr(err: CError) -> PyErr {
    if let CError::HostLang(host_err) = err.without_contexts() {
        // if tunneled Python error
        let any: &dyn Any = host_err.as_ref();
        if let Some(py_err) = any.downcast_ref::<PyErr>() {
            return Python::attach(|py| py_err.clone_ref(py));
        }
    }

    match err.without_contexts() {
        CError::Client { .. } => PyValueError::new_err(format_error_chain_no_backtrace(&err)),
        _ => PyRuntimeError::new_err(format_error_chain(&err)),
    }
}

fn format_error_chain(err: &CError) -> String {
    let mut s = err.to_string();
    let mut current = err;
    while let CError::Context { source, .. } = current {
        write!(&mut s, "\nCaused by: {}", source).ok();
        current = source;
    }
    if let Some(bt) = err.backtrace() {
        write!(&mut s, "\n\n{}", bt).ok();
    }
    s
}

fn format_error_chain_no_backtrace(err: &CError) -> String {
    let mut s = err.to_string();
    let mut current = err;
    while let CError::Context { source, .. } = current {
        write!(&mut s, "\nCaused by: {}", source).ok();
        current = source;
    }
    s
}

pub trait ToCResult<T> {
    fn to_cresult(self) -> CResult<T>;
}

impl<T> ToCResult<T> for PyResult<T> {
    fn to_cresult(self) -> CResult<T> {
        self.map_err(|err| CError::host(err))
    }
}

// Legacy traits down below - kept for backwards compatibility during migration

pub trait ToResultWithPyTrace<T> {
    fn to_result_with_py_trace(self, py: Python<'_>) -> anyhow::Result<T>;
}

impl<T> ToResultWithPyTrace<T> for Result<T, PyErr> {
    fn to_result_with_py_trace(self, py: Python<'_>) -> anyhow::Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
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

pub trait CResultIntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> CResultIntoPyResult<T> for CResult<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_pyerr)
    }
}
