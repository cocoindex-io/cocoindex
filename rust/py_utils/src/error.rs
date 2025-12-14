use cocoindex_utils::error::{CError, CResult};
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyString};
use std::any::Any;
use std::fmt::{self, Display, Write};

pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

#[derive(Debug)]
pub struct PyErrWrapper(PyErr);

impl PyErrWrapper {
    pub fn new(err: PyErr) -> Self {
        Self(err)
    }

    pub fn into_inner(self) -> PyErr {
        self.0
    }
}

impl Display for PyErrWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn cerror_to_pyerr(err: CError) -> PyErr {
    if let Some(host_err) = err.find_host_error() {
        let any: &dyn Any = host_err; // trait upcasting
        if let Some(wrapper) = any.downcast_ref::<PyErrWrapper>() {
            return Python::attach(|py| wrapper.0.clone_ref(py));
        }
    }

    match &err {
        CError::Client { msg, .. } => PyValueError::new_err(msg.clone()),
        CError::HostLang(e) => PyRuntimeError::new_err(e.to_string()),
        CError::Context { .. } | CError::Internal { .. } => {
            PyRuntimeError::new_err(format!("{:?}", err))
        }
    }
}

pub trait ToCResult<T> {
    fn to_cresult(self) -> CResult<T>;
}

impl<T> ToCResult<T> for PyResult<T> {
    fn to_cresult(self) -> CResult<T> {
        self.map_err(|err| CError::host(PyErrWrapper::new(err)))
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
