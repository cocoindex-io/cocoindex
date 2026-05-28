use cocoindex_utils::error::{CError, CResult};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyString};
use std::any::Any;
use std::fmt::{Debug, Display};

pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

pub struct HostedPyErr(PyErr);

impl Display for HostedPyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for HostedPyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let err = &self.0;
        Python::attach(|py| {
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

            match full_trace {
                Ok(trace) => {
                    write!(f, "Error calling Python function:\n{trace}")?;
                }
                Err(_) => {
                    write!(f, "Error calling Python function: {err}")?;
                    if let Some(tb) = err.traceback(py) {
                        write!(f, "\n{}", tb.format().unwrap_or_default())?;
                    }
                }
            };
            Ok(())
        })
    }
}

impl std::error::Error for HostedPyErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl cocoindex_utils::error::HostError for HostedPyErr {
    fn is_cancelled(&self) -> bool {
        Python::attach(|py| {
            let Ok(asyncio) = PyModule::import(py, "asyncio") else {
                return false;
            };
            let Ok(cancelled_cls) = asyncio.getattr("CancelledError") else {
                return false;
            };
            self.0.is_instance(py, &cancelled_cls)
        })
    }

    fn try_clone(&self) -> Option<Box<dyn cocoindex_utils::error::HostError>> {
        // `PyErr::clone_ref` shares the underlying Python exception object
        // (type + value + traceback), so every batch residual recipient gets
        // the original error — catchable by type, with the full Python
        // traceback intact — instead of a flattened string.
        Python::attach(|py| Some(Box::new(HostedPyErr(self.0.clone_ref(py))) as _))
    }
}

fn cerror_to_pyerr(err: CError) -> PyErr {
    let inner = err.without_contexts();
    if let CError::HostLang(host_err) = inner {
        // Pass through tunneled Python errors as-is — preserves the
        // original exception object including traceback and any subclass
        // attributes. This applies to a tunneled `asyncio.CancelledError`
        // too: don't synthesize a fresh one from the cancellation branch
        // below; return the original.
        let any: &dyn Any = host_err.as_ref();
        if let Some(hosted_py_err) = any.downcast_ref::<HostedPyErr>() {
            return Python::attach(|py| hosted_py_err.0.clone_ref(py));
        }
        if let Some(py_err) = any.downcast_ref::<PyErr>() {
            return Python::attach(|py| py_err.clone_ref(py));
        }
    }
    // Cancellation-flavored errors that aren't tunneled Python exceptions
    // (e.g. Rust-constructed `Error::cancelled()` → `CancelledError` HostError)
    // → fresh `asyncio.CancelledError`. This lets Python callers
    // `except CancelledError` uniformly without string-matching the
    // Rust error message.
    if err.is_cancelled() {
        return Python::attach(|py| {
            let msg = format!("{}", err);
            match py
                .import("asyncio")
                .and_then(|m| m.getattr("CancelledError"))
                .and_then(|c| c.call1((msg,)))
            {
                Ok(exc) => PyErr::from_value(exc),
                Err(import_err) => import_err,
            }
        });
    }
    if let CError::Client { .. } = inner {
        return PyValueError::new_err(format!("{}", err));
    }
    PyRuntimeError::new_err(format!("{:?}", err))
}

pub trait FromPyResult<T> {
    fn from_py_result(self) -> CResult<T>;
}

impl<T> FromPyResult<T> for PyResult<T> {
    fn from_py_result(self) -> CResult<T> {
        self.map_err(|err| CError::host(HostedPyErr(err)))
    }
}

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for CResult<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_pyerr)
    }
}
