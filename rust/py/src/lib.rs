use cocoindex_core::engine::environment::{Environment, EnvironmentSettings};
use cocoindex_core::state::state_path::{StatePath, StatePathPart};
use cocoindex_py_utils::{AnyhowIntoPyResult, Pythonized};
use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyBytes, PyInt, PyList, PyString, PyTuple};
use pyo3::{prelude::*, pyclass};
use std::sync::Arc;

struct PyStatePathPart(StatePathPart);

impl<'py> FromPyObject<'py> for PyStatePathPart {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let part = if obj.is_none() {
            StatePathPart::Null
        } else if obj.is_instance_of::<PyBool>() {
            StatePathPart::Bool(obj.extract::<bool>()?)
        } else if obj.is_instance_of::<PyInt>() {
            StatePathPart::Int(obj.extract::<i64>()?)
        } else if obj.is_instance_of::<PyString>() {
            StatePathPart::Str(Arc::from(obj.extract::<String>()?))
        } else if obj.is_instance_of::<PyBytes>() {
            StatePathPart::Bytes(Arc::from(obj.extract::<Vec<u8>>()?))
        } else if obj.is_instance_of::<PyTuple>() || obj.is_instance_of::<PyList>() {
            let len = obj.len()?;
            let mut parts = Vec::with_capacity(len);
            for i in 0..len {
                let item = obj.get_item(i)?;
                parts.push(PyStatePathPart::extract_bound(&item)?.0);
            }
            StatePathPart::Array(Arc::from(parts))
        } else if let Ok(uuid_value) = obj.extract::<uuid::Uuid>() {
            StatePathPart::Uuid(Arc::from(uuid_value))
        } else {
            return Err(PyTypeError::new_err(
                "Unsupported StatePathPart Python type. Only support None, bool, int, str, bytes, tuple, list, and uuid",
            ));
        };
        Ok(Self(part))
    }
}
#[pyclass(name = "StatePath")]
struct PyStatePath(StatePath);

#[pymethods]
impl PyStatePath {
    #[new]
    pub fn new() -> Self {
        Self(StatePath::root())
    }

    pub fn concat(&self, part: PyStatePathPart) -> Self {
        Self(self.0.concat(part.0))
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}
#[pyclass(name = "Environment")]
struct PyEnvironment(Environment);

#[pymethods]
impl PyEnvironment {
    #[new]
    pub fn new(settings: Pythonized<EnvironmentSettings>) -> PyResult<Self> {
        let settings = settings.into_inner();
        let environment = Environment::new(settings).into_py_result()?;
        Ok(Self(environment))
    }
}

#[pymodule]
#[pyo3(name = "_core")]
fn core_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEnvironment>()?;
    m.add_class::<PyStatePath>()?;
    Ok(())
}
