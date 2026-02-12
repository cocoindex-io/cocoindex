use crate::prelude::*;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyBytes, PyInt, PyList, PyString, PyTuple};
use pyo3::{Py, PyAny, Python};

use cocoindex_core::state::stable_path::{StableKey, StablePath};

pub struct PyStableKey(pub(crate) StableKey);

impl FromPyObject<'_, '_> for PyStableKey {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let part = if obj.is_none() {
            StableKey::Null
        } else if obj.is_instance_of::<PyBool>() {
            StableKey::Bool(obj.extract::<bool>()?)
        } else if obj.is_instance_of::<PyInt>() {
            StableKey::Int(obj.extract::<i64>()?)
        } else if obj.is_instance_of::<PyString>() {
            StableKey::Str(Arc::from(obj.extract::<&str>()?))
        } else if obj.is_instance_of::<PyBytes>() {
            StableKey::Bytes(Arc::from(obj.extract::<&[u8]>()?))
        } else if obj.is_instance_of::<PyTuple>() || obj.is_instance_of::<PyList>() {
            let len = obj.len()?;
            let mut parts = Vec::with_capacity(len);
            for item in obj.try_iter()? {
                parts.push(PyStableKey::extract(item?.as_borrowed())?.0);
            }
            StableKey::Array(Arc::from(parts))
        } else if obj.is_instance_of::<PySymbol>() {
            let sym = obj.extract::<PySymbol>()?;
            StableKey::Symbol(sym.0)
        } else if let Ok(uuid_value) = obj.extract::<uuid::Uuid>() {
            StableKey::Uuid(uuid_value)
        } else {
            return Err(PyTypeError::new_err(
                "Unsupported StableKey Python type. Only support None, bool, int, str, bytes, tuple, list, uuid, and Symbol",
            ));
        };
        Ok(Self(part))
    }
}

#[pyclass(name = "Symbol", frozen)]
#[derive(Clone)]
pub struct PySymbol(pub Arc<str>);

#[pymethods]
impl PySymbol {
    #[new]
    pub fn new(name: &str) -> Self {
        Self(Arc::from(name))
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.0
    }

    pub fn __repr__(&self) -> String {
        format!("Symbol({})", self.0)
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

#[pyclass(name = "StablePath")]
#[derive(Clone)]
pub struct PyStablePath(pub StablePath);

#[pymethods]
impl PyStablePath {
    #[new]
    pub fn new() -> Self {
        Self(StablePath::root())
    }

    pub fn concat(&self, part: PyStableKey) -> Self {
        Self(self.0.concat_part(part.0))
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    pub fn __coco_memo_key__(&self) -> String {
        self.0.to_string()
    }

    pub fn parts(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.0
            .as_ref()
            .iter()
            .map(|key| stable_key_to_py(py, key))
            .collect()
    }
}

fn stable_key_to_py(py: Python<'_>, key: &StableKey) -> PyResult<Py<PyAny>> {
    match key {
        StableKey::Null => Ok(py.None().into()),
        StableKey::Bool(b) => Ok((*b).into_py_any(py)?),
        StableKey::Int(i) => Ok((*i).into_py_any(py)?),
        StableKey::Str(s) => Ok(s.as_ref().into_py_any(py)?),
        StableKey::Symbol(s) => Ok(s.as_ref().into_py_any(py)?),
        StableKey::Bytes(b) => Ok(PyBytes::new(py, b.as_ref()).into_py_any(py)?),
        StableKey::Uuid(u) => Ok((*u).into_py_any(py)?),
        StableKey::Array(arr) => {
            let mut items: Vec<Py<PyAny>> = Vec::with_capacity(arr.len());
            for item in arr.iter() {
                items.push(stable_key_to_py(py, item)?);
            }
            let py_tuple = PyTuple::new(py, items)?;
            Ok(py_tuple.into_py_any(py)?)
        }
        StableKey::Fingerprint(fp) => Ok(fp.to_string().into_py_any(py)?),
    }
}
