use crate::prelude::*;
use pyo3::IntoPyObject;

use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyBytes, PyInt, PyList, PyString, PyTuple};

use cocoindex_core::state::stable_path::{StableKey, StablePath};

impl<'py> IntoPyObject<'py> for PyStableKey {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            StableKey::Null => py.None().into_bound_py_any(py),

            StableKey::Bool(b) => PyBool::new(py, b).into_bound_py_any(py),

            StableKey::Int(i) => PyInt::new(py, i).into_bound_py_any(py),

            StableKey::Str(s) => PyString::new(py, s.as_ref()).into_bound_py_any(py),

            StableKey::Bytes(b) => PyBytes::new(py, b.as_ref()).into_bound_py_any(py),

            StableKey::Uuid(u) => u.into_pyobject(py)?.into_bound_py_any(py),

            StableKey::Array(arr) => {
                let items: Vec<PyStableKey> = arr.iter().cloned().map(PyStableKey).collect();

                PyTuple::new(py, items)?.into_bound_py_any(py)
            }

            StableKey::Fingerprint(fp) => PyBytes::new(py, fp.as_ref()).into_bound_py_any(py),
        }
    }
}

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
}
