use crate::prelude::*;

use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyBytes, PyInt, PyList, PyString, PyTuple};

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
        } else if let Ok(uuid_value) = obj.extract::<uuid::Uuid>() {
            StableKey::Uuid(uuid_value)
        } else {
            return Err(PyTypeError::new_err(
                "Unsupported StableKey Python type. Only support None, bool, int, str, bytes, tuple, list, and uuid",
            ));
        };
        Ok(Self(part))
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
