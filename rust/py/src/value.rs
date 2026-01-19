use cocoindex_core::engine::profile::{Persist, StableFingerprint};

use crate::{prelude::*, runtime::python_objects};

pub struct PyKey {
    value: Py<PyAny>,
    serialized: bytes::Bytes,
    fingerprint: utils::fingerprint::Fingerprint,
}

impl PartialEq for PyKey {
    fn eq(&self, other: &Self) -> bool {
        if self.value.is(other.value.as_ref()) {
            return true;
        }
        self.serialized == other.serialized
    }
}

impl Eq for PyKey {}

impl std::hash::Hash for PyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

impl std::fmt::Debug for PyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_py_any(f, &self.value)
    }
}

impl PyKey {
    pub fn new(py: Python<'_>, value: Py<PyAny>) -> Result<Self> {
        let serialized = python_objects().serialize(py, &value.bind(py))?;
        Ok(Self::from_value_and_bytes(value, serialized))
    }

    pub fn value(&self) -> &Py<PyAny> {
        &self.value
    }

    fn from_value_and_bytes(value: Py<PyAny>, bytes: bytes::Bytes) -> Self {
        let mut fingerprinter = utils::fingerprint::Fingerprinter::default();
        fingerprinter.write_raw_bytes(bytes.as_ref());
        let fingerprint = fingerprinter.into_fingerprint();

        Self {
            value,
            serialized: bytes,
            fingerprint,
        }
    }
}

impl Persist for PyKey {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        Ok(self.serialized.clone())
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        let value = Python::attach(|py| python_objects().deserialize(py, data))?;
        Ok(Self::from_value_and_bytes(
            value,
            bytes::Bytes::copy_from_slice(data),
        ))
    }
}

impl StableFingerprint for PyKey {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint {
        self.fingerprint
    }
}

pub struct PyValue {
    data: Py<PyAny>,
}

impl PyValue {
    pub fn new(data: Py<PyAny>) -> Self {
        Self { data }
    }

    pub fn value(&self) -> &Py<PyAny> {
        &self.data
    }

    pub fn into_inner(self) -> Py<PyAny> {
        self.data
    }
}

impl std::fmt::Debug for PyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_py_any(f, &self.data)
    }
}

impl Persist for PyValue {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        let serialized = Python::attach(|py| python_objects().serialize(py, &self.data.bind(py)))?;
        Ok(serialized)
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        let value = Python::attach(|py| python_objects().deserialize(py, data))?;
        Ok(Self { data: value })
    }
}

impl Clone for PyValue {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            data: self.data.clone_ref(py),
        })
    }
}

fn debug_py_any(f: &mut std::fmt::Formatter<'_>, value: &Py<PyAny>) -> std::fmt::Result {
    Python::attach(|py| {
        let value = value.bind(py);
        match value.repr() {
            Ok(repr) => match repr.to_str() {
                Ok(repr_str) => f.write_str(repr_str),
                Err(err) => {
                    error!("Error getting repr: {:?}", err);
                    f.write_str("<error getting repr>")
                }
            },
            Err(err) => {
                error!("Error getting repr: {:?}", err);
                f.write_str("<error getting repr>")
            }
        }
    })
}
