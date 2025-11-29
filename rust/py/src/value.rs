use cocoindex_core::engine::profile::{Persist, StableFingerprint};

use crate::{prelude::*, runtime::python_functions};

struct PyKeyData {
    value: Py<PyAny>,
    serialized: bytes::Bytes,
    fingerprint: utils::fingerprint::Fingerprint,
}
#[derive(Clone)]
pub struct PyKey {
    data: Arc<PyKeyData>,
}

impl PartialEq for PyKey {
    fn eq(&self, other: &Self) -> bool {
        if self.data.value.is(other.data.value.as_ref()) {
            return true;
        }
        self.data.serialized == other.data.serialized
    }
}

impl Eq for PyKey {}

impl std::hash::Hash for PyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data.fingerprint.hash(state);
    }
}

impl PyKey {
    pub fn new(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        let serialized = python_functions().serialize(py, &value.bind(py))?;
        Ok(Self::from_value_and_bytes(value, serialized))
    }

    pub fn value(&self) -> &Py<PyAny> {
        &self.data.value
    }

    fn from_value_and_bytes(value: Py<PyAny>, bytes: bytes::Bytes) -> Self {
        let mut fingerprinter = utils::fingerprint::Fingerprinter::default();
        fingerprinter.write_raw_bytes(bytes.as_ref());
        let fingerprint = fingerprinter.into_fingerprint();

        Self {
            data: Arc::new(PyKeyData {
                value,
                serialized: bytes,
                fingerprint,
            }),
        }
    }
}

impl Persist for PyKey {
    type Error = PyErr;

    fn to_bytes(&self) -> PyResult<bytes::Bytes> {
        Ok(self.data.serialized.clone())
    }

    fn from_bytes(data: bytes::Bytes) -> PyResult<Self> {
        let value = Python::attach(|py| python_functions().deserialize(py, &data))?;
        Ok(Self::from_value_and_bytes(value, data))
    }
}

impl StableFingerprint for PyKey {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint {
        self.data.fingerprint
    }
}
