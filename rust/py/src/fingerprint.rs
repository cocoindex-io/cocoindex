use crate::prelude::*;

use pyo3::types::PyBytes;

/// Python wrapper for `utils::fingerprint::Fingerprint`.
///
/// Exposed to Python as `cocoindex._internal.core.Fingerprint`.
#[pyclass(name = "Fingerprint", frozen)]
#[derive(Clone, Copy)]
pub struct PyFingerprint(pub utils::fingerprint::Fingerprint);

#[pymethods]
impl PyFingerprint {
    /// Return the raw 16-byte digest.
    pub fn as_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.as_slice())
    }

    /// Base64 encoding used by CocoIndex for human-readable fingerprints.
    pub fn to_base64(&self) -> String {
        self.0.to_base64()
    }

    pub fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.as_bytes(py)
    }

    pub fn __str__(&self) -> String {
        self.0.to_string()
    }

    pub fn __repr__(&self) -> String {
        format!("Fingerprint({})", self.0)
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
