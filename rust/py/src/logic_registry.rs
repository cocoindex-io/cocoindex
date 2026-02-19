use crate::fingerprint::PyFingerprint;
use crate::prelude::*;

/// Register a logic fingerprint in the global current logic set.
#[pyfunction]
pub fn register_logic_fingerprint(fp: PyFingerprint) {
    cocoindex_core::engine::logic_registry::register(fp.0);
}

/// Remove a logic fingerprint from the global current logic set.
#[pyfunction]
pub fn unregister_logic_fingerprint(fp: PyFingerprint) {
    cocoindex_core::engine::logic_registry::unregister(&fp.0);
}
