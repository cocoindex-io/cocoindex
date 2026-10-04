use crate::fingerprint::PyFingerprint;
use crate::prelude::*;

/// Register a logic fingerprint in the global current logic set. Balance each
/// call with one `unregister_logic_fingerprint`.
#[pyfunction]
pub fn register_logic_fingerprint(fp: PyFingerprint) {
    cocoindex_core::engine::logic_registry::register(fp.0);
}

/// Release one registration of a logic fingerprint. It leaves the global current
/// logic set when its last registration is released.
#[pyfunction]
pub fn unregister_logic_fingerprint(fp: PyFingerprint) {
    cocoindex_core::engine::logic_registry::unregister(&fp.0);
}
