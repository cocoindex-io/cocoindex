use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

use cocoindex_utils::fingerprint::Fingerprint;

static CURRENT_LOGIC_SET: LazyLock<RwLock<HashSet<Fingerprint>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

/// Register a logic fingerprint in the current logic set.
pub fn register(fp: Fingerprint) {
    CURRENT_LOGIC_SET.write().unwrap().insert(fp);
}

/// Check if a single fingerprint is in the current logic set.
pub fn contains(fp: &Fingerprint) -> bool {
    CURRENT_LOGIC_SET.read().unwrap().contains(fp)
}

/// Check if all fingerprints are in the current logic set.
pub fn all_contained(fps: &[Fingerprint]) -> bool {
    let set = CURRENT_LOGIC_SET.read().unwrap();
    fps.iter().all(|fp| set.contains(fp))
}

/// Remove a logic fingerprint from the current logic set.
pub fn unregister(fp: &Fingerprint) {
    CURRENT_LOGIC_SET.write().unwrap().remove(fp);
}
