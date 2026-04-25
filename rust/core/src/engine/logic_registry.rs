use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

use cocoindex_utils::fingerprint::Fingerprint;

use super::environment::Environment;
use super::profile::EngineProfile;

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

/// Check if all fingerprints are in the global logic set or the environment's logic set.
pub fn all_contained_with_env<Prof: EngineProfile>(
    fps: &[Fingerprint],
    env: &Environment<Prof>,
) -> bool {
    let global_set = CURRENT_LOGIC_SET.read().unwrap();
    fps.iter()
        .all(|fp| global_set.contains(fp) || env.logic_set_contains(fp))
}

/// Remove a logic fingerprint from the current logic set.
pub fn unregister(fp: &Fingerprint) {
    CURRENT_LOGIC_SET.write().unwrap().remove(fp);
}
