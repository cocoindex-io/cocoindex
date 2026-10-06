use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{LazyLock, RwLock};

use cocoindex_utils::fingerprint::Fingerprint;

use super::environment::Environment;
use super::profile::EngineProfile;

/// The current logic set, as the number of live registrations of each
/// fingerprint in it. Counted because several live objects can share one
/// fingerprint (e.g. a function redefined with identical code registers it
/// before the old definition is dropped), and releasing one of them must not
/// remove the fingerprint while the others still hold it.
static CURRENT_LOGIC_REF_COUNTS: LazyLock<RwLock<HashMap<Fingerprint, usize>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Register a logic fingerprint in the current logic set. Balance each call
/// with one [`unregister`] when the registering object goes away.
pub fn register(fp: Fingerprint) {
    *CURRENT_LOGIC_REF_COUNTS
        .write()
        .unwrap()
        .entry(fp)
        .or_default() += 1;
}

/// Check if a single fingerprint is in the current logic set.
pub fn contains(fp: &Fingerprint) -> bool {
    CURRENT_LOGIC_REF_COUNTS.read().unwrap().contains_key(fp)
}

/// Check if all fingerprints are in the current logic set.
pub fn all_contained(fps: &[Fingerprint]) -> bool {
    let ref_counts = CURRENT_LOGIC_REF_COUNTS.read().unwrap();
    fps.iter().all(|fp| ref_counts.contains_key(fp))
}

/// Check if all fingerprints are in the global logic set or the environment's logic set.
pub fn all_contained_with_env<Prof: EngineProfile>(
    fps: &[Fingerprint],
    env: &Environment<Prof>,
) -> bool {
    let ref_counts = CURRENT_LOGIC_REF_COUNTS.read().unwrap();
    fps.iter()
        .all(|fp| ref_counts.contains_key(fp) || env.logic_set_contains(fp))
}

/// Release one registration of a logic fingerprint. It leaves the current logic
/// set when its last registration is released; releasing a fingerprint that is
/// not registered is a no-op.
pub fn unregister(fp: &Fingerprint) {
    let mut ref_counts = CURRENT_LOGIC_REF_COUNTS.write().unwrap();
    if let Entry::Occupied(mut entry) = ref_counts.entry(*fp) {
        *entry.get_mut() -= 1;
        if *entry.get() == 0 {
            entry.remove();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_stays_registered_until_its_last_registration_is_released() {
        // The registry is process-global, so use a fingerprint no other test registers.
        let fp = Fingerprint::from("logic_registry::tests::shared_fingerprint").unwrap();

        // Releasing an unregistered fingerprint is a no-op: it must not offset
        // a later registration.
        unregister(&fp);
        register(fp);
        register(fp);

        unregister(&fp);
        assert!(contains(&fp));
        assert!(all_contained(&[fp]));

        unregister(&fp);
        assert!(!contains(&fp));
        assert!(!all_contained(&[fp]));
    }
}
