//! Link-time registry of `#[coco::function]` logic fingerprints.
//!
//! Each `#[cocoindex::function]` adds one [`FnLogicEntry`] to the [`COCO_FN_LOGIC`]
//! distributed slice (collected across crates at link time). At app/environment
//! startup, [`register_all_fn_logic`] computes each function's logic fingerprint
//! and registers it into the engine's logic set.
//!
//! Why this matters: a memoized caller's entry stores the logic fingerprints of
//! every `#[coco::function]` it transitively called (its `logic_deps`). On lookup
//! the engine validates them via `all_contained_with_env`. Without registration,
//! those fingerprints are never "contained", so any memo that calls a tracked
//! function would be perpetually invalid (it would re-run every time). Registering
//! the *current* fingerprints makes unchanged calls cache, while an edited
//! function (whose fingerprint changes on recompile) leaves the old, now-absent
//! fingerprint in a stale entry — correctly invalidating it.

use cocoindex_core::engine::logic_registry;
use cocoindex_utils::fingerprint::Fingerprint;

/// One `#[coco::function]`'s logic identity, collected at link time.
///
/// The fingerprint computed from these fields must match the one
/// `#[cocoindex::function]` records at call time (see `Ctx::__coco_tracked_fn`):
/// `Fingerprint::from(&("cocoindex_fn", module, name, code_hash))`.
pub struct FnLogicEntry {
    pub module: &'static str,
    pub name: &'static str,
    pub code_hash: u64,
}

#[linkme::distributed_slice]
pub static COCO_FN_LOGIC: [FnLogicEntry] = [..];

/// The logic fingerprint for one registered function — kept in sync with
/// `Ctx::__coco_tracked_fn`.
fn entry_fingerprint(e: &FnLogicEntry) -> Option<Fingerprint> {
    Fingerprint::from(&("cocoindex_fn", e.module, e.name, e.code_hash)).ok()
}

/// Register every `#[coco::function]`'s logic fingerprint into the engine's
/// logic set. Idempotent (the set is a `HashSet`), so it is safe to call on
/// every app/environment build.
pub(crate) fn register_all_fn_logic() {
    for entry in COCO_FN_LOGIC {
        if let Some(fp) = entry_fingerprint(entry) {
            logic_registry::register(fp);
        }
    }
}
