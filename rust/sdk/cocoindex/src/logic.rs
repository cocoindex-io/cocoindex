//! Link-time registry of `#[coco::function]` logic fingerprints.
//!
//! Each `#[cocoindex::function]` adds one [`FnLogicEntry`] to the `COCO_FN_LOGIC`
//! distributed slice (collected across crates at link time). At app/environment
//! startup, `register_all_fn_logic` computes each function's logic fingerprint
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

use std::sync::{Once, OnceLock};

use cocoindex_core::engine::logic_registry;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::error::{Error, Result};

/// One `#[coco::function]`'s logic identity, collected at link time.
///
/// The fingerprint is computed once per process ([`fingerprint`](Self::fingerprint)):
/// registration and every call of the function read the same cached value,
/// so a call hashes nothing.
pub struct FnLogicEntry {
    pub module: &'static str,
    pub name: &'static str,
    pub code_hash: u64,
    /// The function's `deps = ...` value, if any. See [`FnDeps`].
    pub deps: Option<&'static FnDeps>,
    fingerprint: OnceLock<std::result::Result<Fingerprint, String>>,
}

impl FnLogicEntry {
    #[doc(hidden)]
    pub const fn new(
        module: &'static str,
        name: &'static str,
        code_hash: u64,
        deps: Option<&'static FnDeps>,
    ) -> Self {
        Self {
            module,
            name,
            code_hash,
            deps,
            fingerprint: OnceLock::new(),
        }
    }

    /// The function's logic fingerprint, computed on first use:
    /// `crate::ctx::fn_logic_fingerprint(module, name, code_hash, deps_fp)`.
    pub fn fingerprint(&self) -> Result<Fingerprint> {
        self.fingerprint
            .get_or_init(|| {
                let deps_fp = self
                    .deps
                    .map(FnDeps::get)
                    .transpose()
                    .map_err(|err| err.to_string())?;
                crate::ctx::fn_logic_fingerprint(self.module, self.name, self.code_hash, deps_fp)
                    .map_err(|err| err.to_string())
            })
            .clone()
            .map_err(|err| Error::engine(format!("logic fingerprint error: {err}")))
    }
}

/// The fingerprint of a `#[cocoindex::function(deps = ...)]` value, computed
/// once per process: at startup registration or at the first call, whichever
/// comes first, like Python's `deps=`, which is evaluated when the decorator is
/// applied. A per-call value belongs in the arguments instead.
pub struct FnDeps {
    cell: OnceLock<std::result::Result<Fingerprint, String>>,
    compute: fn() -> Result<Fingerprint>,
}

impl FnDeps {
    #[doc(hidden)]
    pub const fn new(compute: fn() -> Result<Fingerprint>) -> Self {
        Self {
            cell: OnceLock::new(),
            compute,
        }
    }

    /// The deps fingerprint, computing it on first use.
    pub fn get(&self) -> Result<Fingerprint> {
        self.cell
            .get_or_init(|| (self.compute)().map_err(|err| err.to_string()))
            .clone()
            .map_err(|err| Error::engine(format!("deps fingerprint error: {err}")))
    }
}

#[linkme::distributed_slice]
pub static COCO_FN_LOGIC: [FnLogicEntry] = [..];

/// Register every `#[coco::function]`'s logic fingerprint into the engine's
/// logic set. The slice is fixed at link time and these registrations are held
/// for the life of the process, so only the first call registers; later calls
/// (one per app/environment build) are no-ops.
pub(crate) fn register_all_fn_logic() {
    static REGISTERED: Once = Once::new();
    REGISTERED.call_once(|| {
        for entry in COCO_FN_LOGIC {
            match entry.fingerprint() {
                Ok(fp) => logic_registry::register(fp),
                Err(err) => tracing::warn!(
                    "{}::{} is not registered in the logic set, so memo entries that recorded it never hit: {err}",
                    entry.module,
                    entry.name
                ),
            }
        }
    });
}
