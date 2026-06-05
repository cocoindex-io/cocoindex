//! Support for the `mount!` / `use_mount!` / `mount_each!` macros.
//!
//! These helpers are called from the generated macro code; they are not meant
//! to be used directly. The macros live in `cocoindex_macros`; see `design.md`
//! §7.2 for the grouped-call shape and the component-level memo fast-path they
//! enable.

use serde::Serialize;

use crate::error::Result;
use crate::memo::{finish_key_fingerprinter, new_key_fingerprinter, write_key_fingerprint_part};
use cocoindex_utils::fingerprint::Fingerprint;

/// Compute a processing component's memo fingerprint from the entry function's
/// logic identity (`module`, `name`, `code_hash`) and the fingerprint of its
/// non-`ctx` arguments.
///
/// The engine checks this fingerprint *before* running the component
/// (`Component::execute_once` → `memo_key_fingerprint`), so on an unchanged hit
/// the whole component — including child mounts and target-state declaration —
/// is skipped and the previous run is replayed.
///
/// Targets passed as mount arguments fingerprint by their stable key (e.g.
/// `DirTarget` by its directory), so pointing a component at a different output
/// invalidates it while reusing the same output is a hit.
#[doc(hidden)]
pub fn component_memo_fp<A: Serialize + ?Sized>(
    module: &str,
    name: &str,
    code_hash: u64,
    args: &A,
) -> Result<Fingerprint> {
    let mut fp = new_key_fingerprinter();
    write_key_fingerprint_part(&mut fp, &"cocoindex_component")?;
    write_key_fingerprint_part(&mut fp, &module)?;
    write_key_fingerprint_part(&mut fp, &name)?;
    write_key_fingerprint_part(&mut fp, &code_hash)?;
    write_key_fingerprint_part(&mut fp, args)?;
    Ok(finish_key_fingerprinter(fp))
}
