//! Support for the `use_mount!` / `mount_each!` macros.
//!
//! These helpers are called from the generated macro code; they are not meant
//! to be used directly. The macros live in `cocoindex_macros`; see `design.md`
//! §7.2 for the grouped-call shape and the component-level memo fast-path they
//! enable.

use std::sync::Arc;

use serde::Serialize;

use crate::error::Result;
use crate::memo::{
    MemoInput, finish_key_fingerprinter, memo_value_fingerprint, new_key_fingerprinter,
    write_key_fingerprint_part,
};
use cocoindex_utils::fingerprint::Fingerprint;

/// The component-memo identity of one mounted call: the fingerprint the engine
/// checks before running the component, and the arguments whose external
/// state ([`MemoInput::memo_state`]) must still be valid for a hit.
#[doc(hidden)]
pub struct ComponentMemo {
    pub(crate) fp: Fingerprint,
    pub(crate) state_inputs: Vec<Arc<dyn MemoInput>>,
}

/// One mount argument's contribution to [`ComponentMemo`].
#[doc(hidden)]
pub struct ComponentArgPart {
    key: Fingerprint,
    state_input: Option<Arc<dyn MemoInput>>,
}

/// Wraps a mount argument so the generated code can pick its memo identity by
/// method resolution (autoref specialization): an argument whose type
/// implements [`MemoInput`] and `Clone` uses [`ComponentArgViaMemoInput`];
/// any other `Serialize` argument uses [`ComponentArgViaSerialize`].
#[doc(hidden)]
pub struct ComponentArg<'a, T: ?Sized>(pub &'a T);

/// Preferred path: the argument's [`MemoInput`] identity, plus its state for
/// validation. A file is therefore keyed by its path and revalidated by
/// mtime, then content, exactly as in a `#[function(memo)]` argument.
#[doc(hidden)]
pub trait ComponentArgViaMemoInput {
    fn __coco_component_arg(&self) -> Result<ComponentArgPart>;
}

impl<T: MemoInput + Clone + 'static> ComponentArgViaMemoInput for &ComponentArg<'_, T> {
    fn __coco_component_arg(&self) -> Result<ComponentArgPart> {
        Ok(ComponentArgPart {
            key: memo_value_fingerprint(self.0)?,
            state_input: Some(Arc::new(self.0.clone())),
        })
    }
}

/// Fallback path: the argument's serialized value, with no state validation.
/// Used for runtime handles such as targets, which serialize to their stable
/// identity.
#[doc(hidden)]
pub trait ComponentArgViaSerialize {
    fn __coco_component_arg(&self) -> Result<ComponentArgPart>;
}

impl<T: Serialize + ?Sized> ComponentArgViaSerialize for ComponentArg<'_, T> {
    fn __coco_component_arg(&self) -> Result<ComponentArgPart> {
        Ok(ComponentArgPart {
            key: Fingerprint::from(self.0)?,
            state_input: None,
        })
    }
}

/// Compute a processing component's memo identity from the entry function's
/// logic identity (`module`, `name`, `code_hash`) and its non-`ctx` arguments.
///
/// The engine checks the fingerprint *before* running the component, then
/// validates the arguments' states, so on an unchanged hit the whole component
/// — including child mounts and target-state declaration — is skipped and the
/// previous run is replayed.
#[doc(hidden)]
pub fn component_memo(
    module: &str,
    name: &str,
    code_hash: u64,
    args: Vec<ComponentArgPart>,
) -> Result<ComponentMemo> {
    let mut fp = new_key_fingerprinter();
    write_key_fingerprint_part(&mut fp, &"cocoindex_component")?;
    write_key_fingerprint_part(&mut fp, &module)?;
    write_key_fingerprint_part(&mut fp, &name)?;
    write_key_fingerprint_part(&mut fp, &code_hash)?;
    let mut state_inputs = Vec::new();
    for arg in args {
        write_key_fingerprint_part(&mut fp, &arg.key)?;
        state_inputs.extend(arg.state_input);
    }
    Ok(ComponentMemo {
        fp: finish_key_fingerprinter(fp),
        state_inputs,
    })
}
