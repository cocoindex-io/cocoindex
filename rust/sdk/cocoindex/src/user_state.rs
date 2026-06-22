//! Persistent per-component user state.
//!
//! Mirrors Python's `coco.use_state()` plus the live-mode `read_committed_state`
//! / `write_committed_state` pair. Values are persisted across runs of the same
//! component (matched by its stable path) and serialized with MessagePack.

use std::sync::Arc;

use cocoindex_core::engine::context::ComponentProcessorContext;
use cocoindex_core::state::stable_path::StableKey;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::profile::RustProfile;

/// A key identifying a persistent component state. Accepts a string (the common
/// case) or a [`StableKey`] directly (e.g. a symbol key), mirroring Python's
/// `use_state`, which takes either.
pub trait IntoStateKey {
    fn into_state_key(self) -> StableKey;
}

impl IntoStateKey for StableKey {
    fn into_state_key(self) -> StableKey {
        self
    }
}

impl IntoStateKey for &str {
    fn into_state_key(self) -> StableKey {
        StableKey::Str(Arc::from(self))
    }
}

impl IntoStateKey for String {
    fn into_state_key(self) -> StableKey {
        StableKey::Str(Arc::from(self.as_str()))
    }
}

pub(crate) fn encode_state<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(value)?)
}

pub(crate) fn decode_state<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(bytes)?)
}

/// Handle for a persistent per-component state value, returned by
/// [`crate::Ctx::use_state`].
///
/// On the first run the handle holds `initial_value`; on later runs it holds the
/// value persisted at the end of the previous run. Read it with
/// [`value`](StateHandle::value); call [`set`](StateHandle::set) to persist a new
/// value for the next run.
pub struct StateHandle<T> {
    key: StableKey,
    value: T,
    comp_ctx: ComponentProcessorContext<RustProfile>,
}

impl<T> StateHandle<T> {
    pub(crate) fn new(
        key: StableKey,
        value: T,
        comp_ctx: ComponentProcessorContext<RustProfile>,
    ) -> Self {
        Self {
            key,
            value,
            comp_ctx,
        }
    }

    /// The current value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Consume the handle, returning the current value.
    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T: Serialize> StateHandle<T> {
    /// Persist `value` as this component's state for the next run, and update the
    /// value held by this handle.
    pub fn set(&mut self, value: T) -> Result<()> {
        self.comp_ctx
            .update_user_state(&self.key, encode_state(&value)?)
            .map_err(Error::from)?;
        self.value = value;
        Ok(())
    }
}
