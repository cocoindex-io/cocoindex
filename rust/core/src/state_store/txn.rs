//! LMDB transaction wrappers and the shared transaction/resize coordinator.
//!
//! The coordinator is an [`EnvSlot`]: a lock around the LMDB env itself.
//! Every read or write LMDB transaction in this process holds a read guard on
//! it for its full lifetime, and reaches the env only through that guard.
//! The write guard is taken to call `unsafe Env::resize()` and to close the
//! env, guaranteeing no participating transaction is active.

use crate::prelude::*;

use std::ops::{Deref, DerefMut};

pub(crate) type Env = heed::Env<heed::WithoutTls>;

/// The LMDB env behind the transaction/resize coordinator. `None` once the
/// storage is closed.
pub(crate) type EnvSlot = tokio::sync::RwLock<Option<Env>>;

/// The env in a guarded [`EnvSlot`], or an error if the storage is closed.
pub(crate) fn opened_env(slot: &Option<Env>) -> Result<&Env> {
    slot.as_ref()
        .ok_or_else(|| client_error!("The CocoIndex environment is closed"))
}

/// Guarded LMDB read transaction returned to callers. Holds a coordinator read
/// lock for the lifetime of the inner `RoTxn`, which owns a clone of the env.
pub struct ReadTxn {
    // Must be declared before `_guard` so the LMDB transaction (and its env
    // clone) is dropped before the coordinator guard is released (Rust drops
    // fields in declaration order).
    txn: heed::RoTxn<'static, heed::WithoutTls>,
    _guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
}

impl ReadTxn {
    pub(crate) fn new(
        guard: tokio::sync::OwnedRwLockReadGuard<Option<Env>>,
        txn: heed::RoTxn<'static, heed::WithoutTls>,
    ) -> Self {
        Self { txn, _guard: guard }
    }
}

impl Deref for ReadTxn {
    type Target = heed::RoTxn<'static, heed::WithoutTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

/// Write transaction wrapper. Threaded through `Storage::run_txn` closures.
pub struct WriteTxn<'env>(pub(crate) heed::RwTxn<'env>);

impl<'env> WriteTxn<'env> {
    pub(crate) fn new(inner: heed::RwTxn<'env>) -> Self {
        Self(inner)
    }

    pub(crate) fn into_inner(self) -> heed::RwTxn<'env> {
        self.0
    }
}

impl<'env> Deref for WriteTxn<'env> {
    type Target = heed::RwTxn<'env>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'env> DerefMut for WriteTxn<'env> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
