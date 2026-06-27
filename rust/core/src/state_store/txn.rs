//! LMDB transaction wrappers and the shared transaction/resize coordinator.
//!
//! Every read or write LMDB transaction in this process acquires a read guard
//! on [`TxnCoordinator`] for its full lifetime. [`TxnRunner`] acquires the
//! write guard before calling `unsafe Env::resize()`, guaranteeing no
//! participating transaction is active.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Serializes LMDB transactions against `Env::resize`.
///
/// * Normal read/write transactions hold a read guard for their lifetime.
/// * Resize holds the write guard, blocking until all guards drop.
pub(crate) type TxnCoordinator = Arc<tokio::sync::RwLock<()>>;

/// Guarded LMDB read transaction returned to callers. Holds a coordinator read
/// lock for the lifetime of the inner `RoTxn` and borrows the parent env via
/// `'store`.
pub struct ReadTxn<'store> {
    // Must be declared before `_guard` so the LMDB transaction is dropped
    // before the coordinator guard is released (Rust drops fields in
    // declaration order).
    txn: heed::RoTxn<'store, heed::WithoutTls>,
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl<'store> ReadTxn<'store> {
    pub(crate) fn new(
        guard: tokio::sync::OwnedRwLockReadGuard<()>,
        txn: heed::RoTxn<'store, heed::WithoutTls>,
    ) -> Self {
        Self { txn, _guard: guard }
    }
}

impl<'store> Deref for ReadTxn<'store> {
    type Target = heed::RoTxn<'store, heed::WithoutTls>;

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
