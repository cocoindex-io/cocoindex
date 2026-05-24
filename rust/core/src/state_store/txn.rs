//! Write transaction wrapper.
//!
//! Wraps the underlying LMDB `heed::RwTxn` so engine code outside
//! `state_store/` doesn't see `heed::*`. Derefs to the inner heed type
//! so internal call sites (within this module) can still reach the
//! heed API.
//!
//! Read access doesn't have a wrapper type: standalone read methods on
//! [`AppStore`](super::AppStore) open a fresh `heed::RoTxn` internally
//! per call, and in-write-txn reads (`*_in_txn`) take this [`WriteTxn`]
//! directly. There is no public read-transaction handle.

use std::ops::{Deref, DerefMut};

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
