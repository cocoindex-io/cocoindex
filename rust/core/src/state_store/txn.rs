//! Transaction wrappers and the shared transaction/resize coordinator.
//!
//! Every read or write transaction opened through [`super::Storage`] acquires
//! a guard on the storage coordinator for its full lifetime. For LMDB this is
//! also what protects `unsafe Env::resize()` from racing an active transaction.
//!
//! `WriteTxn` is deliberately a small enum rather than a backend trait: the
//! engine passes it through `Storage::run_txn`, and enum dispatch keeps that
//! hot path monomorphic while still allowing the LMDB and Postgres adapters to
//! share one transaction seam.

use crate::prelude::*;
use std::ops::{Deref, DerefMut};

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

/// Backend-specific write transaction carried through `Storage::run_txn`.
pub struct WriteTxn<'env>(pub(crate) WriteTxnInner<'env>);

pub(crate) enum WriteTxnInner<'env> {
    Lmdb(heed::RwTxn<'env>),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'env, sqlx::Postgres>),
}

impl<'env> WriteTxn<'env> {
    pub(crate) fn lmdb(inner: heed::RwTxn<'env>) -> Self {
        Self(WriteTxnInner::Lmdb(inner))
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(inner: sqlx::Transaction<'env, sqlx::Postgres>) -> Self {
        Self(WriteTxnInner::Postgres(inner))
    }

    #[cfg(test)]
    pub(crate) fn new(inner: heed::RwTxn<'env>) -> Self {
        Self::lmdb(inner)
    }

    pub(crate) fn into_inner(self) -> WriteTxnInner<'env> {
        self.0
    }

    pub(crate) fn lmdb_mut(&mut self) -> Result<&mut heed::RwTxn<'env>> {
        match &mut self.0 {
            WriteTxnInner::Lmdb(txn) => Ok(txn),
            #[cfg(feature = "postgres")]
            WriteTxnInner::Postgres(_) => {
                client_bail!("attempted to use a Postgres write transaction as an LMDB transaction")
            }
        }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_mut(&mut self) -> Result<&mut sqlx::Transaction<'env, sqlx::Postgres>> {
        match &mut self.0 {
            WriteTxnInner::Postgres(txn) => Ok(txn),
            WriteTxnInner::Lmdb(_) => {
                client_bail!("attempted to use an LMDB write transaction as a Postgres transaction")
            }
        }
    }
}

impl<'env> Deref for WriteTxn<'env> {
    type Target = heed::RwTxn<'env>;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            WriteTxnInner::Lmdb(txn) => txn,
            #[cfg(feature = "postgres")]
            WriteTxnInner::Postgres(_) => {
                panic!("WriteTxn::deref is only available for the LMDB backend")
            }
        }
    }
}

impl<'env> DerefMut for WriteTxn<'env> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            WriteTxnInner::Lmdb(txn) => txn,
            #[cfg(feature = "postgres")]
            WriteTxnInner::Postgres(_) => {
                panic!("WriteTxn::deref_mut is only available for the LMDB backend")
            }
        }
    }
}
