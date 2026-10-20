//! LMDB transaction wrappers and the shared transaction/resize coordinator.
//!
//! The coordinator is an [`EnvSlot`]: a lock around the LMDB env itself.
//! Every read or write LMDB transaction in this process holds a read guard on
//! it for its full lifetime, and reaches the env only through that guard.
//! The write guard is taken to call `unsafe Env::resize()` and to close the
//! env, guaranteeing no participating transaction is active.

use crate::prelude::*;

use std::ops::Deref;

pub(crate) type Env = heed::Env<heed::WithoutTls>;

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

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

/// What becomes of the writes a `run_txn` body makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteMode {
    /// The writes commit with the transaction.
    Apply,
    /// The writes are dropped as they are made: the body runs against the
    /// transaction's snapshot and persists nothing, and reads it makes
    /// afterwards do not see them. A preview's bodies run this way (see
    /// [`super::AppStore::discarding_writes`]).
    Discard,
}

/// Write transaction wrapper. Threaded through `Storage::run_txn` closures.
///
/// It is the one way to write to LMDB inside a batched write txn: it derefs
/// to the read-only `RwTxn` for reads, and every write goes through its own
/// methods ([`Self::put`], [`Self::delete`], [`Self::delete_prefix`]), which
/// honor the running body's [`WriteMode`]. Bodies sharing one txn can run in
/// different modes: the batcher sets the mode before each body runs.
pub struct WriteTxn<'env> {
    txn: heed::RwTxn<'env>,
    write_mode: WriteMode,
}

impl<'env> WriteTxn<'env> {
    pub(crate) fn new(inner: heed::RwTxn<'env>) -> Self {
        Self {
            txn: inner,
            write_mode: WriteMode::Apply,
        }
    }

    pub(crate) fn into_inner(self) -> heed::RwTxn<'env> {
        self.txn
    }

    pub(crate) fn set_write_mode(&mut self, write_mode: WriteMode) {
        self.write_mode = write_mode;
    }

    /// Store `value` under `key` in `db`, or drop the write in
    /// [`WriteMode::Discard`].
    pub(crate) fn put(&mut self, db: Database, key: &[u8], value: &[u8]) -> Result<()> {
        match self.write_mode {
            WriteMode::Apply => db.put(&mut self.txn, key, value)?,
            WriteMode::Discard => {}
        }
        Ok(())
    }

    /// Delete `key` from `db` if present, or drop the write in
    /// [`WriteMode::Discard`].
    pub(crate) fn delete(&mut self, db: Database, key: &[u8]) -> Result<()> {
        match self.write_mode {
            WriteMode::Apply => {
                db.delete(&mut self.txn, key)?;
            }
            WriteMode::Discard => {}
        }
        Ok(())
    }

    /// Delete every entry of `db` whose key starts with `prefix`, or drop the
    /// writes in [`WriteMode::Discard`].
    pub(crate) fn delete_prefix(&mut self, db: Database, prefix: &[u8]) -> Result<()> {
        match self.write_mode {
            WriteMode::Apply => {
                let mut iter = db.prefix_iter_mut(&mut self.txn, prefix)?;
                while iter.next().transpose()?.is_some() {
                    // Safety: the borrowed key/value are dropped before the
                    // next `next()`.
                    unsafe {
                        iter.del_current()?;
                    }
                }
            }
            WriteMode::Discard => {}
        }
        Ok(())
    }
}

impl<'env> Deref for WriteTxn<'env> {
    type Target = heed::RwTxn<'env>;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
