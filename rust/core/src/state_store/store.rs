//! Opaque storage handle and transaction wrappers.
//!
//! These wrap the underlying LMDB types so engine code outside `state_store/`
//! can pass them through as tokens without ever naming `heed::*`. During the
//! refactor migration the wrappers implement `Deref` to the inner heed types
//! so existing call sites continue to compile; once all engine call sites go
//! through `state_store::ops::*` the deref impls can be tightened or removed.
//!
//! See `specs/core/state_store_refactor.md`.

use std::ops::{Deref, DerefMut};

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// Per-app storage handle. Holds the LMDB database handle; transactions are
/// opened from the parent `Environment`.
#[derive(Clone)]
pub struct Store {
    pub(crate) db: Database,
}

impl Store {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Internal accessor for `state_store::ops`. Engine code outside this
    /// module never reaches the raw `Database`.
    pub(crate) fn db(&self) -> Database {
        self.db
    }
}

/// Write transaction wrapper. Threaded through `TxnBatcher::run` closures.
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

/// Read transaction wrapper. Opened from `Environment::read_txn()`.
pub struct ReadTxn<'env>(pub(crate) heed::RoTxn<'env, heed::WithoutTls>);

impl<'env> ReadTxn<'env> {
    pub(crate) fn new(inner: heed::RoTxn<'env, heed::WithoutTls>) -> Self {
        Self(inner)
    }
}

impl<'env> Deref for ReadTxn<'env> {
    type Target = heed::RoTxn<'env, heed::WithoutTls>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Marker trait for operations that only need read access. Implemented by
/// both `ReadTxn` and `WriteTxn` so `state_store::ops` read functions can
/// be called from either context without separate variants.
///
/// The trait is sealed — only the two wrapper types implement it. The hidden
/// methods are accessed by the `ops` module via `pub(crate)` visibility.
pub trait AnyTxn: sealed::Sealed {
    #[doc(hidden)]
    fn db_get_bytes<'a>(
        &'a self,
        db: Database,
        key: &[u8],
    ) -> crate::prelude::Result<Option<&'a [u8]>>;

    #[doc(hidden)]
    fn db_prefix_iter<'a>(
        &'a self,
        db: Database,
        prefix: &[u8],
    ) -> crate::prelude::Result<heed::RoPrefix<'a, heed::types::Bytes, heed::types::Bytes>>;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::ReadTxn<'_> {}
    impl Sealed for super::WriteTxn<'_> {}
}

impl<'env> AnyTxn for ReadTxn<'env> {
    fn db_get_bytes<'a>(
        &'a self,
        db: Database,
        key: &[u8],
    ) -> crate::prelude::Result<Option<&'a [u8]>> {
        Ok(db.get(&self.0, key)?)
    }

    fn db_prefix_iter<'a>(
        &'a self,
        db: Database,
        prefix: &[u8],
    ) -> crate::prelude::Result<heed::RoPrefix<'a, heed::types::Bytes, heed::types::Bytes>> {
        Ok(db.prefix_iter(&self.0, prefix)?)
    }
}

impl<'env> AnyTxn for WriteTxn<'env> {
    fn db_get_bytes<'a>(
        &'a self,
        db: Database,
        key: &[u8],
    ) -> crate::prelude::Result<Option<&'a [u8]>> {
        Ok(db.get(&self.0, key)?)
    }

    fn db_prefix_iter<'a>(
        &'a self,
        db: Database,
        prefix: &[u8],
    ) -> crate::prelude::Result<heed::RoPrefix<'a, heed::types::Bytes, heed::types::Bytes>> {
        Ok(db.prefix_iter(&self.0, prefix)?)
    }
}
