//! Read and write transaction wrappers.
//!
//! These wrap the underlying LMDB transaction types so engine code outside
//! `state_store/` doesn't see `heed::*`. The wrappers `Deref` to the inner
//! heed types so internal call sites (within this module) can still reach
//! the heed API.
//!
//! The [`AnyTxn`] trait lets read-only [`AppStore`](super::AppStore) methods
//! be called from either a `ReadTxn` or `WriteTxn` context.

use crate::state_store::app_store::Database;

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

/// Read transaction wrapper. Opened from [`Storage::read_txn`](super::Storage::read_txn).
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
/// both `ReadTxn` and `WriteTxn` so `AppStore` read methods can be called
/// from either context without separate variants.
///
/// The trait is sealed — only the two wrapper types implement it. The hidden
/// methods are accessed within this module via `pub(crate)` visibility.
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
