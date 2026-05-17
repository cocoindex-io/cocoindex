//! Per-app handle within a [`Storage`](super::Storage).
//!
//! An `AppStore` is a cheap-clone token (the underlying database handle is
//! `Copy`) that identifies which app's entries a `state_store::ops::*` call
//! reads or writes. Operations always pair an `AppStore` with a `WriteTxn`
//! or `ReadTxn` (from [`crate::state_store::txn`]).

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// Per-app handle within a `Storage`.
#[derive(Clone)]
pub struct AppStore {
    pub(crate) db: Database,
}

impl AppStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Internal accessor for `state_store::ops`. Engine code outside this
    /// module never reaches the raw `Database`.
    pub(crate) fn db(&self) -> Database {
        self.db
    }
}
