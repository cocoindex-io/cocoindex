//! Storage layer for engine internal state.
//!
//! Everything LMDB-specific lives in this module: heed types, key encoding,
//! transaction batching, and the typed per-entity I/O methods on
//! [`AppStore`] and [`Storage`]. Engine code outside this module never
//! touches `heed::*`, the key codec, or the msgpack serialization — it
//! only calls methods on these types.
//!
//! Submodules are private; reach types via `state_store::AppStore` etc.

mod app_store;
mod storage;
mod txn;

use crate::prelude::*;

pub use app_store::AppStore;
pub use storage::{Pg40001Backoff, Storage, StorageSettings};
pub use txn::WriteTxn;

/// Returns true if `err` is a PostgreSQL SSI serialization failure
/// (SQLSTATE `40001`). OSS ships only the LMDB backend, which has a
/// single-writer model and never produces `40001`; this stub always
/// returns `false`. The enterprise edition's Postgres backend
/// implements the actual SQLSTATE inspection.
///
/// The function exists as a shared symbol so retry-loop call sites
/// (`Storage::run_txn_with_retry`, submit's pre_commit retry) have the
/// same shape in both editions — making upstream merges into the
/// enterprise edition mechanical.
pub fn is_pg_serialization_failure(_err: &Error) -> bool {
    false
}
