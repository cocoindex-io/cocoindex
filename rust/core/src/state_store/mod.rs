//! Storage layer for engine internal state.
//!
//! The logical byte schema and typed per-entity I/O live on [`AppStore`].
//! [`backend::StorageBackend`] is the byte-oriented seam below it, with LMDB
//! as the default adapter and an optional Postgres adapter behind the
//! `postgres` cargo feature. Engine code outside this module only calls
//! methods on these types; it does not touch `heed::*`, the key codec, or the
//! msgpack serialization.

mod app_store;
mod backend;
mod storage;
mod submit_session;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod txn;

pub use app_store::AppStore;
#[cfg(test)]
pub(crate) use backend::{AppStoreHandle, LmdbBackend, StorageBackend};
pub use storage::{Storage, StorageSettings};
pub use submit_session::{
    CommitPlan, ExistenceReconciler, OwnerStateForPreempt, PrecommitClaimTargetsPlan,
    PrecommitClaimTargetsResult, PrecommitReadPlan, PrecommitReadResult, PrecommitSession,
    PrecommitWritePlan, reconcile_child_existence,
};
pub use txn::{ReadTxn, WriteTxn};
