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
mod submit_session;
mod txn;

pub use app_store::AppStore;
pub use storage::{Storage, StorageSettings};
pub use submit_session::{
    CommitPlan, ExistenceReconciler, OwnerStateForPreempt, PrecommitClaimTargetsPlan,
    PrecommitClaimTargetsResult, PrecommitReadPlan, PrecommitReadResult, PrecommitSession,
    PrecommitWritePlan, reconcile_child_existence,
};
pub use txn::{ReadTxn, WriteTxn};
