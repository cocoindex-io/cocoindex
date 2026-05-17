//! Storage layer for engine internal state.
//!
//! Everything LMDB-specific lives in this module: heed types, key encoding,
//! transaction batching, and the thin per-entity I/O wrappers used by the
//! engine. The engine code outside this module never touches `heed::*`,
//! the key codec, or the msgpack serialization — only typed entity ops.
//!
//! See `specs/core/state_store_refactor.md` for the design rationale.

pub mod app_store;
pub mod ops;
pub mod storage;
pub mod txn;
pub mod txn_batcher;

pub use app_store::{AppStore, Database};
pub use storage::{Storage, StorageSettings};
pub use txn::{AnyTxn, ReadTxn, WriteTxn};
