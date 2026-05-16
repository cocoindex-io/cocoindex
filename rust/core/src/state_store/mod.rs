//! Storage layer for engine internal state.
//!
//! Everything LMDB-specific lives in this module: heed types, key encoding,
//! transaction batching, and the thin per-entity I/O wrappers used by the
//! engine. The engine code outside this module never touches `heed::*`,
//! the key codec, or the msgpack serialization — only typed entity ops.
//!
//! The function signatures in this module are the spec for any alternative
//! backend (e.g. Postgres in the enterprise edition): a backend swap is a
//! replacement of this module's bodies, not its surface.
//!
//! See `specs/core/state_store_refactor.md` for the design rationale.

pub mod ops;
pub mod store;
pub mod txn_batcher;

pub use store::{AnyTxn, Database, ReadTxn, Store, WriteTxn};
pub use txn_batcher::TxnBatcher;
