//! The CocoIndex engine: processing components, change detection, target-state
//! syncing and the LMDB state store.
//!
//! This crate is an internal part of [CocoIndex](https://cocoindex.io), published
//! only so the [`cocoindex`](https://docs.rs/cocoindex) crate can depend on it. It
//! makes no API stability promise: any release may change it. Depend on
//! `cocoindex` instead.

// These docs are for maintainers, and link private items freely. Read them
// with `cargo doc --document-private-items`; a build without it (docs.rs)
// renders such a link as plain code.
#![allow(rustdoc::private_intra_doc_links)]

mod prelude;

pub mod engine;
pub mod inspect;
pub mod state;
pub mod state_store;
pub mod telemetry;
