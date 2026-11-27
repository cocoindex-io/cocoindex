//! Shared utilities for CocoIndex: errors, batching, concurrency control,
//! fingerprints and retries.
//!
//! This crate is an internal part of [CocoIndex](https://cocoindex.io), published
//! only so the [`cocoindex`](https://docs.rs/cocoindex) crate can depend on it. It
//! makes no API stability promise: any release may change it. Depend on
//! `cocoindex` instead.

pub mod batching;
pub mod concur_control;
pub mod db;
pub mod deser;
pub mod error;
pub mod fingerprint;
pub mod gpu_pool;
pub mod immutable;
pub mod ratelimit;
pub mod retryable;
pub mod slow_warn;

pub mod prelude;

#[cfg(feature = "reqwest")]
pub mod http;
#[cfg(feature = "reqwest")]
pub use reqwest;
