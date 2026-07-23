//! Reusable SDK resource abstractions shared across connectors and ops.
//!
//! Mirrors Python's `cocoindex.resources` package. These types carry no heavy
//! dependencies and are always available.

pub mod chunk;
pub mod embedder;
pub mod file;
pub mod fs;
pub mod id;
pub mod live_map;
pub mod rate_limit;
pub mod schema;
