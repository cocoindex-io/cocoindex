//! Reusable SDK resource abstractions shared across connectors and ops.
//!
//! Mirrors Python's `cocoindex.resources` package. These types carry no heavy
//! dependencies and are always available.

pub mod chunk;
pub mod embedder;
pub mod schema;
