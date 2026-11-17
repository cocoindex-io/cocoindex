//! Text processing operations for CocoIndex.
//!
//! This crate is an internal part of [CocoIndex](https://cocoindex.io), published
//! only so the [`cocoindex`](https://docs.rs/cocoindex) crate can depend on it. It
//! makes no API stability promise: any release may change it. Depend on
//! `cocoindex` instead.
//!
//! This crate provides text processing functionality including:
//! - Text splitting by separators
//! - Recursive text chunking with syntax awareness
//!
//! Language detection / the tree-sitter registry and the shared
//! [`CodeSource`](cocoindex_code_ast::CodeSource) input type live in the
//! `cocoindex_code_ast` crate.

pub mod pattern_matcher;
pub mod split;
