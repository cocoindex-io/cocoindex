//! Text processing operations for CocoIndex.
//!
//! This crate provides text processing functionality including:
//! - Programming language detection and tree-sitter support
//! - Text splitting by separators
//! - Recursive text chunking with syntax awareness

pub(crate) mod output_positions;
pub mod pattern_matcher;
pub mod prog_langs;
pub mod split;
