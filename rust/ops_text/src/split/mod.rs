//! Text splitting utilities.
//!
//! This module provides text splitting functionality including:
//! - Splitting by regex separators
//! - Recursive syntax-aware chunking

mod by_separators;
mod recursive;

pub use crate::output_positions::{OutputPosition, TextRange};
pub use by_separators::{KeepSeparator, SeparatorSplitConfig, SeparatorSplitter};
pub use recursive::{
    CustomLanguageConfig, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
};

/// A chunk of text with its range and position information.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Byte range in the original text. Use this to slice the original string.
    pub range: TextRange,
    /// Start position (character offset, line, column).
    pub start: OutputPosition,
    /// End position (character offset, line, column).
    pub end: OutputPosition,
}
