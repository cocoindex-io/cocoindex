//! Text splitting utilities.
//!
//! This module provides text splitting functionality including:
//! - Splitting by regex separators
//! - Recursive syntax-aware chunking

mod by_separators;
mod output_positions;
mod recursive;

pub use by_separators::{KeepSeparator, SeparatorSplitConfig, SeparatorSplitter};
pub use recursive::{
    CustomLanguageConfig, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
};

/// A text range specified by byte offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextRange {
    /// Start byte offset (inclusive).
    pub start: usize,
    /// End byte offset (exclusive).
    pub end: usize,
}

impl TextRange {
    /// Create a new text range.
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Get the length of the range in bytes.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the range is empty.
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Output position information with character offset and line/column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputPosition {
    /// Character (not byte) offset from the start of the text.
    pub char_offset: usize,
    /// 1-based line number.
    pub line: u32,
    /// 1-based column number.
    pub column: u32,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_range() {
        let range = TextRange::new(0, 10);
        assert_eq!(range.len(), 10);
        assert!(!range.is_empty());

        let empty = TextRange::new(5, 5);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }
}
