//! Text chunk types with position information.
//!
//! Mirrors Python's `cocoindex.resources.chunk` (`Chunk`, `TextPosition`).
//! A [`Chunk`] records the byte range and start/end positions of a slice of a
//! source text. Unlike the Python dataclass — which eagerly copies the chunk
//! text into the struct — the Rust [`Chunk`] keeps only the range and exposes
//! the slice on demand via [`Chunk::text`], so chunking large documents does
//! not duplicate their contents.

use std::ops::Range;

use serde::{Deserialize, Serialize};

/// A position in a text, tracked by byte offset, character offset, and
/// 1-based line/column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPosition {
    /// Byte offset from the start of the text.
    pub byte_offset: usize,
    /// Character (not byte) offset from the start of the text.
    pub char_offset: usize,
    /// 1-based line number.
    pub line: u32,
    /// 1-based column number.
    pub column: u32,
}

/// A chunk of text with its range and position information.
///
/// The chunk does not own its text; call [`Chunk::text`] with the original
/// source string to obtain the slice ergonomically:
///
/// ```
/// # use cocoindex::resources::chunk::{Chunk, TextPosition};
/// # let source = "hello world";
/// # let chunk = Chunk::new(0..5,
/// #     TextPosition { byte_offset: 0, char_offset: 0, line: 1, column: 1 },
/// #     TextPosition { byte_offset: 5, char_offset: 5, line: 1, column: 6 });
/// assert_eq!(chunk.text(source), "hello");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    byte_range: Range<usize>,
    /// Start position of the chunk.
    pub start: TextPosition,
    /// End position of the chunk.
    pub end: TextPosition,
}

impl Chunk {
    /// Construct a chunk from a byte range and its start/end positions.
    pub fn new(byte_range: Range<usize>, start: TextPosition, end: TextPosition) -> Self {
        Self {
            byte_range,
            start,
            end,
        }
    }

    /// The byte range of this chunk within the source text.
    pub fn range(&self) -> Range<usize> {
        self.byte_range.clone()
    }

    /// The text of this chunk, sliced from the original `source` it was split
    /// from. Returns `""` if the range does not land on char boundaries of
    /// `source` (which only happens if `source` is not the original text).
    pub fn text<'a>(&self, source: &'a str) -> &'a str {
        source.get(self.byte_range.clone()).unwrap_or("")
    }
}
