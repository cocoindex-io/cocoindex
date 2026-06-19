//! Text splitting and language detection facade.
//!
//! Mirrors Python's `cocoindex.ops.text`. This is a thin, ergonomic wrapper
//! over the `cocoindex_ops_text` crate that returns SDK [`Chunk`] objects
//! (with [`Chunk::text`] access) instead of the lower-level
//! `cocoindex_ops_text::split::Chunk`, so examples and user code do not have to
//! re-slice the source by raw byte offsets.

use cocoindex_ops_text::prog_langs;
use cocoindex_ops_text::split;

use crate::error::{Error, Result};
use crate::resources::chunk::{Chunk, TextPosition};

// Re-export the lightweight config types so callers configure the splitters
// without depending on `cocoindex_ops_text` directly.
pub use cocoindex_ops_text::split::{
    CustomLanguageConfig, KeepSeparator, RecursiveChunkConfig, SeparatorSplitConfig,
};

/// Detect the programming language for a filename from its extension.
///
/// Returns the canonical language name (e.g. `"python"`, `"rust"`) or `None`
/// when the extension is not recognized.
///
/// ```
/// # use cocoindex::ops::text::detect_code_language;
/// assert_eq!(detect_code_language("main.py").as_deref(), Some("python"));
/// assert_eq!(detect_code_language("file.xyz"), None);
/// ```
pub fn detect_code_language(filename: &str) -> Option<String> {
    prog_langs::detect_language(filename).map(str::to_string)
}

fn convert_chunk(c: split::Chunk) -> Chunk {
    Chunk::new(
        c.range.start..c.range.end,
        TextPosition {
            byte_offset: c.range.start,
            char_offset: c.start.char_offset,
            line: c.start.line,
            column: c.start.column,
        },
        TextPosition {
            byte_offset: c.range.end,
            char_offset: c.end.char_offset,
            line: c.end.line,
            column: c.end.column,
        },
    )
}

/// A text splitter that splits by regex separators.
///
/// Construct once and reuse to split many texts. Mirrors Python's
/// `SeparatorSplitter`.
pub struct SeparatorSplitter {
    inner: split::SeparatorSplitter,
}

impl SeparatorSplitter {
    /// Create a splitter that discards separators and trims whitespace (the
    /// common case). `separators_regex` patterns are OR-joined.
    pub fn new(separators_regex: impl IntoIterator<Item = impl Into<String>>) -> Result<Self> {
        Self::with_config(split::SeparatorSplitConfig {
            separators_regex: separators_regex.into_iter().map(Into::into).collect(),
            ..Default::default()
        })
    }

    /// Create a splitter with full control over separator handling, empty
    /// chunks, and trimming.
    pub fn with_config(config: split::SeparatorSplitConfig) -> Result<Self> {
        let inner = split::SeparatorSplitter::new(config)
            .map_err(|e| Error::engine(format!("invalid separator regex: {e}")))?;
        Ok(Self { inner })
    }

    /// Split `text` into chunks with position information.
    pub fn split(&self, text: &str) -> Vec<Chunk> {
        self.inner
            .split(text)
            .into_iter()
            .map(convert_chunk)
            .collect()
    }
}

/// A recursive, syntax-aware text splitter.
///
/// Splits text along syntax boundaries (paragraphs, sentences, and — when a
/// `language` is given — tree-sitter nodes). Construct once and reuse. Mirrors
/// Python's `RecursiveSplitter`.
pub struct RecursiveSplitter {
    inner: split::RecursiveChunker,
}

impl RecursiveSplitter {
    /// Create a splitter with the built-in language support only.
    pub fn new() -> Result<Self> {
        Self::with_custom_languages(Vec::new())
    }

    /// Create a splitter with additional [`CustomLanguageConfig`]s that
    /// supplement (and may override by name/alias) the built-in languages.
    pub fn with_custom_languages(custom_languages: Vec<CustomLanguageConfig>) -> Result<Self> {
        let inner = split::RecursiveChunker::new(split::RecursiveSplitConfig { custom_languages })
            .map_err(Error::engine)?;
        Ok(Self { inner })
    }

    /// Split `text` into chunks targeting `chunk_size` bytes, using defaults
    /// for `min_chunk_size`/`chunk_overlap` and no language hint.
    pub fn split(&self, text: &str, chunk_size: usize) -> Vec<Chunk> {
        self.split_with(
            text,
            RecursiveChunkConfig {
                chunk_size,
                min_chunk_size: None,
                chunk_overlap: None,
                language: None,
            },
        )
    }

    /// Split `text` with full control over chunk size, overlap, and language.
    pub fn split_with(&self, text: &str, config: RecursiveChunkConfig) -> Vec<Chunk> {
        self.inner
            .split(text, config)
            .into_iter()
            .map(convert_chunk)
            .collect()
    }
}
