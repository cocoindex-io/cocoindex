//! Text processing operations exposed to Python.

use cocoindex_ops_text::pattern_matcher::PatternMatcher;
use cocoindex_ops_text::prog_langs;
use cocoindex_ops_text::split::{
    Chunk, CustomLanguageConfig, KeepSeparator, RecursiveChunkConfig, RecursiveChunker,
    RecursiveSplitConfig, SeparatorSplitConfig, SeparatorSplitter,
};
use pyo3::prelude::*;

/// A chunk of text with its range and position information (returned to Python).
///
/// Note: This struct does not include the text content itself. Instead, it provides
/// byte offsets (start_byte, end_byte) so Python can efficiently slice the original
/// text without copying data across the FFI boundary.
#[pyclass(name = "Chunk")]
#[derive(Clone)]
pub struct PyChunk {
    #[pyo3(get)]
    pub start_byte: usize,
    #[pyo3(get)]
    pub end_byte: usize,
    #[pyo3(get)]
    pub start_char_offset: usize,
    #[pyo3(get)]
    pub start_line: u32,
    #[pyo3(get)]
    pub start_column: u32,
    #[pyo3(get)]
    pub end_char_offset: usize,
    #[pyo3(get)]
    pub end_line: u32,
    #[pyo3(get)]
    pub end_column: u32,
}

impl PyChunk {
    fn from_chunk(chunk: &Chunk) -> Self {
        Self {
            start_byte: chunk.range.start,
            end_byte: chunk.range.end,
            start_char_offset: chunk.start.char_offset,
            start_line: chunk.start.line,
            start_column: chunk.start.column,
            end_char_offset: chunk.end.char_offset,
            end_line: chunk.end.line,
            end_column: chunk.end.column,
        }
    }
}

/// Detect programming language from a filename.
///
/// Returns the language name if the file extension is recognized, otherwise None.
#[pyfunction]
#[pyo3(signature = (*, filename))]
pub fn detect_code_language(filename: &str) -> Option<String> {
    prog_langs::detect_language(filename).map(|s| s.to_string())
}

fn parse_keep_separator(keep_separator: Option<&str>) -> PyResult<Option<KeepSeparator>> {
    match keep_separator {
        Some("left") => Ok(Some(KeepSeparator::Left)),
        Some("right") => Ok(Some(KeepSeparator::Right)),
        Some(other) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid keep_separator value: '{}'. Expected 'left', 'right', or None.",
            other
        ))),
        None => Ok(None),
    }
}

/// A text splitter that splits by regex separators.
#[pyclass(name = "SeparatorSplitter")]
pub struct PySeparatorSplitter {
    splitter: SeparatorSplitter,
}

#[pymethods]
impl PySeparatorSplitter {
    /// Create a new separator splitter.
    ///
    /// Args:
    ///     separators_regex: A list of regex patterns for separators. They are OR-joined.
    ///     keep_separator: How to handle separators. "left" includes separator at the end of
    ///         preceding chunk, "right" includes it at the start of following chunk, None discards.
    ///     include_empty: Whether to include empty chunks in the output.
    ///     trim: Whether to trim whitespace from chunks.
    #[new]
    #[pyo3(signature = (separators_regex, keep_separator=None, include_empty=false, trim=true))]
    fn new(
        separators_regex: Vec<String>,
        keep_separator: Option<&str>,
        include_empty: bool,
        trim: bool,
    ) -> PyResult<Self> {
        let keep_sep = parse_keep_separator(keep_separator)?;

        let config = SeparatorSplitConfig {
            separators_regex,
            keep_separator: keep_sep,
            include_empty,
            trim,
        };

        let splitter = SeparatorSplitter::new(config).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid regex: {}", e))
        })?;

        Ok(Self { splitter })
    }

    /// Split the text and return chunks with position information.
    ///
    /// Args:
    ///     text: The text to split.
    ///
    /// Returns:
    ///     A list of chunks.
    fn split(&self, py: Python<'_>, text: &str) -> Vec<PyChunk> {
        py.detach(|| {
            let chunks = self.splitter.split(text);
            chunks.iter().map(PyChunk::from_chunk).collect()
        })
    }
}

/// Configuration for a custom language with regex-based separators.
#[pyclass(name = "CustomLanguageConfig")]
#[derive(Clone)]
pub struct PyCustomLanguageConfig {
    #[pyo3(get)]
    pub language_name: String,
    #[pyo3(get)]
    pub aliases: Vec<String>,
    #[pyo3(get)]
    pub separators_regex: Vec<String>,
}

#[pymethods]
impl PyCustomLanguageConfig {
    /// Create a new custom language configuration.
    ///
    /// Args:
    ///     language_name: The name of the language.
    ///     separators_regex: Regex patterns for separators, in order of priority.
    ///     aliases: Aliases for the language name.
    #[new]
    #[pyo3(signature = (language_name, separators_regex, aliases=vec![]))]
    fn new(language_name: String, separators_regex: Vec<String>, aliases: Vec<String>) -> Self {
        Self {
            language_name,
            aliases,
            separators_regex,
        }
    }
}

/// A recursive text splitter with syntax awareness.
#[pyclass(name = "RecursiveSplitter")]
pub struct PyRecursiveSplitter {
    chunker: RecursiveChunker,
}

#[pymethods]
impl PyRecursiveSplitter {
    /// Create a new recursive splitter.
    ///
    /// Args:
    ///     custom_languages: A list of custom language configurations for syntax-aware splitting.
    #[new]
    #[pyo3(signature = (*, custom_languages=vec![]))]
    fn new(custom_languages: Vec<PyCustomLanguageConfig>) -> PyResult<Self> {
        let config = RecursiveSplitConfig {
            custom_languages: custom_languages
                .into_iter()
                .map(|lang| CustomLanguageConfig {
                    language_name: lang.language_name,
                    aliases: lang.aliases,
                    separators_regex: lang.separators_regex,
                })
                .collect(),
        };

        let chunker = RecursiveChunker::new(config)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        Ok(Self { chunker })
    }

    /// Split the text into chunks according to the configuration.
    ///
    /// Args:
    ///     text: The text to split.
    ///     chunk_size: Target chunk size in bytes.
    ///     min_chunk_size: Minimum chunk size in bytes. Defaults to chunk_size / 2.
    ///     chunk_overlap: Overlap between consecutive chunks in bytes.
    ///     language: Language name or file extension for syntax-aware splitting.
    ///
    /// Returns:
    ///     A list of chunks.
    #[pyo3(signature = (text, chunk_size, min_chunk_size=None, chunk_overlap=None, language=None))]
    fn split(
        &self,
        py: Python<'_>,
        text: &str,
        chunk_size: usize,
        min_chunk_size: Option<usize>,
        chunk_overlap: Option<usize>,
        language: Option<String>,
    ) -> Vec<PyChunk> {
        py.detach(|| {
            let config = RecursiveChunkConfig {
                chunk_size,
                min_chunk_size,
                chunk_overlap,
                language,
            };

            let chunks = self.chunker.split(text, config);
            chunks.iter().map(PyChunk::from_chunk).collect()
        })
    }
}

/// A pattern matcher using globset patterns for filtering file paths.
#[pyclass(name = "PatternMatcher")]
pub struct PyPatternMatcher {
    matcher: PatternMatcher,
}

#[pymethods]
impl PyPatternMatcher {
    /// Create a new PatternMatcher.
    ///
    /// Args:
    ///     included_patterns: Glob patterns for files to include. If None, all files are included.
    ///     excluded_patterns: Glob patterns for files/directories to exclude.
    #[new]
    #[pyo3(signature = (included_patterns=None, excluded_patterns=None))]
    fn new(
        included_patterns: Option<Vec<String>>,
        excluded_patterns: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let matcher = PatternMatcher::new(included_patterns, excluded_patterns)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;
        Ok(Self { matcher })
    }

    /// Check if a directory should be included (traversed).
    fn is_dir_included(&self, path: &str) -> bool {
        self.matcher.is_dir_included(path)
    }

    /// Check if a file should be included based on both include and exclude patterns.
    fn is_file_included(&self, path: &str) -> bool {
        self.matcher.is_file_included(path)
    }
}
