//! Text processing operations exposed to Python.

use cocoindex_code_ast::view::{SegmentKind, SourceView};
use cocoindex_code_ast::{CodeSource, prog_langs};
use cocoindex_ops_text::pattern_matcher::PatternMatcher;
use cocoindex_ops_text::split::{
    Chunk, CustomLanguageConfig, KeepSeparator, RecursiveChunkConfig, RecursiveChunker,
    RecursiveSplitConfig, SeparatorSplitConfig, SeparatorSplitter,
};
use pyo3::prelude::*;

use crate::code::PyCodeSource;

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
    pub(crate) fn from_chunk(chunk: &Chunk) -> Self {
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
    ///     text: The text to split — a `str`, or a `CodeSource` whose cached
    ///         parse is reused (and populated for later consumers).
    ///     chunk_size: Target chunk size in bytes.
    ///     min_chunk_size: Minimum chunk size in bytes. Defaults to chunk_size / 2.
    ///     chunk_overlap: Overlap between consecutive chunks in bytes.
    ///     language: Language name or file extension for syntax-aware splitting.
    ///         Only valid when `text` is a `str` — a `CodeSource` carries its own.
    ///
    /// Returns:
    ///     A list of chunks.
    #[pyo3(signature = (text, chunk_size, min_chunk_size=None, chunk_overlap=None, language=None))]
    fn split(
        &self,
        py: Python<'_>,
        text: &Bound<'_, PyAny>,
        chunk_size: usize,
        min_chunk_size: Option<usize>,
        chunk_overlap: Option<usize>,
        language: Option<String>,
    ) -> PyResult<Vec<PyChunk>> {
        let config = RecursiveChunkConfig {
            chunk_size,
            min_chunk_size,
            chunk_overlap,
        };
        // Reuse path: a CodeSource handle (parse shared across consumers).
        if let Ok(src) = text.cast::<PyCodeSource>() {
            if language.is_some() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "language must not be given when splitting a CodeSource; it carries its own",
                ));
            }
            let src = src.borrow();
            // Capture `&CodeSource` (not the GIL-bound `PyRef`) across `detach`.
            let inner = &src.inner;
            return Ok(py.detach(|| {
                let chunks = self.chunker.split(inner, config);
                chunks.iter().map(PyChunk::from_chunk).collect()
            }));
        }
        // Convenience path: a bare string, wrapped in a borrowed CodeSource.
        let text: &str = text.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("text must be a str or CodeSource")
        })?;
        Ok(py.detach(|| {
            let source = match language {
                Some(language) => CodeSource::with_language(text, language),
                None => CodeSource::new(text),
            };
            let chunks = self.chunker.split(&source, config);
            chunks.iter().map(PyChunk::from_chunk).collect()
        }))
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
    ///         A pattern prefixed with ``!`` negates (un-excludes) paths that would otherwise be
    ///         excluded, enabling gitignore-style exceptions.  For example, combining
    ///         ``"**/.*"`` with ``"!**/.github/**"`` excludes all dot-entries except anything
    ///         inside ``.github/``.
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

/// One contiguous piece of a source view's synthetic text, grounded in the source.
#[pyclass(name = "ViewSegment")]
#[derive(Clone)]
pub struct PyViewSegment {
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
    /// "frame" (context header repeated from enclosing scopes) or "content".
    #[pyo3(get)]
    pub kind: &'static str,
    /// When present, this text stands in for the covered source in the view's text.
    #[pyo3(get)]
    pub summary: Option<String>,
    /// Char range of this segment's rendering within the view's synthetic `text`
    /// (segments form a contiguous partition of it).
    #[pyo3(get)]
    pub rendered_start: usize,
    #[pyo3(get)]
    pub rendered_end: usize,
}

/// A source view: synthetic text plus its source-grounded segments.
#[pyclass(name = "SourceView")]
pub struct PySourceView {
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub segments: Vec<PyViewSegment>,
}

impl PySourceView {
    fn from_view(view: SourceView) -> Self {
        // Segments partition the synthetic text contiguously: one ordered pass
        // converts their byte ranges to char offsets.
        let mut char_cursor = 0usize;
        let segments = view
            .segments
            .into_iter()
            .map(|seg| {
                let rendered_start = char_cursor;
                char_cursor += view.text[seg.text_range.start..seg.text_range.end]
                    .chars()
                    .count();
                PyViewSegment {
                    start_byte: seg.range.start,
                    end_byte: seg.range.end,
                    start_char_offset: seg.start.char_offset,
                    start_line: seg.start.line,
                    start_column: seg.start.column,
                    end_char_offset: seg.end.char_offset,
                    end_line: seg.end.line,
                    end_column: seg.end.column,
                    kind: match seg.kind {
                        SegmentKind::Frame => "frame",
                        SegmentKind::Content => "content",
                    },
                    summary: seg.summary,
                    rendered_start,
                    rendered_end: char_cursor,
                }
            })
            .collect();
        Self {
            text: view.text,
            segments,
        }
    }
}

/// Render source byte ranges into a source view: context frames of the ranges'
/// envelope, each range verbatim, and cues where material is omitted — the
/// code-match rendering path.
///
/// Args:
///     source: The `CodeSource` the ranges refer to (its cached parse is reused).
///     ranges: Byte ranges `(start, end)` in the source, in source order.
///
/// Returns:
///     A source view (synthetic text + source-grounded segments).
#[pyfunction]
pub fn render_ranges(
    py: Python<'_>,
    source: &Bound<'_, PyCodeSource>,
    ranges: Vec<(usize, usize)>,
) -> PySourceView {
    let src = source.borrow();
    // Capture `&CodeSource` (not the GIL-bound `PyRef`) across `detach`.
    let inner = &src.inner;
    py.detach(|| {
        let ranges: Vec<cocoindex_code_ast::TextRange> = ranges
            .iter()
            .map(|&(start, end)| cocoindex_code_ast::TextRange::new(start, end))
            .collect();
        PySourceView::from_view(cocoindex_code_ast::view::render_ranges(inner, &ranges))
    })
}
