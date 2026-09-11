//! Python bindings for code parsing and structural matching: `CodeSource` (the
//! one lazily-parsed handle shared by every AST consumer) and `CodePattern`
//! (compiled by-example patterns). All consumers resolve grammars through the
//! `cocoindex_code_ast` registry (one tree-sitter version workspace-wide), so
//! one tree is sound for all.

use std::collections::HashMap;
use std::ops::Range;

use cocoindex_code_ast::{CodeSource, LineIndex, OutputPosition, ParseOutcome, prog_langs};
use cocoindex_code_match::{Match, Pattern, Prefilter, index_terms_in_tree, lang};
use pyo3::prelude::*;

use crate::ops::PyChunk;

/// A structural-match result: the matched node and its captured metavariables.
#[pyclass(name = "CodeMatch", skip_from_py_object)]
#[derive(Clone)]
pub struct PyCodeMatch {
    /// tree-sitter node kind of the matched node (e.g. `function_definition`).
    #[pyo3(get)]
    pub kind: &'static str,
    /// The matched code region(s), each a `Chunk` with text and line/column
    /// positions. Currently always exactly one (the whole matched node); a future
    /// carve-out feature may return several (e.g. a function's head and tail with
    /// the body elided).
    #[pyo3(get)]
    pub chunks: Vec<PyChunk>,
    /// Captured metavariables: name -> matched region(s). Like `chunks`, each
    /// capture is currently a single chunk, but the list leaves room for a
    /// capture whose region is carved into several pieces.
    #[pyo3(get)]
    pub captures: HashMap<String, Vec<PyChunk>>,
}

#[pymethods]
impl PyCodeMatch {
    fn __repr__(&self) -> String {
        let (s, e) = self
            .chunks
            .first()
            .map(|c| (c.start_byte, c.end_byte))
            .unwrap_or((0, 0));
        let mut names: Vec<&str> = self.captures.keys().map(String::as_str).collect();
        names.sort_unstable();
        format!(
            "CodeMatch(kind={:?}, bytes={s}..{e}, captures={names:?})",
            self.kind
        )
    }
}

fn chunk_from(range: &Range<usize>, s: OutputPosition, e: OutputPosition) -> PyChunk {
    PyChunk {
        start_byte: range.start,
        end_byte: range.end,
        start_char_offset: s.char_offset,
        start_line: s.line,
        start_column: s.column,
        end_char_offset: e.char_offset,
        end_line: e.line,
        end_column: e.column,
    }
}

/// Convert raw matches to `PyCodeMatch`, resolving line/column positions for
/// every match and capture endpoint through a reusable [`LineIndex`] (each
/// offset is an independent lookup, so no per-call full-file scan).
fn build_matches(source: &str, line_index: &LineIndex, raw: Vec<Match<'_>>) -> Vec<PyCodeMatch> {
    let pos = |b: usize| line_index.position(source, b);
    raw.into_iter()
        .map(|m| {
            let chunk = chunk_from(&m.range, pos(m.range.start), pos(m.range.end));
            let captures = m
                .captures
                .iter()
                .map(|(name, c)| {
                    let ch = chunk_from(&c.range, pos(c.range.start), pos(c.range.end));
                    (name.clone(), vec![ch])
                })
                .collect();
            PyCodeMatch {
                kind: m.kind,
                chunks: vec![chunk],
                captures,
            }
        })
        .collect()
}

/// Source text plus a lazily-parsed, memoized AST — the shared input for every
/// API that may need a parse. Construction never parses and never fails: an
/// unknown language simply means consumers take their degraded (non-AST) path.
/// Pass one handle to several APIs (splitters, `CodePattern.match_source`,
/// `index_terms`, …) and the source is parsed at most once.
#[pyclass(name = "CodeSource")]
pub struct PyCodeSource {
    pub(crate) inner: CodeSource<'static>,
}

#[pymethods]
impl PyCodeSource {
    /// Wrap `text` for `language` (name, alias, or file extension; optional).
    /// No parsing happens here; unknown languages are accepted.
    #[new]
    #[pyo3(signature = (text, language=None))]
    fn new(text: String, language: Option<String>) -> Self {
        let inner = match language {
            Some(language) => CodeSource::with_language(text, language),
            None => CodeSource::new(text),
        };
        Self { inner }
    }

    /// The source text.
    #[getter]
    fn text(&self) -> &str {
        self.inner.text()
    }

    /// The language as given at construction (may be an alias or extension).
    #[getter]
    fn language(&self) -> Option<&str> {
        self.inner.requested_language()
    }

    fn __repr__(&self) -> String {
        format!(
            "CodeSource(language={:?}, text_len={})",
            self.inner.requested_language(),
            self.inner.text().len()
        )
    }
}

/// A **compiled** structural pattern + its prefilter, built once and reused across
/// many sources/files — so matching the same pattern over a corpus doesn't recompile
/// it each time. Construct with `CodePattern(pattern, language, min_len=3)`.
#[pyclass(name = "CodePattern")]
pub struct PyCodePattern {
    language: String,
    pattern: Pattern,
    prefilter: Prefilter,
}

#[pymethods]
impl PyCodePattern {
    /// Compile `pattern` for `language` once. `min_len` tunes the prefilter (terms
    /// shorter than this are dropped). Raises `ValueError` if the language is
    /// unsupported for matching or the pattern is malformed.
    #[new]
    #[pyo3(signature = (pattern, language, min_len=3))]
    fn new(pattern: &str, language: String, min_len: usize) -> PyResult<Self> {
        let cfg = lang::by_name(&language).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "structural matching is not supported for language {language:?}"
            ))
        })?;
        let compiled = Pattern::compile(pattern, &cfg)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
        let prefilter = compiled.prefilter(min_len);
        Ok(Self {
            language,
            pattern: compiled,
            prefilter,
        })
    }

    /// The language this pattern was compiled for.
    #[getter]
    fn language(&self) -> &str {
        &self.language
    }

    /// Whether `source` **might** contain a match — a cheap, parse-free prefilter
    /// check. `False` means it definitely can't (skip it); `True` means "maybe".
    /// The scan runs with the GIL released.
    fn might_match(&self, py: Python<'_>, source: &str) -> bool {
        py.detach(|| self.prefilter.might_match(source))
    }

    /// Match against `source` — a `str` (parsed on the spot) or a `CodeSource`
    /// (reusing its cached parse) — skipping the parse entirely when the
    /// prefilter rejects it. Reuses this pattern's compilation across calls.
    fn match_source(
        &self,
        py: Python<'_>,
        source: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<PyCodeMatch>> {
        // Reuse path: a CodeSource handle (its parse and line index are cached).
        if let Ok(src) = source.cast::<PyCodeSource>() {
            let src = src.borrow();
            // Capture `&CodeSource` (not the GIL-bound `PyRef`) across `detach`.
            let inner = &src.inner;
            return Ok(py.detach(|| {
                if !self.prefilter.might_match(inner.text()) {
                    return Vec::new();
                }
                let raw = self.pattern.matches_source(inner);
                if raw.is_empty() {
                    return Vec::new();
                }
                build_matches(inner.text(), inner.line_index(), raw)
            }));
        }
        // Convenience path: a bare string, parsed here (when not prefiltered out).
        let source: String = source.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("source must be a str or CodeSource")
        })?;
        Ok(py.detach(|| {
            let raw = self.pattern.matches_prefiltered(&source, &self.prefilter);
            if raw.is_empty() {
                return Vec::new();
            }
            let line_index = LineIndex::build(&source);
            build_matches(&source, &line_index, raw)
        }))
    }

    /// Read the file at `path`, run the prefilter, and (only if it might match)
    /// parse + match. Returns a [`FileMatch`] with the parsed `CodeSource` and the
    /// matches when there is at least one match, else `None` — so a rejected or
    /// non-matching file never costs a parse beyond what the prefilter needs.
    /// Non-UTF-8 (binary) files are skipped (`None`); other I/O errors raise.
    ///
    /// The expensive work (read + prefilter + parse + match + build) runs **with
    /// the GIL released**, so a Python thread pool can scan many files truly in
    /// parallel; only the final wrap into Python objects re-acquires it.
    fn match_file(&self, py: Python<'_>, path: String) -> PyResult<Option<PyFileMatch>> {
        type Built = (CodeSource<'static>, Vec<PyCodeMatch>);
        let built: Option<Built> = py.detach(|| -> PyResult<Option<Built>> {
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                // binary / non-text file → skip, don't error a corpus scan
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => return Ok(None),
                Err(e) => {
                    return Err(pyo3::exceptions::PyOSError::new_err(format!(
                        "failed to read {path:?}: {e}"
                    )));
                }
            };
            if !self.prefilter.might_match(&content) {
                return Ok(None); // rejected without parsing
            }
            let source = CodeSource::with_language(content, self.language.clone());
            let raw = self.pattern.matches_source(&source);
            if raw.is_empty() {
                return Ok(None);
            }
            let matches = build_matches(source.text(), source.line_index(), raw);
            Ok(Some((source, matches)))
        })?;

        let Some((source, matches)) = built else {
            return Ok(None);
        };
        Ok(Some(PyFileMatch {
            path,
            source: Py::new(py, PyCodeSource { inner: source })?,
            matches,
        }))
    }

    fn __repr__(&self) -> String {
        format!("CodePattern(language={:?})", self.language)
    }
}

/// The result of [`CodePattern::match_file`]: the parsed source and the matches
/// found in one file. The file content is `file_match.source.text`.
#[pyclass(name = "FileMatch")]
pub struct PyFileMatch {
    /// The path that was matched.
    #[pyo3(get)]
    path: String,
    /// The parsed `CodeSource` (reuse it to split or match more patterns
    /// without re-parsing).
    #[pyo3(get)]
    source: Py<PyCodeSource>,
    /// The matches found.
    #[pyo3(get)]
    matches: Vec<PyCodeMatch>,
}

#[pymethods]
impl PyFileMatch {
    fn __repr__(&self) -> String {
        format!(
            "FileMatch(path={:?}, matches={})",
            self.path,
            self.matches.len()
        )
    }
}

/// Extract the indexable terms of `source` (identifiers + string-literal content,
/// ≥ `min_len`, deduped), for building an external prefilter index. `source` is a
/// `str` (with `language` required) or a `CodeSource` (whose cached parse is
/// reused; `language` must be omitted). Raises `ValueError` for an unknown or
/// non-tree-sitter language — silently returning nothing would poison the index
/// with false negatives.
#[pyfunction]
#[pyo3(signature = (source, language=None, min_len=3))]
pub fn index_terms(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    language: Option<String>,
    min_len: usize,
) -> PyResult<Vec<String>> {
    let no_grammar = |language: &str| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "unknown or non-tree-sitter language: {language:?}"
        ))
    };
    // Reuse path: a CodeSource handle.
    if let Ok(src) = source.cast::<PyCodeSource>() {
        if language.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "language must not be given with a CodeSource; it carries its own",
            ));
        }
        let src = src.borrow();
        let inner = &src.inner;
        if inner.treesitter_info().is_none() {
            return Err(no_grammar(inner.requested_language().unwrap_or_default()));
        }
        return Ok(py.detach(|| match inner.tree() {
            ParseOutcome::Parsed(tree) => index_terms_in_tree(tree, inner.text(), min_len),
            _ => Vec::new(),
        }));
    }
    // One-shot path: a bare string; language is required.
    let source: String = source.extract().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err("source must be a str or CodeSource")
    })?;
    let Some(language) = language else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "language is required when source is a str",
        ));
    };
    let info = prog_langs::get_language_info(&language)
        .filter(|i| i.treesitter_info.is_some())
        .ok_or_else(|| no_grammar(&language))?;
    py.detach(|| {
        let src = CodeSource::with_info(source.as_str(), info);
        match src.tree() {
            ParseOutcome::Parsed(tree) => Ok(index_terms_in_tree(tree, src.text(), min_len)),
            _ => Ok(Vec::new()),
        }
    })
}

/// One-shot convenience: match `pattern` against `source` — a `str` (with
/// `language` required) or a `CodeSource` (whose cached parse is reused;
/// `language` must be omitted). Prefer a `CodePattern` when matching the same
/// pattern across many sources (this recompiles per call).
#[pyfunction]
#[pyo3(signature = (pattern, source, language=None))]
pub fn match_code(
    py: Python<'_>,
    pattern: &str,
    source: &Bound<'_, PyAny>,
    language: Option<String>,
) -> PyResult<Vec<PyCodeMatch>> {
    let unsupported = |language: &str| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "structural matching is not supported for language {language:?}"
        ))
    };
    // Reuse path: a CodeSource handle.
    if let Ok(src) = source.cast::<PyCodeSource>() {
        if language.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "language must not be given with a CodeSource; it carries its own",
            ));
        }
        let src = src.borrow();
        let inner = &src.inner;
        let requested = inner.requested_language().unwrap_or_default();
        let cfg = lang::by_name(requested).ok_or_else(|| unsupported(requested))?;
        let compiled = Pattern::compile(pattern, &cfg)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
        return Ok(py.detach(|| {
            let raw = compiled.matches_source(inner);
            if raw.is_empty() {
                return Vec::new();
            }
            build_matches(inner.text(), inner.line_index(), raw)
        }));
    }
    // One-shot path: a bare string; language is required.
    let source: String = source.extract().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err("source must be a str or CodeSource")
    })?;
    let Some(language) = language else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "language is required when source is a str",
        ));
    };
    let cfg = lang::by_name(&language).ok_or_else(|| unsupported(&language))?;
    let compiled = Pattern::compile(pattern, &cfg)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
    Ok(py.detach(|| {
        let raw = compiled.matches(&source);
        if raw.is_empty() {
            return Vec::new();
        }
        let line_index = LineIndex::build(&source);
        build_matches(&source, &line_index, raw)
    }))
}
