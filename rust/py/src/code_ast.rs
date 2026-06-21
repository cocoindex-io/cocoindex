//! `CodeAst` — a Python handle wrapping a single tree-sitter parse, reused by
//! both structural pattern matching (`code_match`) and chunk splitting
//! (`ops_text`) so the source is parsed only once. Both consumers use the same
//! grammar crate per language (pinned to one tree-sitter version workspace-wide),
//! so one tree is sound for both.

use std::collections::HashMap;
use std::ops::Range;

use cocoindex_code_match::lang;
use cocoindex_code_match::{Match, Pattern, Prefilter, index_terms_in_tree};
use cocoindex_ops_text::prog_langs;
use cocoindex_ops_text::split::{
    LineIndex, OutputPosition, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
};
use pyo3::prelude::*;
use tree_sitter::{Parser, Tree};

use crate::ops::PyChunk;

/// A structural-match result: the matched node and its captured metavariables.
#[pyclass(name = "CodeMatch")]
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
/// every match and capture endpoint through the AST's reusable [`LineIndex`]
/// (each offset is an independent lookup, so no per-call full-file scan).
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

/// A parsed code AST. Parse once, then `matches()` structural patterns and/or
/// `split()` into chunks without re-parsing. Construct with
/// `CodeAst(source, language)`.
#[pyclass(name = "CodeAst")]
pub struct PyCodeAst {
    source: String,
    language: String,
    tree: Tree,
    /// Byte→position index over `source`, built on first `matches()` and reused
    /// across subsequent pattern queries (the "one parse, many patterns" case).
    line_index: std::sync::OnceLock<LineIndex>,
}

impl PyCodeAst {
    /// Parse `source` for `language` — the **GIL-free** body of the constructor, so
    /// callers that have already released the GIL (e.g. inside `py.detach`) can build
    /// an AST without re-acquiring it. Touches no Python objects.
    fn parse(source: String, language: String) -> PyResult<Self> {
        let ts = prog_langs::get_language_info(&language)
            .and_then(|i| i.treesitter_info.as_ref())
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown or non-tree-sitter language: {language:?}"
                ))
            })?;
        let mut parser = Parser::new();
        parser.set_language(&ts.tree_sitter_lang).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("failed to load grammar: {e}"))
        })?;
        let tree = parser
            .parse(&source, None)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("failed to parse source"))?;
        Ok(Self {
            source,
            language,
            tree,
            line_index: std::sync::OnceLock::new(),
        })
    }
}

#[pymethods]
impl PyCodeAst {
    /// Parse `source` for `language` (name or alias: `"python"`, `"rust"`,
    /// `"c++"`, …). Raises `ValueError` if the language has no tree-sitter
    /// grammar. The parse runs with the GIL released.
    #[new]
    fn new(py: Python<'_>, source: String, language: String) -> PyResult<Self> {
        py.detach(|| Self::parse(source, language))
    }

    /// The language this AST was parsed for.
    #[getter]
    fn language(&self) -> &str {
        &self.language
    }

    /// The source text.
    #[getter]
    fn source(&self) -> &str {
        &self.source
    }

    /// Find every match of `pattern`, reusing the parse. `pattern` is either a
    /// pattern **string** (compiled on the spot) or a precompiled **`CodePattern`**
    /// (reuses its compilation — preferred when matching the same pattern across
    /// many ASTs). Raises `ValueError` if the language is unsupported for matching,
    /// the pattern string is malformed, or a `CodePattern`'s language differs from
    /// this AST's.
    fn matches(&self, py: Python<'_>, pattern: &Bound<'_, PyAny>) -> PyResult<Vec<PyCodeMatch>> {
        // Reuse path: a precompiled CodePattern.
        if let Ok(cp) = pattern.cast::<PyCodePattern>() {
            let cp = cp.borrow();
            if !same_grammar(&self.language, &cp.language) {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "CodePattern language {:?} does not match this AST's {:?}",
                    cp.language, self.language
                )));
            }
            // Capture `&Pattern` (not the GIL-bound `PyRef`) across `detach`.
            let compiled = &cp.pattern;
            return Ok(py.detach(|| {
                let line_index = self
                    .line_index
                    .get_or_init(|| LineIndex::build(&self.source));
                let raw = compiled.matches_in_tree(&self.tree, &self.source);
                build_matches(&self.source, line_index, raw)
            }));
        }
        // Convenience path: a pattern string, compiled here.
        let pattern: String = pattern.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("pattern must be a str or CodePattern")
        })?;
        let cfg = lang::by_name(&self.language).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "structural matching is not supported for language {:?}",
                self.language
            ))
        })?;
        let compiled = Pattern::compile(&pattern, &cfg)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
        Ok(py.detach(|| {
            let line_index = self
                .line_index
                .get_or_init(|| LineIndex::build(&self.source));
            let raw = compiled.matches_in_tree(&self.tree, &self.source);
            build_matches(&self.source, line_index, raw)
        }))
    }

    /// Split into chunks, reusing the parse. Mirrors `RecursiveSplitter.split`,
    /// but the language is fixed to this AST's.
    #[pyo3(signature = (chunk_size, min_chunk_size=None, chunk_overlap=None))]
    fn split(
        &self,
        py: Python<'_>,
        chunk_size: usize,
        min_chunk_size: Option<usize>,
        chunk_overlap: Option<usize>,
    ) -> PyResult<Vec<PyChunk>> {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default())
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        let chunks = py.detach(|| {
            let config = RecursiveChunkConfig {
                chunk_size,
                min_chunk_size,
                chunk_overlap,
                language: Some(self.language.clone()),
            };
            chunker.split_with_tree(&self.source, config, &self.tree)
        });
        Ok(chunks.iter().map(PyChunk::from_chunk).collect())
    }

    /// The indexable terms of this source (identifiers + string-literal content,
    /// ≥ `min_len`, deduped), reusing the parse — for feeding an external prefilter
    /// index (FTS / n-grams).
    #[pyo3(signature = (min_len=3))]
    fn index_terms(&self, py: Python<'_>, min_len: usize) -> Vec<String> {
        py.detach(|| index_terms_in_tree(&self.tree, &self.source, min_len))
    }

    fn __repr__(&self) -> String {
        format!(
            "CodeAst(language={:?}, source_len={})",
            self.language,
            self.source.len()
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

    /// Match against `source`, parsing it — but skip the parse entirely when the
    /// prefilter rejects it. Reuses this pattern's compilation across calls.
    fn match_source(&self, py: Python<'_>, source: String) -> Vec<PyCodeMatch> {
        py.detach(|| {
            let raw = self.pattern.matches_prefiltered(&source, &self.prefilter);
            if raw.is_empty() {
                return Vec::new();
            }
            let line_index = LineIndex::build(&source);
            build_matches(&source, &line_index, raw)
        })
    }

    /// Read the file at `path`, run the prefilter, and (only if it might match)
    /// parse + match. Returns a [`FileMatch`] with the parsed `CodeAst` and the
    /// matches when there is at least one match, else `None` — so a rejected or
    /// non-matching file never costs a parse beyond what the prefilter needs.
    /// Non-UTF-8 (binary) files are skipped (`None`); other I/O errors raise.
    ///
    /// The expensive work (read + prefilter + parse + match + build) runs **with
    /// the GIL released**, so a Python thread pool can scan many files truly in
    /// parallel; only the final wrap into Python objects re-acquires it.
    fn match_file(&self, py: Python<'_>, path: String) -> PyResult<Option<PyFileMatch>> {
        type Built = (PyCodeAst, Vec<PyCodeMatch>);
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
            let ast = PyCodeAst::parse(content, self.language.clone())?;
            let raw = self.pattern.matches_in_tree(&ast.tree, &ast.source);
            if raw.is_empty() {
                return Ok(None);
            }
            let matches = {
                let line_index = ast.line_index.get_or_init(|| LineIndex::build(&ast.source));
                build_matches(&ast.source, line_index, raw)
            };
            Ok(Some((ast, matches)))
        })?;

        let Some((ast, matches)) = built else {
            return Ok(None);
        };
        Ok(Some(PyFileMatch {
            path,
            ast: Py::new(py, ast)?,
            matches,
        }))
    }

    fn __repr__(&self) -> String {
        format!("CodePattern(language={:?})", self.language)
    }
}

/// The result of [`CodePattern::match_file`]: the parsed AST and the matches found
/// in one file. The file content is `file_match.ast.source`.
#[pyclass(name = "FileMatch")]
pub struct PyFileMatch {
    /// The path that was matched.
    #[pyo3(get)]
    path: String,
    /// The parsed `CodeAst` (reuse it to `split()` or match more patterns).
    #[pyo3(get)]
    ast: Py<PyCodeAst>,
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

/// Whether two language names resolve to the same tree-sitter grammar (so a pattern
/// compiled for one can match a tree parsed for the other) — handles aliases
/// (`c++`/`cpp`). `false` if either is unsupported for matching.
fn same_grammar(a: &str, b: &str) -> bool {
    match (lang::by_name(a), lang::by_name(b)) {
        (Some(ca), Some(cb)) => ca.language == cb.language,
        _ => false,
    }
}

/// Extract the indexable terms of `source` (identifiers + string-literal content,
/// ≥ `min_len`, deduped), for building an external prefilter index. One-shot; use
/// `CodeAst.index_terms` to reuse an existing parse.
#[pyfunction]
#[pyo3(signature = (source, language, min_len=3))]
pub fn index_terms(
    py: Python<'_>,
    source: String,
    language: String,
    min_len: usize,
) -> PyResult<Vec<String>> {
    py.detach(|| {
        let ast = PyCodeAst::parse(source, language)?;
        Ok(index_terms_in_tree(&ast.tree, &ast.source, min_len))
    })
}

/// One-shot convenience: parse `source` for `language` and return all matches of
/// `pattern`. Equivalent to `CodeAst(source, language).matches(pattern)` but
/// without keeping the AST around.
#[pyfunction]
pub fn match_code(
    py: Python<'_>,
    pattern: &str,
    source: String,
    language: String,
) -> PyResult<Vec<PyCodeMatch>> {
    py.detach(|| {
        let ast = PyCodeAst::parse(source, language)?;
        let cfg = lang::by_name(&ast.language).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "structural matching is not supported for language {:?}",
                ast.language
            ))
        })?;
        let compiled = Pattern::compile(pattern, &cfg)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
        let line_index = LineIndex::build(&ast.source);
        let raw = compiled.matches_in_tree(&ast.tree, &ast.source);
        Ok(build_matches(&ast.source, &line_index, raw))
    })
}
