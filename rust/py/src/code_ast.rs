//! `CodeAst` — a Python handle wrapping a single tree-sitter parse, reused by
//! both structural pattern matching (`code_match`) and chunk splitting
//! (`ops_text`) so the source is parsed only once. Both consumers use the same
//! grammar crate per language (pinned to one tree-sitter version workspace-wide),
//! so one tree is sound for both.

use std::collections::HashMap;
use std::ops::Range;

use cocoindex_code_match::lang;
use cocoindex_code_match::{Match, Pattern};
use cocoindex_ops_text::prog_langs;
use cocoindex_ops_text::split::{
    OutputPosition, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
    output_positions_for,
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

/// Convert raw matches to `PyCodeMatch`, computing line/column positions for
/// every match and capture endpoint in a *single* pass over `source` (the way
/// the splitter does). Capture names are sorted so the offset read-back aligns.
fn build_matches(source: &str, raw: Vec<Match<'_>>) -> Vec<PyCodeMatch> {
    let mut offsets = Vec::new();
    let mut per_match_caps: Vec<Vec<(String, Range<usize>)>> = Vec::with_capacity(raw.len());
    for m in &raw {
        offsets.push(m.range.start);
        offsets.push(m.range.end);
        let mut caps: Vec<(String, Range<usize>)> = m
            .captures
            .iter()
            .map(|(k, c)| (k.clone(), c.range.clone()))
            .collect();
        caps.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, r) in &caps {
            offsets.push(r.start);
            offsets.push(r.end);
        }
        per_match_caps.push(caps);
    }

    let pos = output_positions_for(source, &offsets);
    let mut idx = 0usize;
    let mut out = Vec::with_capacity(raw.len());
    for (m, caps) in raw.into_iter().zip(per_match_caps) {
        let chunk = chunk_from(&m.range, pos[idx], pos[idx + 1]);
        idx += 2;
        let mut captures: HashMap<String, Vec<PyChunk>> = HashMap::with_capacity(caps.len());
        for (name, range) in caps {
            captures.insert(name, vec![chunk_from(&range, pos[idx], pos[idx + 1])]);
            idx += 2;
        }
        out.push(PyCodeMatch {
            kind: m.kind,
            chunks: vec![chunk],
            captures,
        });
    }
    out
}

/// A parsed code AST. Parse once, then `matches()` structural patterns and/or
/// `split()` into chunks without re-parsing. Construct with
/// `CodeAst(source, language)`.
#[pyclass(name = "CodeAst")]
pub struct PyCodeAst {
    source: String,
    language: String,
    tree: Tree,
}

#[pymethods]
impl PyCodeAst {
    /// Parse `source` for `language` (name or alias: `"python"`, `"rust"`,
    /// `"c++"`, …). Raises `ValueError` if the language has no tree-sitter
    /// grammar.
    #[new]
    fn new(source: String, language: String) -> PyResult<Self> {
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
        })
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

    /// Find every match of a structural `pattern`, reusing the parse. Raises
    /// `ValueError` if the language is unsupported for matching or the pattern
    /// is malformed.
    fn matches(&self, py: Python<'_>, pattern: &str) -> PyResult<Vec<PyCodeMatch>> {
        let cfg = lang::by_name(&self.language).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "structural matching is not supported for language {:?}",
                self.language
            ))
        })?;
        let compiled = Pattern::compile(pattern, &cfg)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))?;
        Ok(py.detach(|| {
            let raw = compiled.matches_in_tree(&self.tree, &self.source);
            build_matches(&self.source, raw)
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

    fn __repr__(&self) -> String {
        format!(
            "CodeAst(language={:?}, source_len={})",
            self.language,
            self.source.len()
        )
    }
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
    PyCodeAst::new(source, language)?.matches(py, pattern)
}
