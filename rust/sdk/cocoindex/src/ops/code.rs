//! Structural code matching over a reusable parsed AST.
//!
//! Mirrors Python's `cocoindex.ops.code`. Parse source once into a [`CodeAst`],
//! then match by-example structural patterns and/or split without re-parsing.
//! Metavariables in a pattern use the `\` sigil (e.g. `\NAME`, `\(ARGS*\)`).
//!
//! Enabled by the `code_match` feature.
//!
//! # Examples
//!
//! ```rust
//! # #[cfg(feature = "code_match")] {
//! use cocoindex::ops::code::{CodeAst, CodePattern, match_code};
//!
//! // One-shot: parse + match in one call.
//! let matches = match_code(r"\NAME(\(A*\))", "def f(x): pass", "python").unwrap();
//! assert!(!matches.is_empty());
//! assert_eq!(matches[0].captures["NAME"][0].text("def f(x): pass"), "f");
//!
//! // Reuse the parse across multiple patterns.
//! let ast = CodeAst::new("def f(x): return x", "python").unwrap();
//! let m = ast.matches(r"def \NAME(\(A*\)):").unwrap();
//! assert_eq!(m[0].captures["NAME"][0].text(ast.source()), "f");
//!
//! // Reuse the compiled pattern across many sources.
//! let pat = CodePattern::new(r"def \NAME(\(A*\)):", "python", 3).unwrap();
//! assert!(pat.might_match("def foo(): pass"));
//! assert!(!pat.might_match("x = 1"));
//! # }
//! ```

use std::collections::HashMap;
use std::sync::OnceLock;

use cocoindex_code_match::lang;
use cocoindex_code_match::{Pattern, Prefilter, index_terms_in_tree};
use cocoindex_ops_text::prog_langs;
use cocoindex_ops_text::split::{
    LineIndex, OutputPosition, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
};
use tree_sitter::{Parser, Tree};

use crate::error::{Error, Result};
use crate::resources::chunk::{Chunk, TextPosition};

// ─── Internal helpers ────────────────────────────────────────────────────────

fn pos_from(byte_offset: usize, op: OutputPosition) -> TextPosition {
    TextPosition {
        byte_offset,
        char_offset: op.char_offset,
        line: op.line,
        column: op.column,
    }
}

fn build_chunk(line_index: &LineIndex, source: &str, start_byte: usize, end_byte: usize) -> Chunk {
    let sp = line_index.position(source, start_byte);
    let ep = line_index.position(source, end_byte);
    Chunk::new(
        start_byte..end_byte,
        pos_from(start_byte, sp),
        pos_from(end_byte, ep),
    )
}

fn build_sdk_matches(
    source: &str,
    line_index: &LineIndex,
    raw: Vec<cocoindex_code_match::Match<'_>>,
) -> Vec<CodeMatch> {
    raw.into_iter()
        .map(|m| {
            let chunk = build_chunk(line_index, source, m.range.start, m.range.end);
            let captures = m
                .captures
                .iter()
                .map(|(name, c)| {
                    let ch = build_chunk(line_index, source, c.range.start, c.range.end);
                    (name.clone(), vec![ch])
                })
                .collect();
            CodeMatch {
                kind: m.kind,
                chunks: vec![chunk],
                captures,
            }
        })
        .collect()
}

/// Parse `source` for `language`, returning the tree-sitter tree.
///
/// Resolution goes through `prog_langs` (the full tree-sitter table — hundreds
/// of languages), exactly like the Python binding, so any tree-sitter-supported
/// language can be parsed and split (`split` re-resolves through the same table).
/// Structural matching ([`CodeAst::matches`]) additionally requires the language
/// to be in the smaller `code_match` set and errors otherwise — also matching
/// Python. The stored language string is kept verbatim so `split` re-resolves it
/// consistently (both lookups are case-insensitive).
fn parse_tree(source: &str, language: &str) -> Result<(Tree, String)> {
    let ts = prog_langs::get_language_info(language)
        .and_then(|i| i.treesitter_info.as_ref())
        .ok_or_else(|| {
            Error::engine(format!("unknown or non-tree-sitter language: {language:?}"))
        })?;
    let mut parser = Parser::new();
    parser
        .set_language(&ts.tree_sitter_lang)
        .map_err(|e| Error::engine(format!("failed to load grammar for {language:?}: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::engine(format!("failed to parse source as {language:?}")))?;
    Ok((tree, language.to_string()))
}

// ─── Public types ────────────────────────────────────────────────────────────

/// A structural-match result: the matched node and its captured metavariables.
///
/// Chunks are range-only (no owned text). Call `chunk.text(source)` with the
/// source string from [`CodeAst::source`] or your own variable to obtain the
/// slice.
pub struct CodeMatch {
    /// tree-sitter node kind of the matched node (e.g. `"function_definition"`).
    pub kind: &'static str,
    /// The matched code region(s). Currently always exactly one (the whole matched
    /// node); future versions may produce several (e.g. head + tail of a function
    /// with the body elided).
    pub chunks: Vec<Chunk>,
    /// Captured metavariables: name → matched region(s). Each value is currently a
    /// single-element list; the list shape leaves room for future multi-region
    /// captures. Get the text with `m.captures["NAME"][0].text(source)`.
    pub captures: HashMap<String, Vec<Chunk>>,
}

/// A compiled structural pattern + prefilter, built once and reused across many
/// sources or [`CodeAst`]s.
///
/// Compiling a pattern is not free: construct a `CodePattern` once and call
/// [`match_source`](CodePattern::match_source) or
/// [`match_file`](CodePattern::match_file) many times rather than reparsing with
/// [`match_code`] on each source.
pub struct CodePattern {
    language: String,
    pattern: Pattern,
    prefilter: Prefilter,
}

impl CodePattern {
    /// Compile `pattern` for `language` with a custom `min_len` prefilter tuning.
    ///
    /// `min_len` controls the minimum term length considered by the prefilter;
    /// smaller values increase recall (fewer false rejections) at the cost of speed.
    /// The default (used by [`CodePattern::compile`]) is 3.
    pub fn new(pattern: &str, language: impl Into<String>, min_len: usize) -> Result<Self> {
        let language = language.into();
        let cfg = lang::by_name(&language).ok_or_else(|| {
            Error::engine(format!(
                "structural matching is not supported for language {language:?}"
            ))
        })?;
        let compiled = Pattern::compile(pattern, &cfg)?;
        let prefilter = compiled.prefilter(min_len);
        Ok(Self {
            language,
            pattern: compiled,
            prefilter,
        })
    }

    /// Compile with the default `min_len` of 3.
    pub fn compile(pattern: &str, language: impl Into<String>) -> Result<Self> {
        Self::new(pattern, language, 3)
    }

    /// The language this pattern was compiled for.
    pub fn language(&self) -> &str {
        &self.language
    }

    /// Whether `source` *might* contain a match — a cheap, parse-free prefilter
    /// check. `false` means it definitely cannot match (skip the source entirely);
    /// `true` means "maybe" and a full parse is needed to confirm.
    pub fn might_match(&self, source: &str) -> bool {
        self.prefilter.might_match(source)
    }

    /// Match against `source`, parsing it fresh. Skips the parse entirely when the
    /// prefilter rejects `source`. Reuses this pattern's compilation across calls.
    pub fn match_source(&self, source: &str) -> Vec<CodeMatch> {
        let raw = self.pattern.matches_prefiltered(source, &self.prefilter);
        if raw.is_empty() {
            return Vec::new();
        }
        let line_index = LineIndex::build(source);
        build_sdk_matches(source, &line_index, raw)
    }

    /// Read the file at `path`, run the prefilter (parse-free), and — only if it
    /// might match — parse and match.
    ///
    /// Returns `Some(`[`FileMatch`]`)` with the parsed AST and matches when there
    /// is at least one match, `None` when the file is rejected or has no matches.
    /// Non-UTF-8 (binary) files are silently skipped (`None`); other I/O errors
    /// propagate as `Err`.
    pub fn match_file(&self, path: &str) -> Result<Option<FileMatch>> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => return Ok(None),
            Err(e) => {
                return Err(Error::engine(format!("failed to read {path:?}: {e}")));
            }
        };
        if !self.prefilter.might_match(&content) {
            return Ok(None);
        }
        let ast = CodeAst::parse_owned(content, self.language.clone())?;
        let raw = self.pattern.matches_in_tree(&ast.tree, &ast.source);
        if raw.is_empty() {
            return Ok(None);
        }
        let line_index = ast.line_index.get_or_init(|| LineIndex::build(&ast.source));
        let matches = build_sdk_matches(&ast.source, line_index, raw);
        Ok(Some(FileMatch {
            path: path.to_string(),
            ast,
            matches,
        }))
    }
}

/// A parsed code AST: parse once, then [`matches`](CodeAst::matches),
/// [`matches_with`](CodeAst::matches_with), and/or [`split`](CodeAst::split)
/// without re-parsing.
pub struct CodeAst {
    source: String,
    language: String,
    tree: Tree,
    line_index: OnceLock<LineIndex>,
}

impl CodeAst {
    /// Parse `source` for `language` (name or alias: `"python"`, `"rust"`,
    /// `"c++"`, …).
    pub fn new(source: impl Into<String>, language: impl Into<String>) -> Result<Self> {
        Self::parse_owned(source.into(), language.into())
    }

    fn parse_owned(source: String, language: String) -> Result<Self> {
        let (tree, language) = parse_tree(&source, &language)?;
        Ok(Self {
            source,
            language,
            tree,
            line_index: OnceLock::new(),
        })
    }

    /// The language this AST was parsed for.
    pub fn language(&self) -> &str {
        &self.language
    }

    /// The source text.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Find every match of a pattern string, compiling it on the spot. Reuses the
    /// AST parse. Prefer [`matches_with`](CodeAst::matches_with) when matching the
    /// same pattern across many ASTs.
    pub fn matches(&self, pattern: &str) -> Result<Vec<CodeMatch>> {
        let cfg = lang::by_name(&self.language).ok_or_else(|| {
            Error::engine(format!(
                "structural matching is not supported for language {:?}",
                self.language
            ))
        })?;
        let compiled = Pattern::compile(pattern, &cfg)?;
        let line_index = self.line_index.get_or_init(|| LineIndex::build(&self.source));
        let raw = compiled.matches_in_tree(&self.tree, &self.source);
        Ok(build_sdk_matches(&self.source, line_index, raw))
    }

    /// Find every match of a precompiled [`CodePattern`], reusing both the AST and
    /// the pattern's compilation. This is the fast path for matching the same
    /// pattern against many sources.
    pub fn matches_with(&self, pattern: &CodePattern) -> Result<Vec<CodeMatch>> {
        match (lang::by_name(&self.language), lang::by_name(&pattern.language)) {
            (Some(ca), Some(cb)) if ca.language == cb.language => {}
            _ => {
                return Err(Error::engine(format!(
                    "CodePattern language {:?} does not match AST language {:?}",
                    pattern.language, self.language
                )));
            }
        }
        let line_index = self.line_index.get_or_init(|| LineIndex::build(&self.source));
        let raw = pattern.pattern.matches_in_tree(&self.tree, &self.source);
        Ok(build_sdk_matches(&self.source, line_index, raw))
    }

    /// Split into syntax-aware chunks, reusing the parse.
    ///
    /// - `chunk_size`: target size in bytes.
    /// - `min_chunk_size`: minimum size in bytes (defaults to `chunk_size / 2`).
    /// - `chunk_overlap`: overlap between consecutive chunks in bytes.
    pub fn split(
        &self,
        chunk_size: usize,
        min_chunk_size: Option<usize>,
        chunk_overlap: Option<usize>,
    ) -> Result<Vec<Chunk>> {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default())
            .map_err(|e| Error::engine(format!("failed to create chunker: {e}")))?;
        let config = RecursiveChunkConfig {
            chunk_size,
            min_chunk_size,
            chunk_overlap,
            language: Some(self.language.clone()),
        };
        let raw = chunker.split_with_tree(&self.source, config, &self.tree);
        Ok(raw
            .into_iter()
            .map(|c| {
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
            })
            .collect())
    }

    /// Extract indexable terms (identifiers + string-literal content, ≥ `min_len`
    /// chars, deduped), reusing the parse. Suitable for building an external
    /// prefilter index (FTS / n-grams).
    pub fn index_terms(&self, min_len: usize) -> Vec<String> {
        index_terms_in_tree(&self.tree, &self.source, min_len)
    }
}

/// The result of [`CodePattern::match_file`]: the parsed [`CodeAst`] and every
/// match found in the file. The file content is `file_match.ast.source()`.
pub struct FileMatch {
    /// The path that was matched.
    pub path: String,
    /// The parsed AST — reuse it to call [`split`](CodeAst::split),
    /// [`matches`](CodeAst::matches), or [`index_terms`](CodeAst::index_terms)
    /// without re-parsing.
    pub ast: CodeAst,
    /// The matches found (always at least one — [`CodePattern::match_file`]
    /// returns `None` when there are none).
    pub matches: Vec<CodeMatch>,
}

// ─── Free functions ───────────────────────────────────────────────────────────

/// One-shot convenience: parse `source` for `language` and return all matches of
/// `pattern`. Equivalent to `CodeAst::new(source, language)?.matches(pattern)`.
///
/// Prefer [`CodePattern`] when matching the same pattern against many sources.
pub fn match_code(pattern: &str, source: &str, language: &str) -> Result<Vec<CodeMatch>> {
    let ast = CodeAst::parse_owned(source.to_string(), language.to_string())?;
    ast.matches(pattern)
}

/// One-shot convenience: parse `source` for `language` and return indexable terms
/// (identifiers + string-literal content, ≥ `min_len` chars, deduped).
///
/// Equivalent to `CodeAst::new(source, language)?.index_terms(min_len)`.
/// Prefer [`CodeAst::index_terms`] when you have an existing parse.
pub fn index_terms(source: &str, language: &str, min_len: usize) -> Result<Vec<String>> {
    let ast = CodeAst::parse_owned(source.to_string(), language.to_string())?;
    Ok(ast.index_terms(min_len))
}
