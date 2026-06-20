//! Prefiltering: extract a pattern's **required literal content** so callers can
//! cheaply reject sources that can't possibly match, before the expensive parse +
//! precise match. See `specs/code_match/prefilter_design.md`.
//!
//! Build one with [`Pattern::prefilter`]. The result is a conjunction of clauses
//! (CNF): a source can match only if it satisfies **every** clause, and a clause
//! holds if **any** of its terms occurs. This is *sound* — it may pass a source
//! that doesn't actually match (false positive), but never rejects one that would
//! (no false negatives), because we only ever *drop* constraints we can't extract.
//!
//! Terms come from three places in a pattern: identifiers (whole-word), string
//! literal content (whole-word over each alphanumeric run), and the required
//! literals of a regex matcher `\(:/re/)` (substring, via `regex-syntax`).
//! Keywords, punctuation, numbers, and bare metavars contribute nothing.

use std::collections::{HashMap, HashSet};

use aho_corasick::{AhoCorasick, MatchKind};
use tree_sitter::{Node, Parser};

use crate::config::LangConfig;
use crate::lexer::PatternItem;

/// How a required term must occur in the source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundary {
    /// Whole word (`\bterm\b`). A pattern identifier or string-content run: the
    /// precise matcher needs an exact token, so a word-bounded occurrence is
    /// necessary and more selective.
    Word,
    /// Anywhere (substring). A regex-extracted literal, which the regex may match
    /// mid-token.
    Substring,
}

/// A required literal and how it must occur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterTerm {
    pub text: String,
    pub boundary: Boundary,
}

/// One CNF clause: satisfied if **any** of its terms occurs. More than one term
/// only arises from a regex alternation (`\(:/get|set/)`); a plain identifier or
/// string run is a singleton.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterClause {
    pub any_of: Vec<FilterTerm>,
}

/// A required-content prefilter compiled from a pattern. A source can match only
/// if [`might_match`](Prefilter::might_match) returns true. Sound: false positives
/// are possible, false negatives are not.
pub struct Prefilter {
    clauses: Vec<FilterClause>,
    /// Automaton over the distinct term texts (`None` when there are no terms —
    /// the pattern can't be prefiltered, so everything is a "maybe").
    ac: Option<AhoCorasick>,
    /// Per automaton pattern id, the `(clause index, boundary)` of each term with
    /// that text. A text can belong to several clauses / boundaries.
    membership: Vec<Vec<(usize, Boundary)>>,
}

impl Prefilter {
    /// Build from a pattern's items + language config. `min_len`: terms shorter
    /// than this (in chars) are dropped — too short to be selective or to index as
    /// n-grams. A clause is kept only if *all* its alternatives survive (dropping a
    /// disjunction alternative would be unsound), so a too-short alternative drops
    /// the whole clause.
    pub(crate) fn build(items: &[PatternItem], cfg: &LangConfig, min_len: usize) -> Prefilter {
        let mut clauses: Vec<Vec<FilterTerm>> = Vec::new();
        let keep = |s: &str| s.chars().count() >= min_len;

        for item in items {
            match item {
                PatternItem::Token(text) => {
                    if is_identifier_term(text, cfg) && keep(text) {
                        clauses.push(vec![FilterTerm {
                            text: text.clone(),
                            boundary: Boundary::Word,
                        }]);
                    }
                }
                PatternItem::Str(text) => {
                    // Each maximal alphanumeric run of the raw string text is a
                    // whole-word requirement (runs are bounded by quotes/punct in
                    // both pattern and source raw text). Independent AND clauses,
                    // so a too-short run is dropped on its own.
                    for run in word_runs(text) {
                        if keep(run) {
                            clauses.push(vec![FilterTerm {
                                text: run.to_string(),
                                boundary: Boundary::Word,
                            }]);
                        }
                    }
                }
                PatternItem::Meta {
                    regex: Some(re), ..
                } => {
                    for alts in regex_required_literals(re.as_str()) {
                        // Disjunction: keep only if every alternative survives.
                        if !alts.is_empty() && alts.iter().all(|s| keep(s)) {
                            clauses.push(
                                alts.into_iter()
                                    .map(|text| FilterTerm {
                                        text,
                                        boundary: Boundary::Substring,
                                    })
                                    .collect(),
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        Prefilter::compile(clauses)
    }

    fn compile(clause_terms: Vec<Vec<FilterTerm>>) -> Prefilter {
        let mut patterns: Vec<String> = Vec::new();
        let mut id_of: HashMap<String, usize> = HashMap::new();
        let mut membership: Vec<Vec<(usize, Boundary)>> = Vec::new();

        for (ci, terms) in clause_terms.iter().enumerate() {
            for t in terms {
                let pid = *id_of.entry(t.text.clone()).or_insert_with(|| {
                    patterns.push(t.text.clone());
                    membership.push(Vec::new());
                    patterns.len() - 1
                });
                membership[pid].push((ci, t.boundary));
            }
        }

        let ac = (!patterns.is_empty()).then(|| {
            AhoCorasick::builder()
                .match_kind(MatchKind::Standard) // Standard supports overlapping search
                .build(&patterns)
                .expect("aho-corasick build over literal terms")
        });

        let clauses = clause_terms
            .into_iter()
            .map(|any_of| FilterClause { any_of })
            .collect();

        Prefilter {
            clauses,
            ac,
            membership,
        }
    }

    /// True if `source` **might** contain a match — a cheap, parse-free gate. The
    /// caller runs the precise matcher only when this is true. Conservative: a
    /// pattern with no extractable terms always returns true.
    pub fn might_match(&self, source: &str) -> bool {
        if self.clauses.is_empty() {
            return true;
        }
        let Some(ac) = &self.ac else {
            return true;
        };

        let mut satisfied = vec![false; self.clauses.len()];
        let mut remaining = self.clauses.len();
        // Overlapping search so two distinct terms that overlap in the text are
        // both seen (e.g. `abc`/`bcd` in `abcd`).
        for m in ac.find_overlapping_iter(source) {
            for &(ci, boundary) in &self.membership[m.pattern().as_usize()] {
                if satisfied[ci] {
                    continue;
                }
                let ok = match boundary {
                    Boundary::Substring => true,
                    Boundary::Word => word_bounded(source, m.start(), m.end()),
                };
                if ok {
                    satisfied[ci] = true;
                    remaining -= 1;
                    if remaining == 0 {
                        return true;
                    }
                }
            }
        }
        remaining == 0
    }

    /// The required-term clauses (CNF). Exposed for index-query construction and
    /// tests; a caller turns each clause into an index lookup.
    pub fn clauses(&self) -> &[FilterClause] {
        &self.clauses
    }
}

/// Extract the indexable terms of a **source** file — the same content the
/// pattern side requires (identifiers and string-literal content), as deduped
/// word-runs (≥ `min_len`). The caller feeds these into its index (FTS tokens, or
/// n-grams for substring/regex queries) and later queries it with the clauses from
/// [`Pattern::prefilter`](crate::Pattern::prefilter). Use the **same `min_len`** on
/// both sides so the index is a superset of what any pattern can ask for.
///
/// AST traversal (not a text scan): at index-build time you're parsing anyway, so
/// this is category-precise — identifiers (named leaves) and string contents, with
/// comments skipped (the matcher skips them too). Over-collecting would only ever
/// add false positives, never false negatives.
pub fn index_terms(source: &str, cfg: &LangConfig, min_len: usize) -> Vec<String> {
    let mut parser = Parser::new();
    parser
        .set_language(&cfg.language)
        .expect("load language for index_terms");
    let Some(tree) = parser.parse(source, None) else {
        return Vec::new();
    };
    let mut out = HashSet::new();
    collect_index_terms(tree.root_node(), source.as_bytes(), min_len, &mut out);
    out.into_iter().collect()
}

fn collect_index_terms(node: Node, src: &[u8], min_len: usize, out: &mut HashSet<String>) {
    let kind = node.kind();
    if kind.contains("comment") {
        return; // patterns never match comment text (matcher skips them)
    }
    let keep = |s: &str, out: &mut HashSet<String>| {
        if s.chars().count() >= min_len {
            out.insert(s.to_string());
        }
    };
    if is_string_like(kind) {
        // The whole literal's word-runs (its content); don't descend into the
        // quote/content children.
        if let Ok(text) = node.utf8_text(src) {
            for run in word_runs(text) {
                keep(run, out);
            }
        }
        return;
    }
    if node.child_count() == 0 {
        // A named leaf that is word-shaped is an identifier (keywords are
        // anonymous; numbers start with a digit) — the source counterpart of a
        // pattern identifier term.
        if node.is_named()
            && let Ok(text) = node.utf8_text(src)
            && text
                .chars()
                .next()
                .is_some_and(|c| c.is_alphabetic() || c == '_')
        {
            keep(text, out);
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_index_terms(child, src, min_len, out);
    }
}

/// A string/char literal node kind. Liberal on purpose: over-collecting source
/// content into the index is sound (only adds false positives).
fn is_string_like(kind: &str) -> bool {
    kind.contains("string") || kind.contains("char")
}

/// A pattern `Token` is an identifier term iff it starts with an identifier char
/// (letter/`_` — excludes punctuation and numbers) and isn't a grammar keyword.
fn is_identifier_term(text: &str, cfg: &LangConfig) -> bool {
    let Some(first) = text.chars().next() else {
        return false;
    };
    (first.is_alphabetic() || first == '_') && !cfg.keywords.contains(text)
}

/// Maximal `[A-Za-z0-9_]` runs of `s`, in order.
fn word_runs(s: &str) -> impl Iterator<Item = &str> {
    s.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|r| !r.is_empty())
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether the byte range `[start, end)` of `s` sits on word boundaries both sides.
fn word_bounded(s: &str, start: usize, end: usize) -> bool {
    let before = start == 0 || !s[..start].chars().next_back().is_some_and(is_word_char);
    let after = end == s.len() || !s[end..].chars().next().is_some_and(is_word_char);
    before && after
}

/// Required literals of a regex, as CNF: a `Vec` of clauses, each a disjunction of
/// alternative literal strings. Every clause must hold (a literal in it appears);
/// an empty result means the regex imposes no extractable requirement. Sound: we
/// only emit literals that must appear in *every* match of the regex.
fn regex_required_literals(pattern: &str) -> Vec<Vec<String>> {
    match regex_syntax::parse(pattern) {
        Ok(hir) => required_lits(&hir),
        Err(_) => Vec::new(),
    }
}

fn required_lits(hir: &regex_syntax::hir::Hir) -> Vec<Vec<String>> {
    use regex_syntax::hir::HirKind;
    match hir.kind() {
        HirKind::Literal(lit) => match std::str::from_utf8(&lit.0) {
            Ok(s) if !s.is_empty() => vec![vec![s.to_string()]],
            _ => Vec::new(),
        },
        // The sub appears at least once => its literals are required.
        HirKind::Repetition(rep) if rep.min >= 1 => required_lits(&rep.sub),
        HirKind::Capture(cap) => required_lits(&cap.sub),
        // Concatenation: every part's requirements hold (AND).
        HirKind::Concat(subs) => subs.iter().flat_map(required_lits).collect(),
        // Alternation: one representative literal per branch (disjunction). If any
        // branch has no required literal, the whole alternation is unconstrained.
        HirKind::Alternation(subs) => {
            let mut alts = Vec::with_capacity(subs.len());
            for sub in subs {
                match required_lits(sub).first().and_then(|c| c.first()) {
                    Some(rep) => alts.push(rep.clone()),
                    None => return Vec::new(),
                }
            }
            if alts.is_empty() {
                Vec::new()
            } else {
                vec![alts]
            }
        }
        // Empty, Look (anchors), Class, optional Repetition: no required literal.
        _ => Vec::new(),
    }
}
