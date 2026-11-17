//! [`CodeSource`] — source text plus a lazily parsed, memoized tree-sitter AST.

use std::borrow::Cow;
use std::sync::OnceLock;

use crate::hazards::{TreeHazards, scan_tree_hazards};
use crate::positions::LineIndex;
use crate::prog_langs::{self, ProgrammingLanguageInfo, TreeSitterLanguageInfo};

/// The result of asking a [`CodeSource`] for its tree.
///
/// Deliberately **not** encoded here: whether the tree contains `ERROR` nodes.
/// That is per-consumer policy (the recursive splitter happily splits an
/// error-laden tree; other consumers may treat it as reason to degrade). This
/// enum answers only "did the parser produce a tree".
#[derive(Debug, Clone, Copy)]
pub enum ParseOutcome<'s> {
    /// No grammar to parse with: the language is unknown to the registry, or
    /// known but without tree-sitter support.
    NoGrammar,
    /// The parser returned no tree, or its lexer read the text so many times
    /// over that the parse was abandoned (a grammar whose lexer rescans to the
    /// end of the text, on input that is not its language). Rare; memoized,
    /// so repeated consumers don't retry the parse.
    ParseFailed,
    /// The parse produced a tree (possibly containing `ERROR` nodes).
    Parsed(&'s tree_sitter::Tree),
}

/// The parser reads its input in pieces of at most this many bytes, so the
/// bytes it reads count how far its lexer scans.
const PARSE_READ_CHUNK: usize = 1024;

/// A parse may read each byte of the text at most this many times, plus
/// [`PARSE_READ_BUDGET_BASE`] bytes, before it is abandoned as
/// [`ParseOutcome::ParseFailed`].
///
/// Some grammars' lexers scan to the end of the text from every token on
/// input that is not their language, such as Svelte, Scala or Dart on
/// unbalanced brackets, or Lua and Julia on another language's code: the
/// parse takes time quadratic in the text's length, seconds for tens of
/// kilobytes. A parse of source in its own language reads each byte about
/// twice. The budget counts bytes, not time, so whether a text parses is the
/// same on every run and machine, and in debug and release builds.
const PARSE_READ_BUDGET_PER_BYTE: usize = 256;

/// The read budget every parse has whatever the text's length, so a short
/// text never runs out of it.
const PARSE_READ_BUDGET_BASE: usize = 1 << 20;

/// Parses `text`, or returns `None` if the parser returns no tree or reads
/// more than its budget (see [`PARSE_READ_BUDGET_PER_BYTE`]).
fn parse_within_read_budget(
    parser: &mut tree_sitter::Parser,
    text: &str,
) -> Option<tree_sitter::Tree> {
    let budget = text
        .len()
        .saturating_mul(PARSE_READ_BUDGET_PER_BYTE)
        .saturating_add(PARSE_READ_BUDGET_BASE);
    let mut read = 0usize;
    let over_budget = std::cell::Cell::new(false);
    let bytes = text.as_bytes();
    let mut read_chunk = |offset: usize, _: tree_sitter::Point| -> &[u8] {
        if offset >= bytes.len() || over_budget.get() {
            return &[];
        }
        let mut end = (offset + PARSE_READ_CHUNK).min(bytes.len());
        // End on a char boundary, so no chunk splits a UTF-8 sequence.
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        if end <= offset {
            end = (offset + 1..=bytes.len())
                .find(|&i| text.is_char_boundary(i))
                .unwrap_or(bytes.len());
        }
        read += end - offset;
        if read > budget {
            // Reading nothing more ends the parse at the next token; the
            // progress callback cancels it sooner where it can.
            over_budget.set(true);
            return &[];
        }
        &bytes[offset..end]
    };
    let mut cancel_over_budget = |_: &tree_sitter::ParseState| {
        if over_budget.get() {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    };
    let tree = parser.parse_with_options(
        &mut read_chunk,
        None,
        Some(tree_sitter::ParseOptions::new().progress_callback(&mut cancel_over_budget)),
    );
    if over_budget.get() { None } else { tree }
}

/// Source text with an optional, lazily parsed tree-sitter AST.
///
/// This is the shared **input type** for every API that may need an AST
/// (splitters, structural matchers, extractors). Consumers call [`tree`] and
/// handle the three outcomes internally — degradation is never routed by the
/// caller. The parse (and the [`LineIndex`]) happen at most once per source,
/// no matter how many consumers touch it, and a failed parse is memoized too.
///
/// The text is a [`Cow`], so `&str` convenience entry points can wrap a
/// borrowed `CodeSource` on the fly at zero cost, while long-lived handles
/// (e.g. the Python binding) own their text (`CodeSource<'static>`).
///
/// The language is fixed at construction — the cache is only sound per
/// grammar. A consumer that resolves languages through its own table (e.g. a
/// splitter's custom regex languages, or a pattern compiled for a specific
/// language) must check [`info`] for identity first and skip the cache on
/// mismatch.
///
/// [`tree`]: CodeSource::tree
/// [`info`]: CodeSource::info
pub struct CodeSource<'a> {
    text: Cow<'a, str>,
    /// The language as the caller supplied it (name, alias, or extension) —
    /// kept verbatim for consumers that match on the raw name (custom-language
    /// tables) and for user-facing getters.
    requested_language: Option<Cow<'a, str>>,
    /// Registry resolution of `requested_language`, done once at construction.
    info: Option<&'static ProgrammingLanguageInfo>,
    /// Lazy parse; inner `None` = the parser returned no tree (memoized).
    tree: OnceLock<Option<tree_sitter::Tree>>,
    line_index: OnceLock<LineIndex>,
    hazards: OnceLock<TreeHazards>,
}

impl<'a> CodeSource<'a> {
    /// A source with no language: [`tree`](Self::tree) is always
    /// [`ParseOutcome::NoGrammar`].
    pub fn new(text: impl Into<Cow<'a, str>>) -> Self {
        Self::build(text.into(), None, None)
    }

    /// A source for `language` (name, alias, or file extension — resolved
    /// case-insensitively through the registry). An unknown language is not an
    /// error: it resolves to no grammar and consumers degrade.
    pub fn with_language(text: impl Into<Cow<'a, str>>, language: impl Into<Cow<'a, str>>) -> Self {
        let language = language.into();
        let info = prog_langs::get_language_info(&language);
        Self::build(text.into(), Some(language), info)
    }

    /// A source for an already-resolved registry entry (skips name resolution;
    /// grammar identity is `info` by construction).
    pub fn with_info(
        text: impl Into<Cow<'a, str>>,
        info: &'static ProgrammingLanguageInfo,
    ) -> Self {
        Self::build(
            text.into(),
            Some(Cow::Borrowed(info.name.as_ref())),
            Some(info),
        )
    }

    fn build(
        text: Cow<'a, str>,
        requested_language: Option<Cow<'a, str>>,
        info: Option<&'static ProgrammingLanguageInfo>,
    ) -> Self {
        Self {
            text,
            requested_language,
            info,
            tree: OnceLock::new(),
            line_index: OnceLock::new(),
            hazards: OnceLock::new(),
        }
    }

    /// The source text.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The language exactly as the caller supplied it (may be an alias or a
    /// file extension; `None` if constructed without a language).
    pub fn requested_language(&self) -> Option<&str> {
        self.requested_language.as_deref()
    }

    /// The resolved registry entry, if the requested language is known.
    /// Registry entries are per-language singletons, so this reference is also
    /// the language/grammar **identity** (compare with [`std::ptr::eq`]).
    pub fn info(&self) -> Option<&'static ProgrammingLanguageInfo> {
        self.info
    }

    /// The resolved tree-sitter grammar info, if the language has one.
    pub fn treesitter_info(&self) -> Option<&'static TreeSitterLanguageInfo> {
        self.info.and_then(|i| i.treesitter_info.as_ref())
    }

    /// Get-or-parse the AST, memoized (including failure). Thread-safe; safe
    /// to call with the Python GIL released.
    pub fn tree(&self) -> ParseOutcome<'_> {
        let Some(ts) = self.treesitter_info() else {
            return ParseOutcome::NoGrammar;
        };
        let cached = self.tree.get_or_init(|| {
            let mut parser = tree_sitter::Parser::new();
            if parser.set_language(&ts.tree_sitter_lang).is_err() {
                return None;
            }
            parse_within_read_budget(&mut parser, &self.text)
        });
        match cached {
            Some(tree) => ParseOutcome::Parsed(tree),
            None => ParseOutcome::ParseFailed,
        }
    }

    /// Get-or-build the byte→(char offset, line, column) index, memoized.
    pub fn line_index(&self) -> &LineIndex {
        self.line_index.get_or_init(|| LineIndex::build(&self.text))
    }

    /// Get-or-scan the tree-trust hazards of the parsed AST, memoized: a
    /// shallow parse error (error recovery reshaped the tree's top structure —
    /// consumers deriving structure from it should degrade) and pathologically
    /// deep subtrees (recursive walks must treat them as opaque). `None` when
    /// there is no tree to scan.
    pub fn tree_hazards(&self) -> Option<&TreeHazards> {
        let ParseOutcome::Parsed(tree) = self.tree() else {
            return None;
        };
        Some(
            self.hazards
                .get_or_init(|| scan_tree_hazards(tree.root_node())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_language_is_no_grammar() {
        let src = CodeSource::new("fn main() {}");
        assert!(matches!(src.tree(), ParseOutcome::NoGrammar));
        assert!(src.info().is_none());
        assert!(src.requested_language().is_none());
    }

    #[test]
    fn unknown_language_is_no_grammar() {
        let src = CodeSource::with_language("hello", "no-such-language");
        assert!(matches!(src.tree(), ParseOutcome::NoGrammar));
        assert!(src.info().is_none());
        assert_eq!(src.requested_language(), Some("no-such-language"));
    }

    #[test]
    fn known_language_without_grammar_is_no_grammar() {
        // "haskell" is in the registry but has no tree-sitter grammar.
        let src = CodeSource::with_language("main = putStrLn \"hi\"", "haskell");
        assert!(src.info().is_some());
        assert!(matches!(src.tree(), ParseOutcome::NoGrammar));
    }

    #[test]
    fn parse_is_memoized() {
        let src = CodeSource::with_language("def f():\n    return 1\n", ".py");
        let ParseOutcome::Parsed(first) = src.tree() else {
            panic!("expected a parse");
        };
        let ParseOutcome::Parsed(second) = src.tree() else {
            panic!("expected a parse");
        };
        // Same memoized tree, not a re-parse.
        assert!(std::ptr::eq(first, second));
        assert_eq!(first.root_node().kind(), "module");
        // Alias resolved to the canonical entry.
        assert_eq!(src.info().unwrap().name.as_ref(), "python");
        assert_eq!(src.requested_language(), Some(".py"));
    }

    #[test]
    fn nix_source_parses_without_errors() {
        let src = CodeSource::with_language(
            "{ pkgs, ... }:\nlet package-name = pkgs.hello; in { inherit package-name; }\n",
            "nix",
        );
        let ParseOutcome::Parsed(tree) = src.tree() else {
            panic!("expected a Nix parse");
        };
        assert_eq!(tree.root_node().kind(), "source_code");
        assert!(!tree.root_node().has_error());
    }

    #[test]
    fn with_info_matches_with_language() {
        let info = prog_langs::get_language_info("rust").unwrap();
        let src = CodeSource::with_info("fn main() {}", info);
        assert!(std::ptr::eq(src.info().unwrap(), info));
        assert_eq!(src.requested_language(), Some("rust"));
        assert!(matches!(src.tree(), ParseOutcome::Parsed(_)));
    }

    #[test]
    fn a_parse_whose_lexer_rescans_the_text_is_abandoned() {
        // Svelte's lexer scans to the end of the text from every token of
        // unbalanced brackets: reading the text thousands of times over,
        // seconds for these 24 KB. The read budget stops it, the same way on
        // every run.
        let text = "({[".repeat(8_000);
        let src = CodeSource::with_language(text.as_str(), "svelte");
        assert!(matches!(src.tree(), ParseOutcome::ParseFailed));

        // Source in the grammar's own language, and multi-byte text cut into
        // read chunks, parse as they would in one piece.
        for (language, text) in [
            (
                "svelte",
                "<script>\n  let n = 0;\n</script>\n\n<button on:click={() => n++}>{n}</button>\n"
                    .repeat(500),
            ),
            ("rust", "fn 日本() { let s = \"🦀é\"; }\n".repeat(2_000)),
        ] {
            let src = CodeSource::with_language(text.as_str(), language);
            let ParseOutcome::Parsed(tree) = src.tree() else {
                panic!("{language} parses within its budget");
            };
            let mut parser = tree_sitter::Parser::new();
            parser
                .set_language(&src.treesitter_info().unwrap().tree_sitter_lang)
                .unwrap();
            let whole = parser.parse(text.as_str(), None).unwrap();
            assert_eq!(tree.root_node().to_sexp(), whole.root_node().to_sexp());
        }
    }

    #[test]
    fn error_recovery_over_a_long_unparsable_run_is_not_quadratic() {
        // tree-sitter before 0.26 copied every child of the error so far on
        // each recovery: these 160 KB took over 15 s in a release build. Now
        // a fraction of a second, in a debug build too.
        let text = "a🦀".repeat(32_000);
        let src = CodeSource::with_language(text.as_str(), "rust");
        let start = std::time::Instant::now();
        assert!(matches!(src.tree(), ParseOutcome::Parsed(_)));
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "parse took {elapsed:?}"
        );
    }

    #[test]
    fn tree_hazards_memoized_and_gated_on_parse() {
        let src = CodeSource::with_language("def f():\n    return 1\n", "python");
        let first = src.tree_hazards().expect("parsed") as *const TreeHazards;
        let second = src.tree_hazards().expect("parsed") as *const TreeHazards;
        assert_eq!(first, second);
        assert!(!src.tree_hazards().unwrap().parse_error);
        assert!(src.tree_hazards().unwrap().deep_spans.is_empty());

        let no_grammar = CodeSource::new("plain text");
        assert!(no_grammar.tree_hazards().is_none());
    }

    #[test]
    fn line_index_is_shared() {
        let src = CodeSource::new("a\nbc\n");
        let first = src.line_index() as *const LineIndex;
        let second = src.line_index() as *const LineIndex;
        assert_eq!(first, second);
        let pos = src.line_index().position(src.text(), 2);
        assert_eq!((pos.line, pos.column), (2, 1));
    }
}
