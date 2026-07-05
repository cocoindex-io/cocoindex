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
    /// The parser returned no tree. Rare (cancellation/timeout-class failures);
    /// memoized, so repeated consumers don't retry the parse.
    ParseFailed,
    /// The parse produced a tree (possibly containing `ERROR` nodes).
    Parsed(&'s tree_sitter::Tree),
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
            parser.parse(self.text.as_ref(), None)
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
    fn with_info_matches_with_language() {
        let info = prog_langs::get_language_info("rust").unwrap();
        let src = CodeSource::with_info("fn main() {}", info);
        assert!(std::ptr::eq(src.info().unwrap(), info));
        assert_eq!(src.requested_language(), Some("rust"));
        assert!(matches!(src.tree(), ParseOutcome::Parsed(_)));
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
