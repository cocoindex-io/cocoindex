//! Language configuration and the tokenizer framework — the abstraction the
//! engine (`lexer`, `matcher`) consumes. This module is *not* language-specific:
//! the per-language constructors live in `lang`, which depends on this.
//!
//! A language = a tree-sitter grammar + a list of **tokenizers** for the
//! structured token classes (identifiers, numbers, strings, raw strings, …).
//! The operator/punctuation table and the `>>`-style "splittable" set are
//! *derived from the grammar*. Comments need no config (the source side is
//! tree-sitter; `collect` skips comment nodes).

use std::collections::HashSet;
use std::sync::Arc;

use regex::Regex;
use tree_sitter::Language;

// ---------------------------------------------------------------------------
// Tokenizer interface
// ---------------------------------------------------------------------------

/// How a matched pattern token aligns against the source. This is the strong
/// type the lexer carries into `PatternItem::{Token, Str}` — the two are distinct
/// match operations (single leaf vs whole node), not a redundant split; see the
/// note on `PatternItem` in `lexer.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokKind {
    /// Word / number / operator — always a single source *leaf*, matched by text.
    Token,
    /// String / char / raw literal (or any atomic whole-node class) — matched
    /// against a source *node* by text, spanning all its leaves at once.
    Str,
}

/// A pattern tokenizer: given the input at a position, return the byte length of
/// a token at the start, or `None`. `RegexTokenizer` covers the easy cases;
/// hand-written impls (e.g. `lang::cpp`'s raw-string scanner) cover
/// delimiter-balanced forms the `regex` crate can't (no backreferences).
pub trait Tokenizer: Send + Sync {
    fn match_len(&self, input: &str) -> Option<usize>;
}

/// A tokenizer active in any lexer mode (the default mask).
pub const ALL_MODES: u8 = 0xFF;

/// A tokenizer paired with how its match should be emitted and which lexer
/// **modes** it's active in. Most languages have one mode (`ALL_MODES`);
/// context-sensitive ones (HTML/XML) restrict rules per mode (DESIGN: lexer modes).
#[derive(Clone)]
pub struct TokenRule {
    pub tokenizer: Arc<dyn Tokenizer>,
    pub kind: TokKind,
    /// Bitmask of modes this rule applies in (bit `m` ↔ mode `m`).
    pub modes: u8,
}

impl TokenRule {
    pub fn new(tokenizer: impl Tokenizer + 'static, kind: TokKind) -> Self {
        TokenRule {
            tokenizer: Arc::new(tokenizer),
            kind,
            modes: ALL_MODES,
        }
    }
    /// Restrict this rule to a set of lexer modes (a bitmask).
    pub fn in_modes(mut self, modes: u8) -> Self {
        self.modes = modes;
        self
    }
}

/// A position-anchored regex tokenizer (the pattern is compiled with a leading `^`).
pub struct RegexTokenizer {
    re: Regex,
}

impl RegexTokenizer {
    pub fn new(pat: &str) -> Self {
        RegexTokenizer {
            re: Regex::new(pat).expect("valid tokenizer regex"),
        }
    }
}

impl Tokenizer for RegexTokenizer {
    fn match_len(&self, input: &str) -> Option<usize> {
        // `^` anchors at the start, so a match (if any) starts at 0.
        self.re.find(input).map(|m| m.end()).filter(|&l| l > 0)
    }
}

/// Convenience: a regex-based rule.
pub fn regex_rule(pat: &str, kind: TokKind) -> TokenRule {
    TokenRule::new(RegexTokenizer::new(pat), kind)
}

// --- shared (generic) token-class builders, composed by the language modules ---

/// Identifier, Unicode-aware (`XID_Start`/`XID_Continue` + leading `_`), so a
/// pattern can contain non-ASCII identifiers (Python/JS allow them). The source
/// side decides what's a real identifier; over-matching in the pattern is
/// harmless (a pattern identifier the source lacks just won't match).
pub fn identifier() -> TokenRule {
    regex_rule(r"^[_\p{XID_Start}][_\p{XID_Continue}]*", TokKind::Token)
}

/// Number: starts with a digit or `.digit` (so `.5`, `1.`, `1.5` all work),
/// then a run of digits/letters/`_`/`.` (covers `0xFF`, `1_000`, suffixes,
/// fractions) — with `[eEpP][-+]` tried *first* so signed exponents (`1.5e-10`)
/// aren't swallowed by the alnum branch. `extra` adds chars like `'` (C/C++).
/// A leading `.` only starts a number when followed by a digit, so `a.b` member
/// access still splits.
pub fn number(extra: &str) -> TokenRule {
    regex_rule(
        &format!(r"^(?:[0-9]|\.[0-9])(?:[eEpP][-+]|[0-9A-Za-z_.{extra}])*"),
        TokKind::Token,
    )
}

pub fn dq_string() -> TokenRule {
    regex_rule(r#"(?s)^"(?:\\.|[^"\\])*""#, TokKind::Str)
}
pub fn sq_string() -> TokenRule {
    // also a C/C++/Rust char literal; a bare `'a` (Rust lifetime) has no closing
    // quote so this fails and `'` falls through to the op table.
    regex_rule(r"(?s)^'(?:\\.|[^'\\])*'", TokKind::Str)
}
pub fn backtick_string() -> TokenRule {
    regex_rule(r"(?s)^`(?:\\.|[^`\\])*`", TokKind::Str)
}

// --- non-backslash string conventions ---
//
// `dq_string`/`sq_string` above bake in C-style backslash escaping (`"a\"b"` is
// one string). That is *wrong* for languages that escape differently, where it
// would close the string at the wrong quote. The builders below cover the two
// other common conventions; a language must pick the one its grammar uses.

/// Single-quoted string where an embedded quote is written by **doubling** it
/// (`'it''s'`), with no backslash escaping. Used by SQL, Fortran, Pascal.
pub fn sq_string_doubled() -> TokenRule {
    regex_rule(r"(?s)^'(?:''|[^'])*'", TokKind::Str)
}

/// Double-quoted string with doubled-quote escaping (`"a""b"`) — Fortran.
pub fn dq_string_doubled() -> TokenRule {
    regex_rule(r#"(?s)^"(?:""|[^"])*""#, TokKind::Str)
}

/// Single-quoted string with **no** escaping at all — a `'` always closes it
/// (`'$x\n'` is the five characters `$x\n`). Used by POSIX shells and TOML's
/// literal strings.
pub fn sq_string_literal() -> TokenRule {
    regex_rule(r"^'[^']*'", TokKind::Str)
}

/// Backtick string with **no** escaping — a backtick always closes it, and `\`
/// is a literal character (Go raw strings `` `a\b` ``). May span newlines.
pub fn backtick_string_literal() -> TokenRule {
    regex_rule(r"^`[^`]*`", TokKind::Str)
}

/// A free-text run up to (not including) `stop`, emitted atomically like a
/// string node. Used for markup text content (HTML/XML), where the run has no
/// special punctuation and a `"` is a literal char, not a string delimiter.
/// `stop` must be safe inside a regex character class (e.g. `<`).
pub fn free_text(stop: char) -> TokenRule {
    regex_rule(&format!("^[^{stop}]+"), TokKind::Str)
}

/// Triple-double-quoted string `"""..."""` — one node. The generic `"..."`
/// would mis-split the opening `""` as an empty string, so languages with raw /
/// multiline triple strings (Kotlin, Julia, Elm, TOML basic, …) add this.
/// Interpolations inside are matched opaquely via the whole node's text.
pub fn triple_dq_string() -> TokenRule {
    regex_rule(r#"(?s)^""".*?""""#, TokKind::Str)
}

/// Triple-single-quoted string `'''...'''` — one node (TOML literal multiline).
pub fn triple_sq_string() -> TokenRule {
    regex_rule(r"(?s)^'''.*?'''", TokKind::Str)
}

/// The tokenizer profile for **C-style** languages: identifier, number, and the
/// three quote styles with *backslash* escaping. Passed explicitly by the large
/// family (C/C++/Rust/Java/JS/TS/C#/Go/…) to [`LangConfig::from_grammar`].
/// Languages that escape differently (SQL/Fortran/Pascal `''` doubling,
/// shells/TOML literal `'...'`, Go raw backticks) compose their own set from the
/// `*_doubled` / `*_literal` builders above. Verify with a `literal_forms` test.
pub fn c_like_tokenizers() -> Vec<TokenRule> {
    vec![
        identifier(),
        number(""),
        dq_string(),
        sq_string(),
        backtick_string(),
    ]
}

// ---------------------------------------------------------------------------
// LangConfig
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct LangConfig {
    pub language: Language,
    /// Anonymous punctuation/operator tokens, longest-first, excluding the
    /// splittable compounds. Derived from the grammar; used for maximal munch.
    pub op_tokens: Vec<String>,
    /// Compound operators normalized to single chars on both sides (e.g. `>>` ->
    /// `>` `>`). Auto-detected from the grammar.
    pub splittable: HashSet<String>,
    /// The grammar's word-shaped anonymous symbols — its keywords (`if`, `return`,
    /// contextual ones like TS `type`). Complement of `op_tokens` among anonymous
    /// symbols. Used by the prefilter to exclude keywords from identifier terms.
    pub keywords: HashSet<String>,
    /// Metavariable sigil. Default `\` (shell-safe).
    pub meta_char: char,
    /// Tokenizers for literal/identifier classes, tried at each position; the
    /// longest match wins.
    pub tokenizers: Vec<TokenRule>,
    /// Lexer **mode** transitions: in mode `from`, emitting a single-char token
    /// whose text is `trigger` switches the lexer to mode `to`. Empty ⇒ a single
    /// mode (mode 0), the default. HTML/XML use this to flip between text and tag
    /// context (`<` ⇒ tag, `>` ⇒ text).
    pub transitions: Vec<(u8, char, u8)>,
    /// Bitmask of modes that **preserve** whitespace (the lexer does not skip it)
    /// — e.g. HTML text content. Default 0 (whitespace skipped in every mode).
    pub ws_preserve: u8,
}

impl LangConfig {
    /// Build a config from a grammar and an **explicit** tokenizer profile.
    /// There is deliberately no default: every language must state how its
    /// literals tokenize, because string-escaping conventions differ (a `"a\"b"`
    /// that is one string in C is two in a doubled-quote language). C-style
    /// languages pass [`c_like_tokenizers`]; others compose the `*_doubled` /
    /// `*_literal` / `triple_*` builders. Op/splittable tables are derived from
    /// the grammar.
    pub fn from_grammar(language: Language, tokenizers: Vec<TokenRule>) -> Self {
        let single_char = single_char_punct_tokens(&language);
        let splittable = detect_splittable(&language, &single_char);
        let op_tokens = derive_op_tokens(&language, &splittable);
        let keywords = derive_keywords(&language);
        LangConfig {
            language,
            op_tokens,
            splittable,
            keywords,
            meta_char: '\\',
            tokenizers,
            transitions: Vec::new(),
            ws_preserve: 0,
        }
    }

    /// Configure a context-sensitive (multi-mode) lexer: `transitions` flip the
    /// active mode on single-char tokens, `ws_preserve` marks modes whose
    /// whitespace is significant.
    pub fn with_modes(mut self, transitions: Vec<(u8, char, u8)>, ws_preserve: u8) -> Self {
        self.transitions = transitions;
        self.ws_preserve = ws_preserve;
        self
    }

    /// Override the metavariable sigil (default `\`).
    pub fn with_meta_char(mut self, c: char) -> Self {
        self.meta_char = c;
        self
    }

    pub fn is_splittable(&self, text: &str) -> bool {
        self.splittable.contains(text)
    }

    /// Whether mode `m` preserves whitespace (the lexer should not skip it).
    pub fn preserves_ws(&self, mode: u8) -> bool {
        (self.ws_preserve >> mode) & 1 == 1
    }

    /// The mode after emitting an operator token `text` in mode `mode`. A token
    /// *containing* the trigger char flips the mode, so multi-char tag delimiters
    /// work too (`</`, `/>`, `<!--` carry `<`/`>`). Only the caller's *operator*
    /// tokens reach here — string/free-text tokens never trigger, so a `>` inside
    /// an attribute value or text run doesn't spuriously flip the mode.
    pub fn mode_after(&self, mode: u8, text: &str) -> u8 {
        for &(from, trigger, to) in &self.transitions {
            if from == mode && text.contains(trigger) {
                return to;
            }
        }
        mode
    }
}

// ---------------------------------------------------------------------------
// Grammar-derived tables
// ---------------------------------------------------------------------------

fn is_punct_token(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| !c.is_alphanumeric() && c != '_')
}

/// The single-character punctuation tokens the grammar defines.
fn single_char_punct_tokens(lang: &Language) -> HashSet<String> {
    let mut set = HashSet::new();
    for id in 0..lang.node_kind_count() as u16 {
        if lang.node_kind_is_named(id) {
            continue;
        }
        if let Some(s) = lang.node_kind_for_id(id)
            && s.chars().count() == 1
            && is_punct_token(s)
        {
            set.insert(s.to_string());
        }
    }
    set
}

/// A compound token is splittable when it is all-punctuation, longer than one
/// char, and every character is itself a single-char token. Splitting it on both
/// sides aligns context-sensitive tokenizations like C++/Rust `>>`. Over-split
/// (e.g. `==`) is safe — both sides normalize identically.
fn detect_splittable(lang: &Language, single_char: &HashSet<String>) -> HashSet<String> {
    let mut set = HashSet::new();
    for id in 0..lang.node_kind_count() as u16 {
        if lang.node_kind_is_named(id) {
            continue;
        }
        let Some(s) = lang.node_kind_for_id(id) else {
            continue;
        };
        if !is_punct_token(s) || s.chars().count() < 2 {
            continue;
        }
        if s.chars().all(|c| single_char.contains(&c.to_string())) {
            set.insert(s.to_string());
        }
    }
    set
}

/// Operator/punctuation tokens for maximal munch. Dropping the splittables here
/// already implements "remove a compound whose every char is an existing token":
/// what survives is single-char tokens plus the few multi-char tokens with a
/// *non-token* component (e.g. TS `${`, Python `!=` — `$`/`!` aren't standalone
/// tokens there), which must stay whole.
fn derive_op_tokens(lang: &Language, splittable: &HashSet<String>) -> Vec<String> {
    let mut toks: Vec<String> = Vec::new();
    for id in 0..lang.node_kind_count() as u16 {
        if lang.node_kind_is_named(id) {
            continue;
        }
        let Some(s) = lang.node_kind_for_id(id) else {
            continue;
        };
        if s.is_empty() || s == "ERROR" || !is_punct_token(s) || splittable.contains(s) {
            continue;
        }
        toks.push(s.to_string());
    }
    toks.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    toks.dedup();
    toks
}

/// The grammar's word-shaped anonymous symbols — its keywords. These are the
/// anonymous symbols `derive_op_tokens` *excludes* (they fail `is_punct_token`),
/// e.g. `if`, `return`, `async`, contextual keywords like TS `type`. A pattern
/// word in this set is a keyword, not a selective identifier, so the prefilter
/// skips it.
fn derive_keywords(lang: &Language) -> HashSet<String> {
    let mut set = HashSet::new();
    for id in 0..lang.node_kind_count() as u16 {
        if lang.node_kind_is_named(id) {
            continue;
        }
        let Some(s) = lang.node_kind_for_id(id) else {
            continue;
        };
        if s.is_empty() || s == "ERROR" || is_punct_token(s) {
            continue;
        }
        set.insert(s.to_string());
    }
    set
}
