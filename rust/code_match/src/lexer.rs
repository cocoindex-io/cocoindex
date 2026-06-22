//! Pattern lexer: turns a pattern string into a flat `Vec<PatternItem>`.
//!
//! Metavariables are first-class tokens. Everything else is lexed with a small
//! hand engine + the per-language op-token table.
//!
//! Metavar syntax (sigil `S` is configurable, default `\`; delimiters are
//! **symmetric** — a metavar is `S( … S)`, see DESIGN §0). Currently implemented:
//!   `S NAME`        single, named (sugar for `S(NAME S)`)
//!   `S(NAME S)`     single, named
//!   `S(NAME* S)`    many   (zero or more **same-level** sibling nodes)
//!   `S(NAME+ S)`    one-or-more (one or more **same-level** sibling nodes)
//!   `S(NAME? S)`    optional (zero or one node)
//!   `S_` / `S(_ S)` anonymous single        `S*` / `S(* S)`  anonymous many
//!   `S+` / `S(+ S)` anonymous one-or-more   `S?` / `S(? S)`  anonymous optional
//!   `S(NAME:/re/ S)` single, regex-constrained — see below
//!   `S(/re/ S)` / `S/re/`  regex-constrained, **anonymous** (filter, don't capture)
//!   `S{{ INNER S}}` containment: INNER must match some descendant of one node
//!                   here (DESIGN §12). Paired; may nest.
//!   `SS`            a doubled sigil is one **literal** sigil (e.g. `\\` → `\`)
//! `*`/`+` are **same-level** (one parent's direct siblings); a cross-level skip is
//! written as multiple `*`, one per grammar level.
//! Names are `[A-Za-z0-9_]+` (upper/lower/digit, sed-like `\1`); UPPERCASE is a
//! readability convention, not a rule. With the `\` sigil, lowercase `\foo` only
//! collides with bare-`\` escapes (Bash `\n`) / Haskell lambdas.
//!
//! Regex matcher `/re/` constrains a **single-node** metavar: the captured source
//! text must match the regex **anchored to the whole node** (compiled as
//! `^(?:re)$`). So `/set_/` means "text is exactly `set_`", `/set_.*/` means
//! "starts with `set_`"; add `.*` explicitly for prefix/suffix/substring. (This is
//! less surprising than unanchored `is_match`, where a bare `set_.*` would match
//! inside `unset_…`.) Named is `S(NAME:/re/ S)` (the `:` only follows a NAME);
//! anonymous is `S(/re/ S)` or the short form `S/re/`. On a **run** (`S(NAME:/re/* S)`
//! / `+`) the regex constrains *every* node — literal per-node: a non-matching node
//! ends the run. The quantifier sits after the regex term (the after-NAME position
//! `S(NAME*:/re/ S)` is also accepted). The regex is **delimited** —
//! it closes at the first unescaped `/`; *balanced* `()` inside (alternations) need
//! no escaping, escape a literal `/` as `\/`. An unparseable (or unterminated)
//! regex is a `client` error from `lex` / `Pattern::compile`.

use crate::config::{LangConfig, TokKind};
use cocoindex_utils::error::{Error, Result};
use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    /// `\X` — exactly one node
    One,
    /// `\(X*\)` — zero or more sibling nodes
    Many,
    /// `\(X+\)` — one or more sibling nodes (like `Many`, but non-empty)
    OneOrMore,
    /// `\(X?\)` — zero or one node
    Optional,
}

// `Regex` is not `PartialEq`/`Eq`, so `PatternItem` isn't either (it's only ever
// pattern-matched, never compared for equality).
//
// `Token` vs `Str` are two **distinct match operations** (not a vestigial split):
//   - `Token` → one source *leaf* by text, advancing exactly one leaf.
//   - `Str`   → one whole *node* span by text, advancing past all its leaves.
// They can't be unified without loss. An operator/punctuation leaf (`+`, `<`) is
// *anonymous* — it never appears as a named span, so the whole-node path can't
// reach it. A composite literal (`"foo"`, `'a'`, a raw string) spans ≥2 leaves
// (quote + content + quote), so the single-leaf path can't reach it. The split
// also lets the matcher do the minimal correct lookup in its hot path instead of
// speculatively trying both. Critically, the lexer already knows which one a
// token is — for free, from the tokenizer's `TokKind` (config.rs) — so encoding
// it in the variant here is the "exchange to the strong type early" rule: a
// unified variant would discard that and re-derive it per compare at match time.
#[derive(Debug, Clone)]
pub enum PatternItem {
    /// Operator / punctuation / word (identifier / keyword / number) — always a
    /// single source *leaf*. Matched against `leaves[li]` by text (advance 1 leaf).
    Token(String),
    /// Atomic whole-node literal: string / char / raw string, and any other
    /// `TokKind::Str` class (e.g. TOML dates, CSS hex colors). Matched against a
    /// source *node* span by text, atomically (advance past the whole span).
    Str(String),
    /// Metavariable. `name` == None for anonymous (`\_`, `\*`, `\?`). `regex`, if
    /// present, constrains the captured text (whole-node anchored — compiled
    /// `^(?:re)$`); honored for single-node cardinalities (`One`/`Optional`) only.
    /// `Many` (`*`) is always same-level; cross-level skips use multiple `*`.
    Meta {
        name: Option<String>,
        card: Cardinality,
        regex: Option<Regex>,
    },
    /// `\{{` — opens a containment `\{{ INNER \}}` (DESIGN §12). `close` is
    /// the index of the matching `ContainsClose` in the flat items vec
    /// (back-patched by `lex`); `INNER` is `items[self_index+1 .. close]`. Kept
    /// flat (not a nested `Vec`) so the DP runs in one `pi` space with one memo,
    /// and nesting falls out of the back-patched indices.
    ContainsOpen { close: usize },
    /// `\}}` — closes the containment opened by the matching `ContainsOpen`.
    ContainsClose,
    /// `\{` — opens a whole-node boundary `\{ P \}` ("is"): `P` must match an
    /// *entire* node (anchored, no leading/trailing tolerance). Same flat
    /// back-patched representation as `ContainsOpen`; `P` is `items[self+1 .. close]`.
    WholeOpen { close: usize },
    /// `\}` — closes the whole-node boundary opened by the matching `WholeOpen`.
    WholeClose,
}

pub fn lex(pattern: &str, cfg: &LangConfig) -> Result<Vec<PatternItem>> {
    let bytes = pattern.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    // Lexer mode — single-mode (0) for most languages; HTML/XML flip between
    // text (0) and tag (1) on `<`/`>`, which gates the tokenizers + whitespace.
    let mut mode: u8 = 0;

    // We dispatch on the leading `char` at `i` (not a raw byte). `i` is always
    // advanced by char-aligned amounts (a decoded char, a regex match `end()`,
    // or an ASCII op length), so every slice below is on a char boundary —
    // emoji/CJK anywhere can never panic or mis-slice. Inside `lex_metavar`,
    // the byte scans search only for *ASCII* delimiters, which (by UTF-8's
    // self-synchronizing property) never occur inside a multi-byte char.
    while i < bytes.len() {
        let first = pattern[i..].chars().next().expect("i is a char boundary");
        let clen = first.len_utf8();

        // whitespace (decode the char so non-ASCII whitespace, e.g. U+3000, skips
        // too) — unless the current mode preserves it (e.g. HTML text content,
        // where it's part of the text node).
        if first.is_whitespace() && !cfg.preserves_ws(mode) {
            i += clen;
            continue;
        }

        // metavar sigil (compare as a char; any sigil works, not just ASCII)
        if first == cfg.meta_char {
            let after = i + clen;
            // escaped sigil: a doubled sigil (`\\`) is one *literal* sigil — the
            // way to write a literal `\` (or whatever the sigil is) in a pattern.
            if pattern[after..].starts_with(cfg.meta_char) {
                out.push(PatternItem::Token(cfg.meta_char.to_string()));
                i = after + cfg.meta_char.len_utf8();
                continue;
            }
            // containment markers `\{{` / `\}}` (sigil-agnostic: the `{{`/`}}`
            // sits right after the sigil). `close` is back-patched after lexing.
            if pattern[after..].starts_with("{{") {
                out.push(PatternItem::ContainsOpen { close: 0 });
                i = after + 2;
                continue;
            }
            if pattern[after..].starts_with("}}") {
                out.push(PatternItem::ContainsClose);
                i = after + 2;
                continue;
            }
            // whole-node boundary markers `\{` / `\}` (single brace; the doubled
            // `\{{`/`\}}` above already consumed the containment form). Back-patched
            // alongside the containment markers by `resolve_brackets`.
            if pattern[after..].starts_with('{') {
                out.push(PatternItem::WholeOpen { close: 0 });
                i = after + 1;
                continue;
            }
            if pattern[after..].starts_with('}') {
                out.push(PatternItem::WholeClose);
                i = after + 1;
                continue;
            }
            if let Some((item, next)) = lex_metavar(pattern, bytes, after, cfg.meta_char)? {
                out.push(item);
                i = next;
                continue;
            }
            // bare sigil (e.g. `$` in PHP/JS source, or a lone `\`): literal token
            out.push(PatternItem::Token(pattern[i..after].to_string()));
            i = after;
            continue;
        }

        // tokenizers (per-language literal/identifier classes) + operator table:
        // try all, take the LONGEST match (maximal munch). Tokenizers emit Token
        // (word/number → leaf) or Str (string/char/raw → node); operators are Token.
        let rest = &pattern[i..];
        let mut best_len = 0usize;
        let mut best_kind = TokKind::Token;
        for rule in &cfg.tokenizers {
            if rule.modes & (1 << mode) == 0 {
                continue; // not active in the current mode
            }
            if let Some(l) = rule.tokenizer.match_len(rest)
                && l > best_len
            {
                best_len = l;
                best_kind = rule.kind;
            }
        }
        // Linear scan over ~25-30 mostly-single-char ops, longest-first.
        // Deliberately not unioned into a regex/Aho-Corasick automaton: this is a
        // cold path (lexing runs once per `Pattern::compile` on a short pattern;
        // the matcher — the hot path — never touches `op_tokens`), so a single-
        // pass matcher would add per-call overhead + escaping/ordering risk for
        // no measurable gain. If lexing ever profiles hot, switch to anchored
        // Aho-Corasick in LeftmostLongest mode (not a hand regex, which is
        // leftmost-*first* and needs escaping).
        for op in &cfg.op_tokens {
            if rest.starts_with(op.as_str()) {
                if op.len() > best_len {
                    best_len = op.len();
                    best_kind = TokKind::Token;
                }
                break; // op_tokens is sorted longest-first
            }
        }
        if best_len > 0 {
            let text = pattern[i..i + best_len].to_string();
            // Only operator tokens flip the mode (e.g. `<`/`>` in HTML); a string
            // or free-text token carrying `<`/`>` must not.
            if best_kind == TokKind::Token {
                mode = cfg.mode_after(mode, &text);
            }
            out.push(match best_kind {
                TokKind::Str => PatternItem::Str(text),
                TokKind::Token => PatternItem::Token(text),
            });
            i += best_len;
            continue;
        }

        // single-char fallback (a char in neither a literal class nor the op table)
        let text = pattern[i..i + clen].to_string();
        mode = cfg.mode_after(mode, &text);
        out.push(PatternItem::Token(text));
        i += clen;
    }

    resolve_brackets(&mut out)?;
    Ok(out)
}

/// Pair up the bracket markers — containment `\{{`/`\}}` and whole-node `\{`/`\}` —
/// back-patching each open's `close` with its matching close index. A **typed** stack
/// enforces proper nesting per kind: a `\}` must close a `\{`, a `\}}` a `\{{` (so
/// `\{{ … \} … \}}` is the malformed cross-nesting it looks like). Any unmatched or
/// crossed marker is a `client` error (the pattern is malformed).
fn resolve_brackets(items: &mut [PatternItem]) -> Result<()> {
    // (open index, is_containment)
    let mut stack: Vec<(usize, bool)> = Vec::new();
    for idx in 0..items.len() {
        match &items[idx] {
            PatternItem::ContainsOpen { .. } => stack.push((idx, true)),
            PatternItem::WholeOpen { .. } => stack.push((idx, false)),
            PatternItem::ContainsClose => {
                let (open, is_cont) = stack
                    .pop()
                    .ok_or_else(|| Error::client("unmatched `\\}}` in pattern"))?;
                if !is_cont {
                    return Err(Error::client("`\\}}` closing a `\\{` in pattern"));
                }
                if let PatternItem::ContainsOpen { close } = &mut items[open] {
                    *close = idx;
                }
            }
            PatternItem::WholeClose => {
                let (open, is_cont) = stack
                    .pop()
                    .ok_or_else(|| Error::client("unmatched `\\}` in pattern"))?;
                if is_cont {
                    return Err(Error::client("`\\}` closing a `\\{{` in pattern"));
                }
                if let PatternItem::WholeOpen { close } = &mut items[open] {
                    *close = idx;
                }
            }
            _ => {}
        }
    }
    if !stack.is_empty() {
        return Err(Error::client("unmatched `\\{` or `\\{{` in pattern"));
    }
    Ok(())
}

/// Parse a metavar given `s` = the index just past the sigil. `Ok(Some(..))` is
/// the parsed item + index past it; `Ok(None)` means the sigil isn't a valid
/// metavar (the caller treats it as a literal sigil); `Err` is a malformed
/// matcher (e.g. a bad regex). The metavar syntax is ASCII, so byte reads from
/// `s` are safe even if a non-ASCII char follows the sigil (ASCII checks just
/// fail → `Ok(None)`).
fn lex_metavar(
    pattern: &str,
    bytes: &[u8],
    s: usize,
    meta_char: char,
) -> Result<Option<(PatternItem, usize)>> {
    Ok(match bytes.get(s).copied() {
        // qualified form: \( ... \)
        Some(b'(') => return lex_qualified(pattern, bytes, s + 1, meta_char),
        // anonymous short forms: \*  \+  \?
        Some(b'*') => Some((meta(None, Cardinality::Many, None), s + 1)),
        Some(b'+') => Some((meta(None, Cardinality::OneOrMore, None), s + 1)),
        Some(b'?') => Some((meta(None, Cardinality::Optional, None), s + 1)),
        // anonymous regex short form: \/re/  (≡ \(/re/\))
        Some(b'/') => {
            let (re, next) = lex_regex(pattern, bytes, s)?;
            Some((meta(None, Cardinality::One, Some(re)), next))
        }
        // sugar: \NAME / \_  (single). Names may start with any alnum/`_`
        // (lowercase + digit allowed; the `\` sigil makes collisions rare).
        Some(c) if c.is_ascii_alphanumeric() || c == b'_' => {
            let (name, end) = read_name(pattern, bytes, s);
            Some((meta(name_binding(name), Cardinality::One, None), end))
        }
        _ => None,
    })
}

/// Read a cardinality quantifier (`*`/`+`/`?`) at `k`, advancing past it.
/// `None` (and `k` unchanged) if there's no quantifier there.
fn read_card(bytes: &[u8], k: &mut usize) -> Option<Cardinality> {
    let card = match bytes.get(*k).copied() {
        Some(b'*') => Cardinality::Many,
        Some(b'+') => Cardinality::OneOrMore,
        Some(b'?') => Cardinality::Optional,
        _ => return None,
    };
    *k += 1;
    Some(card)
}

/// Parse the inside of a metavar `\( ... \)` given `j` pointing just after `\(`.
/// Forms: `NAME [*+?] \)` (shorthand: quantifier on the implicit `.` body), named
/// regex `NAME : /re/ [*+?] \)`, anonymous regex `/re/ [*+?] \)` (no colon — `:`
/// separates a NAME from its body). The quantifier sits **after the body term**
/// (`\(NAME:/re/*\)`, the locked grammar); the after-NAME position is the shorthand
/// when there's no explicit body, and is also accepted with one for back-compat.
/// The close is the **sigil + `)`** (`\)`); a bad regex is an `Err`, a
/// missing/garbled close is lenient (`Ok(None)` → treat the sigil as literal).
fn lex_qualified(
    pattern: &str,
    bytes: &[u8],
    j: usize,
    meta_char: char,
) -> Result<Option<(PatternItem, usize)>> {
    let (name, mut k) = read_name(pattern, bytes, j);
    // quantifier after the NAME — the shorthand position (`\(NAME*\)`).
    let card_after_name = read_card(bytes, &mut k);
    k = skip_spaces(bytes, k);
    // regex matcher: named `NAME:/re/` (a `:` only ever follows a NAME) or
    // anonymous `/re/` (regex directly, no colon).
    let regex = match bytes.get(k) {
        Some(&b':') if !name.is_empty() => {
            let (re, nk) = lex_regex(pattern, bytes, skip_spaces(bytes, k + 1))?;
            k = nk;
            Some(re)
        }
        Some(&b'/') if name.is_empty() => {
            let (re, nk) = lex_regex(pattern, bytes, k)?;
            k = nk;
            Some(re)
        }
        _ => None,
    };
    // quantifier after the body **term** — the locked position (`\(NAME:/re/*\)`).
    // Wins over the after-NAME one when both are present.
    let card_after_term = if regex.is_some() {
        read_card(bytes, &mut k)
    } else {
        None
    };
    let card = card_after_term
        .or(card_after_name)
        .unwrap_or(Cardinality::One);
    k = skip_spaces(bytes, k);
    // close: the sigil followed by `)` (`\)`). Sigil-agnostic + UTF-8-safe.
    if !pattern[k..].starts_with(meta_char) || bytes.get(k + meta_char.len_utf8()) != Some(&b')') {
        return Ok(None); // unterminated / malformed `\(` — treat the sigil as literal
    }
    let end = k + meta_char.len_utf8() + 1;
    Ok(Some((meta(name_binding(name), card, regex), end)))
}

/// Parse a **delimited** `/regex/` starting at `k` (which must point at the
/// opening `/`). Returns `(compiled_regex, index_past_the_closing_/)`. The regex
/// closes at the first **unescaped** `/`; a literal `/` inside is escaped `\/`
/// (the `regex` crate accepts `\<punct>` as the literal, so the slice is passed
/// through verbatim, and *balanced* `()` need no escaping). A missing `/`, an
/// unterminated regex, or one that fails to compile is a `client` error.
/// Scanning for these ASCII bytes is UTF-8-safe (they never occur mid char).
fn lex_regex(pattern: &str, bytes: &[u8], k: usize) -> Result<(Regex, usize)> {
    if bytes.get(k) != Some(&b'/') {
        return Err(Error::client(
            "metavar matcher must be a regex: expected `/`",
        ));
    }
    let start = k + 1;
    let mut p = start;
    let close = loop {
        match bytes.get(p) {
            None => return Err(Error::client("unterminated regex in metavar matcher")),
            Some(b'\\') => p += 2, // skip the escaped char (\/ \( \) …)
            Some(b'/') => break p, // delimiter close
            Some(_) => p += 1,
        }
    };
    // `start`/`close` land on ASCII (`/`) → char boundaries; safe slice.
    let raw = &pattern[start..close];
    // Anchor the matcher to the **whole** node text: `\(/set_/\)` means "text is
    // exactly `set_`", `\(/set_.*/\)` means "starts with `set_`". Users add `.*`
    // explicitly for prefix/suffix/substring — far less surprising than unanchored
    // `is_match`, where a bare `set_.*` would match inside `unset_…`. Wrapping in a
    // non-capturing group keeps the user's alternations/anchors valid (a redundant
    // `^…$` they wrote still compiles).
    let regex = Regex::new(&format!("^(?:{raw})$"))
        .map_err(|e| Error::client(format!("invalid regex `/{raw}/`: {e}")))?;
    Ok((regex, close + 1))
}

fn skip_spaces(bytes: &[u8], mut k: usize) -> usize {
    while bytes.get(k) == Some(&b' ') {
        k += 1;
    }
    k
}

/// Read a metavar name `[A-Za-z0-9_]*` starting at `j`. Returns the name slice
/// and the index past it.
fn read_name<'a>(pattern: &'a str, bytes: &[u8], j: usize) -> (&'a str, usize) {
    let start = j;
    let mut k = j;
    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
        k += 1;
    }
    (&pattern[start..k], k)
}

/// A name of `""` or `"_"` is anonymous.
fn name_binding(name: &str) -> Option<String> {
    if name.is_empty() || name == "_" {
        None
    } else {
        Some(name.to_string())
    }
}

fn meta(name: Option<String>, card: Cardinality, regex: Option<Regex>) -> PatternItem {
    PatternItem::Meta { name, card, regex }
}
