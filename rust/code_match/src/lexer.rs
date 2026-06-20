//! Pattern lexer: turns a pattern string into a flat `Vec<PatternItem>`.
//!
//! Metavariables are first-class tokens. Everything else is lexed with a small
//! hand engine + the per-language op-token table.
//!
//! Metavar syntax (sigil `S` is configurable, default `\`):
//!   `S NAME`        single, named (sugar for `S(NAME)`)
//!   `S(NAME)`       single, named
//!   `S(NAME*)`      many   (zero or more **same-level** sibling nodes)
//!   `S(NAME?)`      optional (zero or one node)
//!   `S_` / `S(_)`   anonymous single        `S*` / `S(*)`  anonymous many
//!   `S?` / `S(?)`   anonymous optional
//!   `S(NAME:/re/)`  single, regex-constrained — see below
//!   `S(:/re/)`      regex-constrained, **anonymous** (filter without capturing)
//!   `SS`            a doubled sigil is one **literal** sigil (e.g. `\\` → `\`)
//! `*` is **same-level** (one parent's direct siblings); a cross-level skip is
//! written as multiple `*`, one per grammar level.
//! Names are `[A-Za-z0-9_]+` (upper/lower/digit, sed-like `\1`); UPPERCASE is a
//! readability convention, not a rule. With the `\` sigil, lowercase `\foo` only
//! collides with bare-`\` escapes (Bash `\n`) / Haskell lambdas.
//!
//! Regex matcher `:/re/` constrains a **single-node** metavar: the captured
//! source text must `is_match` (unanchored) the regex (use `^…$` for whole-node).
//! The name is optional — `S(:/re/)` constrains a node without capturing it.
//! Applies to `One`/`Optional`; ignored on `Many` (sibling runs, out of scope).
//! The closing `/` is optional (`:/re`): a `/` at the matcher's top paren level
//! closes (delimited), else the matcher's `)` closes (shorthand). Balanced `()`
//! inside the regex (alternations) need no escaping; escape a literal `/` or an
//! unbalanced `)` as `\/` / `\)`. An unparseable (or unterminated) regex is a
//! `client` error from `lex` / `Pattern::compile`.

use crate::config::{LangConfig, TokKind};
use cocoindex_utils::error::{Error, Result};
use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    /// `\X` — exactly one node
    One,
    /// `\(X*)` — zero or more sibling nodes
    Many,
    /// `\(X?)` — zero or one node
    Optional,
}

// `Regex` is not `PartialEq`/`Eq`, so `PatternItem` isn't either (it's only ever
// pattern-matched, never compared for equality).
#[derive(Debug, Clone)]
pub enum PatternItem {
    /// Word (identifier / keyword / number) or operator/punctuation token.
    /// Matched against a single source *leaf* by text.
    Token(String),
    /// String literal (with quotes). Matched against a source string *node* by text.
    Str(String),
    /// Metavariable. `name` == None for anonymous (`\_`, `\*`, `\?`). `regex`, if
    /// present, constrains the captured text (unanchored `is_match`); honored for
    /// single-node cardinalities (`One`/`Optional`) only. `Many` (`*`) is always
    /// same-level; cross-level skips use multiple `*`.
    Meta {
        name: Option<String>,
        card: Cardinality,
        regex: Option<Regex>,
    },
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
            if let Some((item, next)) = lex_metavar(pattern, bytes, after)? {
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

    Ok(out)
}

/// Parse a metavar given `s` = the index just past the sigil. `Ok(Some(..))` is
/// the parsed item + index past it; `Ok(None)` means the sigil isn't a valid
/// metavar (the caller treats it as a literal sigil); `Err` is a malformed
/// matcher (e.g. a bad regex). The metavar syntax is ASCII, so byte reads from
/// `s` are safe even if a non-ASCII char follows the sigil (ASCII checks just
/// fail → `Ok(None)`).
fn lex_metavar(pattern: &str, bytes: &[u8], s: usize) -> Result<Option<(PatternItem, usize)>> {
    Ok(match bytes.get(s).copied() {
        // qualified form: S( ... )
        Some(b'(') => return lex_qualified(pattern, bytes, s + 1),
        // anonymous shorthands: S*  S?
        Some(b'*') => Some((meta(None, Cardinality::Many, None), s + 1)),
        Some(b'?') => Some((meta(None, Cardinality::Optional, None), s + 1)),
        // sugar: S NAME  /  S_  (single). Names may start with any alnum/`_`
        // (lowercase + digit allowed; the `\` sigil makes collisions rare).
        Some(c) if c.is_ascii_alphanumeric() || c == b'_' => {
            let (name, end) = read_name(pattern, bytes, s);
            Some((meta(name_binding(name), Cardinality::One, None), end))
        }
        _ => None,
    })
}

/// Parse the inside of `S( ... )` given `j` pointing just after `(`:
/// `NAME [*|?] [ : /regex/ ] )`. A `:` commits to a regex matcher (a bad regex
/// is an `Err`); a missing closing `)` is lenient (`Ok(None)` → literal sigil).
fn lex_qualified(pattern: &str, bytes: &[u8], j: usize) -> Result<Option<(PatternItem, usize)>> {
    let (name, mut k) = read_name(pattern, bytes, j);
    let card = match bytes.get(k).copied() {
        Some(b'*') => {
            k += 1;
            Cardinality::Many
        }
        Some(b'?') => {
            k += 1;
            Cardinality::Optional
        }
        _ => Cardinality::One,
    };
    // optional `: /regex/` matcher
    k = skip_spaces(bytes, k);
    let regex = if bytes.get(k) == Some(&b':') {
        let (re, nk) = lex_regex(pattern, bytes, skip_spaces(bytes, k + 1))?;
        k = nk;
        Some(re)
    } else {
        None
    };
    k = skip_spaces(bytes, k);
    if bytes.get(k) != Some(&b')') {
        return Ok(None); // unterminated / malformed `S(` — treat the sigil as literal
    }
    Ok(Some((meta(name_binding(name), card, regex), k + 1)))
}

/// Parse `/regex/` (or shorthand `/regex`) starting at `k` (which must point at
/// the opening `/`). Returns `(compiled_regex, index_past_the_regex)`. A missing
/// `/`, an unterminated regex, or one that fails to compile is a `client` error.
///
/// Delimiting uses **paren depth** (the matcher's `\(` is depth 1): a `/` at
/// depth 1 closes (delimited form); a `)` that drops depth to 0 closes (shorthand
/// form). So *balanced* `()` inside the regex — alternations like `(foo|bar)` —
/// are free, no escaping. Escape a literal `/` (or an unbalanced `)`) as `\/` /
/// `\)`; the `regex` crate accepts `\<punct>` as the literal, so the slice is
/// passed through verbatim. Scanning for these ASCII bytes is UTF-8-safe (they
/// never occur mid multi-byte char).
fn lex_regex(pattern: &str, bytes: &[u8], k: usize) -> Result<(Regex, usize)> {
    if bytes.get(k) != Some(&b'/') {
        return Err(Error::client(
            "metavar matcher must be a regex: expected `/` after `:`",
        ));
    }
    let start = k + 1;
    let mut p = start;
    let mut depth = 1usize; // the matcher's `\(` is open
    let (close, delimited) = loop {
        match bytes.get(p) {
            None => return Err(Error::client("unterminated regex in metavar matcher")),
            Some(b'\\') => p += 2, // skip the escaped char (\/ \) \( …)
            Some(b'(') => {
                depth += 1;
                p += 1;
            }
            Some(b')') => {
                depth -= 1;
                if depth == 0 {
                    break (p, false); // matcher close → shorthand
                }
                p += 1;
            }
            Some(b'/') if depth == 1 => break (p, true), // top-level `/` → delimited
            Some(_) => p += 1,
        }
    };
    // `start`/`close` land on ASCII (`/`, `)`) → char boundaries; safe slice.
    let raw = &pattern[start..close];
    let regex =
        Regex::new(raw).map_err(|e| Error::client(format!("invalid regex `/{raw}/`: {e}")))?;
    let next = if delimited { close + 1 } else { close };
    Ok((regex, next))
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
