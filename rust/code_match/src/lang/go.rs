//! Go: raw string literals use backticks with **no** escaping (`` `a\b` `` is
//! the literal text `a\b`) — the generic backtick form treats `\` as an escape
//! and would miss a `` `...\` ``. Interpreted strings `"..."` and runes `'r'`
//! use the generic backslash forms.
use crate::config::*;
use std::sync::LazyLock;

pub fn go() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),
            sq_string(),               // rune 'r'
            backtick_string_literal(), // `raw, no escapes`
        ];
        LangConfig::from_registry("go", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::go;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "package main\nfunc m() { foo(a, b) }";
        let ms = matches(go(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over Go literal forms (raw backtick strings, runes, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "package m\nvar x = \"hi\""),
            ("`raw`", "package m\nvar x = `raw`"),
            ("`a\\b`", "package m\nvar x = `a\\b`"), // backslash is literal
            ("'r'", "package m\nvar x = 'r'"),
            ("0xFF", "package m\nvar x = 0xFF"),
            ("1_000", "package m\nvar x = 1_000"),
        ] {
            assert!(!matches(go(), lit, ctx).is_empty(), "Go `{lit}`");
        }
    }
}
