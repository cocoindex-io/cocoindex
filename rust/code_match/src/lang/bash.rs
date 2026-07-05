//! Bash. Single-quoted strings have **no** escaping — a `'` always closes and
//! `$`/`\` are literal (`'$x'` is the literal text `$x`). Double-quoted strings
//! use backslash and `$`-expansion (matched opaquely via the node's text).
use crate::config::*;
use std::sync::LazyLock;

pub fn bash() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),         // "..." backslash + $-expansion
            sq_string_literal(), // '...' literal, no escaping
        ];
        LangConfig::from_registry("bash", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::bash;
    use crate::lang::testutil::*;

    #[test]
    fn command() {
        // `\MSG` (not `$MSG`) — the `\` sigil keeps this shell-safe.
        let src = "echo hello";
        let ms = matches(bash(), r"echo \MSG", src);
        assert_eq!(cap(&ms, "MSG").as_deref(), Some("hello"));
    }

    /// Conformance over Bash string forms (literal single quotes).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("'$HOME'", "x='$HOME'"), // literal, $ not expanded
            ("'a b'", "x='a b'"),
            ("\"hi\"", "x=\"hi\""),
        ] {
            assert!(!matches(bash(), lit, ctx).is_empty(), "Bash `{lit}`");
        }
    }
}
