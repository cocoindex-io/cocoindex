//! YAML: single-quoted scalars escape a quote by **doubling** it (`'it''s'`),
//! not with a backslash; double-quoted scalars use backslash. Note: a *plain*
//! (unquoted) multi-word scalar like `hello world` is a single node with no
//! delimiters, so it can't be written as an exact literal pattern (the lexer
//! would see two identifiers) — match it with a metavar (`key: \V`) instead.
use crate::config::*;
use std::sync::LazyLock;

pub fn yaml() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),         // "..." backslash escaping
            sq_string_doubled(), // '...' with '' escaping
        ];
        LangConfig::from_registry("yaml", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::yaml;
    use crate::lang::testutil::*;

    #[test]
    fn mapping_value() {
        let ms = matches(yaml(), r"a: \V", "a: 1");
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }

    #[test]
    fn multiword_plain_scalar_via_metavar() {
        // A plain multi-word scalar is one node — capture it with a metavar.
        let ms = matches(yaml(), r"a: \V", "a: hello world");
        assert_eq!(cap(&ms, "V").as_deref(), Some("hello world"));
    }

    /// Conformance over YAML scalar forms (single-word/quoted).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("1", "a: 1"),
            ("hello", "a: hello"),
            ("\"hi\"", "a: \"hi\""),
            ("'hi'", "a: 'hi'"),
            ("'it''s'", "a: 'it''s'"),
        ] {
            assert!(!matches(yaml(), lit, ctx).is_empty(), "YAML `{lit}`");
        }
    }
}
