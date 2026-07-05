//! Julia: triple-quoted strings and non-standard (prefixed) string literals
//! `r"..."`, `b"..."`, `raw"""..."""i` — `<prefix>"..."<flags>`, one node each.
//! `$`-interpolation is matched opaquely via the whole node's text.
use crate::config::*;
use std::sync::LazyLock;

pub fn julia() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),
            sq_string(),       // char literal 'c'
            backtick_string(), // command `...`
            triple_dq_string(),
            // prefixed triple `r"""..."""flags` and single `r"..."flags`. The
            // leading identifier + immediately-following quote is a macro string;
            // longest-match beats the bare `identifier` / `"..."` tokenizers.
            regex_rule(
                r#"(?s)^[A-Za-z][A-Za-z0-9_]*""".*?"""[A-Za-z0-9_]*"#,
                TokKind::Str,
            ),
            regex_rule(
                r#"^[A-Za-z][A-Za-z0-9_]*"(?:\\.|[^"\\])*"[A-Za-z0-9_]*"#,
                TokKind::Str,
            ),
        ];
        LangConfig::from_registry("julia", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::julia;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(julia(), r"foo(\(A*\))", "foo(a, b)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    /// Conformance over Julia literal forms (triple/prefixed strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "x = \"hi\""),
            ("\"\"\"hi\"\"\"", "x = \"\"\"hi\"\"\""),
            ("'c'", "x = 'c'"),
            ("r\"reg\"", "x = r\"reg\""),
            ("r\"reg\"i", "x = r\"reg\"i"),
            ("b\"data\"", "x = b\"data\""),
            ("0xFF", "x = 0xFF"),
            ("1_000", "x = 1_000"),
            ("1.5e10", "x = 1.5e10"),
            ("1.5f0", "x = 1.5f0"),
        ] {
            assert!(!matches(julia(), lit, ctx).is_empty(), "Julia `{lit}`");
        }
    }
}
