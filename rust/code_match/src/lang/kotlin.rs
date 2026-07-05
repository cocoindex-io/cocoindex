//! Kotlin: triple-quoted (raw) strings `"""..."""`.
use crate::config::*;
use std::sync::LazyLock;

pub fn kotlin() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(triple_dq_string());
        LangConfig::from_registry("kotlin", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::kotlin;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(kotlin(), r"foo(\(A*\))", "fun m() { foo(a, b) }");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    #[test]
    fn triple_string_literal() {
        let src = "val x = \"\"\"a\nb\"\"\"";
        assert!(!matches(kotlin(), src, src).is_empty());
    }

    /// Conformance over Kotlin literal forms (raw/interpolated strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"\"\"raw\"\"\"", "val x = \"\"\"raw\"\"\""),
            ("\"hi\"", "val x = \"hi\""),
            ("'c'", "val x = 'c'"),
            ("0xFF", "val x = 0xFF"),
            ("1_000", "val x = 1_000"),
            ("1.5e10", "val x = 1.5e10"),
            ("1L", "val x = 1L"),
            ("1.0f", "val x = 1.0f"),
            ("0b1010", "val x = 0b1010"),
            ("\"a${b}c\"", "val x = \"a${b}c\""),
        ] {
            assert!(!matches(kotlin(), lit, ctx).is_empty(), "Kotlin `{lit}`");
        }
    }
}
