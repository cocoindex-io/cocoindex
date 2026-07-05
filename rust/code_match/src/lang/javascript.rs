//! JavaScript: C-style escaping + backtick template strings.
use crate::config::*;
use std::sync::LazyLock;

pub fn javascript() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("javascript", c_like_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::javascript;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "foo(a, b);";
        let ms = matches(javascript(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over JS literal forms — the generic profile (backslash
    /// escaping, backtick template strings) is correct here.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "var x = \"hi\""),
            ("'hi'", "var x = 'hi'"),
            ("`tmpl`", "var x = `tmpl`"),
            ("`a${b}c`", "var x = `a${b}c`"),
            ("0xFF", "var x = 0xFF"),
            ("1_000", "var x = 1_000"),
            ("1.5e-10", "var x = 1.5e-10"),
        ] {
            assert!(!matches(javascript(), lit, ctx).is_empty(), "JS `{lit}`");
        }
    }
}
