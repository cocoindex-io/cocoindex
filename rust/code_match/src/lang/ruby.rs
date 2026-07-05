//! Ruby: C-style escaping (the common case; `%w[...]`, heredocs use a metavar).
use crate::config::*;
use std::sync::LazyLock;

pub fn ruby() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("ruby", c_like_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::ruby;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "foo(a, b)";
        let ms = matches(ruby(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over Ruby literal forms — the generic backslash profile fits
    /// the common cases (`%w[...]`, heredocs are best matched with a metavar).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "x = \"hi\""),
            ("'hi'", "x = 'hi'"),
            ("42", "x = 42"),
            ("0xFF", "x = 0xFF"),
            ("1_000", "x = 1_000"),
            ("3.14", "x = 3.14"),
        ] {
            assert!(!matches(ruby(), lit, ctx).is_empty(), "Ruby `{lit}`");
        }
    }
}
