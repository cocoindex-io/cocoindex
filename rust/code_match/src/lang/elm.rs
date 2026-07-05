//! Elm: triple-quoted strings `"""..."""` (backslash escaping like the generic
//! `"..."`/`'c'`).
use crate::config::*;
use std::sync::LazyLock;

pub fn elm() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(triple_dq_string());
        LangConfig::from_registry("elm", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::elm;
    use crate::lang::testutil::*;

    #[test]
    fn declaration() {
        let ms = matches(elm(), r"x = \V", "x = 1");
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }

    /// Conformance over Elm literal forms.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "x = \"hi\""),
            ("\"\"\"hi\"\"\"", "x = \"\"\"hi\"\"\""),
            ("'c'", "x = 'c'"),
            ("0xFF", "x = 0xFF"),
            ("1.5e10", "x = 1.5e10"),
            ("42", "x = 42"),
        ] {
            assert!(!matches(elm(), lit, ctx).is_empty(), "Elm `{lit}`");
        }
    }
}
