//! Scala: triple-quoted strings `"""..."""`. Interpolators (`s"..."`) and char
//! literals (`'c'`) use the generic backslash forms; bare `'sym` (no closing
//! quote) falls through to the operator table like a Rust lifetime.
use crate::config::*;
use std::sync::LazyLock;

pub fn scala() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(triple_dq_string());
        LangConfig::from_registry("scala", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::scala;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "object O { def m = foo(a, b) }";
        let ms = matches(scala(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over Scala literal forms (triple-quoted strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "object O { val x = \"hi\" }"),
            ("\"\"\"hi\"\"\"", "object O { val x = \"\"\"hi\"\"\" }"),
            ("'c'", "object O { val x = 'c' }"),
            ("42", "object O { val x = 42 }"),
            ("1.5e10", "object O { val x = 1.5e10 }"),
            ("0xFF", "object O { val x = 0xFF }"),
        ] {
            assert!(!matches(scala(), lit, ctx).is_empty(), "Scala `{lit}`");
        }
    }
}
