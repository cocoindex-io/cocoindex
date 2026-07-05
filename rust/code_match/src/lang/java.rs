//! Java: text blocks `"""..."""` (Java 15+). Normal strings/chars use the
//! generic backslash forms.
use crate::config::*;
use std::sync::LazyLock;

pub fn java() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(triple_dq_string());
        LangConfig::from_registry("java", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::java;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "class C { void m() { foo(a, b); } }";
        let ms = matches(java(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over Java literal forms (text blocks, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "class C { String s = \"hi\"; }"),
            (
                "\"\"\"\nhi\"\"\"",
                "class C { String s = \"\"\"\nhi\"\"\"; }",
            ),
            ("'c'", "class C { char c = 'c'; }"),
            ("0xFF", "class C { int x = 0xFF; }"),
            ("1_000_000L", "class C { long x = 1_000_000L; }"),
            ("1.5e-10", "class C { double x = 1.5e-10; }"),
        ] {
            assert!(!matches(java(), lit, ctx).is_empty(), "Java `{lit}`");
        }
    }
}
