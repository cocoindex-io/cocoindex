//! C#: verbatim strings `@"a""b"` use doubled-quote escaping (and `\` is
//! literal), and raw strings `"""..."""` (C# 11) are one node. Normal/interpolated
//! `"..."` use the generic backslash form.
use crate::config::*;
use std::sync::LazyLock;

pub fn csharp() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(triple_dq_string()); // raw string """..."""
        toks.push(regex_rule(r#"(?s)^@"(?:""|[^"])*""#, TokKind::Str)); // verbatim @"a""b"
        LangConfig::from_registry("csharp", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::csharp;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "class C { void M() { foo(a, b); } }";
        let ms = matches(csharp(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    /// Conformance over C# literal forms (verbatim/raw strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "class C { string s = \"hi\"; }"),
            ("@\"a\"\"b\"", "class C { string s = @\"a\"\"b\"; }"),
            ("\"\"\"hi\"\"\"", "class C { string s = \"\"\"hi\"\"\"; }"),
            ("'c'", "class C { char c = 'c'; }"),
            ("0xFF", "class C { int x = 0xFF; }"),
            ("1_000L", "class C { long x = 1_000L; }"),
        ] {
            assert!(!matches(csharp(), lit, ctx).is_empty(), "C# `{lit}`");
        }
    }
}
