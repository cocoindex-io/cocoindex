//! C: `'` digit separators, no backtick strings.

use crate::config::*;
use std::sync::LazyLock;

/// Identifier, number (with `'` separators), `"…"` and `'…'`.
pub(crate) fn c_tokenizers() -> Vec<TokenRule> {
    vec![identifier(), number("'"), dq_string(), sq_string()]
}

pub fn c() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("c", c_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::c;
    use crate::lang::testutil::*;

    #[test]
    fn call_multi_args() {
        let src = "void g(){ foo(a, b); }";
        let ms = matches(c(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    #[test]
    fn balanced_nested() {
        let src = "void g(){ foo(bar(x), baz); }";
        let ms = matches(c(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("bar(x), baz"));
    }

    /// Conformance: a literal in a pattern must lex as one token and match the
    /// source construct (the tokenizer aligns with tree-sitter).
    #[test]
    fn number_forms() {
        for (lit, ctx) in [
            ("0xFF", "int x = 0xFF;"),
            ("0b1010", "int x = 0b1010;"),
            ("017", "int x = 017;"),
            ("42u", "unsigned x = 42u;"),
            ("100UL", "unsigned long x = 100UL;"),
            ("3.14", "double x = 3.14;"),
            (".5", "double x = .5;"),
            ("1.", "double x = 1.;"),
            ("1.5e-10", "double x = 1.5e-10;"),
            ("0x1p4", "double x = 0x1p4;"),
            ("1'000", "int x = 1'000;"),
        ] {
            assert!(!matches(c(), lit, ctx).is_empty(), "C number `{lit}`");
        }
    }

    #[test]
    fn string_and_char_forms() {
        for (lit, ctx) in [
            (r#""hi""#, r#"const char* s = "hi";"#),
            (r#""a\tb""#, r#"const char* s = "a\tb";"#),
            ("'a'", "char c = 'a';"),
            (r"'\n'", r"char c = '\n';"),
        ] {
            assert!(!matches(c(), lit, ctx).is_empty(), "C literal `{lit}`");
        }
    }

    #[test]
    fn pointer_member_arrow_split() {
        // `->` splits into `-` `>`; member access must still match.
        let src = "void g(){ p->field = 1; }";
        let ms = matches(c(), r"\P->\F", src);
        assert_eq!(cap(&ms, "P").as_deref(), Some("p"));
        assert_eq!(cap(&ms, "F").as_deref(), Some("field"));
    }
}
