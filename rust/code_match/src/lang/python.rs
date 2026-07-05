//! Python: triple-quoted strings.

use crate::config::*;
use std::sync::LazyLock;

pub fn python() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        // triple-quoted (with optional r/b/f/u prefixes), then prefixed single
        // line strings. Interpolation inside f-strings is treated opaquely.
        toks.push(regex_rule(r#"(?s)^[rbfuRBFU]{0,2}""".*?""""#, TokKind::Str));
        toks.push(regex_rule(r"(?s)^[rbfuRBFU]{0,2}'''.*?'''", TokKind::Str));
        toks.push(regex_rule(
            r#"^[rbfuRBFU]{1,2}"(?:\\.|[^"\\])*""#,
            TokKind::Str,
        ));
        toks.push(regex_rule(
            r"^[rbfuRBFU]{1,2}'(?:\\.|[^'\\])*'",
            TokKind::Str,
        ));
        LangConfig::from_registry("python", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::python;
    use crate::lang::testutil::*;

    #[test]
    fn call_multi_args() {
        let src = "foo(a, b, c)";
        let ms = matches(python(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b, c"));
    }

    #[test]
    fn string_atomic() {
        let src = r#"print("a)b")"#;
        let ms = matches(python(), r"print(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some(r#""a)b""#));
    }

    #[test]
    fn keyword_args() {
        let src = "f(x=1, y=2)";
        let ms = matches(python(), r"f(\(KW*\))", src);
        assert_eq!(cap(&ms, "KW").as_deref(), Some("x=1, y=2"));
    }

    #[test]
    fn triple_string_literal() {
        let src = "x = \"\"\"a\nb\"\"\"\n";
        let ms = matches(python(), "x = \"\"\"a\nb\"\"\"", src);
        assert!(!ms.is_empty(), "python triple-quoted string should match");
    }

    /// Conformance over Python string/number forms (prefixed strings included).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            (r#""hi""#, r#"x = "hi""#),
            (r#"r"a\b""#, r#"x = r"a\b""#),
            (r#"f"hi""#, r#"x = f"hi""#),
            (r#"b"hi""#, r#"x = b"hi""#),
            ("0xFF", "x = 0xFF"),
            ("1_000", "x = 1_000"),
            ("1.5e-10", "x = 1.5e-10"),
            (".5", "x = .5"),
        ] {
            assert!(
                !matches(python(), lit, ctx).is_empty(),
                "Python literal `{lit}`"
            );
        }
    }
}
