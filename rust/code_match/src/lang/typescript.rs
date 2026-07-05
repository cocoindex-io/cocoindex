//! TypeScript / TSX: C-style escaping + backtick template strings.
use crate::config::*;
use std::sync::LazyLock;

pub fn typescript() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("typescript", c_like_tokenizers()));
    CFG.clone()
}

pub fn tsx() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("tsx", c_like_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::typescript;
    use crate::lang::testutil::*;

    #[test]
    fn call_multi_args() {
        let src = r#"console.log("a", b);"#;
        let ms = matches(typescript(), r"console.log(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some(r#""a", b"#));
    }

    #[test]
    fn nested_generics() {
        let src = "let x: Array<Array<number>> = mk();";
        let ms = matches(typescript(), r"Array<Array<\T>>", src);
        assert_eq!(cap(&ms, "T").as_deref(), Some("number"));
    }

    #[test]
    fn typed_parameter() {
        // `\VAR: string` matches a type-annotated parameter; VAR snaps to the
        // name leaf, `: string` is matched literally. The candidate is the whole
        // `required_parameter` node (which the pattern covers exactly).
        let src = "function foo(x: string) { return x; }";
        let ms = matches(typescript(), r"\VAR: string", src);
        assert_eq!(cap(&ms, "VAR").as_deref(), Some("x"));
        assert!(has_kind(&ms, "required_parameter"));
    }

    /// Conformance over the generic literal profile (numbers, strings, template).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("0xff", "let x = 0xff;"),
            ("0b1010", "let x = 0b1010;"),
            ("1_000", "let x = 1_000;"),
            ("1.5e-10", "let x = 1.5e-10;"),
            (".5", "let x = .5;"),
            (r#""hi""#, r#"let s = "hi";"#),
            ("'hi'", "let s = 'hi';"),
            ("`hi`", "let s = `hi`;"),
        ] {
            assert!(
                !matches(typescript(), lit, ctx).is_empty(),
                "TS literal `{lit}`"
            );
        }
    }
}
