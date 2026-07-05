//! R: `%...%` special/infix operators (`%>%`, `%in%`, `%*%`) are single tokens,
//! and numeric literals are matched as whole nodes — tree-sitter-r models the
//! `L` suffix of `1L` as a child token and hides the digits, so the leaf frontier
//! of `1L` is just `L`; matching the `integer` node by text (Str) handles it.
use crate::config::*;
use std::sync::LazyLock;

pub fn r() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            // Str so suffixed integers (`1L`, `2i`) match the numeric node whole.
            regex_rule(
                r"^(?:[0-9]|\.[0-9])(?:[eEpP][-+]|[0-9A-Za-z_.])*",
                TokKind::Str,
            ),
            dq_string(),
            sq_string(),
            backtick_string(), // non-syntactic names `` `x y` ``
            // `%...%` infix/special operators, one token.
            regex_rule(r"^%[^%]*%", TokKind::Token),
        ];
        LangConfig::from_registry("r", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::r;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(r(), r"foo(\(A*\))", "foo(a, b)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    #[test]
    fn pipe_operator() {
        let ms = matches(r(), r"x %>% \F(y)", "z <- x %>% filter(y)");
        assert_eq!(cap(&ms, "F").as_deref(), Some("filter"));
    }

    /// Conformance over R literal/operator forms.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "x <- \"hi\""),
            ("'hi'", "x <- 'hi'"),
            ("1L", "x <- 1L"),
            ("42", "x <- 42"),
            ("1e5", "x <- 1e5"),
            ("0xFF", "x <- 0xFF"),
            ("3.14", "x <- 3.14"),
            ("x %>% y", "z <- x %>% y"),
            ("x %in% y", "z <- x %in% y"),
        ] {
            assert!(!matches(r(), lit, ctx).is_empty(), "R `{lit}`");
        }
    }
}
