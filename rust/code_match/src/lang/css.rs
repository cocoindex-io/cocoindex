//! CSS: identifiers are kebab-case (`font-size`, `--var`, `-webkit-x`), and the
//! grammar fuses several lexical forms into one node while hiding the lead:
//! dimensions `10px`/`1.5em`/`100%` (leaf is just the unit) and hex colors
//! `#fff` (leaf is just `#`) must be matched as whole nodes (Str). At-keywords
//! `@media` and `!important` are single tokens (Token).
use crate::config::*;
use std::sync::LazyLock;

pub fn css() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            // kebab-case names incl. custom props `--x` and vendor `-webkit-x`.
            regex_rule(r"^-{0,2}[A-Za-z_][A-Za-z0-9_-]*", TokKind::Token),
            // dimensions / percentages / numbers — whole node by text.
            regex_rule(r"^(?:[0-9]|\.[0-9])[0-9A-Za-z_.%]*", TokKind::Str),
            dq_string(),
            sq_string(),
            // hex colors `#fff` and id selectors `#id` — whole node by text.
            regex_rule(r"^#[0-9A-Za-z_-]+", TokKind::Str),
            // at-keywords `@media`, `@import` — one token.
            regex_rule(r"^@[0-9A-Za-z_-]+", TokKind::Token),
            // `!important` / `!default` — one token.
            regex_rule(r"^![A-Za-z]+", TokKind::Token),
        ];
        LangConfig::from_registry("css", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::css;
    use crate::lang::testutil::*;

    #[test]
    fn declaration_value() {
        let ms = matches(css(), r"color: \V", "a { color: red; }");
        assert_eq!(cap(&ms, "V").as_deref(), Some("red"));
    }

    /// Conformance over CSS forms: kebab names, dimensions, colors, at-rules.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("font-size", "a { font-size: 10px; }"),
            ("--main-color", "a { --main-color: blue; }"),
            (".foo-bar", ".foo-bar { color: red; }"),
            ("#fff", "a { color: #fff; }"),
            ("#id", "#id { color: red; }"),
            ("10px", "a { width: 10px; }"),
            ("1.5em", "a { width: 1.5em; }"),
            ("100%", "a { width: 100%; }"),
            ("@media screen", "@media screen {}"),
            ("!important", "a { color: red !important; }"),
        ] {
            assert!(!matches(css(), lit, ctx).is_empty(), "CSS `{lit}`");
        }
    }
}
