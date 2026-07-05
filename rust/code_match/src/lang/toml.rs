//! TOML: basic strings `"..."` use backslash escaping, but **literal** strings
//! `'...'` have none (a `'` always closes); multiline `"""..."""` / `'''...'''`;
//! integers may be signed; and date/time values (`1979-05-27`, `07:32:00`,
//! `...T...Z`) are single nodes that the generic profile would split on `-`/`:`.
use crate::config::*;
use std::sync::LazyLock;

pub fn toml() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(), // bare keys, true/false/inf/nan
            // date-times before date/time before number (longest-match anyway).
            regex_rule(
                r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})?",
                TokKind::Str,
            ),
            regex_rule(r"^\d{4}-\d{2}-\d{2}", TokKind::Str),
            regex_rule(r"^\d{2}:\d{2}:\d{2}(?:\.\d+)?", TokKind::Str),
            regex_rule(
                r"^[+-]?(?:[0-9]|\.[0-9])(?:[eEpP][-+]|[0-9A-Za-z_.])*",
                TokKind::Token,
            ),
            dq_string(),         // basic "..."
            sq_string_literal(), // literal '...'
            triple_dq_string(),  // """..."""
            triple_sq_string(),  // '''...'''
        ];
        LangConfig::from_registry("toml", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::toml;
    use crate::lang::testutil::*;

    #[test]
    fn pair_value() {
        let ms = matches(toml(), r"a = \V", "a = 1");
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }

    /// Conformance over TOML value forms (strings, multiline, dates, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "a = \"hi\""),
            ("'literal'", "a = 'literal'"),
            ("\"\"\"multi\"\"\"", "a = \"\"\"multi\"\"\""),
            ("'''multi'''", "a = '''multi'''"),
            ("1_000", "a = 1_000"),
            ("0xFF", "a = 0xFF"),
            ("-17", "a = -17"),
            ("3.14", "a = 3.14"),
            ("1979-05-27", "a = 1979-05-27"),
            ("07:32:00", "a = 07:32:00"),
            ("1979-05-27T07:32:00Z", "a = 1979-05-27T07:32:00Z"),
        ] {
            assert!(!matches(toml(), lit, ctx).is_empty(), "TOML `{lit}`");
        }
    }
}
