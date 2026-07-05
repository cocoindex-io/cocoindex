//! JSON: numbers may be negative (`-1`, `-1.5e3`) — one node including the sign,
//! which the generic (unsigned) number tokenizer would split off as an operator.
use crate::config::*;
use std::sync::LazyLock;

pub fn json() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(), // true / false / null
            regex_rule(
                r"^-?(?:[0-9]|\.[0-9])(?:[eEpP][-+]|[0-9A-Za-z_.])*",
                TokKind::Token,
            ),
            dq_string(),
        ];
        LangConfig::from_registry("json", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::json;
    use crate::lang::testutil::*;

    #[test]
    fn pair_value() {
        // Match over the value with a metavar.
        let ms = matches(json(), r#""a": \V"#, r#"{"a": 1}"#);
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }

    /// Conformance over JSON literal forms (signed numbers, keywords).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "{\"k\": \"hi\"}"),
            ("42", "{\"k\": 42}"),
            ("-1", "{\"k\": -1}"),
            ("1.5e-10", "{\"k\": 1.5e-10}"),
            ("true", "{\"k\": true}"),
            ("null", "{\"k\": null}"),
        ] {
            assert!(!matches(json(), lit, ctx).is_empty(), "JSON `{lit}`");
        }
    }
}
