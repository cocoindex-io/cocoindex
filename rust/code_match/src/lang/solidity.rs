//! Solidity: `hex"00ff"` and `unicode"..."` string literals (and `'`-quoted
//! variants) are single nodes; the `hex`/`unicode` prefix would otherwise split
//! off as an identifier. Normal strings use generic backslash escaping.
use crate::config::*;
use std::sync::LazyLock;

pub fn solidity() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),
            sq_string(),
            // prefixed string literals (longest-match beats `identifier` + string)
            regex_rule(r#"^hex"[0-9a-fA-F_]*""#, TokKind::Str),
            regex_rule(r"^hex'[0-9a-fA-F_]*'", TokKind::Str),
            regex_rule(r#"^unicode"(?:\\.|[^"\\])*""#, TokKind::Str),
            regex_rule(r"^unicode'(?:\\.|[^'\\])*'", TokKind::Str),
        ];
        LangConfig::from_registry("solidity", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::solidity;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "contract C { function f() public { foo(a, b); } }";
        let ms = matches(solidity(), r"foo(\(A*\))", src);
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    /// Conformance over Solidity literal forms (hex/unicode strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "contract C { string x = \"hi\"; }"),
            ("'hi'", "contract C { string x = 'hi'; }"),
            ("hex\"00ff\"", "contract C { bytes x = hex\"00ff\"; }"),
            ("unicode\"hi\"", "contract C { string x = unicode\"hi\"; }"),
            ("0x1234", "contract C { uint x = 0x1234; }"),
            ("1000", "contract C { uint x = 1000; }"),
        ] {
            assert!(
                !matches(solidity(), lit, ctx).is_empty(),
                "Solidity `{lit}`"
            );
        }
    }
}
