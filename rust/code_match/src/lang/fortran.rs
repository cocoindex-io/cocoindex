//! Fortran: string literals use doubled-quote escaping (`'it''s'`, `"a""b"`) —
//! the generic `'...'`/`"..."` would stop at the first inner quote. Plus BOZ
//! literals `Z'FF'` / `B'1010'` / `O'17'`.
use crate::config::*;
use std::sync::LazyLock;

pub fn fortran() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""), // 1.0d0, 1.5e10, 1.0_dp all covered by the alnum tail
            // doubled-quote escaping: `''`/`""` is a literal quote, not a close.
            sq_string_doubled(),
            dq_string_doubled(),
            // BOZ literals (binary/octal/hex), e.g. Z'FF', b'1010'.
            regex_rule(r"^[BOZboz]'[0-9A-Fa-f]*'", TokKind::Str),
            regex_rule(r#"^[BOZboz]"[0-9A-Fa-f]*""#, TokKind::Str),
        ];
        LangConfig::from_registry("fortran", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::fortran;
    use crate::lang::testutil::*;

    #[test]
    fn assignment() {
        let ms = matches(fortran(), r"\V = 1", "program p\nx = 1\nend program p\n");
        assert_eq!(cap(&ms, "V").as_deref(), Some("x"));
    }

    /// Conformance over Fortran literal forms (doubled-quote strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("'hi'", "program p\nx = 'hi'\nend program p\n"),
            ("\"hi\"", "program p\nx = \"hi\"\nend program p\n"),
            ("'it''s'", "program p\nx = 'it''s'\nend program p\n"),
            ("\"a\"\"b\"", "program p\nx = \"a\"\"b\"\nend program p\n"),
            ("1.0d0", "program p\nx = 1.0d0\nend program p\n"),
            ("1.5e10", "program p\nx = 1.5e10\nend program p\n"),
        ] {
            assert!(!matches(fortran(), lit, ctx).is_empty(), "Fortran `{lit}`");
        }
    }
}
