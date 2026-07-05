//! SQL (tree-sitter-sequel). String literals use **doubled-quote** escaping,
//! not backslash: `'it''s'` is one literal, and `"a""b"` / delimited identifiers
//! double the quote. The generic backslash profile would close at the wrong
//! quote, so SQL picks the doubled-quote string builders.
use crate::config::*;
use std::sync::LazyLock;

pub fn sql() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            sq_string_doubled(), // 'it''s'
            dq_string_doubled(), // "delimited ""id"""
            backtick_string(),   // `mysql_id`
        ];
        LangConfig::from_registry("sql", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::sql;
    use crate::lang::testutil::*;

    #[test]
    fn select() {
        let src = "SELECT name FROM users";
        let ms = matches(sql(), r"SELECT \COL FROM \TBL", src);
        assert!(!ms.is_empty(), "expected SELECT pattern to match");
    }

    /// Conformance over SQL literal forms (doubled-quote escaping).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("'hi'", "SELECT 'hi'"),
            ("'it''s'", "SELECT 'it''s'"),
            ("42", "SELECT 42"),
            ("3.14", "SELECT 3.14"),
        ] {
            assert!(!matches(sql(), lit, ctx).is_empty(), "SQL `{lit}`");
        }
    }
}
