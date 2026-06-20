//! SQL (tree-sitter-sequel).
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn sql() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_sequel::LANGUAGE)));
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
}
