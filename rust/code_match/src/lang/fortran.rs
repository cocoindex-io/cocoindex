//! fortran.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn fortran() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_fortran::LANGUAGE)));
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
}
