//! elm.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn elm() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_elm::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::elm;
    use crate::lang::testutil::*;

    #[test]
    fn declaration() {
        let ms = matches(elm(), r"x = \V", "x = 1");
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }
}
