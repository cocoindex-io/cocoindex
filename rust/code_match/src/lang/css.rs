//! css.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn css() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_css::LANGUAGE)));
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
}
