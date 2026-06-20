//! toml.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn toml() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_toml_ng::LANGUAGE)));
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
}
