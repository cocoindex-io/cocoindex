//! yaml.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn yaml() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_yaml::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::yaml;
    use crate::lang::testutil::*;

    #[test]
    fn mapping_value() {
        let ms = matches(yaml(), r"a: \V", "a: 1");
        assert_eq!(cap(&ms, "V").as_deref(), Some("1"));
    }
}
