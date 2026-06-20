//! json.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn json() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_json::LANGUAGE)));
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
}
