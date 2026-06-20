//! javascript.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn javascript() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_javascript::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::javascript;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "foo(a, b);";
        let ms = matches(javascript(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
