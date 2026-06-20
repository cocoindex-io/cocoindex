//! pascal.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn pascal() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_pascal::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::pascal;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(pascal(), r"foo(\(A*))", "begin foo(a, b); end.");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
