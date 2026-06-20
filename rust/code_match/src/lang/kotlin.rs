//! kotlin.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn kotlin() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_kotlin_ng::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::kotlin;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(kotlin(), r"foo(\(A*))", "fun m() { foo(a, b) }");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
