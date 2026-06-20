//! java.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn java() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_java::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::java;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "class C { void m() { foo(a, b); } }";
        let ms = matches(java(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
