//! csharp.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn csharp() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_c_sharp::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::csharp;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "class C { void M() { foo(a, b); } }";
        let ms = matches(csharp(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
