//! go.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn go() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_go::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::go;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "package main\nfunc m() { foo(a, b) }";
        let ms = matches(go(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
