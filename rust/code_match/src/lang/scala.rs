//! scala.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn scala() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_scala::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::scala;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "object O { def m = foo(a, b) }";
        let ms = matches(scala(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
