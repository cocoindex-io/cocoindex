//! ruby.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn ruby() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_ruby::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::ruby;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "foo(a, b)";
        let ms = matches(ruby(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }
}
