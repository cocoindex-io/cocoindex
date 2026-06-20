//! r.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn r() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_r::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::r;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(r(), r"foo(\(A*))", "foo(a, b)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
