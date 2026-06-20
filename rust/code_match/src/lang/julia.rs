//! julia.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn julia() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_julia::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::julia;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(julia(), r"foo(\(A*))", "foo(a, b)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
