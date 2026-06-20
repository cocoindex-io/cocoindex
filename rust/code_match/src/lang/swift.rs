//! swift.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn swift() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_swift::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::swift;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(swift(), r"foo(\(A*))", "func m() { foo(a, b) }");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
