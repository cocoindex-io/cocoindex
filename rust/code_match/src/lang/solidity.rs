//! solidity.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn solidity() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_solidity::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::solidity;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let src = "contract C { function f() public { foo(a, b); } }";
        let ms = matches(solidity(), r"foo(\(A*))", src);
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }
}
