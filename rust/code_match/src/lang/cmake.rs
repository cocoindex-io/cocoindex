//! cmake.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn cmake() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_cmake::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::cmake;
    use crate::lang::testutil::*;

    #[test]
    fn command() {
        // CMake command arguments are space-separated.
        let ms = matches(cmake(), r"set(\(A*))", "set(x 1)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("x 1"));
    }
}
