//! Bash.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn bash() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_bash::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::bash;
    use crate::lang::testutil::*;

    #[test]
    fn command() {
        // `\MSG` (not `$MSG`) — the `\` sigil keeps this shell-safe.
        let src = "echo hello";
        let ms = matches(bash(), r"echo \MSG", src);
        assert_eq!(cap(&ms, "MSG").as_deref(), Some("hello"));
    }
}
