//! PHP. Config-wise generic; `$` is ordinary source (the sigil is `\`).
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn php() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_php::LANGUAGE_PHP)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::php;
    use crate::lang::testutil::*;

    #[test]
    fn call_captures_dollar_vars() {
        let src = "<?php foo($a, $b); ?>";
        let ms = matches(php(), r"foo(\(ARGS*))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("$a, $b"));
    }

    #[test]
    fn literal_dollar_variable() {
        let src = "<?php $result = compute(); ?>";
        let ms = matches(php(), r"$result = \VAL", src);
        assert_eq!(cap(&ms, "VAL").as_deref(), Some("compute()"));
    }
}
