//! PHP: C-style escaping; `$` is ordinary source (the sigil is `\`).
use crate::config::*;
use std::sync::LazyLock;

pub fn php() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("php", c_like_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::php;
    use crate::lang::testutil::*;

    #[test]
    fn call_captures_dollar_vars() {
        let src = "<?php foo($a, $b); ?>";
        let ms = matches(php(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("$a, $b"));
    }

    #[test]
    fn literal_dollar_variable() {
        let src = "<?php $result = compute(); ?>";
        let ms = matches(php(), r"$result = \VAL", src);
        assert_eq!(cap(&ms, "VAL").as_deref(), Some("compute()"));
    }

    /// Conformance over PHP literal forms — the generic backslash profile fits.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "<?php $x = \"hi\"; ?>"),
            ("'hi'", "<?php $x = 'hi'; ?>"),
            ("42", "<?php $x = 42; ?>"),
            ("0xFF", "<?php $x = 0xFF; ?>"),
            ("1_000", "<?php $x = 1_000; ?>"),
            ("1.5e-10", "<?php $x = 1.5e-10; ?>"),
        ] {
            assert!(!matches(php(), lit, ctx).is_empty(), "PHP `{lit}`");
        }
    }
}
