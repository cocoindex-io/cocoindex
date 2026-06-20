//! html.
//!
//! Quote tokenization is context-sensitive: in an *attribute* a `"` is paired
//! (`class="x"`), but in *text* content it's a literal char (`<p>a"b</p>` is
//! valid, and quotes in separate elements are unrelated). The flat pattern lexer
//! can't see that context, so write **structural** patterns: anchor at tags and
//! use a metavar for text content (`<p>\X</p>`), where quotes only appear in
//! attribute position. A *literal* quote in pattern text would wrongly pair
//! across structure — unsupported, same as literal free text in general.
//! Lifting this (literal `attr="v"` *and* safe text) needs a context-sensitive
//! lexer that tokenizes differently inside vs outside `<…>` — a future addition,
//! since the stateless tokenizer can't see that region.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn html() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_html::LANGUAGE)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::html;
    use crate::lang::testutil::*;

    #[test]
    fn element_text() {
        let ms = matches(html(), r"<p>\X</p>", "<p>hi</p>");
        assert_eq!(cap(&ms, "X").as_deref(), Some("hi"));
    }

    #[test]
    fn metavar_captures_quoted_text() {
        // A `"` in text content is just part of the captured node — the metavar
        // handles it; no literal quote needed in the pattern.
        let ms = matches(html(), r"<div>\X</div>", r#"<div>a"b</div>"#);
        assert_eq!(cap(&ms, "X").as_deref(), Some(r#"a"b"#));
    }
}
