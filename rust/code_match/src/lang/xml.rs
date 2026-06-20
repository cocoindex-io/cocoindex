//! xml.
//!
//! Like HTML, quote tokenization is context-sensitive (paired in attributes,
//! literal in text). Write structural patterns and use a metavar for text
//! content (`<tag>\X</tag>`); literal quotes in pattern text are unsupported.
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn xml() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_xml::LANGUAGE_XML)));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::xml;
    use crate::lang::testutil::*;

    #[test]
    fn element_text() {
        let ms = matches(xml(), r"<a>\X</a>", "<a>hi</a>");
        assert_eq!(cap(&ms, "X").as_deref(), Some("hi"));
    }
}
