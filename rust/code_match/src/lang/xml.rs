//! xml.
//!
//! Same context-sensitive lexing as HTML (see `html.rs`): a **text** mode
//! outside `<…>` (free-text run, `"` literal) and a **tag** mode inside (paired
//! attribute strings), flipped by `<` / `>`.
use crate::config::*;
use std::sync::LazyLock;

const TEXT: u8 = 0;
const TAG: u8 = 1;

pub fn xml() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            free_text('<').in_modes(1 << TEXT),
            identifier().in_modes(1 << TAG),
            number("").in_modes(1 << TAG),
            dq_string().in_modes(1 << TAG),
            sq_string().in_modes(1 << TAG),
        ];
        LangConfig::from_registry("xml", toks)
            .with_modes(vec![(TEXT, '<', TAG), (TAG, '>', TEXT)], 1 << TEXT)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::xml;
    use crate::lang::testutil::*;

    #[test]
    fn metavar_over_text() {
        let ms = matches(xml(), r"<a>\X</a>", "<a>hi</a>");
        assert_eq!(cap(&ms, "X").as_deref(), Some("hi"));
    }

    #[test]
    fn literal_text() {
        assert!(has_kind(
            &matches(xml(), "<a>hello world</a>", "<a>hello world</a>"),
            "element"
        ));
    }

    #[test]
    fn quote_in_text_is_literal() {
        let ms = matches(xml(), r"<a>\X</a>", r#"<a>x"y</a>"#);
        assert_eq!(cap(&ms, "X").as_deref(), Some(r#"x"y"#));
    }

    #[test]
    fn attribute_string() {
        let ms = matches(xml(), r#"<a id="x">\*</a>"#, r#"<a id="x">hi</a>"#);
        assert!(has_kind(&ms, "element"));
    }
}
