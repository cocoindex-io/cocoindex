//! html.
//!
//! Quote tokenization is context-sensitive: in an *attribute* a `"` is paired
//! (`class="x"`), but in *text* it's a literal char (`<p>a"b</p>` is valid, and
//! quotes in separate elements are unrelated). So the lexer runs in two modes:
//!   - **text** (outside `<…>`, mode 0): a free-text run, emitted atomically like
//!     a string node; whitespace is significant; `"` is a literal char.
//!   - **tag** (inside `<…>`, mode 1): identifiers/numbers + paired attribute
//!     strings; whitespace skipped.
//!
//! `<` flips text→tag, `>` flips tag→text. A pattern thus matches text content
//! either literally (`<p>hello</p>`) or with a metavar over the whole text
//! (`<p>\X</p>`); attribute strings (`class="x"`) tokenize correctly.
use crate::config::*;
use std::sync::LazyLock;

const TEXT: u8 = 0;
const TAG: u8 = 1;

pub fn html() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            free_text('<').in_modes(1 << TEXT),
            identifier().in_modes(1 << TAG),
            number("").in_modes(1 << TAG),
            dq_string().in_modes(1 << TAG),
            sq_string().in_modes(1 << TAG),
        ];
        LangConfig::from_registry("html", toks)
            .with_modes(vec![(TEXT, '<', TAG), (TAG, '>', TEXT)], 1 << TEXT)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::html;
    use crate::lang::testutil::*;

    #[test]
    fn metavar_over_text() {
        let ms = matches(html(), r"<p>\X</p>", "<p>hi</p>");
        assert_eq!(cap(&ms, "X").as_deref(), Some("hi"));
    }

    #[test]
    fn literal_text() {
        // text content matches literally (a free-text run, not split into words).
        assert!(has_kind(
            &matches(html(), "<p>hello world</p>", "<p>hello world</p>"),
            "element"
        ));
    }

    #[test]
    fn quote_in_text_is_literal() {
        // a `"` in text is part of the text run — captured by a metavar, and not
        // paired across structure.
        let ms = matches(html(), r"<div>\X</div>", r#"<div>a"b</div>"#);
        assert_eq!(cap(&ms, "X").as_deref(), Some(r#"a"b"#));
    }

    #[test]
    fn no_cross_element_quote_pairing() {
        // The hazard: with a context-free `"…"` tokenizer the two text quotes
        // would pair across the tags. Here each `<div>` matches independently and
        // nothing spans the gap.
        let src = r#"<div>a"b</div><div>c"d</div>"#;
        let ms = matches(html(), r"<div>\X</div>", src);
        let texts: Vec<&str> = ms
            .iter()
            .filter(|m| m.kind == "element")
            .filter_map(|m| m.capture_text("X"))
            .collect();
        assert!(
            texts.contains(&r#"a"b"#) && texts.contains(&r#"c"d"#),
            "got {texts:?}"
        );
    }

    #[test]
    fn attribute_string() {
        // inside a tag, `"x"` is a paired string and matches the attribute value.
        let ms = matches(
            html(),
            r#"<div class="x">\*</div>"#,
            r#"<div class="x">hi</div>"#,
        );
        assert!(has_kind(&ms, "element"));
    }

    #[test]
    fn attribute_metavar() {
        // a metavar over the attribute value.
        let ms = matches(html(), r"<a href=\V>\*</a>", r#"<a href="/x">hi</a>"#);
        assert_eq!(cap(&ms, "V").as_deref(), Some(r#""/x""#));
    }
}
