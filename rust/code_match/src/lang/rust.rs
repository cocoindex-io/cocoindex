//! Rust: raw strings `r#"..."#` (any `#` count), byte strings.

use crate::config::*;
use std::sync::LazyLock;

pub fn rust() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),
            sq_string(),
            TokenRule::new(RustRawString, TokKind::Str),
            regex_rule(r#"(?s)^b"(?:\\.|[^"\\])*""#, TokKind::Str), // byte string b"..."
        ];
        LangConfig::from_registry("rust", toks)
    });
    CFG.clone()
}

/// `r#*"..."#*` with a matching number of `#` (and an optional `b` prefix). The
/// `#` count is a balance the `regex` crate can't express, so it's a hand scanner.
struct RustRawString;

impl Tokenizer for RustRawString {
    fn match_len(&self, input: &str) -> Option<usize> {
        let b = input.as_bytes();
        let mut p = 0;
        if b.first() == Some(&b'b') {
            p = 1; // br#"..."#
        }
        if b.get(p) != Some(&b'r') {
            return None;
        }
        p += 1;
        let hashes = {
            let start = p;
            while b.get(p) == Some(&b'#') {
                p += 1;
            }
            p - start
        };
        if b.get(p) != Some(&b'"') {
            return None;
        }
        p += 1;
        loop {
            while p < b.len() && b[p] != b'"' {
                p += 1;
            }
            if p >= b.len() {
                return None; // unterminated
            }
            // need `hashes` '#' right after the closing quote
            let mut q = p + 1;
            let mut h = 0;
            while h < hashes && b.get(q) == Some(&b'#') {
                h += 1;
                q += 1;
            }
            if h == hashes {
                return Some(q);
            }
            p += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rust;
    use crate::lang::testutil::*;

    #[test]
    fn call_multi_args() {
        let src = "fn m(){ foo(a, b); }";
        let ms = matches(rust(), r"foo(\(ARGS*\))", src);
        assert_eq!(cap(&ms, "ARGS").as_deref(), Some("a, b"));
    }

    #[test]
    fn nested_generics_split() {
        let src = "fn m(){ let v: Vec<Vec<i32>> = mk(); }";
        let ms = matches(rust(), r"Vec<Vec<\T>>", src);
        assert_eq!(cap(&ms, "T").as_deref(), Some("i32"));
    }

    #[test]
    fn path_separator_split() {
        let src = "fn m(){ let x = std::mem::size_of(); }";
        let ms = matches(rust(), r"std::mem::\F()", src);
        assert_eq!(cap(&ms, "F").as_deref(), Some("size_of"));
    }

    #[test]
    fn raw_string_literal() {
        let src = r##"fn m() { log(r#"a"b"#); }"##;
        let ms = matches(rust(), r##"log(r#"a"b"#)"##, src);
        assert!(has_kind(&ms, "call_expression"));
    }

    #[test]
    fn raw_string_metavar() {
        let src = r##"fn m() { log(r#"a"b"#); }"##;
        let ms = matches(rust(), r"log(\S)", src);
        assert_eq!(cap(&ms, "S").as_deref(), Some(r##"r#"a"b"#"##));
    }

    #[test]
    fn number_suffix_and_separator() {
        let src = "fn m() { let n = 1_000u64; }";
        assert!(!matches(rust(), "1_000u64", src).is_empty());
    }

    #[test]
    fn signature_ignores_visibility_and_body() {
        // Leading/trailing tolerance: `fn clone(self)` matches the function
        // regardless of a leading visibility modifier and the trailing body.
        for src in [
            "pub fn clone(self) {}",
            "pub(crate) fn clone(self) {}",
            "fn clone(self) {}",
        ] {
            let ms = matches(rust(), r"fn clone(self)", src);
            // exactly one match — the function_item (kind), no enclosing-level
            // (source_file) noise. The reported range is the matched *fragment*
            // (`fn clone(self)`), not the whole node incl. the body.
            assert_eq!(ms.len(), 1, "one match for `{src}`");
            assert_eq!(ms[0].kind, "function_item");
            assert_eq!(ms[0].text, "fn clone(self)");
        }
    }

    /// Conformance over Rust literal forms.
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("0xFFu8", "fn m(){ let x = 0xFFu8; }"),
            ("0b1010_i32", "fn m(){ let x = 0b1010_i32; }"),
            ("1_000_000", "fn m(){ let x = 1_000_000; }"),
            ("1.5e-10", "fn m(){ let x = 1.5e-10; }"),
            ("2.0f64", "fn m(){ let x = 2.0f64; }"),
            (r#"b"hi""#, r#"fn m(){ let s = b"hi"; }"#),
            (r#"r"a\b""#, r#"fn m(){ let s = r"a\b"; }"#),
            (r##"r#"a"b"#"##, r##"fn m(){ let s = r#"a"b"#; }"##),
            (
                r###"r##"a"#b"##"###,
                r###"fn m(){ let s = r##"a"#b"##; }"###,
            ),
        ] {
            assert!(
                !matches(rust(), lit, ctx).is_empty(),
                "Rust literal `{lit}`"
            );
        }
    }
}
