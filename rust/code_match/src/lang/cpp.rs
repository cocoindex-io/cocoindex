//! C++: C-like literals plus raw strings `R"tag(...)tag"`.

use super::c::c_tokenizers;
use crate::config::*;
use std::sync::LazyLock;

pub fn cpp() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_tokenizers();
        toks.push(TokenRule::new(CppRawString, TokKind::Str));
        LangConfig::from_registry("cpp", toks)
    });
    CFG.clone()
}

/// `R"delim(...)delim"` — a delimiter-balanced raw string. The matching `delim`
/// is a backreference the `regex` crate can't express, so this is a hand scanner.
struct CppRawString;

impl Tokenizer for CppRawString {
    fn match_len(&self, input: &str) -> Option<usize> {
        let b = input.as_bytes();
        if b.first() != Some(&b'R') || b.get(1) != Some(&b'"') {
            return None;
        }
        let dstart = 2;
        let mut p = dstart;
        // delimiter: up to 16 chars, none of ( ) \ " whitespace
        while p < b.len() && b[p] != b'(' && p - dstart < 16 {
            let ch = b[p];
            if ch == b')' || ch == b'\\' || ch == b'"' || ch.is_ascii_whitespace() {
                return None;
            }
            p += 1;
        }
        if b.get(p) != Some(&b'(') {
            return None;
        }
        let delim = &input[dstart..p];
        let close = format!("){delim}\"");
        input[p..].find(&close).map(|idx| p + idx + close.len())
    }
}

#[cfg(test)]
mod tests {
    use super::cpp;
    use crate::lang::testutil::*;

    #[test]
    fn nested_generics_split() {
        let src = "vector<vector<int>> v;";
        let ms = matches(cpp(), r"vector<vector<\T>>", src);
        assert_eq!(cap(&ms, "T").as_deref(), Some("int"));
    }

    #[test]
    fn right_shift() {
        let src = "int n = a >> 2;";
        let ms = matches(cpp(), r"\X >> \Y", src);
        assert_eq!(cap(&ms, "X").as_deref(), Some("a"));
        assert_eq!(cap(&ms, "Y").as_deref(), Some("2"));
    }

    #[test]
    fn apostrophe_separator_literal() {
        let src = "int x = 1'000;";
        assert!(!matches(cpp(), "1'000", src).is_empty());
    }

    #[test]
    fn raw_string_with_custom_delimiter() {
        // R"x(a)b)x" contains a literal `)` that only the matching delimiter ends.
        let src = r#"const char* s = R"x(a)b)x";"#;
        let ms = matches(cpp(), r#"R"x(a)b)x""#, src);
        assert!(!ms.is_empty(), "C++ raw string with delimiter should match");
    }

    #[test]
    fn class_with_destructor() {
        // `~\NAME()` ties the destructor name to the class name (metavar
        // equality); the same-level `\*` holes absorb an optional base/access
        // section and the members before the destructor. The two trailing `\*`
        // are one per grammar level: the first absorbs the destructor's own tail
        // (inline body or `;`), the second the following members.
        let pat = r"class \NAME \* { \* ~\NAME() \* \* }";
        // inlined destructor, with a leading access specifier
        let ms = matches(cpp(), pat, "class Foo { public: ~Foo() {} };");
        assert_eq!(cap(&ms, "NAME").as_deref(), Some("Foo"));
        // declared (non-inline) destructor, members before and after
        assert!(has_kind(
            &matches(cpp(), pat, "class Bar { int x; ~Bar(); void f(); };"),
            "class_specifier",
        ));
        // no destructor → no match
        assert!(
            matches(cpp(), pat, "class Baz { int x; void f(); };").is_empty(),
            "class without a destructor must not match",
        );
    }
}
