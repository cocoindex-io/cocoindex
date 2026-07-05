//! Pascal: string literals use doubled-quote escaping (`'it''s'`), and numbers
//! have radix prefixes — `$FF` (hex), `&777` (octal), `%1010` (binary), `#65`
//! (char code). Each is one `literalNumber`/`literalString` node, matched whole.
use crate::config::*;
use std::sync::LazyLock;

pub fn pascal() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),                                     // decimal / real
            sq_string_doubled(),                            // '' escape
            regex_rule(r"^\$[0-9A-Fa-f]+", TokKind::Str),   // hex
            regex_rule(r"^&[0-7]+", TokKind::Str),          // octal
            regex_rule(r"^%[01]+", TokKind::Str),           // binary
            regex_rule(r"^#\$?[0-9A-Fa-f]+", TokKind::Str), // char code #65 / #$41
        ];
        LangConfig::from_registry("pascal", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::pascal;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(pascal(), r"foo(\(A*\))", "begin foo(a, b); end.");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    /// Conformance over Pascal literal forms (doubled-quote strings, radix nums).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("'hi'", "begin x := 'hi'; end."),
            ("'it''s'", "begin x := 'it''s'; end."),
            ("42", "begin x := 42; end."),
            ("3.14", "begin x := 3.14; end."),
            ("$FF", "begin x := $FF; end."),
        ] {
            assert!(!matches(pascal(), lit, ctx).is_empty(), "Pascal `{lit}`");
        }
    }
}
