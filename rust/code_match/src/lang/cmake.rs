//! CMake: bracket arguments `[[...]]` / `[=[...]=]` (matching `=` count) are
//! single raw-text nodes the `regex` crate can't balance. Quoted args use the
//! generic backslash escaping; `${VAR}` references match via the generic profile.
use crate::config::*;
use std::sync::LazyLock;

pub fn cmake() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let mut toks = c_like_tokenizers();
        toks.push(TokenRule::new(CmakeBracket, TokKind::Str));
        LangConfig::from_registry("cmake", toks)
    });
    CFG.clone()
}

/// `[=*[ ... ]=*]` with a matching number of `=`.
struct CmakeBracket;

impl Tokenizer for CmakeBracket {
    fn match_len(&self, input: &str) -> Option<usize> {
        let b = input.as_bytes();
        if b.first() != Some(&b'[') {
            return None;
        }
        let mut p = 1;
        let eq_start = p;
        while b.get(p) == Some(&b'=') {
            p += 1;
        }
        let n = p - eq_start;
        if b.get(p) != Some(&b'[') {
            return None;
        }
        p += 1;
        loop {
            while p < b.len() && b[p] != b']' {
                p += 1;
            }
            if p >= b.len() {
                return None;
            }
            let mut q = p + 1;
            let mut e = 0;
            while e < n && b.get(q) == Some(&b'=') {
                e += 1;
                q += 1;
            }
            if e == n && b.get(q) == Some(&b']') {
                return Some(q + 1);
            }
            p += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::cmake;
    use crate::lang::testutil::*;

    #[test]
    fn command() {
        // CMake command arguments are space-separated.
        let ms = matches(cmake(), r"set(\(A*\))", "set(x 1)");
        assert_eq!(cap(&ms, "A").as_deref(), Some("x 1"));
    }

    /// Conformance over CMake argument forms (quoted, bracket, variable ref).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "set(x \"hi\")"),
            ("${VAR}", "set(x ${VAR})"),
            ("[[raw]]", "set(x [[raw]])"),
            ("[=[ra]]w]=]", "set(x [=[ra]]w]=])"),
        ] {
            assert!(!matches(cmake(), lit, ctx).is_empty(), "CMake `{lit}`");
        }
    }
}
