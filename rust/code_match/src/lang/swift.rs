//! Swift: multiline strings `"""..."""` and raw strings `#"..."#` (any `#`
//! count, single or triple quote). Interpolation `\(expr)` is matched opaquely
//! via the whole string node's text.
use crate::config::*;
use std::sync::LazyLock;

pub fn swift() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            identifier(),
            number(""),
            dq_string(),
            triple_dq_string(),
            TokenRule::new(SwiftRawString, TokKind::Str),
        ];
        LangConfig::from_registry("swift", toks)
    });
    CFG.clone()
}

/// `#*"..."#*` / `#*"""..."""#*` with a matching `#` count (≥1) — the balance
/// the `regex` crate can't express, so it's a hand scanner. A normal (`#`-less)
/// string is left to `dq_string` / `triple_dq_string`.
struct SwiftRawString;

impl Tokenizer for SwiftRawString {
    fn match_len(&self, input: &str) -> Option<usize> {
        let b = input.as_bytes();
        let mut p = 0;
        while b.get(p) == Some(&b'#') {
            p += 1;
        }
        let hashes = p;
        if hashes == 0 {
            return None;
        }
        let triple =
            b.get(p) == Some(&b'"') && b.get(p + 1) == Some(&b'"') && b.get(p + 2) == Some(&b'"');
        let quotes = if triple {
            3
        } else if b.get(p) == Some(&b'"') {
            1
        } else {
            return None;
        };
        p += quotes;
        loop {
            while p < b.len() && b[p] != b'"' {
                p += 1;
            }
            if p >= b.len() {
                return None; // unterminated
            }
            let mut q = p;
            let mut got = 0;
            while got < quotes && b.get(q) == Some(&b'"') {
                got += 1;
                q += 1;
            }
            if got == quotes {
                let mut h = 0;
                while h < hashes && b.get(q) == Some(&b'#') {
                    h += 1;
                    q += 1;
                }
                if h == hashes {
                    return Some(q);
                }
            }
            p += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::swift;
    use crate::lang::testutil::*;

    #[test]
    fn call() {
        let ms = matches(swift(), r"foo(\(A*\))", "func m() { foo(a, b) }");
        assert_eq!(cap(&ms, "A").as_deref(), Some("a, b"));
    }

    /// Conformance over Swift literal forms (multiline/raw strings, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "let x = \"hi\""),
            ("\"\"\"\nhi\n\"\"\"", "let x = \"\"\"\nhi\n\"\"\""),
            ("#\"raw\"#", "let x = #\"raw\"#"),
            ("##\"ra\"#w\"##", "let x = ##\"ra\"#w\"##"),
            ("#\"\"\"\nr\n\"\"\"#", "let x = #\"\"\"\nr\n\"\"\"#"),
            ("0xFF", "let x = 0xFF"),
            ("0b1010", "let x = 0b1010"),
            ("0o17", "let x = 0o17"),
            ("1_000", "let x = 1_000"),
            ("1.5e10", "let x = 1.5e10"),
        ] {
            assert!(!matches(swift(), lit, ctx).is_empty(), "Swift `{lit}`");
        }
    }
}
