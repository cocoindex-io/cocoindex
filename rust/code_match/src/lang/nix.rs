//! Nix. Identifiers may contain hyphens and apostrophes, strings interpolate
//! with `${...}`, and indented strings use paired single quotes.
use crate::config::*;
use std::sync::LazyLock;

struct IndentedString;

impl Tokenizer for IndentedString {
    fn match_len(&self, input: &str) -> Option<usize> {
        if !input.starts_with("''") {
            return None;
        }

        let mut offset = 2;
        while offset < input.len() {
            let rest = &input[offset..];
            if rest.starts_with("'''") {
                offset += 3;
            } else if rest.starts_with("''$") {
                offset += 3;
            } else if rest.starts_with("''\\") {
                offset += 3;
                let escaped = input[offset..].chars().next()?;
                offset += escaped.len_utf8();
            } else if rest.starts_with("''") {
                return Some(offset + 2);
            } else {
                offset += rest.chars().next()?.len_utf8();
            }
        }
        None
    }
}

pub fn nix() -> LangConfig {
    static CFG: LazyLock<LangConfig> = LazyLock::new(|| {
        let toks = vec![
            regex_rule(r"^[A-Za-z_][A-Za-z0-9_'\-]*", TokKind::Token),
            number(""),
            dq_string(),
            TokenRule::new(IndentedString, TokKind::Str),
            regex_rule(r"^(?:\./|\.\./|/|~/)[A-Za-z0-9._+\-/]+", TokKind::Str),
            regex_rule(r"^<[A-Za-z0-9._+\-/]+>", TokKind::Str),
            regex_rule(
                r"^[A-Za-z][A-Za-z0-9+.-]*:[A-Za-z0-9%/?:@&=+$,_.!~*'\-]+",
                TokKind::Str,
            ),
        ];
        LangConfig::from_registry("nix", toks)
    });
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::nix;
    use crate::lang::testutil::*;

    #[test]
    fn binding() {
        let ms = matches(
            nix(),
            r"package-name = \VALUE;",
            "{ package-name = pkgs.hello; }",
        );
        assert_eq!(cap(&ms, "VALUE").as_deref(), Some("pkgs.hello"));
    }

    #[test]
    fn function_formals() {
        let ms = matches(
            nix(),
            r"{ pkgs, \LIB, ... }: \BODY",
            "{ pkgs, lib, ... }: pkgs.callPackage ./package.nix {}",
        );
        assert_eq!(cap(&ms, "LIB").as_deref(), Some("lib"));
        assert_eq!(
            cap(&ms, "BODY").as_deref(),
            Some("pkgs.callPackage ./package.nix {}")
        );
    }

    #[test]
    fn inherit() {
        let ms = matches(nix(), r"inherit \NAMES;", "{ inherit lib pkgs; }");
        assert_eq!(cap(&ms, "NAMES").as_deref(), Some("lib pkgs"));
    }

    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("package-name", "{ package-name = 1; }"),
            ("1.5", "{ value = 1.5; }"),
            ("\"hello ${name}\"", "{ value = \"hello ${name}\"; }"),
            ("''hello ${name}''", "{ value = ''hello ${name}''; }"),
            ("''hello ''${name}''", "{ value = ''hello ''${name}''; }"),
            ("./package.nix", "{ src = ./package.nix; }"),
            ("<nixpkgs>", "{ src = <nixpkgs>; }"),
            (
                "https://example.com/source",
                "{ src = https://example.com/source; }",
            ),
            ("http://h:8080/x", "{ u = http://h:8080/x; }"),
        ] {
            assert!(!matches(nix(), lit, ctx).is_empty(), "Nix `{lit}`");
        }
    }
}
