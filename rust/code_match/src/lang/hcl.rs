//! hcl (HashiCorp Configuration Language / Terraform). The C-style profile fits:
//! strings use backslash escaping and `${...}` interpolation is matched opaquely
//! via the whole string node. Heredocs (`<<EOF ... EOF`) are one node — match
//! them with a metavar rather than an exact literal.
use crate::config::*;
use std::sync::LazyLock;

pub fn hcl() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_registry("hcl", c_like_tokenizers()));
    CFG.clone()
}

#[cfg(test)]
mod tests {
    use super::hcl;
    use crate::lang::testutil::*;

    #[test]
    fn attribute() {
        let ms = matches(hcl(), r"a = \V", "a = \"b\"");
        assert_eq!(cap(&ms, "V").as_deref(), Some("\"b\""));
    }

    #[test]
    fn heredoc_via_metavar() {
        // Heredocs are one node — capturable with a metavar.
        let ms = matches(hcl(), r"a = \V", "a = <<EOF\nhello\nEOF\n");
        assert_eq!(cap(&ms, "V").as_deref(), Some("<<EOF\nhello\nEOF"));
    }

    /// Conformance over HCL literal forms (strings, interpolation, numbers).
    #[test]
    fn literal_forms() {
        for (lit, ctx) in [
            ("\"hi\"", "a = \"hi\""),
            ("\"a${b}c\"", "a = \"a${b}c\""),
            ("1.5", "a = 1.5"),
            ("42", "a = 42"),
            ("true", "a = true"),
        ] {
            assert!(!matches(hcl(), lit, ctx).is_empty(), "HCL `{lit}`");
        }
    }
}
