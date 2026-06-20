//! hcl (HashiCorp Configuration Language / Terraform).
use crate::config::LangConfig;
use std::sync::LazyLock;
use tree_sitter::Language;

pub fn hcl() -> LangConfig {
    static CFG: LazyLock<LangConfig> =
        LazyLock::new(|| LangConfig::from_grammar(Language::new(tree_sitter_hcl::LANGUAGE)));
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
}
