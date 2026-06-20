//! Per-language constructors. Each language is a private submodule holding its
//! `LangConfig` constructor, its bespoke tokenizers, and its tests; the
//! constructor is re-exported here as `lang::<name>()`. The shared framework
//! (`LangConfig`, `Tokenizer`, …) lives in `crate::config`, which this depends on.

mod bash;
mod c;
mod cmake;
mod cpp;
mod csharp;
mod css;
mod elm;
mod fortran;
mod go;
mod hcl;
mod html;
mod java;
mod javascript;
mod json;
mod julia;
mod kotlin;
mod pascal;
mod php;
mod python;
mod r;
mod ruby;
mod rust;
mod scala;
mod solidity;
mod sql;
mod swift;
mod toml;
mod typescript;
mod xml;
mod yaml;

pub use bash::bash;
pub use c::c;
pub use cmake::cmake;
pub use cpp::cpp;
pub use csharp::csharp;
pub use css::css;
pub use elm::elm;
pub use fortran::fortran;
pub use go::go;
pub use hcl::hcl;
pub use html::html;
pub use java::java;
pub use javascript::javascript;
pub use json::json;
pub use julia::julia;
pub use kotlin::kotlin;
pub use pascal::pascal;
pub use php::php;
pub use python::python;
pub use r::r;
pub use ruby::ruby;
pub use rust::rust;
pub use scala::scala;
pub use solidity::solidity;
pub use sql::sql;
pub use swift::swift;
pub use toml::toml;
pub use typescript::{tsx, typescript};
pub use xml::xml;
pub use yaml::yaml;

#[cfg(test)]
pub(crate) mod testutil {
    use crate::config::LangConfig;
    use crate::{Match, Pattern};

    pub fn matches<'s>(cfg: LangConfig, pat: &str, src: &'s str) -> Vec<Match<'s>> {
        Pattern::compile(pat, &cfg)
            .expect("valid test pattern")
            .matches(src)
    }

    pub fn cap(ms: &[Match], name: &str) -> Option<String> {
        ms.iter()
            .find_map(|m| m.capture_text(name).map(str::to_string))
    }

    pub fn has_kind(ms: &[Match], kind: &str) -> bool {
        ms.iter().any(|m| m.kind == kind)
    }
}
