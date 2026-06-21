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

/// Resolve a language name to its [`LangConfig`], case-insensitively and with
/// common aliases (`c++`, `c#`, `js`, `py`, `golang`, …). Returns `None` for a
/// language code_match doesn't support. Used by the Python binding to map a
/// user-supplied language string to a matcher config.
pub fn by_name(name: &str) -> Option<crate::config::LangConfig> {
    Some(match name.to_ascii_lowercase().as_str() {
        "bash" | "shell" | "sh" => bash(),
        "c" => c(),
        "cpp" | "c++" => cpp(),
        "csharp" | "c#" | "cs" => csharp(),
        "cmake" => cmake(),
        "css" => css(),
        "elm" => elm(),
        "fortran" | "f90" => fortran(),
        "go" | "golang" => go(),
        "hcl" | "terraform" | "tf" => hcl(),
        "html" => html(),
        "java" => java(),
        "javascript" | "js" => javascript(),
        "json" => json(),
        "julia" | "jl" => julia(),
        "kotlin" | "kt" => kotlin(),
        "pascal" | "delphi" => pascal(),
        "php" => php(),
        "python" | "py" => python(),
        "r" => r(),
        "ruby" | "rb" => ruby(),
        "rust" | "rs" => rust(),
        "scala" => scala(),
        "solidity" | "sol" => solidity(),
        "sql" => sql(),
        "swift" => swift(),
        "toml" => toml(),
        "typescript" | "ts" => typescript(),
        "tsx" => tsx(),
        "xml" => xml(),
        "yaml" | "yml" => yaml(),
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) mod testutil {
    use crate::config::LangConfig;
    use crate::{Match, Pattern};

    /// Run the pattern against `src`. Every call also cross-checks the **prefilter**:
    /// it must never reject a source that actually matches (soundness — no false
    /// negatives), and `matches_prefiltered` must agree with the plain run. So every
    /// feature / per-language test doubles as a prefilter soundness test for free.
    /// `min_len = 1` keeps even short terms, exercising the most prefilter logic.
    pub fn matches<'s>(cfg: LangConfig, pat: &str, src: &'s str) -> Vec<Match<'s>> {
        let compiled = Pattern::compile(pat, &cfg).expect("valid test pattern");
        let out = compiled.matches(src);
        let pf = compiled.prefilter(1);
        assert!(
            out.is_empty() || pf.might_match(src),
            "prefilter wrongly rejected a matching source\n  pattern: {pat:?}\n  source:  {src:?}",
        );
        assert_eq!(
            compiled.matches_prefiltered(src, &pf).len(),
            out.len(),
            "matches_prefiltered disagrees with matches\n  pattern: {pat:?}\n  source:  {src:?}",
        );
        out
    }

    pub fn cap(ms: &[Match], name: &str) -> Option<String> {
        ms.iter()
            .find_map(|m| m.capture_text(name).map(str::to_string))
    }

    pub fn has_kind(ms: &[Match], kind: &str) -> bool {
        ms.iter().any(|m| m.kind == kind)
    }
}
