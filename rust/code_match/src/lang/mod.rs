//! Per-language constructors. Each language is a private submodule holding its
//! `LangConfig` constructor, its bespoke tokenizers, and its tests; the
//! constructor is re-exported here as `lang::<name>()`. The shared framework
//! (`LangConfig`, `Tokenizer`, …) lives in `crate::config`, which this depends on.

mod bash;
mod c;
mod cpp;
mod csharp;
mod go;
mod java;
mod javascript;
mod php;
mod python;
mod ruby;
mod rust;
mod scala;
mod sql;
mod typescript;

pub use bash::bash;
pub use c::c;
pub use cpp::cpp;
pub use csharp::csharp;
pub use go::go;
pub use java::java;
pub use javascript::javascript;
pub use php::php;
pub use python::python;
pub use ruby::ruby;
pub use rust::rust;
pub use scala::scala;
pub use sql::sql;
pub use typescript::{tsx, typescript};

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
