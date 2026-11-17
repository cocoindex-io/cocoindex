//! `cocoindex_code_match` — match by-example structural patterns against
//! tree-sitter ASTs (14 languages). Patterns are flat token+metavar skeletons;
//! the source is a full tree-sitter parse, so precedence/balancing/context come
//! from the AST for free.
//!
//! This crate is an internal part of [CocoIndex](https://cocoindex.io), published
//! only so the [`cocoindex`](https://docs.rs/cocoindex) crate can depend on it. It
//! makes no API stability promise: any release may change it. Depend on
//! `cocoindex` instead.
//!
//! Pattern compilation is fallible: a malformed metavar regex surfaces as a
//! [`cocoindex_utils::error::Error::client`] via [`Pattern::compile`].

mod config;
pub mod lang;
mod lexer;
mod matcher;
mod prefilter;

pub use config::{LangConfig, RegexTokenizer, TokKind, TokenRule, Tokenizer};
pub use lexer::{Cardinality, PatternItem};
pub use matcher::{Capture, Match, Pattern};
pub use prefilter::{
    Boundary, FilterClause, FilterTerm, Prefilter, index_terms, index_terms_in_tree,
    regex_required_literals,
};

// The crate's fallible surface uses the workspace-standard error type.
pub use cocoindex_utils::error::{Error, Result};
