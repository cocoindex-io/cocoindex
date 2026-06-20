//! `cocoindex_code_match` — match by-example structural patterns against
//! tree-sitter ASTs (14 languages). Patterns are flat token+metavar skeletons;
//! the source is a full tree-sitter parse, so precedence/balancing/context come
//! from the AST for free.
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
pub use prefilter::{Boundary, FilterClause, FilterTerm, Prefilter, index_terms};

// The crate's fallible surface uses the workspace-standard error type.
pub use cocoindex_utils::error::{Error, Result};
