//! Shared tree-sitter foundation for CocoIndex code/text operations.
//!
//! This crate owns:
//! - the **language registry** ([`prog_langs`]): language names, aliases, file
//!   extensions, and tree-sitter grammars — the single place in the workspace
//!   that links grammar crates;
//! - [`CodeSource`]: source text plus a **lazily parsed, memoized** tree-sitter
//!   AST, so several consumers (splitters, structural matchers, …) share one
//!   parse per source and each handles parse degradation internally;
//! - byte→position machinery ([`positions`]): [`LineIndex`], [`OutputPosition`],
//!   [`TextRange`];
//! - **per-language structural knowledge** ([`elements`]): declaration /
//!   namespace / reference tables, language hooks, and the code-element model
//!   types — facts about the pinned grammars, colocated with them;
//! - the **source-view layer** ([`view`]): the `SourceView` rendered schema,
//!   layer/scope classification, and context-frame extraction.

pub mod elements;
pub mod graph;
mod hazards;
pub mod positions;
pub mod prog_langs;
mod source;
pub mod view;

/// Re-exported so consumers can name tree-sitter types without their own
/// dependency (grammars are pinned to one version workspace-wide here).
pub use tree_sitter;

pub use hazards::TreeHazards;
pub use positions::{LineIndex, OutputPosition, TextRange};
pub use source::{CodeSource, ParseOutcome};
