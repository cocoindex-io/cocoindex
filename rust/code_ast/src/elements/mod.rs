//! Per-language structural knowledge: declaration/namespace/reference tables,
//! language hooks, and the code-element model types.
//!
//! These are *facts about the pinned grammars* (node kinds, field names, extraction
//! and classification shapes), colocated with the grammar registry so a grammar
//! version bump touches one crate. Consumers: the code-element extractor
//! (`cocoindex_ops_text::ast`) and the structural classification in [`crate::view`].

mod config;
mod hooks;
mod lang;
mod types;

pub use config::{
    CodeElementsDeclarationConfig, CodeElementsLanguageConfig, CodeElementsNamespaceConfig,
    CodeElementsReferenceConfig, CodeElementsTypeListConfig, CompiledLanguageConfig,
    ExtractorConfig, LanguageExtractorConfig,
};
pub use hooks::LanguageHooks;
pub use types::{CodeElements, Declaration, DeclarationKind, Reference};
