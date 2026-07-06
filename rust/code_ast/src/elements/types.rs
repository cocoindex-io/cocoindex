//! Public output types for code-element extraction.

use serde::{Deserialize, Serialize};

use crate::positions::OutputPosition;

// ── Public output types ────────────────────────────────────────────────────

/// Normalized, cross-language classification of a [`Declaration`].
///
/// Unlike `ast_node_kind` (the raw TreeSitter node type, which differs per grammar), `kind`
/// is a stable vocabulary shared across all languages. It is computed in three layers:
/// a per-node-type default ([`CodeElementsDeclarationConfig::kind`]); an optional language
/// refinement ([`LanguageHooks::refine_declaration_kind`], for grammars where one node type
/// covers several kinds, e.g. Go `type_spec` or Swift `class_declaration`); and engine-level
/// promotion of a free kind to its member form inside a type scope (`function` → `method`,
/// `variable` → `field`).
///
/// Every variant is emitted by at least one built-in language config; `others` is the
/// catch-all for user-supplied configs that don't set a kind. New kinds are introduced
/// alongside the extraction that produces them (e.g. `destructor`, `module`), rather than
/// reserved ahead of use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclarationKind {
    Class,
    Interface,
    Struct,
    Union,
    Enum,
    Trait,
    TypeAlias,
    Function,
    Method,
    Constructor,
    Property,
    Constant,
    Variable,
    Field,
    /// Adds members to an existing type: Rust `impl`, Swift `extension`, Objective-C category.
    Extension,
    /// Anything not covered by a more specific kind.
    #[default]
    Others,
}

impl DeclarationKind {
    /// Stable snake_case string form (matches the serde representation), for consumers that
    /// want a plain string (e.g. the Python binding).
    pub fn as_str(self) -> &'static str {
        match self {
            DeclarationKind::Class => "class",
            DeclarationKind::Interface => "interface",
            DeclarationKind::Struct => "struct",
            DeclarationKind::Union => "union",
            DeclarationKind::Enum => "enum",
            DeclarationKind::Trait => "trait",
            DeclarationKind::TypeAlias => "type_alias",
            DeclarationKind::Function => "function",
            DeclarationKind::Method => "method",
            DeclarationKind::Constructor => "constructor",
            DeclarationKind::Property => "property",
            DeclarationKind::Constant => "constant",
            DeclarationKind::Variable => "variable",
            DeclarationKind::Field => "field",
            DeclarationKind::Extension => "extension",
            DeclarationKind::Others => "others",
        }
    }

    /// Whether declarations directly inside this one are members — so a child `function`/
    /// `variable` is promoted to `method`/`field`. True for type-like scopes and extensions.
    pub fn is_type_scope(self) -> bool {
        matches!(
            self,
            DeclarationKind::Class
                | DeclarationKind::Interface
                | DeclarationKind::Struct
                | DeclarationKind::Union
                | DeclarationKind::Enum
                | DeclarationKind::Trait
                | DeclarationKind::Extension
        )
    }

    /// Member form of a free kind, applied when this declaration sits directly in a type scope.
    pub fn as_member(self) -> DeclarationKind {
        match self {
            DeclarationKind::Function => DeclarationKind::Method,
            DeclarationKind::Variable => DeclarationKind::Field,
            other => other,
        }
    }

    /// Whether this declaration is a callable whose body holds *local* declarations. Declarations
    /// nested inside one of these (a nested function, a local variable/class) are scoped to the
    /// function body and are dropped from the output — the extractor indexes module / namespace /
    /// type-level declarations only.
    pub fn is_function_like(self) -> bool {
        matches!(
            self,
            DeclarationKind::Function | DeclarationKind::Method | DeclarationKind::Constructor
        )
    }

    /// Whether this is a leaf value declaration (`field` / `variable` / `constant`): it binds one
    /// or more names but holds no nested declarations, so the engine emits one declaration per
    /// bound name and does not push it onto the entity stack.
    pub fn is_value_leaf(self) -> bool {
        matches!(
            self,
            DeclarationKind::Field | DeclarationKind::Variable | DeclarationKind::Constant
        )
    }
}

/// A structural declaration found in source code (class, function, method, etc.).
#[derive(Debug, Clone)]
pub struct Declaration {
    /// Namespace at the point of declaration (e.g. `"MyApp.Services"`).
    pub namespace: String,
    /// Fully qualified entity name within its namespace (e.g. `"OrderService.PlaceOrder"`).
    pub entity_name: String,
    /// Entity name of the enclosing declaration, if any.
    pub parent_entity_name: Option<String>,
    /// Simple (unqualified) name of this declaration.
    pub base_name: String,
    /// Normalized cross-language classification (e.g. `class`, `method`, `type_alias`).
    pub kind: DeclarationKind,
    /// The TreeSitter node type (e.g. `"class_declaration"`).
    pub ast_node_kind: String,
    /// Whether the declaration has a meaningful body.
    pub has_body: bool,
    /// Start position of the declaration node.
    pub start: OutputPosition,
    /// End position of the declaration node.
    pub end: OutputPosition,
}

/// A call or type-instantiation reference found in source code.
#[derive(Debug, Clone)]
pub struct Reference {
    /// Namespace at the point of the reference.
    pub namespace: String,
    /// Entity name of the enclosing declaration, if any.
    pub parent_entity_name: Option<String>,
    /// Simple (unqualified) name of the referenced entity.
    pub referenced_base_name: String,
    /// Fully qualified path of the referenced entity (e.g. `"helper.Process"`).
    pub referenced_full_path: String,
    /// The TreeSitter node type (e.g. `"invocation_expression"`).
    pub ast_node_kind: String,
    /// Start position of the reference node.
    pub start: OutputPosition,
    /// End position of the reference node.
    pub end: OutputPosition,
}

/// All code elements extracted from a source file.
#[derive(Debug, Clone, Default)]
pub struct CodeElements {
    pub declarations: Vec<Declaration>,
    pub references: Vec<Reference>,
}
