//! The `LanguageHooks` trait: per-language procedural logic.

use crate::elements::types::DeclarationKind;

// ── LanguageHooks trait ────────────────────────────────────────────────────

/// Language-specific logic for namespace tracking, `has_body` determination,
/// and path-expression rendering.
pub trait LanguageHooks: Send + Sync {
    /// Separator used to join entity name components and namespace stack components.
    /// `"."` for C#, Python, Java, JS/TS/Go; `"::"` for C++, Rust.
    fn separator(&self) -> &str;

    /// Return initial namespace stack components before traversal.
    fn get_initial_namespace(
        &self,
        root: &tree_sitter::Node,
        source: &[u8],
        base_namespace: Option<&str>,
    ) -> Vec<String>;

    /// Render the name node of a block-scoped namespace declaration to a single string component.
    fn extract_namespace_name(&self, name_node: &tree_sitter::Node, source: &[u8]) -> String;

    /// Extract the simple (unqualified) name of a declaration node.
    ///
    /// The default reads the `name_field` named child as text, which is correct for grammars
    /// that expose the name directly (C#, Python, Java, JS/TS, Go, Rust, Swift, …). Languages
    /// whose name is nested behind another node (e.g. C/C++ `function_definition`, whose name
    /// lives inside a `function_declarator`) override this.
    fn extract_declaration_name(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> String {
        node.child_by_field_name(name_field)
            .and_then(|n| n.utf8_text(source).ok())
            .unwrap_or("")
            .to_string()
    }

    /// Extract every name bound by a leaf value declaration (`field` / `variable` / `constant`),
    /// each with its own byte span. Used by the engine to emit one declaration per name — a
    /// single `field_declaration` like `int x, y;` binds two names. The default returns the one
    /// name from [`Self::extract_declaration_name`] spanning the whole node; languages with
    /// multi-declarator fields (C/C++/C#/Go/Java/Objective-C) override it.
    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        vec![(
            self.extract_declaration_name(node, name_field, source),
            node.start_byte(),
            node.end_byte(),
        )]
    }

    /// Refine the normalized [`DeclarationKind`] for grammars where one AST node type covers
    /// several kinds — e.g. Go `type_spec` (struct / interface / type_alias) or Swift and
    /// Kotlin `class_declaration` (class / struct / enum / interface / extension). The default
    /// returns the config-supplied `static_kind` unchanged; engine-level member-promotion
    /// (`function` → `method`) is applied afterward and is not the hook's concern.
    fn refine_declaration_kind(
        &self,
        _node: &tree_sitter::Node,
        static_kind: DeclarationKind,
        _source: &[u8],
    ) -> DeclarationKind {
        static_kind
    }

    /// Determine `has_body` for a declaration node given the optional body field name.
    ///
    /// The default is "a body exists iff the configured `body_field` child is present", which
    /// holds for grammars that expose the body as a named field (C#, Java, JS/TS, Go, Rust,
    /// Swift, C/C++). Languages whose body is a positional child (Kotlin) or whose body may be
    /// present-but-empty (Python stubs) override this.
    fn check_has_body(
        &self,
        node: &tree_sitter::Node,
        body_field: Option<&str>,
        _source: &[u8],
    ) -> bool {
        body_field.is_some_and(|f| node.child_by_field_name(f).is_some())
    }

    /// Extract `(referenced_full_path, referenced_base_name)` from a path-expression node.
    /// Returns `None` if the node represents a built-in type that should be unconditionally
    /// excluded (e.g. C# `predefined_type`).
    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)>;

    /// Return type references embedded in a declaration node (e.g. Python base classes).
    ///
    /// Each element: `(full_path, base_name, ast_node_kind, start_byte, end_byte)`.
    /// Default implementation returns an empty vec (no extra refs).
    fn extract_declaration_type_refs(
        &self,
        _decl_node: &tree_sitter::Node,
        _source: &[u8],
    ) -> Vec<(String, String, String, usize, usize)> {
        vec![]
    }
}
