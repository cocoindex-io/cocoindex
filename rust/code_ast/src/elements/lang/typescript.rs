//! TypeScript / TSX language hooks and configuration for `CodeElementsExtractor`.
//!
//! The same hooks and node-kind maps drive both the `.ts` (`LANGUAGE_TYPESCRIPT`)
//! and `.tsx` (`LANGUAGE_TSX`) grammars; only the tree-sitter language differs.

use std::collections::HashMap;

use super::javascript::{
    extract_member_path, js_like_declarations, js_like_references, js_variable_names,
};
use crate::elements::{
    CodeElementsNamespaceConfig, CodeElementsReferenceConfig, CodeElementsTypeListConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct TypeScriptHooks;

impl LanguageHooks for TypeScriptHooks {
    fn separator(&self) -> &str {
        "."
    }

    fn get_initial_namespace(
        &self,
        _root: &tree_sitter::Node,
        _source: &[u8],
        base_namespace: Option<&str>,
    ) -> Vec<String> {
        match base_namespace {
            Some(ns) if !ns.is_empty() => vec![ns.to_string()],
            _ => vec![],
        }
    }

    fn extract_namespace_name(&self, name_node: &tree_sitter::Node, source: &[u8]) -> String {
        // `internal_module` (the `namespace X {}` / `module X {}` form) has an identifier name.
        name_node.utf8_text(source).unwrap_or("").to_string()
    }

    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        js_variable_names(self, node, name_field, source)
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // Built-in types (`number`, `string`, `boolean`, `void`, …) — exclude automatically.
            "predefined_type" => None,
            // A parameter's `type` field points at a `type_annotation` wrapping the real type.
            "type_annotation" => {
                let mut cursor = path_node.walk();
                let inner = path_node.named_children(&mut cursor).next();
                match inner {
                    Some(n) => self.extract_path(&n, source),
                    None => None,
                }
            }
            // Generic type like `Array<T>` — extract the base name only.
            "generic_type" => match path_node.child_by_field_name("name") {
                Some(n) => self.extract_path(&n, source),
                None => {
                    let text = path_node.utf8_text(source).unwrap_or("").to_string();
                    Some((text.clone(), text))
                }
            },
            _ => extract_member_path(self, path_node, source),
        }
    }
}

/// Build the shared node-kind config used by both `.ts` and `.tsx`.
fn typescript_config(language_name: &str) -> LanguageExtractorConfig {
    let declaration_node_kinds = js_like_declarations(&[
        ("abstract_class_declaration", DeclarationKind::Class),
        ("interface_declaration", DeclarationKind::Interface),
        ("enum_declaration", DeclarationKind::Enum),
        ("type_alias_declaration", DeclarationKind::TypeAlias),
        ("method_signature", DeclarationKind::Method),
        ("abstract_method_signature", DeclarationKind::Method),
        ("function_signature", DeclarationKind::Function),
        // Interface property members (`{ name: string }`) → property.
        ("property_signature", DeclarationKind::Property),
        // Class fields (`x: number = 0`) → field (single name, in the `name` field).
        ("public_field_definition", DeclarationKind::Field),
    ]);

    let mut reference_node_kinds = js_like_references();
    // Parameter type annotations: `x: DataRow`.
    for kind in ["required_parameter", "optional_parameter"] {
        reference_node_kinds.insert(
            kind.to_string(),
            CodeElementsReferenceConfig {
                path_expr_field: "type".to_string(),
            },
        );
    }

    // `extends_clause` and `implements_clause` each list base types as named children.
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert("extends_clause".to_string(), CodeElementsTypeListConfig {});
    type_list_node_kinds.insert(
        "implements_clause".to_string(),
        CodeElementsTypeListConfig {},
    );

    // `namespace X {}` / `module X {}` introduce a scope (`internal_module`).
    let mut namespace_node_kinds = HashMap::new();
    namespace_node_kinds.insert(
        "internal_module".to_string(),
        CodeElementsNamespaceConfig {
            name_field: "name".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info(language_name),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds,
        hooks: Box::new(TypeScriptHooks),
        exclude_reference_patterns: Vec::new(),
    }
}

/// Returns the default TypeScript (`.ts`) language extractor configuration.
pub fn default_typescript_config() -> LanguageExtractorConfig {
    typescript_config("typescript")
}

/// Returns the default TSX (`.tsx`) language extractor configuration.
pub fn default_tsx_config() -> LanguageExtractorConfig {
    typescript_config("tsx")
}
