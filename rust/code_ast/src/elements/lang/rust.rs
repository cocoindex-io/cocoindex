//! Rust language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsNamespaceConfig, CodeElementsReferenceConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct RustHooks;

impl LanguageHooks for RustHooks {
    fn separator(&self) -> &str {
        "::"
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
        // `mod m { ... }` — the name is a plain identifier.
        name_node.utf8_text(source).unwrap_or("").to_string()
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // Built-in scalar types (`i32`, `bool`, `str`, …) — exclude automatically.
            "primitive_type" => None,
            // `a::b::c` in expression or type position.
            "scoped_identifier" | "scoped_type_identifier" => {
                let path = path_node.child_by_field_name("path");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match path {
                    Some(n) => self.extract_path(&n, source)?.0,
                    None => String::new(),
                };
                let full_path = if left_path.is_empty() {
                    base_name.clone()
                } else {
                    format!("{left_path}::{base_name}")
                };
                Some((full_path, base_name))
            }
            // `value.field` / `value.method` — field/method access uses `.`.
            "field_expression" => {
                let value = path_node.child_by_field_name("value");
                let field = path_node.child_by_field_name("field");
                let base_name = match field {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match value {
                    Some(n) => self.extract_path(&n, source)?.0,
                    None => String::new(),
                };
                let full_path = if left_path.is_empty() {
                    base_name.clone()
                } else {
                    format!("{left_path}.{base_name}")
                };
                Some((full_path, base_name))
            }
            // `&T`, `&mut T` — unwrap to the referent type.
            "reference_type" => match path_node.child_by_field_name("type") {
                Some(n) => self.extract_path(&n, source),
                None => None,
            },
            // `Vec<T>` — extract the base type name only.
            "generic_type" => match path_node.child_by_field_name("type") {
                Some(n) => self.extract_path(&n, source),
                None => {
                    let text = path_node.utf8_text(source).unwrap_or("").to_string();
                    Some((text.clone(), text))
                }
            },
            _ => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
        }
    }
}

/// Returns the default Rust language extractor configuration.
pub fn default_rust_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `function_item` / `function_signature_item` are free functions, promoted to `method`
    // inside an `impl` (extension) or `trait` body.
    for (node_kind, kind) in [
        ("struct_item", DeclarationKind::Struct),
        ("enum_item", DeclarationKind::Enum),
        ("union_item", DeclarationKind::Union),
        ("trait_item", DeclarationKind::Trait),
        ("function_item", DeclarationKind::Function),
        ("function_signature_item", DeclarationKind::Function),
        // Struct fields (`x: i32`) — single name per declaration.
        ("field_declaration", DeclarationKind::Field),
        // `static X: T = …;` → variable; `const X: T = …;` → constant (incl. associated consts).
        ("static_item", DeclarationKind::Variable),
        ("const_item", DeclarationKind::Constant),
    ] {
        declaration_node_kinds.insert(
            node_kind.to_string(),
            CodeElementsDeclarationConfig {
                name_field: "name".to_string(),
                body_field: Some("body".to_string()),
                kind,
            },
        );
    }
    // `impl Type { ... }` associates its methods with `Type`; name comes from the `type` field
    // so nested methods get the `Type::method` entity path. It is an `extension` (a type scope),
    // so its `fn` items are promoted to `method`.
    declaration_node_kinds.insert(
        "impl_item".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "type".to_string(),
            body_field: Some("body".to_string()),
            kind: DeclarationKind::Extension,
        },
    );

    let mut reference_node_kinds = HashMap::new();
    reference_node_kinds.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    // Parameter type references: `x: DataRow`.
    reference_node_kinds.insert(
        "parameter".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    // `mod m { ... }` introduces a `::`-joined namespace scope.
    let mut namespace_node_kinds = HashMap::new();
    namespace_node_kinds.insert(
        "mod_item".to_string(),
        CodeElementsNamespaceConfig {
            name_field: "name".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("rust"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds: HashMap::new(),
        namespace_node_kinds,
        hooks: Box::new(RustHooks),
        // Built-in scalar types are handled via `primitive_type` node detection in extract_path.
        exclude_reference_patterns: Vec::new(),
    }
}
