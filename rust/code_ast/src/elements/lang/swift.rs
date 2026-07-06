//! Swift language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, CodeElementsTypeListConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct SwiftHooks;

impl LanguageHooks for SwiftHooks {
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

    fn extract_namespace_name(&self, _name_node: &tree_sitter::Node, _source: &[u8]) -> String {
        // Unreachable: Swift has no block-scoped namespace nodes.
        String::new()
    }

    fn extract_declaration_name(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> String {
        // A property's `name` field is a `pattern`; for a `protocol_property_declaration` it is a
        // value-binding pattern that includes the `var`/`let` keyword. Dig out the bound
        // `simple_identifier` rather than rendering the whole pattern text.
        if matches!(
            node.kind(),
            "property_declaration" | "protocol_property_declaration"
        ) {
            return node
                .child_by_field_name("name")
                .and_then(|pat| first_simple_identifier(&pat, source))
                .unwrap_or_default();
        }
        node.child_by_field_name(name_field)
            .and_then(|n| n.utf8_text(source).ok())
            .unwrap_or("")
            .to_string()
    }

    fn refine_declaration_kind(
        &self,
        node: &tree_sitter::Node,
        static_kind: DeclarationKind,
        _source: &[u8],
    ) -> DeclarationKind {
        // `class_declaration` covers class / struct / enum / extension / actor — the keyword is
        // held in the `declaration_kind` field.
        if node.kind() == "class_declaration" {
            return match node
                .child_by_field_name("declaration_kind")
                .map(|n| n.kind())
            {
                Some("struct") => DeclarationKind::Struct,
                Some("enum") => DeclarationKind::Enum,
                Some("extension") => DeclarationKind::Extension,
                // `class` and `actor` are both reference types → class.
                _ => DeclarationKind::Class,
            };
        }
        static_kind
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // `call_expression`'s callee is its first named child.
            "call_expression" => {
                let mut cursor = path_node.walk();
                match path_node.named_children(&mut cursor).next() {
                    Some(n) => self.extract_path(&n, source),
                    None => None,
                }
            }
            // `a.b` — `target` receiver + `suffix` (a `navigation_suffix` holding the member).
            "navigation_expression" => {
                let target = path_node.child_by_field_name("target");
                let suffix = path_node.child_by_field_name("suffix");
                let base_name = match suffix {
                    Some(n) => {
                        let mut cursor = n.walk();
                        n.named_children(&mut cursor)
                            .next()
                            .map(|c| c.utf8_text(source).unwrap_or("").to_string())
                            .unwrap_or_default()
                    }
                    None => String::new(),
                };
                let left_path = match target {
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
            // Inheritance: `inheritance_specifier` → `user_type` → type_identifier.
            "inheritance_specifier" | "user_type" => {
                let mut cursor = path_node.walk();
                match path_node.named_children(&mut cursor).next() {
                    Some(n) => self.extract_path(&n, source),
                    None => None,
                }
            }
            _ => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
        }
    }
}

/// Find the first `simple_identifier` within a Swift pattern node (the bound name).
fn first_simple_identifier(node: &tree_sitter::Node, source: &[u8]) -> Option<String> {
    if node.kind() == "simple_identifier" {
        return node.utf8_text(source).ok().map(|s| s.to_string());
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(|child| first_simple_identifier(&child, source))
}

/// Returns the default Swift language extractor configuration.
pub fn default_swift_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `class_declaration` covers `class` / `struct` / `enum` / `extension` / `actor` (refined by
    // the hook). `function_declaration` is a free function, promoted to `method` inside a type
    // scope; `protocol_function_declaration` is always a protocol member → `method`.
    // `property_declaration` / `protocol_property_declaration` (stored or computed `var`/`let`)
    // → property; the `name` field is a `pattern` whose text is the bound name.
    for (node_kind, kind) in [
        ("class_declaration", DeclarationKind::Class),
        ("protocol_declaration", DeclarationKind::Interface),
        ("function_declaration", DeclarationKind::Function),
        ("protocol_function_declaration", DeclarationKind::Method),
        ("property_declaration", DeclarationKind::Property),
        ("protocol_property_declaration", DeclarationKind::Property),
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

    let mut reference_node_kinds = HashMap::new();
    // The callee is the first positional child, so hand the whole node to the hook.
    reference_node_kinds.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: String::new(),
        },
    );

    // `class Foo: Bar` — each `inheritance_specifier` wraps a base `user_type`.
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert(
        "inheritance_specifier".to_string(),
        CodeElementsTypeListConfig {},
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("swift"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(SwiftHooks),
        exclude_reference_patterns: Vec::new(),
    }
}
