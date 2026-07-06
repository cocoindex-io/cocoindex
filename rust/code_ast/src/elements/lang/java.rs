//! Java language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, CodeElementsTypeListConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct JavaHooks;

impl LanguageHooks for JavaHooks {
    fn separator(&self) -> &str {
        "."
    }

    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        // `int x, y;` → one `field_declaration` with several `declarator` (`variable_declarator`)
        // fields, each carrying a `name`.
        if node.kind() == "field_declaration" {
            let mut names = Vec::new();
            let mut cursor = node.walk();
            if cursor.goto_first_child() {
                loop {
                    if cursor.field_name() == Some("declarator") {
                        if let Some(name_node) = cursor.node().child_by_field_name("name") {
                            let name = name_node.utf8_text(source).unwrap_or("").to_string();
                            if !name.is_empty() {
                                names.push((name, name_node.start_byte(), name_node.end_byte()));
                            }
                        }
                    }
                    if !cursor.goto_next_sibling() {
                        break;
                    }
                }
            }
            return names;
        }
        vec![(
            self.extract_declaration_name(node, name_field, source),
            node.start_byte(),
            node.end_byte(),
        )]
    }

    fn refine_declaration_kind(
        &self,
        node: &tree_sitter::Node,
        static_kind: DeclarationKind,
        source: &[u8],
    ) -> DeclarationKind {
        // A `static final` field is a compile-time constant → constant, not field.
        if node.kind() == "field_declaration" {
            let mut cursor = node.walk();
            if let Some(mods) = node.children(&mut cursor).find(|c| c.kind() == "modifiers") {
                let text = mods.utf8_text(source).unwrap_or("");
                if text.contains("static") && text.contains("final") {
                    return DeclarationKind::Constant;
                }
            }
        }
        static_kind
    }

    fn get_initial_namespace(
        &self,
        root: &tree_sitter::Node,
        source: &[u8],
        _base_namespace: Option<&str>,
    ) -> Vec<String> {
        // `package com.app;` at the top of the file seeds the namespace.
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "package_declaration" {
                let mut inner = child.walk();
                if let Some(name) = child.named_children(&mut inner).next() {
                    let text = name.utf8_text(source).unwrap_or("").to_string();
                    if !text.is_empty() {
                        return vec![text];
                    }
                }
            }
        }
        vec![]
    }

    fn extract_namespace_name(&self, name_node: &tree_sitter::Node, source: &[u8]) -> String {
        // Unreachable: Java has no block-scoped namespace nodes (package handled above).
        name_node.utf8_text(source).unwrap_or("").to_string()
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // Primitive types are distinct node kinds, never `type_identifier` — exclude them.
            "void_type" | "integral_type" | "floating_point_type" | "boolean_type" => None,
            // `obj.method(...)` — `object` (optional) + `name` siblings.
            "method_invocation" => {
                let object = path_node.child_by_field_name("object");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match object {
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
            // `a.b` field access chain.
            "field_access" => {
                let object = path_node.child_by_field_name("object");
                let field = path_node.child_by_field_name("field");
                let base_name = match field {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match object {
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
            // `pkg.Outer.Inner` in type position — full text as path, last segment as base name.
            "scoped_type_identifier" => {
                let full = path_node.utf8_text(source).unwrap_or("").to_string();
                let base_name = full.rsplit('.').next().unwrap_or(&full).trim().to_string();
                Some((full, base_name))
            }
            // `List<DataRow>` — extract the base type name only (first named child).
            "generic_type" => {
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

/// Returns the default Java language extractor configuration.
pub fn default_java_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    for (node_kind, kind) in [
        ("class_declaration", DeclarationKind::Class),
        ("interface_declaration", DeclarationKind::Interface),
        ("enum_declaration", DeclarationKind::Enum),
        ("record_declaration", DeclarationKind::Class),
        ("method_declaration", DeclarationKind::Method),
        ("constructor_declaration", DeclarationKind::Constructor),
        // `int x, y;` fields → field (one per declarator, via `extract_declaration_names`).
        ("field_declaration", DeclarationKind::Field),
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
    // `method_invocation`'s callee is split across `object`/`name`, so hand the whole node
    // to the hook via an empty path field.
    reference_node_kinds.insert(
        "method_invocation".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: String::new(),
        },
    );
    reference_node_kinds.insert(
        "object_creation_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );
    reference_node_kinds.insert(
        "formal_parameter".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    // `extends Bar` (superclass → type_identifier) and `implements A, B` (super_interfaces →
    // type_list → type_identifiers).
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert("superclass".to_string(), CodeElementsTypeListConfig {});
    type_list_node_kinds.insert("type_list".to_string(), CodeElementsTypeListConfig {});

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("java"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(JavaHooks),
        // Java primitive types are distinct node kinds, excluded in `extract_path`.
        exclude_reference_patterns: Vec::new(),
    }
}
