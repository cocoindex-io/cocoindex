//! Kotlin language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, CodeElementsTypeListConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct KotlinHooks;

impl LanguageHooks for KotlinHooks {
    fn separator(&self) -> &str {
        "."
    }

    fn get_initial_namespace(
        &self,
        root: &tree_sitter::Node,
        source: &[u8],
        base_namespace: Option<&str>,
    ) -> Vec<String> {
        // `package com.app` seeds the namespace; fall back to a caller-supplied base.
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "package_header" {
                let mut inner = child.walk();
                if let Some(name) = child.named_children(&mut inner).next() {
                    let text = name.utf8_text(source).unwrap_or("").to_string();
                    if !text.is_empty() {
                        return vec![text];
                    }
                }
            }
        }
        match base_namespace {
            Some(ns) if !ns.is_empty() => vec![ns.to_string()],
            _ => vec![],
        }
    }

    fn extract_namespace_name(&self, _name_node: &tree_sitter::Node, _source: &[u8]) -> String {
        // Unreachable: Kotlin has no block-scoped namespace nodes.
        String::new()
    }

    fn extract_declaration_name(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> String {
        // A `property_declaration` (`val x` / `var x`) carries its name in a nested
        // `variable_declaration` → `identifier`, not a `name` field.
        if node.kind() == "property_declaration" {
            let mut cursor = node.walk();
            if let Some(var_decl) = node
                .named_children(&mut cursor)
                .find(|c| c.kind() == "variable_declaration")
            {
                let mut inner = var_decl.walk();
                return var_decl
                    .named_children(&mut inner)
                    .find(|c| c.kind() == "identifier")
                    .and_then(|c| c.utf8_text(source).ok())
                    .unwrap_or("")
                    .to_string();
            }
            return String::new();
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
        source: &[u8],
    ) -> DeclarationKind {
        // `class_declaration` covers `class`, `interface` (incl. `fun interface`), and
        // `enum class`. The `interface` keyword and the `enum_class_body` child disambiguate.
        if node.kind() == "class_declaration" {
            let mut cursor = node.walk();
            let mut is_interface = false;
            let mut is_enum = false;
            for child in node.children(&mut cursor) {
                match child.kind() {
                    "interface" => is_interface = true,
                    "enum_class_body" => is_enum = true,
                    _ => {}
                }
            }
            return if is_interface {
                DeclarationKind::Interface
            } else if is_enum {
                DeclarationKind::Enum
            } else {
                DeclarationKind::Class
            };
        }
        // `const val X = …` is a compile-time constant → constant, not property. The `const`
        // keyword is a `property_modifier` inside the `modifiers` child.
        if node.kind() == "property_declaration" {
            let mut cursor = node.walk();
            let is_const = node.children(&mut cursor).any(|c| {
                c.kind() == "modifiers" && {
                    let mut mc = c.walk();
                    c.children(&mut mc)
                        .any(|m| m.utf8_text(source).map(|t| t == "const").unwrap_or(false))
                }
            });
            if is_const {
                return DeclarationKind::Constant;
            }
        }
        static_kind
    }

    fn check_has_body(
        &self,
        node: &tree_sitter::Node,
        _body_field: Option<&str>,
        _source: &[u8],
    ) -> bool {
        // Kotlin bodies are positional children, not named fields.
        let mut cursor = node.walk();
        node.named_children(&mut cursor)
            .any(|c| matches!(c.kind(), "function_body" | "class_body" | "enum_class_body"))
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
            // `a.b` — first named child is the receiver, last is the member.
            "navigation_expression" => {
                let mut cursor = path_node.walk();
                let children: Vec<_> = path_node.named_children(&mut cursor).collect();
                if children.is_empty() {
                    return None;
                }
                let member_node = children[children.len() - 1];
                let base_name = member_text(&member_node, source);
                let left_path = self.extract_path(&children[0], source)?.0;
                let full_path = if left_path.is_empty() || left_path == base_name {
                    base_name.clone()
                } else {
                    format!("{left_path}.{base_name}")
                };
                Some((full_path, base_name))
            }
            // Inheritance: `delegation_specifier` → `user_type` → identifier.
            "delegation_specifier" | "user_type" => {
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

/// Text of a `navigation_expression`'s member node, unwrapping a `navigation_suffix` if present.
fn member_text(node: &tree_sitter::Node, source: &[u8]) -> String {
    if node.kind() == "navigation_suffix" {
        let mut cursor = node.walk();
        node.named_children(&mut cursor)
            .next()
            .map(|n| n.utf8_text(source).unwrap_or("").to_string())
            .unwrap_or_default()
    } else {
        node.utf8_text(source).unwrap_or("").to_string()
    }
}

/// Returns the default Kotlin language extractor configuration.
pub fn default_kotlin_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `class_declaration` covers `class`, `interface`, and `enum class` (refined by the hook);
    // `object_declaration` (singleton) maps to `class`; `function_declaration` is a free
    // function, promoted to `method` inside a class/interface/object/enum body.
    // `property_declaration` (`val`/`var`) → property; its name is dug out by
    // `extract_declaration_name`.
    for (node_kind, kind) in [
        ("class_declaration", DeclarationKind::Class),
        ("object_declaration", DeclarationKind::Class),
        ("function_declaration", DeclarationKind::Function),
        ("property_declaration", DeclarationKind::Property),
    ] {
        declaration_node_kinds.insert(
            node_kind.to_string(),
            CodeElementsDeclarationConfig {
                name_field: "name".to_string(),
                // Body is positional; `check_has_body` is overridden, so this is unused.
                body_field: None,
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

    // `class Foo : Bar` — `delegation_specifiers` lists base types as named children.
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert(
        "delegation_specifiers".to_string(),
        CodeElementsTypeListConfig {},
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("kotlin"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(KotlinHooks),
        exclude_reference_patterns: Vec::new(),
    }
}
