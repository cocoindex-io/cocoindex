//! Go language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, DeclarationKind,
    LanguageExtractorConfig, LanguageHooks,
};

pub struct GoHooks;

impl LanguageHooks for GoHooks {
    fn separator(&self) -> &str {
        "."
    }

    fn get_initial_namespace(
        &self,
        _root: &tree_sitter::Node,
        _source: &[u8],
        base_namespace: Option<&str>,
    ) -> Vec<String> {
        // Go's package path is not spelled out as a nestable scope in source; the caller
        // supplies it (e.g. the import path) via `base_namespace`.
        match base_namespace {
            Some(ns) if !ns.is_empty() => vec![ns.to_string()],
            _ => vec![],
        }
    }

    fn extract_namespace_name(&self, _name_node: &tree_sitter::Node, _source: &[u8]) -> String {
        // Unreachable: Go has no block-scoped namespace nodes.
        String::new()
    }

    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        // `X, Y int` struct fields and `var X, Y = …` / `const A, B = …` specs all carry several
        // `name` fields on one node.
        if matches!(node.kind(), "field_declaration" | "var_spec" | "const_spec") {
            let mut names = Vec::new();
            let mut cursor = node.walk();
            if cursor.goto_first_child() {
                loop {
                    if cursor.field_name() == Some("name") {
                        let n = cursor.node();
                        let name = n.utf8_text(source).unwrap_or("").to_string();
                        if !name.is_empty() {
                            names.push((name, n.start_byte(), n.end_byte()));
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
        _source: &[u8],
    ) -> DeclarationKind {
        // `type_spec` covers `type Foo struct { … }` / `interface { … }` / defined types and
        // aliases — classify by the underlying type node in the `type` field.
        if node.kind() == "type_spec" {
            return match node.child_by_field_name("type").map(|t| t.kind()) {
                Some("struct_type") => DeclarationKind::Struct,
                Some("interface_type") => DeclarationKind::Interface,
                _ => DeclarationKind::TypeAlias,
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
            // `pkg.Func` in expression position, `pkg.Type` is `qualified_type` below.
            "selector_expression" => {
                let operand = path_node.child_by_field_name("operand");
                let field = path_node.child_by_field_name("field");
                let base_name = match field {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match operand {
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
            // `pkg.Type` in type position.
            "qualified_type" => {
                let package = path_node.child_by_field_name("package");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let pkg = match package {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let full_path = if pkg.is_empty() {
                    base_name.clone()
                } else {
                    format!("{pkg}.{base_name}")
                };
                Some((full_path, base_name))
            }
            // `*T`, `[]T` — unwrap to the element type (first named child).
            "pointer_type" | "slice_type" | "array_type" => {
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

/// Returns the default Go language extractor configuration.
pub fn default_go_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `type Foo struct/interface { ... }` — the type's shape lives in the `type` field, and
    // the kind is refined to struct/interface/type_alias by `refine_declaration_kind`.
    declaration_node_kinds.insert(
        "type_spec".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "name".to_string(),
            body_field: Some("type".to_string()),
            kind: DeclarationKind::TypeAlias,
        },
    );
    for (node_kind, kind) in [
        ("function_declaration", DeclarationKind::Function),
        ("method_declaration", DeclarationKind::Method),
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
    // Struct fields (`X, Y int`) → field; package-level `var`/`const` specs → variable / constant.
    // All bind one or more `name`s on a single node (via `extract_declaration_names`).
    for (node_kind, kind) in [
        ("field_declaration", DeclarationKind::Field),
        ("var_spec", DeclarationKind::Variable),
        ("const_spec", DeclarationKind::Constant),
    ] {
        declaration_node_kinds.insert(
            node_kind.to_string(),
            CodeElementsDeclarationConfig {
                name_field: "name".to_string(),
                body_field: None,
                kind,
            },
        );
    }

    let mut reference_node_kinds = HashMap::new();
    reference_node_kinds.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    // `Widget{...}` struct literals reference the type.
    reference_node_kinds.insert(
        "composite_literal".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );
    // Parameter type references: `x DataRow`.
    reference_node_kinds.insert(
        "parameter_declaration".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("go"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds: HashMap::new(),
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(GoHooks),
        // Go's predeclared types are plain `type_identifier`s, indistinguishable from user types.
        exclude_reference_patterns: vec![
            r"int|int8|int16|int32|int64|uint|uint8|uint16|uint32|uint64|uintptr|float32|float64|complex64|complex128|string|bool|byte|rune|error|any".to_string(),
        ],
    }
}
