//! C and C++ language hooks and configuration for `CodeElementsExtractor`.
//!
//! The two grammars share enough structure (nested function declarators, primitive-type
//! nodes, `field_expression` member access) that one `CFamilyHooks` drives both; only the
//! declaration / reference / namespace maps differ between [`default_c_config`] and
//! [`default_cpp_config`].

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsNamespaceConfig, CodeElementsReferenceConfig,
    CodeElementsTypeListConfig, DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct CFamilyHooks;

impl LanguageHooks for CFamilyHooks {
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
        // C++ `namespace ns { ... }` — `name` is a `namespace_identifier`.
        name_node.utf8_text(source).unwrap_or("").to_string()
    }

    fn extract_declaration_name(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> String {
        // For `function_definition`, `name_field` ("declarator") points at a `function_declarator`
        // whose own `declarator` field nests through pointers/references down to the identifier.
        // For type declarations it points straight at a `type_identifier`. `dig_declarator_name`
        // handles both.
        match node.child_by_field_name(name_field) {
            Some(child) => dig_declarator_name(&child, source),
            None => String::new(),
        }
    }

    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        if node.kind() == "field_declaration" {
            return field_declarator_names(node, source);
        }
        vec![(
            self.extract_declaration_name(node, name_field, source),
            node.start_byte(),
            node.end_byte(),
        )]
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // Built-in types (`int`, `void`, `unsigned long`, …) — exclude automatically.
            "primitive_type" | "sized_type_specifier" => None,
            // C++ base-class clauses include `public`/`private` keywords — not type references.
            "access_specifier" => None,
            // `obj.method` / `obj->method` member access.
            "field_expression" => {
                let argument = path_node.child_by_field_name("argument");
                let field = path_node.child_by_field_name("field");
                let base_name = match field {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match argument {
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
            // C++ `ns::Type` / `ns::func`.
            "qualified_identifier" => {
                let scope = path_node.child_by_field_name("scope");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => self.extract_path(&n, source)?.1,
                    None => String::new(),
                };
                let left_path = match scope {
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
            // C++ `vector<int>` — extract the base template name only.
            "template_type" => match path_node.child_by_field_name("name") {
                Some(n) => self.extract_path(&n, source),
                None => None,
            },
            _ => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
        }
    }
}

/// Follow `declarator` fields through pointer/reference/array/function declarators down to the
/// underlying name, returning its simple (unqualified) text.
///
/// Shared with the Objective-C hooks, whose C-derived `function_definition` / `struct_specifier`
/// declarations have the same nested-declarator shape.
pub(super) fn dig_declarator_name(node: &tree_sitter::Node, source: &[u8]) -> String {
    match node.kind() {
        "function_declarator"
        | "pointer_declarator"
        | "reference_declarator"
        | "parenthesized_declarator"
        | "array_declarator"
        | "init_declarator" => match node.child_by_field_name("declarator") {
            Some(inner) => dig_declarator_name(&inner, source),
            None => node.utf8_text(source).unwrap_or("").to_string(),
        },
        // Objective-C `@property` wraps its declarator in a `struct_declarator` whose inner
        // declarator (the first named child) holds the name.
        "struct_declarator" => {
            let mut cursor = node.walk();
            match node.named_children(&mut cursor).next() {
                Some(inner) => dig_declarator_name(&inner, source),
                None => node.utf8_text(source).unwrap_or("").to_string(),
            }
        }
        // `Foo::bar` out-of-line definition — keep just the final segment.
        "qualified_identifier" => {
            let full = node.utf8_text(source).unwrap_or("");
            full.rsplit("::").next().unwrap_or(full).to_string()
        }
        _ => node.utf8_text(source).unwrap_or("").to_string(),
    }
}

/// Names bound by a C-family `field_declaration` (`int x, y;` → two), each paired with the byte
/// span of its declarator. Iterates the (possibly repeated) `declarator` field. Shared with the
/// Objective-C hooks, whose C-derived struct fields have the same shape.
pub(super) fn field_declarator_names(
    node: &tree_sitter::Node,
    source: &[u8],
) -> Vec<(String, usize, usize)> {
    let mut names = Vec::new();
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            if cursor.field_name() == Some("declarator") {
                let d = cursor.node();
                let name = dig_declarator_name(&d, source);
                if !name.is_empty() {
                    names.push((name, d.start_byte(), d.end_byte()));
                }
            }
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
    names
}

/// Returns the default C language extractor configuration.
pub fn default_c_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    for (node_kind, kind) in [
        ("struct_specifier", DeclarationKind::Struct),
        ("union_specifier", DeclarationKind::Union),
        ("enum_specifier", DeclarationKind::Enum),
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
    declaration_node_kinds.insert(
        "function_definition".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "declarator".to_string(),
            body_field: Some("body".to_string()),
            kind: DeclarationKind::Function,
        },
    );
    // `int x, y;` struct/union fields → field (one per declarator, via `extract_declaration_names`).
    declaration_node_kinds.insert(
        "field_declaration".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "declarator".to_string(),
            body_field: None,
            kind: DeclarationKind::Field,
        },
    );

    let mut reference_node_kinds = HashMap::new();
    reference_node_kinds.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("c"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds: HashMap::new(),
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(CFamilyHooks),
        exclude_reference_patterns: Vec::new(),
    }
}

/// Returns the default C++ language extractor configuration.
pub fn default_cpp_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    for (node_kind, kind) in [
        ("class_specifier", DeclarationKind::Class),
        ("struct_specifier", DeclarationKind::Struct),
        ("union_specifier", DeclarationKind::Union),
        ("enum_specifier", DeclarationKind::Enum),
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
    // `function_definition` → Function (promoted to method inside a class/struct body).
    declaration_node_kinds.insert(
        "function_definition".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "declarator".to_string(),
            body_field: Some("body".to_string()),
            kind: DeclarationKind::Function,
        },
    );
    // Class/struct member fields → field (one per declarator, via `extract_declaration_names`).
    declaration_node_kinds.insert(
        "field_declaration".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "declarator".to_string(),
            body_field: None,
            kind: DeclarationKind::Field,
        },
    );

    let mut reference_node_kinds = HashMap::new();
    reference_node_kinds.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    // Parameter type references: `DataRow x`.
    reference_node_kinds.insert(
        "parameter_declaration".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    // `class Foo : public Bar` — `base_class_clause` lists base types (access keywords excluded
    // via `extract_path` returning `None` for `access_specifier`).
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert(
        "base_class_clause".to_string(),
        CodeElementsTypeListConfig {},
    );

    // `namespace ns { ... }` introduces a `::`-joined scope.
    let mut namespace_node_kinds = HashMap::new();
    namespace_node_kinds.insert(
        "namespace_definition".to_string(),
        CodeElementsNamespaceConfig {
            name_field: "name".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("cpp"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds,
        hooks: Box::new(CFamilyHooks),
        exclude_reference_patterns: Vec::new(),
    }
}
