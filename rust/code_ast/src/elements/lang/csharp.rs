//! C# language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsNamespaceConfig, CodeElementsReferenceConfig,
    CodeElementsTypeListConfig, DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

/// Extract the plain identifier text from a node that may be a `generic_name` or plain identifier.
///
/// In tree-sitter-c-sharp, `generic_name` has positional children (no field names):
/// `identifier` followed by `type_argument_list`. This helper extracts the identifier text
/// regardless of whether the node is a `generic_name`, `identifier_name`, or `identifier`.
fn generic_name_identifier(node: &tree_sitter::Node, source: &[u8]) -> String {
    if node.kind() == "generic_name" {
        let mut cursor = node.walk();
        node.named_children(&mut cursor)
            .find(|n| n.kind() == "identifier")
            .and_then(|n| n.utf8_text(source).ok())
            .unwrap_or("")
            .to_string()
    } else {
        node.utf8_text(source).unwrap_or("").to_string()
    }
}

pub struct CSharpHooks;

impl LanguageHooks for CSharpHooks {
    fn separator(&self) -> &str {
        "."
    }

    fn extract_declaration_names(
        &self,
        node: &tree_sitter::Node,
        name_field: &str,
        source: &[u8],
    ) -> Vec<(String, usize, usize)> {
        // `public int X, Y;` → `field_declaration` → `variable_declaration` →
        // `variable_declarator`(name)…
        if node.kind() == "field_declaration" {
            let mut names = Vec::new();
            let mut cursor = node.walk();
            if let Some(var_decl) = node
                .named_children(&mut cursor)
                .find(|c| c.kind() == "variable_declaration")
            {
                let mut inner = var_decl.walk();
                for vd in var_decl.named_children(&mut inner) {
                    if vd.kind() != "variable_declarator" {
                        continue;
                    }
                    if let Some(name_node) = vd.child_by_field_name("name") {
                        let name = name_node.utf8_text(source).unwrap_or("").to_string();
                        if !name.is_empty() {
                            names.push((name, name_node.start_byte(), name_node.end_byte()));
                        }
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

    fn get_initial_namespace(
        &self,
        root: &tree_sitter::Node,
        source: &[u8],
        _base_namespace: Option<&str>,
    ) -> Vec<String> {
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "file_scoped_namespace_declaration" {
                if let Some(name_node) = child.child_by_field_name("name") {
                    return vec![self.extract_namespace_name(&name_node, source)];
                }
            }
        }
        vec![]
    }

    fn extract_namespace_name(&self, name_node: &tree_sitter::Node, source: &[u8]) -> String {
        match name_node.kind() {
            "qualified_name" => {
                let left = name_node.child_by_field_name("qualifier");
                let right = name_node.child_by_field_name("name");
                let left_str = match left {
                    Some(n) => self.extract_namespace_name(&n, source),
                    None => String::new(),
                };
                let right_str = match right {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                if left_str.is_empty() {
                    right_str
                } else {
                    format!("{left_str}.{right_str}")
                }
            }
            "identifier" | "identifier_name" => {
                name_node.utf8_text(source).unwrap_or("").to_string()
            }
            _ => name_node.utf8_text(source).unwrap_or("").to_string(),
        }
    }

    fn refine_declaration_kind(
        &self,
        node: &tree_sitter::Node,
        static_kind: DeclarationKind,
        source: &[u8],
    ) -> DeclarationKind {
        // `record struct Foo` is a value type → struct; `record` / `record class` → class.
        // The value/reference distinction is an anonymous `struct` keyword token child of
        // `record_declaration` (no named field exposes it).
        if node.kind() == "record_declaration" {
            let mut cursor = node.walk();
            if node.children(&mut cursor).any(|c| c.kind() == "struct") {
                return DeclarationKind::Struct;
            }
        }
        // A `const` field (`const int X = 5;`) is a compile-time constant → constant, not field.
        if node.kind() == "field_declaration" {
            let mut cursor = node.walk();
            let is_const = node.children(&mut cursor).any(|c| {
                c.kind() == "modifier" && c.utf8_text(source).map(|t| t == "const").unwrap_or(false)
            });
            if is_const {
                return DeclarationKind::Constant;
            }
        }
        static_kind
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            // Built-in types (int, string, bool, void, etc.) — exclude automatically.
            "predefined_type" => None,
            "member_access_expression" => {
                let expr = path_node.child_by_field_name("expression");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => generic_name_identifier(&n, source),
                    None => String::new(),
                };
                let left_path = match expr {
                    Some(n) => {
                        let (full, _) = self.extract_path(&n, source)?;
                        full
                    }
                    None => String::new(),
                };
                let full_path = if left_path.is_empty() {
                    base_name.clone()
                } else {
                    format!("{left_path}.{base_name}")
                };
                Some((full_path, base_name))
            }
            // Type-context qualified name: `SqlMapper.TypeHandler` in base lists / type positions.
            // Fields are `qualifier` (left) and `name` (right), analogous to member_access_expression.
            "qualified_name" => {
                let qualifier = path_node.child_by_field_name("qualifier");
                let name = path_node.child_by_field_name("name");
                let base_name = match name {
                    Some(n) => generic_name_identifier(&n, source),
                    None => String::new(),
                };
                let left_path = match qualifier {
                    Some(n) => {
                        let (full, _) = self.extract_path(&n, source)?;
                        full
                    }
                    None => String::new(),
                };
                let full_path = if left_path.is_empty() {
                    base_name.clone()
                } else {
                    format!("{left_path}.{base_name}")
                };
                Some((full_path, base_name))
            }
            // Wrapper types: unwrap to inner type via the "type" field.
            // array_type:    `DataRow[]`  → `DataRow`
            // nullable_type: `SomeType?`  → `SomeType`
            "array_type" | "nullable_type" => {
                let inner = path_node.child_by_field_name("type");
                match inner {
                    Some(n) => self.extract_path(&n, source),
                    None => {
                        let text = path_node.utf8_text(source).unwrap_or("").to_string();
                        Some((text.clone(), text))
                    }
                }
            }
            "identifier_name" | "identifier" => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
            // generic_name in tree-sitter-c-sharp has positional children (no field names):
            // identifier + type_argument_list. Extract the identifier.
            "generic_name" => {
                let name = {
                    let mut cursor = path_node.walk();
                    path_node
                        .named_children(&mut cursor)
                        .find(|n| n.kind() == "identifier")
                        .and_then(|n| n.utf8_text(source).ok())
                        .unwrap_or("")
                        .to_string()
                };
                Some((name.clone(), name))
            }
            _ => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
        }
    }
}

/// Returns the default C# language extractor configuration.
pub fn default_csharp_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `record` / `record class` (reference types) → class; `record struct` (value type) is
    // refined to `struct` by `refine_declaration_kind`.
    for (node_kind, kind) in [
        ("class_declaration", DeclarationKind::Class),
        ("struct_declaration", DeclarationKind::Struct),
        ("interface_declaration", DeclarationKind::Interface),
        ("enum_declaration", DeclarationKind::Enum),
        ("record_declaration", DeclarationKind::Class),
        ("method_declaration", DeclarationKind::Method),
        ("constructor_declaration", DeclarationKind::Constructor),
        // Auto- and full properties; `has_body` is false (no `body` field, only `accessors`).
        ("property_declaration", DeclarationKind::Property),
        // `int X, Y;` member fields → field (one per declarator, via `extract_declaration_names`).
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
    reference_node_kinds.insert(
        "invocation_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    reference_node_kinds.insert(
        "object_creation_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );
    // Parameter type reference: `DataRow[] source` → emits `DataRow`
    reference_node_kinds.insert(
        "parameter".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    // Each named child of base_list is a base class/interface type reference.
    // Each named child of type_argument_list is a generic type argument reference.
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert("base_list".to_string(), CodeElementsTypeListConfig {});
    type_list_node_kinds.insert(
        "type_argument_list".to_string(),
        CodeElementsTypeListConfig {},
    );

    let mut namespace_node_kinds = HashMap::new();
    namespace_node_kinds.insert(
        "namespace_declaration".to_string(),
        CodeElementsNamespaceConfig {
            name_field: "name".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("csharp"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds,
        namespace_node_kinds,
        hooks: Box::new(CSharpHooks),
        // C# built-in types are handled via `predefined_type` node detection in extract_path.
        exclude_reference_patterns: Vec::new(),
    }
}
