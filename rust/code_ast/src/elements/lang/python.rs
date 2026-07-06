//! Python language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, DeclarationKind,
    LanguageExtractorConfig, LanguageHooks,
};

pub struct PythonHooks;

impl LanguageHooks for PythonHooks {
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
        // Unreachable: Python has no namespace_node_kinds.
        String::new()
    }

    fn check_has_body(
        &self,
        node: &tree_sitter::Node,
        body_field: Option<&str>,
        source: &[u8],
    ) -> bool {
        let body_field = match body_field {
            Some(f) => f,
            None => return false,
        };
        let body_node = match node.child_by_field_name(body_field) {
            Some(n) => n,
            None => return false,
        };

        // Iterate named children of the body block.
        // If every statement is a docstring or ellipsis expression_statement, has_body=false.
        let mut cursor = body_node.walk();
        let named_children: Vec<_> = body_node.named_children(&mut cursor).collect();

        if named_children.is_empty() {
            return false;
        }

        for child in &named_children {
            if !is_stub_statement(child, source) {
                return true;
            }
        }

        false
    }

    fn extract_path(
        &self,
        path_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Option<(String, String)> {
        match path_node.kind() {
            "attribute" => {
                let obj = path_node.child_by_field_name("object");
                let attr = path_node.child_by_field_name("attribute");
                let base_name = match attr {
                    Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                    None => String::new(),
                };
                let left_path = match obj {
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
            // Generic/parameterized type: `Optional[int]`, `List[DataRow]` — extract base only.
            "subscript" => {
                let value = path_node.child_by_field_name("value");
                match value {
                    Some(n) => self.extract_path(&n, source),
                    None => {
                        let text = path_node.utf8_text(source).unwrap_or("").to_string();
                        Some((text.clone(), text))
                    }
                }
            }
            "identifier" => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
            _ => {
                let text = path_node.utf8_text(source).unwrap_or("").to_string();
                Some((text.clone(), text))
            }
        }
    }

    fn extract_declaration_type_refs(
        &self,
        decl_node: &tree_sitter::Node,
        source: &[u8],
    ) -> Vec<(String, String, String, usize, usize)> {
        if decl_node.kind() != "class_definition" {
            return vec![];
        }
        let Some(superclasses) = decl_node.child_by_field_name("superclasses") else {
            return vec![];
        };
        let mut refs = Vec::new();
        let mut cursor = superclasses.walk();
        for child in superclasses.named_children(&mut cursor) {
            // Skip keyword arguments like `metaclass=ABCMeta`.
            if child.kind() == "keyword_argument" {
                continue;
            }
            let Some((full_path, base_name)) = self.extract_path(&child, source) else {
                continue;
            };
            if !full_path.is_empty() {
                refs.push((
                    full_path,
                    base_name,
                    "class_base".to_string(),
                    child.start_byte(),
                    child.end_byte(),
                ));
            }
        }
        refs
    }
}

/// Returns true if `node` is a stub statement (docstring or ellipsis expression_statement).
fn is_stub_statement(node: &tree_sitter::Node, _source: &[u8]) -> bool {
    if node.kind() != "expression_statement" {
        return false;
    }
    // An expression_statement has one named child: the expression itself.
    let mut cursor = node.walk();
    let children: Vec<_> = node.named_children(&mut cursor).collect();
    if children.len() != 1 {
        return false;
    }
    let expr = &children[0];
    match expr.kind() {
        "string" => true, // docstring
        "ellipsis" => true,
        _ => false,
    }
}

/// Returns the default Python language extractor configuration.
pub fn default_python_config() -> LanguageExtractorConfig {
    let mut declaration_node_kinds = HashMap::new();
    // `function_definition` is a free function here; the engine promotes it to `method` when it
    // sits directly inside a class body.
    for (node_kind, kind) in [
        ("class_definition", DeclarationKind::Class),
        ("function_definition", DeclarationKind::Function),
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
        "call".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    // Parameter type annotations: `x: DataRow` and `x: DataRow = None`
    reference_node_kinds.insert(
        "typed_parameter".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );
    reference_node_kinds.insert(
        "typed_default_parameter".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "type".to_string(),
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("python"),
        declaration_node_kinds,
        reference_node_kinds,
        type_list_node_kinds: HashMap::new(),
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(PythonHooks),
        // Python built-in types are indistinguishable from user-defined identifiers at the AST
        // level, so we exclude them via a default regex pattern.
        exclude_reference_patterns: vec![
            r"int|str|float|bool|list|dict|set|tuple|bytes|complex|object|None|type".to_string(),
        ],
    }
}
