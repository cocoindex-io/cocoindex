//! JavaScript language hooks and configuration for `CodeElementsExtractor`.

use std::collections::HashMap;

use crate::elements::{
    CodeElementsDeclarationConfig, CodeElementsReferenceConfig, CodeElementsTypeListConfig,
    DeclarationKind, LanguageExtractorConfig, LanguageHooks,
};

pub struct JavaScriptHooks;

impl LanguageHooks for JavaScriptHooks {
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
        // Unreachable: plain JavaScript has no namespace declarations.
        String::new()
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
        extract_member_path(self, path_node, source)
    }
}

/// Names bound by a JS/TS `lexical_declaration` (`const a = 1, b = 2`) or `variable_declaration`
/// (`var x`): one per `variable_declarator` with a plain `identifier` name (destructuring patterns
/// are skipped). Other declaration kinds fall back to the single configured name.
pub(super) fn js_variable_names<H: LanguageHooks + ?Sized>(
    hooks: &H,
    node: &tree_sitter::Node,
    name_field: &str,
    source: &[u8],
) -> Vec<(String, usize, usize)> {
    if matches!(node.kind(), "lexical_declaration" | "variable_declaration") {
        let mut names = Vec::new();
        let mut cursor = node.walk();
        for vd in node.named_children(&mut cursor) {
            if vd.kind() != "variable_declarator" {
                continue;
            }
            if let Some(name_node) = vd.child_by_field_name("name") {
                if name_node.kind() == "identifier" {
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
        hooks.extract_declaration_name(node, name_field, source),
        node.start_byte(),
        node.end_byte(),
    )]
}

/// Shared JS/TS member-access path rendering.
///
/// `member_expression` has fields `object` (left) and `property` (right). A leaf
/// `identifier` / `property_identifier` renders to its own text.
pub(super) fn extract_member_path<H: LanguageHooks + ?Sized>(
    hooks: &H,
    path_node: &tree_sitter::Node,
    source: &[u8],
) -> Option<(String, String)> {
    match path_node.kind() {
        "member_expression" => {
            let object = path_node.child_by_field_name("object");
            let property = path_node.child_by_field_name("property");
            let base_name = match property {
                Some(n) => n.utf8_text(source).unwrap_or("").to_string(),
                None => String::new(),
            };
            let left_path = match object {
                Some(n) => hooks.extract_path(&n, source)?.0,
                None => String::new(),
            };
            let full_path = if left_path.is_empty() {
                base_name.clone()
            } else {
                format!("{left_path}.{base_name}")
            };
            Some((full_path, base_name))
        }
        _ => {
            let text = path_node.utf8_text(source).unwrap_or("").to_string();
            Some((text.clone(), text))
        }
    }
}

/// Declaration node kinds shared by JS and TS (name + body both named fields), each paired
/// with its normalized [`DeclarationKind`]. `method_definition` is always a class member, so it
/// is `method` directly; `function_declaration` is a free function (the engine never promotes it
/// in JS/TS since functions don't nest directly in a class body).
pub(super) fn js_like_declarations(
    extra: &[(&str, DeclarationKind)],
) -> HashMap<String, CodeElementsDeclarationConfig> {
    let mut decls = HashMap::new();
    let base = [
        ("class_declaration", DeclarationKind::Class),
        ("function_declaration", DeclarationKind::Function),
        ("generator_function_declaration", DeclarationKind::Function),
        ("method_definition", DeclarationKind::Method),
        // Module-level `const`/`let` and `var` bindings → variable (locals inside function
        // bodies are dropped by the scope filter). `const` is binding immutability, not a
        // compile-time constant, so it is `variable` here, not `constant`.
        ("lexical_declaration", DeclarationKind::Variable),
        ("variable_declaration", DeclarationKind::Variable),
    ];
    for (node_kind, kind) in base.iter().copied().chain(extra.iter().copied()) {
        decls.insert(
            node_kind.to_string(),
            CodeElementsDeclarationConfig {
                name_field: "name".to_string(),
                body_field: Some("body".to_string()),
                kind,
            },
        );
    }
    decls
}

/// Reference node kinds shared by JS and TS (calls and `new` expressions).
pub(super) fn js_like_references() -> HashMap<String, CodeElementsReferenceConfig> {
    let mut refs = HashMap::new();
    refs.insert(
        "call_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "function".to_string(),
        },
    );
    refs.insert(
        "new_expression".to_string(),
        CodeElementsReferenceConfig {
            path_expr_field: "constructor".to_string(),
        },
    );
    refs
}

/// Returns the default JavaScript language extractor configuration.
pub fn default_javascript_config() -> LanguageExtractorConfig {
    // `class_heritage` directly wraps the superclass identifier in JS.
    let mut type_list_node_kinds = HashMap::new();
    type_list_node_kinds.insert("class_heritage".to_string(), CodeElementsTypeListConfig {});

    let mut declaration_node_kinds = js_like_declarations(&[]);
    // Class fields (`x = 1`) — the name is in the `property` field (single name per declaration).
    declaration_node_kinds.insert(
        "field_definition".to_string(),
        CodeElementsDeclarationConfig {
            name_field: "property".to_string(),
            body_field: None,
            kind: DeclarationKind::Field,
        },
    );

    LanguageExtractorConfig {
        info: crate::elements::config::registry_info("javascript"),
        declaration_node_kinds,
        reference_node_kinds: js_like_references(),
        type_list_node_kinds,
        namespace_node_kinds: HashMap::new(),
        hooks: Box::new(JavaScriptHooks),
        exclude_reference_patterns: Vec::new(),
    }
}
