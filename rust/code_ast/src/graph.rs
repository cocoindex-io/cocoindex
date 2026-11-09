//! Conservative C/C++ syntax facts, without compiler name or type resolution.
//!
//! References preserve source spelling; they never claim to identify a target.
//! Preprocessor nodes (including conditional bodies), friend declarations,
//! and error subtrees are opaque. Unsupported declarator forms are omitted
//! rather than guessed.

use std::collections::HashMap;

use tree_sitter::Node;

use crate::{CodeSource, ParseOutcome};

/// A named declaration, with a source header suitable for syntactic identity.
#[derive(Debug)]
pub struct SyntaxDeclaration {
    pub kind: String,
    pub name: String,
    pub signature: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub parent_start_byte: Option<usize>,
}

/// An unresolved, explicitly written call or base-type reference.
#[derive(Debug)]
pub struct SyntaxReference {
    pub kind: String,
    pub spelling: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub source_start_byte: Option<usize>,
}

#[derive(Debug)]
pub struct GraphSyntax {
    pub declarations: Vec<SyntaxDeclaration>,
    pub references: Vec<SyntaxReference>,
    pub has_errors: bool,
}

struct Scope {
    qualification: String,
    owner: Option<usize>,
    class_members: bool,
}

fn text<'a>(node: Node<'_>, source: &'a str) -> &'a str {
    &source[node.byte_range()]
}

fn qualify(scope: &Scope, name: &str) -> String {
    if scope.qualification.is_empty() || name.starts_with("::") {
        name.to_owned()
    } else {
        format!("{}::{name}", scope.qualification)
    }
}

/// Find the declared name and the operator nearest it. In `(*f)(int)` that
/// operator is a pointer, whereas in `*f(int)` it is a function. Looking only
/// for a descendant function_declarator would mislabel function-pointer fields.
fn declarator_name(mut node: Node<'_>) -> Option<(Node<'_>, bool)> {
    let mut function = false;
    loop {
        match node.kind() {
            "identifier"
            | "field_identifier"
            | "qualified_identifier"
            | "destructor_name"
            | "operator_name"
            | "template_function"
            | "template_method" => {
                return Some((node, function));
            }
            "function_declarator" => function = true,
            "pointer_declarator" | "reference_declarator" | "array_declarator" => {
                function = false;
            }
            "init_declarator" | "parenthesized_declarator" => {}
            _ => return None,
        }
        node = node.child_by_field_name("declarator").or_else(|| {
            // Parenthesized/reference declarators have an unnamed child field.
            let mut cursor = node.walk();
            node.named_children(&mut cursor).find(|child| {
                child.kind().ends_with("declarator")
                    || matches!(child.kind(), "identifier" | "field_identifier")
            })
        })?;
    }
}

fn header_start(mut node: Node<'_>) -> usize {
    while let Some(parent) = node.parent()
        && parent.kind() == "template_declaration"
    {
        node = parent;
    }
    node.start_byte()
}

fn header_end(node: Node<'_>) -> usize {
    node.child_by_field_name("body")
        .map_or(node.end_byte(), |body| body.start_byte())
}

/// Inline type definitions and lambda default arguments can put implementation
/// bodies inside another declaration's header. Keep their written headers but
/// replace those nested bodies, so a field identity does not depend on a
/// method body inside its inline aggregate type.
fn header_text(mut node: Node<'_>, source: &str, start: usize, end: usize) -> String {
    // A function/class template's signature starts in its template parent.
    while node.start_byte() > start {
        let Some(parent) = node.parent() else {
            break;
        };
        node = parent;
    }
    let mut bodies = Vec::new();
    let mut pending = vec![node];
    while let Some(node) = pending.pop() {
        let body = if matches!(
            node.kind(),
            "class_specifier"
                | "struct_specifier"
                | "union_specifier"
                | "function_definition"
                | "lambda_expression"
        ) {
            node.child_by_field_name("body")
        } else {
            None
        };
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if child.end_byte() <= start || child.start_byte() >= end {
                continue;
            }
            if Some(child) == body && child.start_byte() >= start && child.end_byte() <= end {
                bodies.push(child.byte_range());
            } else {
                pending.push(child);
            }
        }
    }
    bodies.sort_unstable_by_key(|body| body.start);
    let mut signature = String::new();
    let mut position = start;
    for body in bodies {
        signature.push_str(&source[position..body.start]);
        signature.push_str("{}");
        position = body.end;
    }
    signature.push_str(&source[position..end]);
    signature
}

fn is_class_declaration(node: Node<'_>) -> bool {
    node.child_by_field_name("body").is_some()
        || node.parent().is_some_and(|parent| {
            // An elaborated type in `struct Item *value` or a parameter is a
            // type use, not another declaration of Item. Standalone forward
            // declarations are direct children of these grammar containers.
            matches!(
                parent.kind(),
                "translation_unit"
                    | "declaration_list"
                    | "field_declaration_list"
                    | "compound_statement"
                    | "template_declaration"
            )
        })
}

fn valid_header(node: Node<'_>) -> bool {
    let body = node.child_by_field_name("body");
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .filter(|child| Some(*child) != body)
        .all(|child| !child.has_error() && !child.is_missing())
}

fn push_reference(
    result: &mut GraphSyntax,
    kind: &str,
    node: Node<'_>,
    source: &str,
    owner: Option<usize>,
) {
    if !node.has_error() && !node.is_missing() {
        result.references.push(SyntaxReference {
            kind: kind.to_owned(),
            spelling: text(node, source).to_owned(),
            start_byte: node.start_byte(),
            end_byte: node.end_byte(),
            source_start_byte: owner,
        });
    }
}

/// Extract explicit syntax using the source's shared parser. Only C and C++
/// are supported. A parser failure is an error, not an empty successful graph.
///
/// Out-of-line qualified definitions retain their written qualification but
/// have kind `function`: syntax alone cannot distinguish a class qualifier
/// from a namespace qualifier. Only lexically enclosed methods use `method`.
pub fn extract_code_graph(source: &CodeSource<'_>) -> Result<GraphSyntax, String> {
    let language = source.info().map(|info| info.name.as_ref());
    if !matches!(language, Some("c" | "cpp")) {
        return Err("code graph extraction supports only C and C++".to_owned());
    }
    let ParseOutcome::Parsed(tree) = source.tree() else {
        return Err("C/C++ parser did not produce a syntax tree".to_owned());
    };
    let source_text = source.text();
    let mut result = GraphSyntax {
        declarations: Vec::new(),
        references: Vec::new(),
        has_errors: tree.root_node().has_error(),
    };
    let mut scopes = vec![Scope {
        qualification: String::new(),
        owner: None,
        class_members: false,
    }];
    // Explicit work stack: deeply nested input does not recurse on the Rust
    // stack. Scope strings are allocated only at namespace/declaration entries.
    let mut pending = vec![(tree.root_node(), 0)];
    while let Some((node, scope_index)) = pending.pop() {
        if node.is_error()
            || node.is_missing()
            || node.kind().starts_with("preproc_")
            || node.kind() == "friend_declaration"
        {
            continue;
        }
        let scope = &scopes[scope_index];
        let mut child_scope = scope_index;
        let mut declarator_scopes = HashMap::new();
        match node.kind() {
            "namespace_definition" => {
                let name = node
                    .child_by_field_name("name")
                    .map_or("(anonymous namespace)", |name| text(name, source_text));
                scopes.push(Scope {
                    qualification: qualify(scope, name),
                    owner: scope.owner,
                    class_members: false,
                });
                child_scope = scopes.len() - 1;
            }
            "class_specifier" | "struct_specifier" | "union_specifier"
                if is_class_declaration(node) =>
            {
                if !valid_header(node) {
                    continue;
                }
                if let Some(name) = node.child_by_field_name("name")
                    && !name.has_error()
                    && !name.is_missing()
                {
                    let qualified_name = qualify(scope, text(name, source_text));
                    result.declarations.push(SyntaxDeclaration {
                        kind: node.kind().trim_end_matches("_specifier").to_owned(),
                        name: qualified_name.clone(),
                        signature: header_text(
                            node,
                            source_text,
                            header_start(node),
                            header_end(node),
                        )
                        .trim()
                        .to_owned(),
                        start_byte: node.start_byte(),
                        end_byte: node.end_byte(),
                        parent_start_byte: scope.owner,
                    });
                    scopes.push(Scope {
                        qualification: qualified_name,
                        owner: Some(node.start_byte()),
                        class_members: true,
                    });
                    child_scope = scopes.len() - 1;
                } else {
                    // Anonymous aggregates have no named declaration, but their
                    // fields must not become members of an enclosing class.
                    scopes.push(Scope {
                        qualification: scope.qualification.clone(),
                        owner: None,
                        class_members: false,
                    });
                    child_scope = scopes.len() - 1;
                }
            }
            "function_definition" => {
                if !valid_header(node) {
                    continue;
                }
                if let Some(declarator) = node.child_by_field_name("declarator")
                    && !declarator.has_error()
                    && let Some((name, true)) = declarator_name(declarator)
                {
                    let qualified_name = qualify(scope, text(name, source_text));
                    result.declarations.push(SyntaxDeclaration {
                        kind: if scope.class_members {
                            "method"
                        } else {
                            "function"
                        }
                        .to_owned(),
                        name: qualified_name.clone(),
                        signature: header_text(
                            node,
                            source_text,
                            header_start(node),
                            header_end(node),
                        )
                        .trim()
                        .to_owned(),
                        start_byte: node.start_byte(),
                        end_byte: node.end_byte(),
                        parent_start_byte: scope.owner,
                    });
                    scopes.push(Scope {
                        qualification: qualified_name,
                        owner: Some(node.start_byte()),
                        class_members: false,
                    });
                    child_scope = scopes.len() - 1;
                } else {
                    // An unsupported declarator (for example a conversion
                    // operator) must not attribute its body calls to a class.
                    scopes.push(Scope {
                        qualification: scope.qualification.clone(),
                        owner: None,
                        class_members: false,
                    });
                    child_scope = scopes.len() - 1;
                }
            }
            "declaration" | "field_declaration" if !node.has_error() => {
                let mut cursor = node.walk();
                let declarators: Vec<_> = node
                    .children_by_field_name("declarator", &mut cursor)
                    .collect();
                if let Some(first) = declarators.first() {
                    let prefix =
                        header_text(node, source_text, header_start(node), first.start_byte());
                    for declarator in declarators {
                        let Some((name, function)) = declarator_name(declarator) else {
                            continue;
                        };
                        let field = node.kind() == "field_declaration" && scope.class_members;
                        if !function && !field {
                            continue;
                        }
                        let qualified_name = qualify(scope, text(name, source_text));
                        result.declarations.push(SyntaxDeclaration {
                            kind: if function {
                                if scope.class_members && language == Some("cpp") {
                                    "method"
                                } else {
                                    "function"
                                }
                            } else {
                                "field"
                            }
                            .to_owned(),
                            name: qualified_name.clone(),
                            signature: format!(
                                "{prefix}{}",
                                header_text(
                                    declarator,
                                    source_text,
                                    declarator.start_byte(),
                                    declarator.end_byte()
                                )
                            )
                            .trim()
                            .to_owned(),
                            start_byte: declarator.start_byte(),
                            end_byte: declarator.end_byte(),
                            parent_start_byte: scope.owner,
                        });
                        declarator_scopes.insert(
                            declarator.id(),
                            Scope {
                                qualification: qualified_name,
                                owner: Some(declarator.start_byte()),
                                class_members: false,
                            },
                        );
                    }
                }
            }
            "base_class_clause" => {
                let mut cursor = node.walk();
                for base in node.named_children(&mut cursor) {
                    if matches!(
                        base.kind(),
                        "type_identifier" | "qualified_identifier" | "template_type"
                    ) {
                        push_reference(&mut result, "inherits", base, source_text, scope.owner);
                    }
                }
            }
            "call_expression" if !node.has_error() => {
                if let Some(callee) = node.child_by_field_name("function") {
                    push_reference(&mut result, "call", callee, source_text, scope.owner);
                }
            }
            _ => {}
        }
        // Reverse source order in the LIFO stack for deterministic output.
        let mut cursor = node.walk();
        if cursor.goto_last_child() {
            loop {
                let child = cursor.node();
                if child.is_named() {
                    let scope_index = if let Some(scope) = declarator_scopes.remove(&child.id()) {
                        scopes.push(scope);
                        scopes.len() - 1
                    } else {
                        child_scope
                    };
                    pending.push((child, scope_index));
                }
                if !cursor.goto_previous_sibling() {
                    break;
                }
            }
        }
    }
    Ok(result)
}
