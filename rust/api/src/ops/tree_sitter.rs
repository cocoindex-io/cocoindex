//! Tree-sitter entity extraction — pure operation called from memoized closures.

use std::path::{Path, PathBuf};

use blake2::{Blake2b, Digest};
use blake2::digest::typenum;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Code entity extracted from source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub name: String,
    pub kind: EntityKind,
    pub visibility: Visibility,
    /// Everything except the body block (the API surface).
    pub signature: String,
    pub doc: Option<String>,
    /// Blake2b-128 of the body text (for change detection).
    pub body_hash: [u8; 16],
    pub source_file: PathBuf,
    /// 1-indexed line number.
    pub line: usize,
}

/// An import/use statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedItem {
    pub path: String,
    pub source_file: PathBuf,
}

/// All entities and imports extracted from one file.
#[derive(Debug, Clone)]
pub struct FileExtraction {
    pub file: PathBuf,
    pub entities: Vec<Entity>,
    pub imports: Vec<ImportedItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityKind {
    Function,
    Struct,
    Enum,
    Trait,
    Impl,
    TypeAlias,
    Const,
    Static,
    Method,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    Public,
    Crate,
    Super,
    Private,
}

#[derive(Debug, Clone, Copy)]
pub enum Language {
    Rust,
}

/// Extract code entities from source code.
pub fn extract(source: &str, path: &Path, language: Language) -> Result<FileExtraction> {
    match language {
        Language::Rust => extract_rust(source, path),
    }
}

/// Convenience: extract from Rust source.
pub fn extract_rust(source: &str, path: &Path) -> Result<FileExtraction> {
    use ::tree_sitter::Parser;

    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .expect("failed to load Rust grammar");

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::engine(format!("tree-sitter failed to parse {}", path.display())))?;

    let root = tree.root_node();
    let bytes = source.as_bytes();

    let mut entities = Vec::new();
    let mut imports = Vec::new();

    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        match child.kind() {
            "function_item" => {
                if let Some(e) = extract_function(&child, bytes, path, None) {
                    entities.push(e);
                }
            }
            "struct_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::Struct) {
                    entities.push(e);
                }
            }
            "enum_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::Enum) {
                    entities.push(e);
                }
            }
            "trait_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::Trait) {
                    entities.push(e);
                }
            }
            "impl_item" => {
                extract_impl(&child, bytes, path, &mut entities);
            }
            "type_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::TypeAlias) {
                    entities.push(e);
                }
            }
            "const_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::Const) {
                    entities.push(e);
                }
            }
            "static_item" => {
                if let Some(e) = extract_named_item(&child, bytes, path, EntityKind::Static) {
                    entities.push(e);
                }
            }
            "use_declaration" => {
                if let Some(imp) = extract_use(&child, bytes, path) {
                    imports.push(imp);
                }
            }
            _ => {}
        }
    }

    Ok(FileExtraction {
        file: path.to_path_buf(),
        entities,
        imports,
    })
}

// --- Internal helpers ---

fn node_text(node: &::tree_sitter::Node, bytes: &[u8]) -> String {
    node.utf8_text(bytes).unwrap_or("").to_string()
}

fn extract_visibility(node: &::tree_sitter::Node, bytes: &[u8]) -> Visibility {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "visibility_modifier" {
            let text = node_text(&child, bytes);
            return match text.as_str() {
                "pub" => Visibility::Public,
                "pub(crate)" => Visibility::Crate,
                "pub(super)" => Visibility::Super,
                _ if text.starts_with("pub") => Visibility::Public,
                _ => Visibility::Private,
            };
        }
    }
    Visibility::Private
}

fn extract_doc_comments(node: &::tree_sitter::Node, bytes: &[u8]) -> Option<String> {
    let mut comments = Vec::new();
    let mut sibling = node.prev_sibling();
    while let Some(s) = sibling {
        if s.kind() == "line_comment" {
            let text = node_text(&s, bytes);
            if text.starts_with("///") {
                comments.push(text.trim_start_matches("///").trim().to_string());
            } else {
                break;
            }
        } else if s.kind() == "attribute_item" || s.kind() == "inner_attribute_item" {
            sibling = s.prev_sibling();
            continue;
        } else {
            break;
        }
        sibling = s.prev_sibling();
    }
    if comments.is_empty() {
        return None;
    }
    comments.reverse();
    Some(comments.join("\n"))
}

fn compute_signature(node: &::tree_sitter::Node, bytes: &[u8]) -> String {
    if let Some(body) = node.child_by_field_name("body") {
        let sig_bytes = &bytes[node.start_byte()..body.start_byte()];
        String::from_utf8_lossy(sig_bytes).trim().to_string()
    } else {
        node_text(node, bytes)
    }
}

/// Blake2b-128 of the body block text.
fn compute_body_hash(node: &::tree_sitter::Node, bytes: &[u8]) -> [u8; 16] {
    let body_bytes = if let Some(body) = node.child_by_field_name("body") {
        &bytes[body.start_byte()..body.end_byte()]
    } else {
        &bytes[node.start_byte()..node.end_byte()]
    };
    let mut hasher = Blake2b::<typenum::U16>::new();
    hasher.update(body_bytes);
    hasher.finalize().into()
}

fn extract_function(
    node: &::tree_sitter::Node,
    bytes: &[u8],
    path: &Path,
    impl_type: Option<&str>,
) -> Option<Entity> {
    let name_node = node.child_by_field_name("name")?;
    let raw_name = node_text(&name_node, bytes);
    let name = match impl_type {
        Some(ty) => format!("{ty}::{raw_name}"),
        None => raw_name,
    };
    let kind = if impl_type.is_some() {
        EntityKind::Method
    } else {
        EntityKind::Function
    };
    Some(Entity {
        name,
        kind,
        visibility: extract_visibility(node, bytes),
        signature: compute_signature(node, bytes),
        doc: extract_doc_comments(node, bytes),
        body_hash: compute_body_hash(node, bytes),
        source_file: path.to_path_buf(),
        line: node.start_position().row + 1,
    })
}

fn extract_named_item(
    node: &::tree_sitter::Node,
    bytes: &[u8],
    path: &Path,
    kind: EntityKind,
) -> Option<Entity> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(&name_node, bytes);
    Some(Entity {
        name,
        kind,
        visibility: extract_visibility(node, bytes),
        signature: compute_signature(node, bytes),
        doc: extract_doc_comments(node, bytes),
        body_hash: compute_body_hash(node, bytes),
        source_file: path.to_path_buf(),
        line: node.start_position().row + 1,
    })
}

fn extract_impl(
    node: &::tree_sitter::Node,
    bytes: &[u8],
    path: &Path,
    entities: &mut Vec<Entity>,
) {
    let type_name = impl_type_name(node, bytes);
    entities.push(Entity {
        name: format!("impl {type_name}"),
        kind: EntityKind::Impl,
        visibility: Visibility::Public,
        signature: compute_signature(node, bytes),
        doc: extract_doc_comments(node, bytes),
        body_hash: compute_body_hash(node, bytes),
        source_file: path.to_path_buf(),
        line: node.start_position().row + 1,
    });

    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            if child.kind() == "function_item" {
                if let Some(e) = extract_function(&child, bytes, path, Some(&type_name)) {
                    entities.push(e);
                }
            }
        }
    }
}

fn impl_type_name(node: &::tree_sitter::Node, bytes: &[u8]) -> String {
    let type_node = node.child_by_field_name("type");
    let trait_node = node.child_by_field_name("trait");
    match (trait_node, type_node) {
        (Some(tr), Some(ty)) => format!("{} for {}", node_text(&tr, bytes), node_text(&ty, bytes)),
        (None, Some(ty)) => node_text(&ty, bytes),
        _ => "Unknown".to_string(),
    }
}

fn extract_use(
    node: &::tree_sitter::Node,
    bytes: &[u8],
    path: &Path,
) -> Option<ImportedItem> {
    let arg = node.child_by_field_name("argument")?;
    let use_path = node_text(&arg, bytes);
    Some(ImportedItem {
        path: use_path,
        source_file: path.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_RUST: &str = r#"
use std::collections::HashMap;

/// A sample struct.
#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub value: i32,
}

pub enum Kind {
    A,
    B(i32),
}

/// A trait for processing.
pub trait Process {
    fn run(&self) -> bool;
}

pub type Alias = HashMap<String, i32>;

pub const MAX_SIZE: usize = 1024;

pub static GLOBAL: &str = "hello";

impl Config {
    /// Create a new Config.
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), value: 0 }
    }
}

impl Process for Config {
    fn run(&self) -> bool {
        self.value > 0
    }
}

fn top_level_private() -> bool {
    true
}
"#;

    #[test]
    fn extract_rust_entities() {
        let result = extract_rust(SAMPLE_RUST, Path::new("test.rs")).unwrap();

        let names: Vec<&str> = result.entities.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Config"));
        assert!(names.contains(&"Kind"));
        assert!(names.contains(&"Process"));
        assert!(names.contains(&"Alias"));
        assert!(names.contains(&"MAX_SIZE"));
        assert!(names.contains(&"GLOBAL"));
        assert!(names.contains(&"impl Config"));
        assert!(names.contains(&"Config::new"));
        assert!(names.contains(&"impl Process for Config"));
        assert!(names.contains(&"top_level_private"));
    }

    #[test]
    fn extract_entity_body_hash() {
        let v1 = "pub fn foo() -> i32 { 1 }";
        let v2 = "pub fn foo() -> i32 { 2 }";

        let e1 = extract_rust(v1, Path::new("a.rs")).unwrap();
        let e2 = extract_rust(v2, Path::new("a.rs")).unwrap();

        assert_ne!(e1.entities[0].body_hash, e2.entities[0].body_hash);
        assert_eq!(e1.entities[0].signature, e2.entities[0].signature);
    }

    #[test]
    fn extract_signature_without_body() {
        let src = "pub fn add(a: i32, b: i32) -> i32 { a + b }";
        let result = extract_rust(src, Path::new("a.rs")).unwrap();
        let sig = &result.entities[0].signature;
        assert!(sig.contains("pub fn add(a: i32, b: i32) -> i32"));
        assert!(!sig.contains("a + b"));
    }

    #[test]
    fn body_hash_stable() {
        let src = "pub fn foo() -> i32 { 42 }";
        let e1 = extract_rust(src, Path::new("a.rs")).unwrap();
        let e2 = extract_rust(src, Path::new("a.rs")).unwrap();
        assert_eq!(e1.entities[0].body_hash, e2.entities[0].body_hash);
    }

    #[test]
    fn extract_visibility_kinds() {
        let result = extract_rust(SAMPLE_RUST, Path::new("test.rs")).unwrap();

        let config = result.entities.iter().find(|e| e.name == "Config" && e.kind == EntityKind::Struct).unwrap();
        assert_eq!(config.visibility, Visibility::Public);

        let private_fn = result.entities.iter().find(|e| e.name == "top_level_private").unwrap();
        assert_eq!(private_fn.visibility, Visibility::Private);
    }

    #[test]
    fn extract_imports() {
        let result = extract_rust(SAMPLE_RUST, Path::new("test.rs")).unwrap();
        assert_eq!(result.imports.len(), 1);
        assert_eq!(result.imports[0].path, "std::collections::HashMap");
    }

    #[test]
    fn extract_doc_comments() {
        let result = extract_rust(SAMPLE_RUST, Path::new("test.rs")).unwrap();
        let config = result.entities.iter().find(|e| e.name == "Config" && e.kind == EntityKind::Struct).unwrap();
        assert!(config.doc.as_deref().unwrap().contains("sample struct"));
    }
}
