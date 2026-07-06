//! Per-language and top-level extractor configuration.

use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::elements::hooks::LanguageHooks;
use crate::elements::lang;
use crate::elements::types::DeclarationKind;

// ── Config types ───────────────────────────────────────────────────────────

/// Configuration for a declaration AST node type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeElementsDeclarationConfig {
    /// Named child field that holds the declaration's base name.
    pub name_field: String,
    /// Named child field that holds the body, if any.
    pub body_field: Option<String>,
    /// Default normalized [`DeclarationKind`] for this node type, before language refinement
    /// and member-promotion. Defaults to [`DeclarationKind::Others`] when omitted from a spec.
    #[serde(default)]
    pub kind: DeclarationKind,
}

/// Configuration for a reference AST node type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeElementsReferenceConfig {
    /// Named child field that holds the path expression.
    pub path_expr_field: String,
}

/// Configuration for a type-list AST node type.
/// Each named child of a matching node is emitted as a separate type reference,
/// with `ast_node_kind` set to the type-list node's own kind (e.g. `"type_argument_list"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeElementsTypeListConfig {}

/// Configuration for a namespace AST node type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeElementsNamespaceConfig {
    /// Named child field that holds the namespace name.
    pub name_field: String,
}

/// Serializable per-language configuration for [`ExtractorConfig::from_spec`].
///
/// Specifies which AST node kinds to treat as declarations, references,
/// type-lists, and namespaces. The tree-sitter grammar and language hooks
/// are always provided by the built-in defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeElementsLanguageConfig {
    #[serde(default)]
    pub declaration_node_kinds: HashMap<String, CodeElementsDeclarationConfig>,
    #[serde(default)]
    pub reference_node_kinds: HashMap<String, CodeElementsReferenceConfig>,
    #[serde(default)]
    pub type_list_node_kinds: HashMap<String, CodeElementsTypeListConfig>,
    #[serde(default)]
    pub namespace_node_kinds: HashMap<String, CodeElementsNamespaceConfig>,
    /// Regex patterns matched against `referenced_full_path`. Any matching reference is dropped.
    /// Patterns are `|`-joined and precompiled into a single `regex::Regex` at construction time.
    #[serde(default)]
    pub exclude_reference_patterns: Vec<String>,
}

// ── LanguageExtractorConfig ────────────────────────────────────────────────

/// Resolve a `cocoindex_code_ast` registry entry for a built-in extractor
/// config. Panics if the name is unknown or has no tree-sitter grammar —
/// built-in configs are static configuration, so this is a build-time
/// invariant, not runtime input.
pub fn registry_info(name: &str) -> &'static crate::prog_langs::ProgrammingLanguageInfo {
    let info = crate::prog_langs::get_language_info(name)
        .unwrap_or_else(|| panic!("language {name:?} is not in the code_ast registry"));
    assert!(
        info.treesitter_info.is_some(),
        "language {name:?} has no tree-sitter grammar"
    );
    info
}

/// Per-language extraction configuration.
pub struct LanguageExtractorConfig {
    /// The registry entry this config extracts for. Per-language singleton —
    /// its identity ties the config to a [`CodeSource`](crate::CodeSource)'s
    /// resolved language, and its grammar is what `extract` parses with.
    pub info: &'static crate::prog_langs::ProgrammingLanguageInfo,
    pub declaration_node_kinds: HashMap<String, CodeElementsDeclarationConfig>,
    pub reference_node_kinds: HashMap<String, CodeElementsReferenceConfig>,
    /// Nodes whose named children are each emitted as a type reference.
    pub type_list_node_kinds: HashMap<String, CodeElementsTypeListConfig>,
    pub namespace_node_kinds: HashMap<String, CodeElementsNamespaceConfig>,
    pub hooks: Box<dyn LanguageHooks>,
    /// Regex patterns matched against `referenced_full_path`. Any matching reference is dropped.
    pub exclude_reference_patterns: Vec<String>,
}

impl LanguageExtractorConfig {
    /// Replace all node-kind maps and exclusion patterns with values from a user-supplied
    /// [`CodeElementsLanguageConfig`]. The tree-sitter grammar and hooks are unchanged.
    pub fn apply_config(&mut self, cfg: CodeElementsLanguageConfig) {
        self.declaration_node_kinds = cfg.declaration_node_kinds;
        self.reference_node_kinds = cfg.reference_node_kinds;
        self.type_list_node_kinds = cfg.type_list_node_kinds;
        self.namespace_node_kinds = cfg.namespace_node_kinds;
        self.exclude_reference_patterns = cfg.exclude_reference_patterns;
    }
}

/// Compiled per-language config: wraps `LanguageExtractorConfig` plus a precompiled exclusion regex.
pub struct CompiledLanguageConfig {
    pub config: LanguageExtractorConfig,
    exclude_regex: Option<Regex>,
}

impl CompiledLanguageConfig {
    fn new(config: LanguageExtractorConfig) -> Self {
        let exclude_regex = if config.exclude_reference_patterns.is_empty() {
            None
        } else {
            // Each pattern is wrapped in a non-capturing group and the whole expression
            // is anchored with ^ and $ so patterns match the full `referenced_full_path`.
            // Users write e.g. `[A-Z]` instead of `^[A-Z]$`.
            let alternatives: Vec<String> = config
                .exclude_reference_patterns
                .iter()
                .map(|p| format!("(?:{p})"))
                .collect();
            let combined = alternatives.join("|");
            Some(
                Regex::new(&format!("^(?:{combined})$"))
                    .expect("invalid exclude_reference_patterns regex"),
            )
        };
        Self {
            config,
            exclude_regex,
        }
    }

    /// Returns true if the given `referenced_full_path` should be excluded.
    pub fn is_excluded(&self, full_path: &str) -> bool {
        self.exclude_regex
            .as_ref()
            .is_some_and(|re| re.is_match(full_path))
    }
}

// ── ExtractorConfig ────────────────────────────────────────────────────────

/// Top-level extractor configuration mapping language names to their configs.
pub struct ExtractorConfig {
    pub languages: HashMap<String, LanguageExtractorConfig>,
}

impl ExtractorConfig {
    /// Returns a config with built-in defaults for all supported languages.
    ///
    /// Language keys match the names returned by [`crate::prog_langs::detect_language`]
    /// (e.g. `"javascript"`, `"cpp"`, `"csharp"`), so a language detected from a filename can
    /// be passed straight to [`CodeElementsExtractor::extract`].
    pub fn with_defaults() -> Self {
        let mut languages = HashMap::new();
        languages.insert("c".to_string(), lang::cfamily::default_c_config());
        languages.insert("cpp".to_string(), lang::cfamily::default_cpp_config());
        languages.insert("csharp".to_string(), lang::csharp::default_csharp_config());
        languages.insert("go".to_string(), lang::go::default_go_config());
        languages.insert("java".to_string(), lang::java::default_java_config());
        languages.insert(
            "javascript".to_string(),
            lang::javascript::default_javascript_config(),
        );
        languages.insert("kotlin".to_string(), lang::kotlin::default_kotlin_config());
        languages.insert("python".to_string(), lang::python::default_python_config());
        languages.insert("rust".to_string(), lang::rust::default_rust_config());
        languages.insert("swift".to_string(), lang::swift::default_swift_config());
        languages.insert(
            "typescript".to_string(),
            lang::typescript::default_typescript_config(),
        );
        languages.insert("tsx".to_string(), lang::typescript::default_tsx_config());
        Self { languages }
    }

    /// Build a config from an optional per-language spec map.
    ///
    /// - `None` → all built-in defaults.
    /// - `Some(map)` → only languages present in the map are enabled.
    ///   The tree-sitter grammar and hooks are always taken from the built-ins;
    ///   the node-type maps are replaced by the user-supplied values.
    ///   Language names are matched case-insensitively. Unknown names are silently ignored.
    pub fn from_spec(languages: Option<HashMap<String, CodeElementsLanguageConfig>>) -> Self {
        let Some(spec_langs) = languages else {
            return Self::with_defaults();
        };
        let mut defaults = Self::with_defaults();
        let mut result = HashMap::new();
        for (spec_name, lang_cfg) in spec_langs {
            let key = defaults
                .languages
                .keys()
                .find(|k| k.eq_ignore_ascii_case(&spec_name))
                .cloned();
            if let Some(key) = key {
                let mut lang = defaults.languages.remove(&key).unwrap();
                lang.apply_config(lang_cfg);
                result.insert(key, lang);
            }
        }
        Self { languages: result }
    }

    /// Compile all language configs, precompiling exclusion regexes.
    pub fn compile(self) -> HashMap<String, CompiledLanguageConfig> {
        self.languages
            .into_iter()
            .map(|(k, v)| (k, CompiledLanguageConfig::new(v)))
            .collect()
    }
}
