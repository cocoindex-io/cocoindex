//! Per-language configuration and layer classification for the structural walks —
//! shared by structural chunking (regions, folding) and context-frame extraction.
//!
//! Almost everything is derived from token structure; the per-language surface is the small
//! `StructuralLanguageExt` plus the shared AST-extraction tables (`crate::ast`). See
//! `specs/structural_chunking/spec.md` §Per-language configuration.

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::elements::{DeclarationKind, ExtractorConfig, LanguageExtractorConfig};
use crate::positions::TextRange;

/// A delimiter pair recognized as an elidable region.
pub struct DelimiterPair {
    pub open: &'static str,
    pub close: &'static str,
    /// Regions smaller than this stay verbatim in skeletons. Soft pairs carry a higher
    /// floor so ordinary parameter/argument lists survive in signatures.
    pub skeleton_floor: usize,
    /// Soft pairs (parens/brackets) defer to hard (brace) regions inside them during
    /// skeleton elision — keeps `useMemo(() => { ... }, [deps])` instead of `useMemo( ... )`.
    pub soft: bool,
    /// Fence-style pairs (Markdown ``` / ~~~) keep their info-string line on the spine.
    pub fence: bool,
}

/// Token pairs recognized as elidable regions in every language.
const UNIVERSAL_DELIMITER_PAIRS: &[DelimiterPair] = &[
    DelimiterPair {
        open: "{",
        close: "}",
        skeleton_floor: 64,
        soft: false,
        fence: false,
    },
    DelimiterPair {
        open: "(",
        close: ")",
        skeleton_floor: 192,
        soft: true,
        fence: false,
    },
    DelimiterPair {
        open: "[",
        close: "]",
        skeleton_floor: 192,
        soft: true,
        fence: false,
    },
];

/// Skeleton floor for indentation/implicitly-delimited regions (hard, like braces).
pub const HARD_REGION_SKELETON_FLOOR: usize = 64;

/// Comment node kinds recognized as attached-prefix material in every language.
const UNIVERSAL_COMMENT_KINDS: &[&str] =
    &["comment", "line_comment", "block_comment", "doc_comment"];

/// Where a language keeps a structure's documentation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DocConvention {
    /// Doc comments precede the declaration as sibling comment nodes (default).
    PrecedingComments,
    /// Python-style: the doc is the leading string expression of the body block.
    Docstring,
}

/// A named-value declaration kind: a node binding an identifier to a value
/// (`CONFIG = {…}`, `const fn = () => {…}`). See spec §Named values.
pub struct ValueDeclKind {
    pub kind: &'static str,
    pub name_field: &'static str,
    pub value_field: &'static str,
}

/// The irreducible per-language facts (spec §Per-language configuration).
pub struct StructuralLanguageExt {
    /// Region kinds delimited by indentation / implicit extent instead of token pairs.
    pub indent_region_kinds: &'static [&'static str],
    /// Extra delimiter pairs beyond the universal set (Markdown code fences).
    pub extra_delimiter_pairs: &'static [(&'static str, &'static str)],
    /// Kinds excluded from region detection despite matching a delimiter pair.
    pub non_region_kinds: &'static [&'static str],
    /// Named-value declaration kinds (§Named values).
    pub value_decl_kinds: &'static [ValueDeclKind],
    /// Attached-prefix kinds beyond the universal comment set (attributes, decorators).
    pub extra_attached_prefix_kinds: &'static [&'static str],
    /// Line-comment prefix, for roster rendering and doc-paragraph detection.
    pub line_comment_prefix: &'static str,
    /// Where docs live.
    pub doc_convention: DocConvention,
    /// Roster visibility predicate.
    pub is_public: fn(name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool,
    /// Markdown-style heading sections: (section kind, heading kinds).
    pub section_kinds: Option<(&'static str, &'static [&'static str])>,
}

fn always_public(_name: &str, _node: &tree_sitter::Node, _source: &[u8]) -> bool {
    true
}

fn python_is_public(name: &str, _node: &tree_sitter::Node, _source: &[u8]) -> bool {
    !name.starts_with('_')
}

fn rust_is_public(_name: &str, node: &tree_sitter::Node, _source: &[u8]) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "visibility_modifier"
        {
            return true;
        }
    }
    false
}

fn ts_is_public(name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool {
    if name.starts_with('#') {
        return false;
    }
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "accessibility_modifier"
        {
            let text = child.utf8_text(source).unwrap_or("");
            return text != "private" && text != "protected";
        }
    }
    true
}

fn go_is_public(name: &str, _node: &tree_sitter::Node, _source: &[u8]) -> bool {
    name.chars().next().is_some_and(|c| c.is_uppercase())
}

fn java_is_public(_name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "modifiers"
        {
            return child.utf8_text(source).unwrap_or("").contains("public");
        }
    }
    false
}

/// Swift defaults to `internal` visibility: list unless explicitly `private` /
/// `fileprivate`.
fn swift_is_public(_name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "modifiers"
        {
            return !child.utf8_text(source).unwrap_or("").contains("private");
        }
    }
    true
}

/// Kotlin defaults to `public`: list unless explicitly `private` / `protected`.
fn kotlin_is_public(_name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "modifiers"
        {
            let text = child.utf8_text(source).unwrap_or("");
            return !text.contains("private") && !text.contains("protected");
        }
    }
    true
}

fn csharp_is_public(_name: &str, node: &tree_sitter::Node, source: &[u8]) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind().starts_with("modifier")
        {
            return child.utf8_text(source).unwrap_or("").contains("public");
        }
    }
    false
}

static PYTHON_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &["block"],
    extra_delimiter_pairs: &[],
    non_region_kinds: &["interpolation"],
    value_decl_kinds: &[ValueDeclKind {
        kind: "assignment",
        name_field: "left",
        value_field: "right",
    }],
    extra_attached_prefix_kinds: &["decorator"],
    line_comment_prefix: "#",
    doc_convention: DocConvention::Docstring,
    is_public: python_is_public,
    section_kinds: None,
};

static RUST_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[
        ValueDeclKind {
            kind: "let_declaration",
            name_field: "pattern",
            value_field: "value",
        },
        ValueDeclKind {
            kind: "const_item",
            name_field: "name",
            value_field: "value",
        },
        ValueDeclKind {
            kind: "static_item",
            name_field: "name",
            value_field: "value",
        },
    ],
    extra_attached_prefix_kinds: &["attribute_item"],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: rust_is_public,
    section_kinds: None,
};

static TS_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[ValueDeclKind {
        kind: "variable_declarator",
        name_field: "name",
        value_field: "value",
    }],
    extra_attached_prefix_kinds: &["decorator"],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: ts_is_public,
    section_kinds: None,
};

static GO_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[
        ValueDeclKind {
            kind: "var_spec",
            name_field: "name",
            value_field: "value",
        },
        ValueDeclKind {
            kind: "const_spec",
            name_field: "name",
            value_field: "value",
        },
    ],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: go_is_public,
    section_kinds: None,
};

static JAVA_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: java_is_public,
    section_kinds: None,
};

static SWIFT_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: swift_is_public,
    section_kinds: None,
};

static KOTLIN_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: kotlin_is_public,
    section_kinds: None,
};

static CFAMILY_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    // C has no per-declaration visibility; C++ access labels are positional
    // (`public:` sections), not readable off the declaration node.
    is_public: always_public,
    section_kinds: None,
};

static CSHARP_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: csharp_is_public,
    section_kinds: None,
};

/// Generic brace-language tier (no ast tables): layers come from the anonymous
/// machinery only — delimiter regions fold, head lines frame (frames v2), cues
/// render. No rosters or name-based classification.
static SCALA_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: None,
};

static BASH_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "#",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: None,
};

static SQL_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "--",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: None,
};

static HASH_COMMENT_GENERIC_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "#",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: None,
};

static SLASH_COMMENT_GENERIC_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: "//",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: None,
};

static MARKDOWN_EXT: StructuralLanguageExt = StructuralLanguageExt {
    indent_region_kinds: &[],
    extra_delimiter_pairs: &[("```", "```"), ("~~~", "~~~")],
    non_region_kinds: &[],
    value_decl_kinds: &[],
    extra_attached_prefix_kinds: &[],
    line_comment_prefix: ">",
    doc_convention: DocConvention::PrecedingComments,
    is_public: always_public,
    section_kinds: Some(("section", &["atx_heading", "setext_heading"])),
};

/// Structural support registry, keyed by canonical `prog_langs` language name.
static STRUCTURAL_EXTS: LazyLock<HashMap<&'static str, &'static StructuralLanguageExt>> =
    LazyLock::new(|| {
        HashMap::from([
            ("python", &PYTHON_EXT),
            ("rust", &RUST_EXT),
            ("typescript", &TS_EXT),
            ("tsx", &TS_EXT),
            ("javascript", &TS_EXT),
            ("go", &GO_EXT),
            ("java", &JAVA_EXT),
            ("swift", &SWIFT_EXT),
            ("kotlin", &KOTLIN_EXT),
            ("c", &CFAMILY_EXT),
            ("cpp", &CFAMILY_EXT),
            ("objc", &CFAMILY_EXT),
            ("objcpp", &CFAMILY_EXT),
            ("csharp", &CSHARP_EXT),
            ("scala", &SCALA_EXT),
            ("bash", &BASH_EXT),
            ("sql", &SQL_EXT),
            ("r", &HASH_COMMENT_GENERIC_EXT),
            ("toml", &HASH_COMMENT_GENERIC_EXT),
            ("json", &SLASH_COMMENT_GENERIC_EXT),
            ("css", &SLASH_COMMENT_GENERIC_EXT),
            ("markdown", &MARKDOWN_EXT),
        ])
    });

/// Shared AST-extraction tables (scoped-layer source of truth).
static AST_TABLES: LazyLock<ExtractorConfig> = LazyLock::new(ExtractorConfig::with_defaults);

/// Everything classification needs about the active language.
pub struct LangCtx {
    pub ext: &'static StructuralLanguageExt,
    /// The shared extraction tables for this language, if any (Markdown has none).
    pub tables: Option<&'static LanguageExtractorConfig>,
    /// Merged delimiter pairs (universal + per-language extras).
    pub pairs: Vec<DelimiterPair>,
    /// Merged attached-prefix kinds (universal comments + per-language extras).
    pub attached_kinds: Vec<&'static str>,
}

impl LangCtx {
    /// Structural language context for a canonical language name, or `None` when the
    /// language has no structural support (→ degraded path).
    pub fn for_language(canonical_name: &str) -> Option<Self> {
        let ext = *STRUCTURAL_EXTS.get(canonical_name)?;
        let tables = AST_TABLES.languages.get(canonical_name);
        let mut pairs: Vec<DelimiterPair> = UNIVERSAL_DELIMITER_PAIRS
            .iter()
            .map(|p| DelimiterPair { ..*p })
            .collect();
        pairs.extend(
            ext.extra_delimiter_pairs
                .iter()
                .map(|(open, close)| DelimiterPair {
                    open,
                    close,
                    skeleton_floor: HARD_REGION_SKELETON_FLOOR,
                    soft: false,
                    fence: matches!(*open, "```" | "~~~"),
                }),
        );
        let mut attached_kinds = UNIVERSAL_COMMENT_KINDS.to_vec();
        attached_kinds.extend_from_slice(ext.extra_attached_prefix_kinds);
        Some(Self {
            ext,
            tables,
            pairs,
            attached_kinds,
        })
    }

    pub fn is_attached_prefix_kind(&self, kind: &str) -> bool {
        self.attached_kinds.iter().any(|k| *k == kind)
    }
}

/// `DeclarationKind`s that make a table declaration a scoped layer unconditionally.
/// Variable/Constant kinds participate only via the named-value rule; Field/Property/
/// TypeAlias never do (spec §Per-language configuration).
fn scope_bearing(kind: DeclarationKind) -> bool {
    matches!(
        kind,
        DeclarationKind::Class
            | DeclarationKind::Interface
            | DeclarationKind::Struct
            | DeclarationKind::Union
            | DeclarationKind::Enum
            | DeclarationKind::Trait
            | DeclarationKind::Function
            | DeclarationKind::Method
            | DeclarationKind::Constructor
            | DeclarationKind::Extension
    )
}

/// Is `node` an elidable region? (Spec §Terminology: delimiter-pair rule on direct
/// children, plus indent kinds, minus exclusions.)
pub fn is_region(node: &tree_sitter::Node, ctx: &LangCtx, src: &str) -> bool {
    let kind = node.kind();
    if ctx.ext.non_region_kinds.contains(&kind) {
        return false;
    }
    if ctx.ext.indent_region_kinds.contains(&kind) {
        return true;
    }
    let n = node.child_count();
    if n < 2 {
        return false;
    }
    let (Some(first), Some(last)) = (node.child(0), node.child(n - 1)) else {
        return false;
    };
    let first_text = &src[first.byte_range()];
    let last_text = &src[last.byte_range()];
    ctx.pairs
        .iter()
        .any(|pair| first_text == pair.open && last_text == pair.close)
}

/// The skeleton floor and softness of `node` if it is a region: indentation/implicit
/// regions are hard with the base floor; token-delimited regions take their pair's traits.
pub fn region_traits(node: &tree_sitter::Node, ctx: &LangCtx, src: &str) -> Option<(usize, bool)> {
    let kind = node.kind();
    if ctx.ext.non_region_kinds.contains(&kind) {
        return None;
    }
    if ctx.ext.indent_region_kinds.contains(&kind) {
        return Some((HARD_REGION_SKELETON_FLOOR, false));
    }
    let n = node.child_count();
    if n < 2 {
        return None;
    }
    let (Some(first), Some(last)) = (node.child(0), node.child(n - 1)) else {
        return None;
    };
    let first_text = &src[first.byte_range()];
    let last_text = &src[last.byte_range()];
    ctx.pairs
        .iter()
        .find(|pair| first_text == pair.open && last_text == pair.close)
        .map(|pair| (pair.skeleton_floor, pair.soft))
}

/// First direct child of `node` that is a region, preferring hard (brace/indent)
/// regions over soft (paren/bracket) ones: an `if`'s primary is its block, not its
/// parenthesized condition.
pub fn direct_region_child<'t>(
    node: &tree_sitter::Node<'t>,
    ctx: &LangCtx,
    src: &str,
) -> Option<tree_sitter::Node<'t>> {
    let mut first_soft: Option<tree_sitter::Node<'t>> = None;
    for i in 0..node.child_count() {
        let child = node.child(i)?;
        match region_traits(&child, ctx, src) {
            Some((_, false)) => return Some(child),
            Some((_, true)) if first_soft.is_none() => first_soft = Some(child),
            _ => {}
        }
    }
    first_soft
}

/// The interior of a region: what elision removes and `process` opens.
/// Token-delimited regions keep their delimiters outside the interior; fence-style
/// pairs (Markdown ``` / ~~~) also keep their info string on the spine, so a folded
/// fence renders as ```` ```rust ... ``` ```` (spec §Markdown).
pub fn region_interior(region: &tree_sitter::Node, ctx: &LangCtx, src: &str) -> TextRange {
    let kind = region.kind();
    if ctx.ext.indent_region_kinds.contains(&kind) {
        return TextRange::new(region.start_byte(), region.end_byte());
    }
    let n = region.child_count();
    debug_assert!(n >= 2);
    let (Some(first), Some(last)) = (region.child(0), region.child(n - 1)) else {
        return TextRange::new(region.start_byte(), region.end_byte());
    };
    let mut interior_start = first.end_byte();
    let interior_end = last.start_byte();
    let first_text = &src[first.byte_range()];
    let is_fence = ctx
        .pairs
        .iter()
        .any(|pair| pair.fence && first_text == pair.open);
    if is_fence && let Some(newline) = src[interior_start..interior_end].find('\n') {
        interior_start += newline;
    }
    TextRange::new(interior_start, interior_end)
}

/// How a layer is scoped, if it is.
pub enum LayerClass {
    Scoped(ScopeInfo),
    Anonymous,
}

pub struct ScopeInfo {
    /// Span of the name node (frame line = the line containing its start).
    pub name_range: TextRange,
    /// Roster-eligible (type-like declarations and Markdown sections).
    pub type_like: bool,
}

/// The primary region a layer opens when processed.
#[derive(Clone, Copy)]
pub enum PrimaryRegion<'t> {
    Node(tree_sitter::Node<'t>),
    /// Markdown section body: no node, just the span after the heading.
    Implicit(TextRange),
}

/// A classified layer, before span extension over attached prefixes.
pub struct Classified<'t> {
    pub class: LayerClass,
    pub primary: PrimaryRegion<'t>,
    /// Span end override: virtual setext sections own the sibling run past their
    /// heading node (tree-sitter-md wraps a whole setext run in one flat `section`).
    pub span_end: Option<usize>,
}

/// Resolve a scoped declaration's primary region: the body child if it is a region,
/// else the body's direct region child (Go `type_spec` → `struct_type` → field list).
fn resolve_body_region<'t>(
    body: tree_sitter::Node<'t>,
    ctx: &LangCtx,
    src: &str,
) -> Option<tree_sitter::Node<'t>> {
    if is_region(&body, ctx, src) {
        return Some(body);
    }
    direct_region_child(&body, ctx, src)
}

/// True when `kind` is a Markdown heading node kind for this language.
pub fn is_section_heading(ctx: &LangCtx, kind: &str) -> bool {
    ctx.ext
        .section_kinds
        .is_some_and(|(_, heading_kinds)| heading_kinds.contains(&kind))
}

fn count_headings(node: &tree_sitter::Node, heading_kinds: &[&str]) -> usize {
    (0..node.child_count())
        .filter_map(|i| node.child(i))
        .filter(|c| heading_kinds.contains(&c.kind()))
        .count()
}

/// Heading level: 1-6 from the underline / ATX marker child kind.
fn heading_level(node: &tree_sitter::Node) -> usize {
    for i in 0..node.child_count() {
        let Some(child) = node.child(i) else { continue };
        match child.kind() {
            "setext_h1_underline" => return 1,
            "setext_h2_underline" => return 2,
            k => {
                if let Some(rest) = k.strip_prefix("atx_h")
                    && let Some(d) = rest.strip_suffix("_marker")
                    && let Ok(n) = d.parse::<usize>()
                {
                    return n;
                }
            }
        }
    }
    6
}

/// Classify `node` as a layer, if it is one. Does NOT look through wrapper ancestors
/// (see `engine::cut_candidate` for that) and does not extend spans.
pub fn classify<'t>(
    node: &tree_sitter::Node<'t>,
    ctx: &LangCtx,
    src: &str,
) -> Option<Classified<'t>> {
    let kind = node.kind();
    let bytes = src.as_bytes();

    // Markdown sections.
    if let Some((section_kind, heading_kinds)) = ctx.ext.section_kinds {
        if kind == section_kind {
            // tree-sitter-md wraps an entire setext-heading run in ONE flat section
            // (no per-heading nesting). A multi-heading section is not a layer:
            // descend, and let each heading classify as a virtual section below.
            if count_headings(node, heading_kinds) > 1 {
                return None;
            }
            for i in 0..node.child_count() {
                let child = node.child(i)?;
                if heading_kinds.contains(&child.kind()) {
                    let body = TextRange::new(child.end_byte(), node.end_byte());
                    return Some(Classified {
                        span_end: None,
                        class: LayerClass::Scoped(ScopeInfo {
                            name_range: TextRange::new(child.start_byte(), child.end_byte()),
                            type_like: true,
                        }),
                        primary: PrimaryRegion::Implicit(body),
                    });
                }
            }
            return None;
        }
        // Virtual section: a heading inside a flat multi-heading section owns the
        // sibling run up to the next heading of its level or higher.
        if heading_kinds.contains(&kind)
            && let Some(parent) = node.parent()
            && parent.kind() == section_kind
            && count_headings(&parent, heading_kinds) > 1
        {
            let level = heading_level(node);
            let mut end = parent.end_byte();
            let mut sib = *node;
            while let Some(next) = sib.next_sibling() {
                if heading_kinds.contains(&next.kind()) && heading_level(&next) <= level {
                    end = next.start_byte();
                    break;
                }
                sib = next;
            }
            let name_start = node.start_byte();
            let name_end = src[name_start..node.end_byte()]
                .find('\n')
                .map_or(node.end_byte(), |i| name_start + i);
            return Some(Classified {
                span_end: Some(end),
                class: LayerClass::Scoped(ScopeInfo {
                    name_range: TextRange::new(name_start, name_end),
                    type_like: true,
                }),
                primary: PrimaryRegion::Implicit(TextRange::new(node.end_byte(), end)),
            });
        }
    }

    if let Some(tables) = ctx.tables {
        // Scope-bearing declaration tables (with runtime validation of the configured
        // children — the tables are extraction-oriented and sometimes aspirational).
        if let Some(decl) = tables.declaration_node_kinds.get(kind) {
            let refined = tables.hooks.refine_declaration_kind(node, decl.kind, bytes);
            if scope_bearing(refined)
                && let Some(name) = node.child_by_field_name(&decl.name_field)
                && let Some(body_field) = &decl.body_field
                && let Some(body) = node.child_by_field_name(body_field)
                && let Some(region) = resolve_body_region(body, ctx, src)
            {
                let type_like = !matches!(
                    refined,
                    DeclarationKind::Function
                        | DeclarationKind::Method
                        | DeclarationKind::Constructor
                );
                return Some(Classified {
                    span_end: None,
                    class: LayerClass::Scoped(ScopeInfo {
                        name_range: TextRange::new(name.start_byte(), name.end_byte()),
                        type_like,
                    }),
                    primary: PrimaryRegion::Node(region),
                });
            }
        }

        // Namespace tables (mod / namespace): name-only entries; region via delimiter rule.
        if let Some(ns) = tables.namespace_node_kinds.get(kind)
            && let Some(name) = node.child_by_field_name(&ns.name_field)
            && let Some(region) = direct_region_child(node, ctx, src)
        {
            return Some(Classified {
                span_end: None,
                class: LayerClass::Scoped(ScopeInfo {
                    name_range: TextRange::new(name.start_byte(), name.end_byte()),
                    type_like: false,
                }),
                primary: PrimaryRegion::Node(region),
            });
        }
    }

    // Named values: identifier bound to a value that is (or directly contains) a region.
    for vd in ctx.ext.value_decl_kinds {
        if vd.kind == kind
            && let Some(name) = node.child_by_field_name(vd.name_field)
            && name.kind() == "identifier"
            && let Some(value) = node.child_by_field_name(vd.value_field)
        {
            let region = if is_region(&value, ctx, src) {
                Some(value)
            } else {
                direct_region_child(&value, ctx, src)
            };
            if let Some(region) = region {
                return Some(Classified {
                    span_end: None,
                    class: LayerClass::Scoped(ScopeInfo {
                        name_range: TextRange::new(name.start_byte(), name.end_byte()),
                        type_like: false,
                    }),
                    primary: PrimaryRegion::Node(region),
                });
            }
        }
    }

    // Anonymous layers: the node is a region, or directly contains one.
    if is_region(node, ctx, src) {
        return Some(Classified {
            span_end: None,
            class: LayerClass::Anonymous,
            primary: PrimaryRegion::Node(*node),
        });
    }
    if let Some(region) = direct_region_child(node, ctx, src) {
        return Some(Classified {
            span_end: None,
            class: LayerClass::Anonymous,
            primary: PrimaryRegion::Node(region),
        });
    }
    None
}

/// A classified cut point: `node` is the classification anchor, `top` the
/// wrapper-extended outermost node whose span the layer owns.
pub struct RawCut<'t> {
    pub node: tree_sitter::Node<'t>,
    pub top: tree_sitter::Node<'t>,
    pub class: LayerClass,
    pub primary: PrimaryRegion<'t>,
    /// Span end override (virtual setext sections; see `Classified::span_end`).
    pub span_end: Option<usize>,
}

/// Classify `node` as a cut candidate, looking through wrapper ancestors: nodes whose
/// children, besides one substantive child, are only unnamed tokens and attached-prefix
/// nodes (`export_statement`, `decorated_definition`, `expression_statement`).
pub fn cut_candidate<'t>(
    node: &tree_sitter::Node<'t>,
    ctx: &LangCtx,
    src: &str,
) -> Option<RawCut<'t>> {
    if let Some(classified) = classify(node, ctx, src) {
        return Some(RawCut {
            node: *node,
            top: *node,
            class: classified.class,
            primary: classified.primary,
            span_end: classified.span_end,
        });
    }
    let mut substantive: Option<tree_sitter::Node<'t>> = None;
    for i in 0..node.child_count() {
        let child = node.child(i)?;
        if !child.is_named() || ctx.is_attached_prefix_kind(child.kind()) {
            continue;
        }
        if substantive.is_some() {
            return None; // more than one substantive child: not a wrapper
        }
        substantive = Some(child);
    }
    let inner = substantive?;
    cut_candidate(&inner, ctx, src).map(|mut cut| {
        cut.top = *node;
        cut
    })
}

/// True for comment nodes that document their *enclosing* scope (Rust `//!`) and must
/// never attach to the following item.
pub fn is_inner_doc(node: &tree_sitter::Node) -> bool {
    for i in 0..node.child_count() {
        if let Some(child) = node.child(i)
            && child.kind() == "inner_doc_comment_marker"
        {
            return true;
        }
    }
    false
}
