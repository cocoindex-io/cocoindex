//! Context frames: one-line headers identifying the structures enclosing a span.
//!
//! A frame is an enclosing structure's head line — the *name* line for scoped layers
//! (a decorated definition frames as its `def`/`fn` line, not its decorator), the
//! layer's first line for anonymous ones (`describe(...)`, `if cond:`, a fence's
//! info line, even a bare `{` — which still says "inside an object literal").
//! Semantics: `specs/structural_chunking/spec.md` §Context frames,
//! `specs/source_view/spec.md` §Context-frame extraction.

use crate::positions::TextRange;

use super::lang::{
    LangCtx, LayerClass, PrimaryRegion, RawCut, cut_candidate, is_section_heading, region_interior,
};

/// A frame line longer than this is truncated with `…` (defends against
/// minified/generated lines). A defense, not a display budget.
pub const FRAME_LINE_MAX_BYTES: usize = 200;

/// A context frame: one source line identifying an enclosing structure.
#[derive(Debug, Clone)]
pub struct ContextFrame {
    /// The line's source range, including its trailing newline when present.
    pub line: TextRange,
    /// Rendered form: trimmed/truncated line + `\n`.
    pub rendered: String,
    /// From a named (scoped) layer — vs. an anonymous structure head line.
    pub scoped: bool,
}

/// First substantive line at/after `from` (within `end`): skips lines that are
/// blank or hold only annotations (`@...`).
fn skip_annotation_lines(src: &str, from: usize, end: usize) -> usize {
    let mut anchor = from;
    for _ in 0..8 {
        let Some(nl) = src[anchor..end].find('\n') else {
            break;
        };
        let line = src[anchor..anchor + nl].trim();
        if (line.starts_with('@') || line.is_empty()) && anchor + nl + 1 < end {
            anchor += nl + 1;
        } else {
            break;
        }
    }
    anchor
}

/// The frame for a layer given its classification and span: the name line for scoped
/// layers, the first substantive line (annotation lines skipped) for anonymous ones.
pub fn frame_for(
    src: &str,
    class: &LayerClass,
    decl_start: usize,
    span_end: usize,
) -> ContextFrame {
    let anchor = match class {
        LayerClass::Scoped(scope) => scope.name_range.start,
        // Skip leading annotation lines (`@Since(...)`): Scala/Java-style
        // annotations are children of the definition node, and an annotation
        // line names nothing.
        LayerClass::Anonymous => skip_annotation_lines(src, decl_start, span_end),
    };
    let line_start = src[..anchor].rfind('\n').map_or(0, |i| i + 1);
    let line_end = src[anchor..].find('\n').map_or(src.len(), |i| anchor + i);
    let raw = &src[line_start..line_end];
    let mut text = raw.trim_end();
    let mut truncated = false;
    if text.len() > FRAME_LINE_MAX_BYTES {
        let mut end = FRAME_LINE_MAX_BYTES;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text = &text[..end];
        truncated = true;
    }
    let mut rendered = text.to_string();
    if truncated {
        rendered.push('…');
    }
    rendered.push('\n');
    let line_incl_nl = TextRange::new(line_start, (line_end + 1).min(src.len()));
    ContextFrame {
        line: line_incl_nl,
        rendered,
        scoped: matches!(class, LayerClass::Scoped(_)),
    }
}

/// Push a frame unless the stack already ends with the same source line (a named
/// value and the closure on its line would otherwise frame twice).
pub fn push_frame(stack: &mut Vec<ContextFrame>, frame: ContextFrame) {
    if stack.last().is_none_or(|last| last.line != frame.line) {
        stack.push(frame);
    }
}

/// Context frames of the layers enclosing `range` (byte range in `source`'s text):
/// head lines of enclosing scopes, outermost first.
///
/// The full, uncapped stack of *proper* ancestors — frames whose line overlaps
/// `range` (or does not sit strictly above it) are suppressed, so a range on a
/// declaration's own line gets no frame for that declaration. Same tree-trust gates
/// as the structural chunker: a shallow parse error (error recovery reshaped the
/// tree's skeleton) or an unsupported language yields no frames, and pathologically
/// deep subtrees are opaque. Reuses `source`'s cached parse.
pub fn context_frames(source: &crate::CodeSource<'_>, range: TextRange) -> Vec<ContextFrame> {
    let src = source.text();
    if range.start > range.end || range.end > src.len() {
        return Vec::new();
    }
    let Some(info) = source.info() else {
        return Vec::new();
    };
    let Some(ctx) = LangCtx::for_language(&info.name) else {
        return Vec::new();
    };
    let crate::ParseOutcome::Parsed(tree) = source.tree() else {
        return Vec::new();
    };
    let Some(hazards) = source.tree_hazards() else {
        return Vec::new();
    };
    if hazards.parse_error {
        return Vec::new();
    }

    let mut frames: Vec<ContextFrame> = Vec::new();
    let mut walk = tree.root_node();
    let mut interior = TextRange::new(0, src.len());
    while let Some(cut) = find_cut_on_path(walk, interior, range, &hazards.deep_spans, &ctx, src) {
        let decl_start = cut.top.start_byte();
        let span_end = cut.span_end.unwrap_or_else(|| cut.top.end_byte());
        push_frame(
            &mut frames,
            frame_for(src, &cut.class, decl_start, span_end),
        );
        // Descend into the layer's primary region (mirrors the engine's descent).
        match cut.primary {
            PrimaryRegion::Node(region) => {
                interior = region_interior(&region, &ctx, src);
                walk = region;
            }
            PrimaryRegion::Implicit(body) => {
                // Virtual setext sections: the body's sibling nodes live under the
                // flat parent section, not under the heading node itself.
                walk = if is_section_heading(&ctx, cut.node.kind()) {
                    cut.node.parent().unwrap_or(cut.node)
                } else {
                    cut.node
                };
                interior = body;
            }
        }
    }
    // A frame must sit strictly above the range it labels (the emit-side overlap
    // suppression, applied to `range`).
    frames.retain(|f| f.line.end <= range.start);
    frames
}

/// The layer cut at this level whose span contains `range`, looking through non-layer
/// structure (the cut-through walk restricted to the path toward `range`).
fn find_cut_on_path<'t>(
    node: tree_sitter::Node<'t>,
    interior: TextRange,
    range: TextRange,
    deep_spans: &[TextRange],
    ctx: &LangCtx,
    src: &str,
) -> Option<RawCut<'t>> {
    for i in 0..node.child_count() {
        let Some(child) = node.child(i) else { continue };
        if child.end_byte() <= interior.start || child.start_byte() >= interior.end {
            continue;
        }
        if deep_spans
            .iter()
            .any(|s| child.start_byte() < s.end && s.start < child.end_byte())
        {
            continue; // opaque deep subtree: no frames from inside it
        }
        if let Some(cut) = cut_candidate(&child, ctx, src) {
            let start = cut.top.start_byte();
            let end = cut.span_end.unwrap_or_else(|| cut.top.end_byte());
            // Layer spans at one level are disjoint, so at most one contains `range`.
            if start <= range.start && range.end <= end {
                return Some(cut);
            }
            continue;
        }
        if child.start_byte() <= range.start
            && range.end <= child.end_byte()
            && child.child_count() > 0
        {
            // Cut-through: a non-layer node containing the range (an `#ifdef` body,
            // an expression statement) — keep looking inside it.
            return find_cut_on_path(child, interior, range, deep_spans, ctx, src);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodeSource;

    /// Frames for the range covering `needle` in `src`, as (rendered, scoped) pairs.
    fn frames_at<'a>(src: &'a str, language: &str, needle: &str) -> Vec<(String, bool)> {
        let start = src.find(needle).expect("needle in src");
        let source = CodeSource::with_language(src, language);
        context_frames(&source, TextRange::new(start, start + needle.len()))
            .into_iter()
            .map(|f| (f.rendered, f.scoped))
            .collect()
    }

    #[test]
    fn nested_scopes_outermost_first() {
        let src = "\
class Foo(Base):
    def process(self, req):
        if req.cache_ok:
            value = compute()
            return value
";
        assert_eq!(
            frames_at(src, "python", "value = compute()"),
            [
                ("class Foo(Base):\n".to_string(), true),
                ("    def process(self, req):\n".to_string(), true),
                ("        if req.cache_ok:\n".to_string(), false),
            ]
        );
    }

    #[test]
    fn decorated_definition_frames_at_name_line() {
        let src = "\
@app.route(\"/items\")
def handler(req):
    return 1
";
        assert_eq!(
            frames_at(src, "python", "return 1"),
            [("def handler(req):\n".to_string(), true)]
        );
    }

    #[test]
    fn overlapping_frame_suppressed() {
        // A range on the `def` line itself keeps only the ancestors above it.
        let src = "\
class Foo:
    def process(self, req):
        return 1
";
        assert_eq!(
            frames_at(src, "python", "def process"),
            [("class Foo:\n".to_string(), true)]
        );
    }

    #[test]
    fn top_level_range_has_no_frames() {
        let src = "x = 1\ny = 2\n";
        assert_eq!(frames_at(src, "python", "y = 2"), []);
    }

    #[test]
    fn named_value_frames() {
        let src = "\
CONFIG = {
    \"timeout\": 30,
    \"retries\": 3,
}
";
        assert_eq!(
            frames_at(src, "python", "\"retries\": 3"),
            [("CONFIG = {\n".to_string(), true)]
        );
    }

    #[test]
    fn rust_mod_and_fn() {
        let src = "\
mod outer {
    fn compute() -> i32 {
        let x = 1;
        x + 1
    }
}
";
        assert_eq!(
            frames_at(src, "rust", "let x = 1;"),
            [
                ("mod outer {\n".to_string(), true),
                ("    fn compute() -> i32 {\n".to_string(), true),
            ]
        );
    }

    #[test]
    fn markdown_sections() {
        let src = "\
# Title

Intro paragraph.

## Install

Run the installer now.
";
        assert_eq!(
            frames_at(src, "markdown", "Run the installer now."),
            [
                ("# Title\n".to_string(), true),
                ("## Install\n".to_string(), true),
            ]
        );
    }

    #[test]
    fn small_enclosing_layers_still_frame() {
        // Unlike the chunker (which leaves sub-`min` layers uncut), the standalone
        // query returns every enclosing layer regardless of size.
        let src = "def tiny():\n    return 1\n";
        assert_eq!(
            frames_at(src, "python", "return 1"),
            [("def tiny():\n".to_string(), true)]
        );
    }

    #[test]
    fn unsupported_language_yields_no_frames() {
        assert_eq!(
            frames_at("some plain text\nmore\n", "no-such-language", "more"),
            []
        );
    }

    #[test]
    fn shallow_parse_error_yields_no_frames() {
        // Same fixture as the chunker's ParseError gate: error recovery reshapes the
        // tree's skeleton, so no frame derived from it can be trusted.
        let src = "class Pipeline @Since(\"1.4.0\") (\n  @Since(\"1.4.0\") val uid: String) {\n  def f(): Int = 1\n}\n";
        assert_eq!(frames_at(src, "scala", "def f()"), []);
    }
}
