//! Source views: partial, elided views of a source file — synthetic text plus
//! source-grounded segments.
//!
//! Mirrors Python's `cocoindex.resources.view` (`SourceView`, `ViewSegment`).
//! The shared output schema of code-match rendering
//! ([`ops::code::render_match`](crate::ops::code::render_match)).

use crate::resources::chunk::TextPosition;

/// Whether a segment is repeated context or the view's own material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentKind {
    /// Context header repeated from enclosing scopes.
    Frame,
    /// The view's own material.
    Content,
}

/// One contiguous piece of a source view's text, grounded in the source.
///
/// The view's `text` is exactly the in-order concatenation of each segment's
/// rendering: `summary` if present, else the source slice `start..end`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewSegment {
    /// Start of the covered source range.
    pub start: TextPosition,
    /// End of the covered source range.
    pub end: TextPosition,
    pub kind: SegmentKind,
    /// When present, stands in for the covered source in the view's text (a
    /// folded layer's overview, a truncated frame line).
    pub summary: Option<String>,
    /// Char offset where this segment's rendering starts in the view's `text`.
    pub rendered_start: usize,
    /// Char offset where this segment's rendering ends in the view's `text`.
    /// Segments' renderings form a contiguous partition of `text`: use these to
    /// map parts of the synthetic text (e.g. its line numbers) back to segments
    /// and their source positions.
    pub rendered_end: usize,
}

/// A source view: synthetic text plus its source-grounded segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceView {
    /// Full synthetic text — what gets embedded / displayed.
    pub text: String,
    /// Ordered segments; ranges are ascending and pairwise disjoint within a view.
    pub segments: Vec<ViewSegment>,
}

impl SourceView {
    /// Start of the view's own content (frames excluded) — use for citations.
    /// `None` for an empty view.
    pub fn start(&self) -> Option<TextPosition> {
        self.segments
            .iter()
            .find(|seg| seg.kind == SegmentKind::Content)
            .or(self.segments.first())
            .map(|seg| seg.start)
    }

    /// End of the view's own content (frames excluded) — use for citations.
    /// `None` for an empty view.
    pub fn end(&self) -> Option<TextPosition> {
        self.segments
            .iter()
            .rev()
            .find(|seg| seg.kind == SegmentKind::Content)
            .or(self.segments.last())
            .map(|seg| seg.end)
    }
}
