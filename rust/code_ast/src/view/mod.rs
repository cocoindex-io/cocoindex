//! Source views: the shared rendered-view schema for partial, elided views of a
//! source file — synthetic `text` plus source-grounded `segments`.
//!
//! Both structural chunking (`cocoindex_ops_text::split::structural`) and code-match rendering
//! produce this shape: verbatim spans, one-line context frames borrowed from
//! enclosing scopes, and synthetic stand-ins (overviews, cues) where material is
//! omitted. Design: `specs/source_view/spec.md`.

pub mod lang;

pub mod frames;
mod render;

pub use frames::{ContextFrame, context_frames};
pub use render::{CONT_PREFIX, GAP_MARKER, MARKER_INDENT_MAX_BYTES, render_ranges};

use crate::positions::{OutputPosition, Position, TextRange, set_output_positions};

/// Whether a segment is repeated context or the view's own material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    /// Context-header material repeated from enclosing scopes.
    Frame,
    /// The view's own material: verbatim source, wrapper skeleton, overviews.
    Content,
}

/// One contiguous piece of a view's synthetic text, grounded in the source.
#[derive(Debug, Clone)]
pub struct ViewSegment {
    /// Byte range in the original text this segment covers.
    pub range: TextRange,
    /// Start position of `range` in the original text.
    pub start: OutputPosition,
    /// End position of `range` in the original text.
    pub end: OutputPosition,
    /// Byte range of this segment's rendering within the view's synthetic `text`.
    /// Segments form a contiguous partition of `text` (stored, rather than derived,
    /// so consumers need not re-implement the rendering rule to locate segments).
    pub text_range: TextRange,
    pub kind: SegmentKind,
    /// When present, this text appears in the view's synthetic text in place of the
    /// covered source (a folded layer's overview, a truncated frame line). When absent,
    /// the source slice appears verbatim.
    pub summary: Option<String>,
}

/// A source view: synthetic text plus its source-grounded segments.
///
/// Invariant: `text` is exactly the in-order concatenation of each segment's rendering
/// (`summary` if present, else the verbatim source slice).
#[derive(Debug, Clone)]
pub struct SourceView {
    /// Full synthetic text — what gets embedded / displayed.
    pub text: String,
    /// Ordered segments; ranges are ascending and pairwise disjoint within a view.
    pub segments: Vec<ViewSegment>,
}

/// A segment before positions are filled: range + kind + optional synthetic text.
pub struct RawSeg {
    pub range: TextRange,
    pub kind: SegmentKind,
    pub summary: Option<String>,
}

/// A view before rendering: just its ordered raw segments.
pub struct RawView {
    pub segs: Vec<RawSeg>,
}

/// Render texts and fill positions for all raw views in one pass.
pub fn finalize(text: &str, raw_views: Vec<RawView>) -> Vec<SourceView> {
    let mut positions: Vec<Position> = Vec::new();
    for view in &raw_views {
        for seg in &view.segs {
            positions.push(Position::new(seg.range.start));
            positions.push(Position::new(seg.range.end));
        }
    }
    set_output_positions(text, positions.iter_mut());

    let mut result = Vec::with_capacity(raw_views.len());
    let mut pos_idx = 0;
    for view in raw_views {
        let mut view_text = String::new();
        let mut segments = Vec::with_capacity(view.segs.len());
        for seg in view.segs {
            let start = positions[pos_idx].output.unwrap();
            let end = positions[pos_idx + 1].output.unwrap();
            pos_idx += 2;
            let text_start = view_text.len();
            match &seg.summary {
                Some(summary) => view_text.push_str(summary),
                None => view_text.push_str(&text[seg.range.start..seg.range.end]),
            }
            segments.push(ViewSegment {
                range: seg.range,
                start,
                end,
                text_range: TextRange::new(text_start, view_text.len()),
                kind: seg.kind,
                summary: seg.summary,
            });
        }
        result.push(SourceView {
            text: view_text,
            segments,
        });
    }
    result
}
