//! Rendering source ranges into a [`SourceView`]: context frames of the ranges'
//! envelope, each range verbatim, and cues where material is omitted — the
//! render path for code-match results. Spec: `specs/source_view/spec.md`
//! §Match rendering.

use crate::CodeSource;
use crate::positions::TextRange;

use super::frames::context_frames;
use super::{RawSeg, RawView, SegmentKind, SourceView, finalize};

/// Rendered cues (structural chunking spec §Rendered cues — one convention for
/// chunker output and rendered matches). `GAP_MARKER` is an elision line where
/// whole lines are omitted; `CONT_PREFIX` marks content resuming mid-line.
/// Cues render as zero-length Frame segments, so Content-only citation
/// envelopes are unaffected.
pub const GAP_MARKER: &str = "...\n";
pub const CONT_PREFIX: &str = "... ";
/// Cap on a gap marker's indentation (mirrors the following content's line).
pub const MARKER_INDENT_MAX_BYTES: usize = 12;

/// Whether `pos` sits mid-line: preceded on its source line by non-whitespace.
fn is_mid_line(src: &str, pos: usize) -> bool {
    let line_start = src[..pos].rfind('\n').map_or(0, |i| i + 1);
    !src[line_start..pos].trim_start().is_empty()
}

/// Leading whitespace of the line containing `pos` (up to `pos`), capped at
/// `MARKER_INDENT_MAX_BYTES` on a char boundary.
fn line_indent(src: &str, pos: usize) -> &str {
    let line_start = src[..pos].rfind('\n').map_or(0, |i| i + 1);
    let line = &src[line_start..pos];
    let ws_len = line.len() - line.trim_start().len();
    let mut end = ws_len.min(MARKER_INDENT_MAX_BYTES);
    while end > 0 && !line.is_char_boundary(end) {
        end -= 1;
    }
    &line[..end]
}

/// A zero-length Frame segment at `pos` whose `summary` renders `text`.
fn cue(pos: usize, text: String) -> RawSeg {
    RawSeg {
        range: TextRange::new(pos, pos),
        kind: SegmentKind::Frame,
        summary: Some(text),
    }
}

/// The cue between two consecutive rendered ranges (spec §Match rendering):
///
/// - whitespace-only omission (ranges on consecutive lines): not an elision —
///   the cue carries the omitted whitespace verbatim as glue, so lines don't
///   fuse and no `...` implies code that isn't there;
/// - the next range resumes at a line start: a `GAP_MARKER` elision line,
///   indented like that line (plus a newline of glue when the previous range
///   ended mid-line);
/// - the next range resumes mid-line: an inline `CONT_PREFIX` (separated from
///   a non-whitespace previous end by one space): `foo( ... )`.
fn between_cue(src: &str, prev_end: usize, next_start: usize) -> RawSeg {
    let omitted = &src[prev_end..next_start];
    let summary = if omitted.trim().is_empty() {
        omitted.to_string()
    } else if is_mid_line(src, next_start) {
        let sep = match src[..prev_end].chars().next_back() {
            Some(c) if !c.is_whitespace() => " ",
            _ => "",
        };
        format!("{sep}{CONT_PREFIX}")
    } else {
        let nl = if src[..prev_end].ends_with('\n') {
            ""
        } else {
            "\n"
        };
        let indent = line_indent(src, next_start);
        format!("{nl}{indent}{GAP_MARKER}")
    };
    cue(next_start, summary)
}

/// Render source ranges (e.g. a code match's chunks) into a [`SourceView`]:
/// context frames of the ranges' envelope, then each range verbatim, with
/// cues where material is omitted.
///
/// The ranges are rendered **exactly** — no whitespace trims and no widening
/// to line starts (a terminal renderer that wants whole-line display can widen
/// using the segment positions and the original source). Ranges are clamped to
/// the source, empty ranges dropped, and the rest rendered in source order;
/// no ranges leaves an empty view. The view's `start`/`end` (Content envelope)
/// equal the ranges' envelope — the citation span.
pub fn render_ranges(source: &CodeSource<'_>, ranges: &[TextRange]) -> SourceView {
    let src = source.text();
    let mut clean: Vec<TextRange> = ranges
        .iter()
        .map(|r| TextRange::new(r.start.min(src.len()), r.end.min(src.len())))
        .filter(|r| r.start < r.end)
        .collect();
    clean.sort_by_key(|r| r.start);
    let Some(&first) = clean.first() else {
        return SourceView {
            text: String::new(),
            segments: Vec::new(),
        };
    };
    let envelope = TextRange::new(first.start, clean.last().unwrap().end);
    let frames = context_frames(source, envelope);

    let mut segs: Vec<RawSeg> = Vec::new();
    for frame in &frames {
        let raw = &src[frame.line.start..frame.line.end];
        segs.push(RawSeg {
            range: frame.line,
            kind: SegmentKind::Frame,
            summary: (raw != frame.rendered).then(|| frame.rendered.clone()),
        });
    }
    // Cue between the innermost frame and the first range (spec §Rendered cues):
    // a `CONT_PREFIX` when the range starts mid-line, else a `GAP_MARKER` line
    // when non-whitespace source is omitted between the frame's line and the
    // range's line (blank-line-only separation counts as adjacency).
    if let Some(innermost) = frames.last() {
        if is_mid_line(src, first.start) {
            segs.push(cue(first.start, CONT_PREFIX.to_string()));
        } else {
            let line_start = src[..first.start].rfind('\n').map_or(0, |i| i + 1);
            if line_start >= innermost.line.end
                && !src[innermost.line.end..line_start].trim().is_empty()
            {
                let indent = line_indent(src, first.start);
                segs.push(cue(first.start, format!("{indent}{GAP_MARKER}")));
            }
        }
    }
    let mut prev_end: Option<usize> = None;
    for r in &clean {
        if let Some(pe) = prev_end
            && r.start > pe
        {
            segs.push(between_cue(src, pe, r.start));
        }
        segs.push(RawSeg {
            range: *r,
            kind: SegmentKind::Content,
            summary: None,
        });
        prev_end = Some(r.end.max(prev_end.unwrap_or(0)));
    }
    finalize(src, vec![RawView { segs }])
        .pop()
        .expect("one raw view in, one view out")
}

#[cfg(test)]
mod tests {
    use super::*;

    const PY: &str = "\
class Foo(Base):
    def process(self, req):
        if req.cache_ok:
            value = compute()
            return value

    def other(self):
        return 2
";

    fn range_of(src: &str, needle: &str) -> TextRange {
        let start = src.find(needle).expect("needle in src");
        TextRange::new(start, start + needle.len())
    }

    /// Render and check the segment invariant: `text` is exactly the in-order
    /// concatenation of each segment's rendering, renderings partition `text`,
    /// and source ranges are ascending.
    fn render_checked(source: &CodeSource<'_>, ranges: &[TextRange]) -> SourceView {
        let view = render_ranges(source, ranges);
        let src = source.text();
        let mut cursor = 0usize;
        let mut prev_end = 0usize;
        for seg in &view.segments {
            let rendering = match &seg.summary {
                Some(text) => text.as_str(),
                None => &src[seg.range.start..seg.range.end],
            };
            assert_eq!(seg.text_range.start, cursor, "renderings partition text");
            assert_eq!(
                &view.text[seg.text_range.start..seg.text_range.end],
                rendering
            );
            cursor = seg.text_range.end;
            assert!(seg.range.start >= prev_end, "segment ranges ascending");
            prev_end = seg.range.end.max(prev_end);
        }
        assert_eq!(cursor, view.text.len(), "renderings cover all of text");
        view
    }

    #[test]
    fn single_range_with_frames_and_gap_marker() {
        let source = CodeSource::with_language(PY, "python");
        let view = render_checked(&source, &[range_of(PY, "return value")]);
        // Frames of all enclosing layers, then an elision line for the omitted
        // `value = compute()` line, then the exact matched range.
        assert_eq!(
            view.text,
            "class Foo(Base):\n\
             \x20   def process(self, req):\n\
             \x20       if req.cache_ok:\n\
             \x20           ...\n\
             return value"
        );
        // Citation span (Content-segment envelope) == the match envelope.
        let content: Vec<_> = view
            .segments
            .iter()
            .filter(|s| s.kind == SegmentKind::Content)
            .collect();
        assert_eq!(
            content.first().unwrap().range.start,
            PY.find("return value").unwrap()
        );
        assert_eq!(
            content.last().unwrap().range.end,
            PY.find("return value").unwrap() + "return value".len()
        );
    }

    #[test]
    fn body_adjacent_to_frame_gets_no_gap_marker() {
        let source = CodeSource::with_language(PY, "python");
        let view = render_checked(&source, &[range_of(PY, "value = compute()")]);
        assert_eq!(
            view.text,
            "class Foo(Base):\n\
             \x20   def process(self, req):\n\
             \x20       if req.cache_ok:\n\
             value = compute()"
        );
    }

    #[test]
    fn multi_range_line_elision() {
        // Two ranges with a full line omitted between them.
        let src = "\
def f(x):
    a = 1
    b = 2
    c = 3
";
        let source = CodeSource::with_language(src, "python");
        let view = render_checked(&source, &[range_of(src, "a = 1"), range_of(src, "c = 3")]);
        assert_eq!(view.text, "def f(x):\na = 1\n    ...\nc = 3");
        // The cue is a zero-length Frame segment.
        let cues: Vec<_> = view
            .segments
            .iter()
            .filter(|s| s.kind == SegmentKind::Frame && s.range.start == s.range.end)
            .collect();
        assert_eq!(cues.len(), 1);
        assert_eq!(cues[0].summary.as_deref(), Some("\n    ...\n"));
    }

    #[test]
    fn adjacent_lines_glue_without_elision() {
        // Whitespace-only omission (consecutive lines): glue, not `...`.
        let src = "def f(x):\n    a = 1\n    b = 2\n";
        let source = CodeSource::with_language(src, "python");
        let view = render_checked(&source, &[range_of(src, "a = 1"), range_of(src, "b = 2")]);
        assert_eq!(view.text, "def f(x):\na = 1\n    b = 2");
    }

    #[test]
    fn mid_line_elision_uses_inline_cue() {
        let src = "foo(bar, baz)\n";
        let source = CodeSource::with_language(src, "python");
        let view = render_checked(&source, &[range_of(src, "foo("), range_of(src, ")")]);
        assert_eq!(view.text, "foo( ... )");
    }

    #[test]
    fn top_level_range_is_plain_verbatim() {
        let src = "x = 1\ny = 2\n";
        let source = CodeSource::with_language(src, "python");
        let view = render_checked(&source, &[range_of(src, "y = 2")]);
        assert_eq!(view.text, "y = 2");
        assert_eq!(view.segments.len(), 1);
        assert_eq!(view.segments[0].kind, SegmentKind::Content);
    }

    #[test]
    fn mid_line_first_range_gets_continuation_cue_under_frames() {
        let source = CodeSource::with_language(PY, "python");
        let view = render_checked(&source, &[range_of(PY, "compute()")]);
        assert_eq!(
            view.text,
            "class Foo(Base):\n\
             \x20   def process(self, req):\n\
             \x20       if req.cache_ok:\n\
             ... compute()"
        );
    }

    #[test]
    fn overlapping_frame_suppressed() {
        // A range on the `def` line itself keeps only the ancestors above it.
        let source = CodeSource::with_language(PY, "python");
        let view = render_checked(&source, &[range_of(PY, "def process(self, req):")]);
        assert_eq!(view.text, "class Foo(Base):\ndef process(self, req):");
    }

    #[test]
    fn unsupported_language_renders_frameless() {
        let src = "alpha\nbeta\ngamma\n";
        let source = CodeSource::with_language(src, "no-such-language");
        let view = render_checked(&source, &[range_of(src, "beta")]);
        assert_eq!(view.text, "beta");
    }

    #[test]
    fn shallow_parse_error_renders_frameless() {
        // Same fixture as the frames tests: a reshaped tree yields no frames,
        // but the match content still renders.
        let src = "class Pipeline @Since(\"1.4.0\") (\n  @Since(\"1.4.0\") val uid: String) {\n  def f(): Int = 1\n}\n";
        let source = CodeSource::with_language(src, "scala");
        let view = render_checked(&source, &[range_of(src, "def f(): Int = 1")]);
        assert_eq!(view.text, "def f(): Int = 1");
    }

    #[test]
    fn empty_and_out_of_bounds_ranges_are_sanitized() {
        let src = "x = 1\n";
        let source = CodeSource::with_language(src, "python");
        assert_eq!(render_ranges(&source, &[]).text, "");
        let view = render_checked(&source, &[TextRange::new(0, 999)]);
        assert_eq!(view.text, src);
    }
}
