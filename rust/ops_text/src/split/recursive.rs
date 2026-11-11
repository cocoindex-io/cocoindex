//! Recursive text chunking with syntax awareness.

use regex::{Matches, Regex};
use std::collections::HashMap;
use std::num::NonZeroU16;
use std::sync::{Arc, LazyLock};
use unicase::UniCase;

use super::{Chunk, TextRange};
use cocoindex_code_ast::positions::{Position, set_output_positions};
use cocoindex_code_ast::prog_langs::TreeSitterLanguageInfo;
use cocoindex_code_ast::{CodeSource, ParseOutcome};

const SYNTAX_LEVEL_GAP_COST: usize = 512;
const MISSING_OVERLAP_COST: usize = 512;
const PER_LINE_BREAK_LEVEL_GAP_COST: usize = 64;
/// The cost of a chunk with no content at all: as much as 16 syntax-level gaps.
/// A chunk under `min_chunk_size` pays this times its squared shortfall.
const TOO_SMALL_CHUNK_COST: u64 = 16 * SYNTAX_LEVEL_GAP_COST as u64;
/// Extra cost of a chunk under a quarter of `chunk_size` that is also under
/// `min_chunk_size`. A chunk at or over `min_chunk_size` pays nothing, so an
/// explicit floor below `chunk_size / 4` stays authoritative.
const TINY_CHUNK_COST: usize = 6 * SYNTAX_LEVEL_GAP_COST;
/// The same with overlap on: a chunk that is mostly overlap adds little.
const TINY_CHUNK_WITH_OVERLAP_COST: usize = 16 * SYNTAX_LEVEL_GAP_COST;
/// Cost per side of a chunk boundary inside a syntax node that fits in
/// `chunk_size`. Both chunks that meet at the boundary pay it, as they do
/// `SYNTAX_LEVEL_GAP_COST`, so one cut costs twice this in total.
const CUT_FITTING_NODE_COST: u32 = 8 * SYNTAX_LEVEL_GAP_COST as u32;
/// Cost per side of a chunk boundary just after a node's header, which leaves
/// the header without its body (see `node_header`). Both chunks that meet at
/// the boundary pay it, so one cut costs twice this.
const CUT_AFTER_HEADER_COST: u32 = 2 * SYNTAX_LEVEL_GAP_COST as u32;
/// Cost per side of a chunk boundary inside a node's header, which splits it:
/// worse than leaving the header whole without its body, so a cut never moves
/// into the header to avoid `CUT_AFTER_HEADER_COST`.
const CUT_INSIDE_HEADER_COST: u32 = 2 * CUT_AFTER_HEADER_COST;
/// The fields that name the child a header introduces: a definition's or a
/// loop's `body`, an `if` or `elif` branch's `consequence`, and the
/// `definition` that Python decorators precede.
const BODY_FIELD_NAMES: [&str; 3] = ["body", "consequence", "definition"];
/// The cost is computed in fixed point, without a division per chunk:
/// `(shortfall · (2³² - 1) / min_chunk_size)² >> TOO_SMALL_CHUNK_COST_SHIFT`,
/// which is `TOO_SMALL_CHUNK_COST · (shortfall / min_chunk_size)²`. The scaled
/// shortfall is under 2³², as `shortfall <= min_chunk_size`, so its square
/// fits in a `u64`.
const TOO_SMALL_CHUNK_COST_SHIFT: u32 = 64 - TOO_SMALL_CHUNK_COST.ilog2();
const _: () = assert!(TOO_SMALL_CHUNK_COST.is_power_of_two());

/// Configuration for a custom language with regex-based separators.
#[derive(Debug, Clone)]
pub struct CustomLanguageConfig {
    /// The name of the language.
    pub language_name: String,
    /// Aliases for the language name.
    pub aliases: Vec<String>,
    /// Regex patterns for separators, in order of priority.
    pub separators_regex: Vec<String>,
}

/// Configuration for recursive text splitting.
#[derive(Debug, Clone, Default)]
pub struct RecursiveSplitConfig {
    /// Custom language configurations.
    pub custom_languages: Vec<CustomLanguageConfig>,
}

/// Configuration for a single chunking operation. The language comes from the
/// [`CodeSource`] passed to [`RecursiveChunker::split`], not from here.
#[derive(Debug, Clone)]
pub struct RecursiveChunkConfig {
    /// Target chunk size in bytes.
    pub chunk_size: usize,
    /// Minimum chunk size in bytes. It is a soft floor: a chunk may be smaller
    /// where that avoids a worse split point. Defaults to chunk_size / 2.
    pub min_chunk_size: Option<usize>,
    /// Overlap between consecutive chunks in bytes.
    pub chunk_overlap: Option<usize>,
}

struct SimpleLanguageConfig {
    name: String,
    aliases: Vec<String>,
    separator_regex: Vec<Regex>,
}

static DEFAULT_LANGUAGE_CONFIG: LazyLock<SimpleLanguageConfig> =
    LazyLock::new(|| SimpleLanguageConfig {
        name: "_DEFAULT".to_string(),
        aliases: vec![],
        separator_regex: [
            r"\n\n+",
            r"\n",
            r"[\.\?!]\s+|。|？|！",
            r"[;:\-—]\s+|；|：|—+",
            r",\s+|，",
            r"\s+",
        ]
        .into_iter()
        .map(|s| Regex::new(s).unwrap())
        .collect(),
    });

enum ChunkKind<'t> {
    TreeSitterNode {
        tree_sitter_info: &'t TreeSitterLanguageInfo,
        node: tree_sitter::Node<'t>,
    },
    RegexpSepChunk {
        lang_config: &'t SimpleLanguageConfig,
        next_regexp_sep_id: usize,
    },
}

struct InternalChunk<'t, 's: 't> {
    full_text: &'s str,
    range: TextRange,
    kind: ChunkKind<'t>,
}

struct TextChunksIter<'t, 's: 't> {
    lang_config: &'t SimpleLanguageConfig,
    full_text: &'s str,
    range: TextRange,
    matches_iter: Matches<'t, 's>,
    regexp_sep_id: usize,
    next_start_pos: Option<usize>,
}

impl<'t, 's: 't> TextChunksIter<'t, 's> {
    fn new(
        lang_config: &'t SimpleLanguageConfig,
        full_text: &'s str,
        range: TextRange,
        regexp_sep_id: usize,
    ) -> Self {
        let std_range = range.start..range.end;
        Self {
            lang_config,
            full_text,
            range,
            matches_iter: lang_config.separator_regex[regexp_sep_id]
                .find_iter(&full_text[std_range.clone()]),
            regexp_sep_id,
            next_start_pos: Some(std_range.start),
        }
    }
}

impl<'t, 's: 't> Iterator for TextChunksIter<'t, 's> {
    type Item = InternalChunk<'t, 's>;

    fn next(&mut self) -> Option<Self::Item> {
        let start_pos = self.next_start_pos?;
        let end_pos = match self.matches_iter.next() {
            Some(grp) => {
                self.next_start_pos = Some(self.range.start + grp.end());
                self.range.start + grp.start()
            }
            None => {
                self.next_start_pos = None;
                if start_pos >= self.range.end {
                    return None;
                }
                self.range.end
            }
        };
        Some(InternalChunk {
            full_text: self.full_text,
            range: TextRange::new(start_pos, end_pos),
            kind: ChunkKind::RegexpSepChunk {
                lang_config: self.lang_config,
                next_regexp_sep_id: self.regexp_sep_id + 1,
            },
        })
    }
}

struct TreeSitterNodeIter<'t, 's: 't> {
    lang_config: &'t TreeSitterLanguageInfo,
    full_text: &'s str,
    cursor: Option<tree_sitter::TreeCursor<'t>>,
    next_start_pos: usize,
    end_pos: usize,
}

impl<'t, 's: 't> TreeSitterNodeIter<'t, 's> {
    fn fill_gap(
        next_start_pos: &mut usize,
        gap_end_pos: usize,
        full_text: &'s str,
    ) -> Option<InternalChunk<'t, 's>> {
        let start_pos = *next_start_pos;
        if start_pos < gap_end_pos {
            *next_start_pos = gap_end_pos;
            Some(InternalChunk {
                full_text,
                range: TextRange::new(start_pos, gap_end_pos),
                kind: ChunkKind::RegexpSepChunk {
                    lang_config: &DEFAULT_LANGUAGE_CONFIG,
                    next_regexp_sep_id: 0,
                },
            })
        } else {
            None
        }
    }
}

impl<'t, 's: 't> Iterator for TreeSitterNodeIter<'t, 's> {
    type Item = InternalChunk<'t, 's>;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = if let Some(cursor) = &mut self.cursor {
            cursor
        } else {
            return Self::fill_gap(&mut self.next_start_pos, self.end_pos, self.full_text);
        };
        let node = cursor.node();
        if let Some(gap) =
            Self::fill_gap(&mut self.next_start_pos, node.start_byte(), self.full_text)
        {
            return Some(gap);
        }
        if !cursor.goto_next_sibling() {
            self.cursor = None;
        }
        self.next_start_pos = node.end_byte();
        Some(InternalChunk {
            full_text: self.full_text,
            range: TextRange::new(node.start_byte(), node.end_byte()),
            kind: ChunkKind::TreeSitterNode {
                tree_sitter_info: self.lang_config,
                node,
            },
        })
    }
}

enum ChunkIterator<'t, 's: 't> {
    TreeSitter(TreeSitterNodeIter<'t, 's>),
    Text(TextChunksIter<'t, 's>),
    Once(std::iter::Once<InternalChunk<'t, 's>>),
}

impl<'t, 's: 't> Iterator for ChunkIterator<'t, 's> {
    type Item = InternalChunk<'t, 's>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ChunkIterator::TreeSitter(iter) => iter.next(),
            ChunkIterator::Text(iter) => iter.next(),
            ChunkIterator::Once(iter) => iter.next(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LineBreakLevel {
    Inline,
    Newline,
    DoubleNewline,
}

impl LineBreakLevel {
    fn ord(self) -> usize {
        match self {
            LineBreakLevel::Inline => 0,
            LineBreakLevel::Newline => 1,
            LineBreakLevel::DoubleNewline => 2,
        }
    }
}

fn line_break_level(c: &str) -> LineBreakLevel {
    let mut lb_level = LineBreakLevel::Inline;
    let mut iter = c.chars();
    while let Some(c) = iter.next() {
        if c == '\n' || c == '\r' {
            lb_level = LineBreakLevel::Newline;
            for c2 in iter.by_ref() {
                if c2 == '\n' || c2 == '\r' {
                    if c == c2 {
                        return LineBreakLevel::DoubleNewline;
                    }
                } else {
                    break;
                }
            }
        }
    }
    lb_level
}

const INLINE_SPACE_CHARS: [char; 2] = [' ', '\t'];

struct AtomChunk {
    range: TextRange,
    boundary_syntax_level: usize,
    internal_lb_level: LineBreakLevel,
    boundary_lb_level: LineBreakLevel,
    /// Cost of ending a chunk, or starting one, at this atom's start: the
    /// boundary is inside a syntax node that fits in `chunk_size`, or inside
    /// or just after a node's header.
    boundary_cut_cost: u32,
}

struct AtomChunksCollector<'s> {
    full_text: &'s str,
    curr_level: usize,
    min_level: usize,
    atom_chunks: Vec<AtomChunk>,
}

impl<'s> AtomChunksCollector<'s> {
    fn collect(&mut self, range: TextRange, boundary_cut_cost: u32) {
        // Trim trailing whitespaces.
        let end_trimmed_text = &self.full_text[range.start..range.end].trim_end();
        if end_trimmed_text.is_empty() {
            return;
        }

        // Trim leading whitespaces.
        let trimmed_text = end_trimmed_text.trim_start();
        let new_start = range.start + (end_trimmed_text.len() - trimmed_text.len());
        let new_end = new_start + trimmed_text.len();

        // Align to beginning of the line if possible.
        let prev_end = self.atom_chunks.last().map_or(0, |chunk| chunk.range.end);
        let gap = &self.full_text[prev_end..new_start];
        let boundary_lb_level = line_break_level(gap);
        let range = if boundary_lb_level != LineBreakLevel::Inline {
            let trimmed_gap = gap.trim_end_matches(INLINE_SPACE_CHARS);
            TextRange::new(prev_end + trimmed_gap.len(), new_end)
        } else {
            TextRange::new(new_start, new_end)
        };

        self.atom_chunks.push(AtomChunk {
            range,
            boundary_syntax_level: self.min_level,
            internal_lb_level: line_break_level(trimmed_text),
            boundary_lb_level,
            boundary_cut_cost,
        });
        self.min_level = self.curr_level;
    }

    fn into_atom_chunks(mut self) -> Vec<AtomChunk> {
        self.atom_chunks.push(AtomChunk {
            range: TextRange::new(self.full_text.len(), self.full_text.len()),
            boundary_syntax_level: self.min_level,
            internal_lb_level: LineBreakLevel::Inline,
            boundary_lb_level: LineBreakLevel::DoubleNewline,
            boundary_cut_cost: 0,
        });
        self.atom_chunks
    }
}

struct ChunkOutput {
    start_pos: Position,
    end_pos: Position,
}

struct InternalRecursiveChunker<'s> {
    full_text: &'s str,
    chunk_size: usize,
    chunk_overlap: usize,
    min_chunk_size: usize,
    /// `(2³² - 1) / min_chunk_size`: the fixed-point scale for `too_small_chunk_cost`.
    too_small_chunk_cost_scale: u64,
    min_atom_chunk_size: usize,
    tiny_chunk_cost: usize,
    /// See `body_field_ids`; empty without a syntax tree.
    body_field_ids: Vec<NonZeroU16>,
}

/// A node's header and the body it introduces (see `node_header`).
#[derive(Clone, Copy)]
struct NodeHeader {
    /// A chunk boundary at any position `p` with `start < p <= end` splits
    /// the header or leaves it without its body.
    range: TextRange,
    body: TextRange,
}

/// The header of `node`: the text that introduces its body on a later line,
/// such as `def f(x) -> int:`, `if x {` or a Markdown heading.
///
/// The range starts at the first content on the line where `node` starts,
/// so the header line as a whole stays together, as in `} else {`. It ends
/// at the body's first content, past any `{`, `(` or `[` that opens it.
///
/// The body is the child in one of `BODY_FIELD_NAMES`, given as the tree's
/// `body_field_ids`; else the last named child, if it opens with a bracket
/// and a keyword or operator token comes just before it (`else {`,
/// `return [`, `x = {`); else, if the first named child is a heading, the
/// child after it.
fn node_header(
    full_text: &str,
    body_field_ids: &[NonZeroU16],
    node: tree_sitter::Node<'_>,
) -> Option<NodeHeader> {
    let bytes = full_text.as_bytes();
    let body = body_field_ids
        .iter()
        .find_map(|&field| node.child_by_field_id(field.get()))
        .or_else(|| {
            let last = node.named_child(node.named_child_count().checked_sub(1)?)?;
            let opens_with_bracket =
                matches!(bytes.get(last.start_byte()), Some(b'{' | b'(' | b'['));
            (opens_with_bracket && last.prev_sibling().is_some_and(|token| !token.is_named()))
                .then_some(last)
        })
        .or_else(|| {
            let first = node.named_child(0)?;
            if first.kind().ends_with("heading") {
                first.next_named_sibling()
            } else {
                None
            }
        })?;
    if body.start_byte() <= node.start_byte() {
        return None;
    }
    let mut end = body.start_byte();
    while end < node.end_byte()
        && (bytes[end].is_ascii_whitespace() || matches!(bytes[end], b'{' | b'(' | b'['))
    {
        end += 1;
    }
    // A header ends a line: a body whose content starts on the header's line,
    // as in `{x}` or `f(x)`, can't be left in the next chunk by a line break.
    if end >= node.end_byte() || !bytes[node.start_byte()..end].contains(&b'\n') {
        return None;
    }
    let mut start = bytes[..node.start_byte()]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |i| i + 1);
    while start < node.start_byte() && matches!(bytes[start], b' ' | b'\t') {
        start += 1;
    }
    Some(NodeHeader {
        range: TextRange::new(start, end),
        body: TextRange::new(body.start_byte(), body.end_byte()),
    })
}

/// The ids of `BODY_FIELD_NAMES` in the language of `tree`, resolved once per
/// split: resolving a field name scans all of the grammar's field names.
fn body_field_ids(tree: &tree_sitter::Tree) -> Vec<NonZeroU16> {
    let language = tree.language();
    BODY_FIELD_NAMES
        .iter()
        .filter_map(|name| language.field_id_for_name(name))
        .collect()
}

/// A frame of the syntax walk in `collect_atom_chunks`.
struct WalkFrame<'t, 's: 't> {
    iter: ChunkIterator<'t, 's>,
    /// The syntax node this frame walks inside of, if it fits in `chunk_size`:
    /// a cut inside it is avoidable, since the node can stay whole.
    fitting_node: Option<TextRange>,
    /// The header of that syntax node, if it has one.
    header: Option<NodeHeader>,
    /// The content of that syntax node, trimmed, if it is the body of a node
    /// that does not fit, or all of such a body's text: it may fit, but is not
    /// kept whole (see `node_frame`).
    unfitting_body: Option<TextRange>,
}

impl<'t, 's: 't> InternalRecursiveChunker<'s> {
    /// `range`, a syntax node's range, if the node fits in `chunk_size`. A
    /// chunk that starts after only indentation on its line starts at the
    /// line's beginning, so the indentation counts toward the node's size.
    fn fitting_node(&self, range: TextRange) -> Option<TextRange> {
        if range.len() > self.chunk_size {
            return None;
        }
        let bytes = self.full_text.as_bytes();
        let mut line_start = range.start;
        while line_start > 0 && matches!(bytes[line_start - 1], b' ' | b'\t') {
            line_start -= 1;
        }
        if line_start > 0 && bytes[line_start - 1] != b'\n' {
            line_start = range.start;
        }
        (range.end - line_start <= self.chunk_size).then_some(range)
    }

    /// `range` without its leading and trailing whitespace.
    fn trimmed(&self, range: TextRange) -> TextRange {
        let text = &self.full_text[range.start..range.end];
        let start = range.start + (text.len() - text.trim_start().len());
        TextRange::new(start, range.start + text.trim_end().len())
    }

    /// The frame that walks inside `node`, a child of the `parent` frame.
    ///
    /// The body of a node that does not fit is not kept whole, even if it fits
    /// on its own: the unit is the header with its body, and keeping the body
    /// whole would leave the header alone in a chunk. The header cost keeps
    /// the header with the start of the body instead. The same holds for a
    /// node that is all of such a body's content and `splits_as_text`, such as
    /// the `inline` text of a Markdown paragraph, which splits at sentences.
    /// A body with a header of its own, such as a decorated `def`, is a unit
    /// itself and stays whole where it fits.
    fn node_frame(
        &self,
        iter: ChunkIterator<'t, 's>,
        node: tree_sitter::Node<'_>,
        splits_as_text: bool,
        parent: Option<&WalkFrame<'t, 's>>,
    ) -> WalkFrame<'t, 's> {
        let range = TextRange::new(node.start_byte(), node.end_byte());
        let content = self.trimmed(range);
        let header = node_header(self.full_text, &self.body_field_ids, node);
        let unfitting_body = (header.is_none()
            && parent.is_some_and(|parent| {
                (parent.fitting_node.is_none() && parent.header.is_some_and(|h| h.body == range))
                    || (splits_as_text && parent.unfitting_body == Some(content))
            }))
        .then_some(content);
        WalkFrame {
            iter,
            fitting_node: if unfitting_body.is_some() {
                None
            } else {
                self.fitting_node(range)
            },
            header,
            unfitting_body,
        }
    }

    /// The cost of cutting at the start of the atom at `range`, given the
    /// frames of the syntax walk that the atom is inside of.
    ///
    /// A cut inside a node that fits in `chunk_size` costs
    /// `CUT_FITTING_NODE_COST`, a cut inside a node's header costs
    /// `CUT_INSIDE_HEADER_COST`, and one just after it `CUT_AFTER_HEADER_COST`,
    /// however many such nodes there are.
    fn boundary_cut_cost(&self, frames: &[WalkFrame<'t, 's>], range: TextRange) -> u32 {
        let text = &self.full_text[range.start..range.end];
        let pos = range.start + (text.len() - text.trim_start().len());
        let mut cost = 0;
        let headers = || frames.iter().filter_map(|frame| frame.header);
        if headers().any(|h| h.range.start < pos && pos < h.range.end) {
            cost += CUT_INSIDE_HEADER_COST;
        } else if headers().any(|h| pos == h.range.end) {
            cost += CUT_AFTER_HEADER_COST;
        }
        if frames
            .iter()
            .filter_map(|frame| frame.fitting_node)
            .any(|node| node.start < pos && pos < node.end)
        {
            cost += CUT_FITTING_NODE_COST;
        }
        cost
    }

    fn collect_atom_chunks(
        &self,
        chunk: InternalChunk<'t, 's>,
        atom_collector: &mut AtomChunksCollector<'s>,
    ) {
        let root_node = match &chunk.kind {
            ChunkKind::TreeSitterNode { node, .. } => Some(*node),
            ChunkKind::RegexpSepChunk { .. } => None,
        };
        let root_iter = ChunkIterator::Once(std::iter::once(chunk));
        let mut frames = vec![match root_node {
            Some(node) => self.node_frame(root_iter, node, false, None),
            None => WalkFrame {
                iter: root_iter,
                fitting_node: None,
                header: None,
                unfitting_body: None,
            },
        }];

        while !frames.is_empty() {
            atom_collector.curr_level = frames.len();

            if let Some(current_chunk) = frames.last_mut().unwrap().iter.next() {
                if current_chunk.range.len() <= self.min_atom_chunk_size {
                    let cost = self.boundary_cut_cost(&frames, current_chunk.range);
                    atom_collector.collect(current_chunk.range, cost);
                } else {
                    match current_chunk.kind {
                        ChunkKind::TreeSitterNode {
                            tree_sitter_info: lang_config,
                            node,
                        } => {
                            if !lang_config.terminal_node_kind_ids.contains(&node.kind_id()) {
                                let mut cursor = node.walk();
                                if cursor.goto_first_child() {
                                    let iter = ChunkIterator::TreeSitter(TreeSitterNodeIter {
                                        lang_config,
                                        full_text: self.full_text,
                                        cursor: Some(cursor),
                                        // The chunk's range, not the node's: a
                                        // root node may not span the whole text
                                        // (YAML's stops before a stray BOM), and
                                        // the gaps around its children must
                                        // still be covered.
                                        next_start_pos: current_chunk.range.start,
                                        end_pos: current_chunk.range.end,
                                    });
                                    let frame = self.node_frame(iter, node, false, frames.last());
                                    frames.push(frame);
                                    continue;
                                }
                            }
                            // A terminal node, or one with no children, is split
                            // as text; a cut inside it still cuts the node.
                            let iter = ChunkIterator::Once(std::iter::once(InternalChunk {
                                full_text: self.full_text,
                                range: current_chunk.range,
                                kind: ChunkKind::RegexpSepChunk {
                                    lang_config: &DEFAULT_LANGUAGE_CONFIG,
                                    next_regexp_sep_id: 0,
                                },
                            }));
                            let frame = self.node_frame(iter, node, true, frames.last());
                            frames.push(frame);
                        }
                        ChunkKind::RegexpSepChunk {
                            lang_config,
                            next_regexp_sep_id,
                        } => {
                            if next_regexp_sep_id >= lang_config.separator_regex.len() {
                                let cost = self.boundary_cut_cost(&frames, current_chunk.range);
                                atom_collector.collect(current_chunk.range, cost);
                            } else {
                                frames.push(WalkFrame {
                                    iter: ChunkIterator::Text(TextChunksIter::new(
                                        lang_config,
                                        current_chunk.full_text,
                                        current_chunk.range,
                                        next_regexp_sep_id,
                                    )),
                                    fitting_node: None,
                                    header: None,
                                    unfitting_body: None,
                                });
                            }
                        }
                    }
                }
            } else {
                frames.pop();
                let level_after_pop = frames.len();
                atom_collector.curr_level = level_after_pop;
                if level_after_pop < atom_collector.min_level {
                    atom_collector.min_level = level_after_pop;
                }
            }
        }
        atom_collector.curr_level = 0;
    }

    /// The cost of a chunk of `chunk_size` bytes for falling short of
    /// `min_chunk_size`: `TOO_SMALL_CHUNK_COST` times the square of the
    /// shortfall as a fraction of `min_chunk_size`. A chunk a little under the
    /// floor costs less than a cut through a syntax node, so it wins over one;
    /// a chunk far under it costs as much as many such cuts, so it doesn't.
    fn too_small_chunk_cost(&self, chunk_size: usize) -> usize {
        if chunk_size >= self.min_chunk_size {
            return 0;
        }
        let scaled_shortfall =
            (self.min_chunk_size - chunk_size) as u64 * self.too_small_chunk_cost_scale;
        let graded = ((scaled_shortfall * scaled_shortfall) >> TOO_SMALL_CHUNK_COST_SHIFT) as usize;
        if chunk_size.saturating_mul(4) < self.chunk_size {
            graded + self.tiny_chunk_cost
        } else {
            graded
        }
    }

    fn get_overlap_cost_base(&self, offset: usize) -> usize {
        ((self.full_text.len() - offset) * MISSING_OVERLAP_COST)
            .checked_div(self.chunk_overlap)
            .unwrap_or(0)
    }

    fn merge_atom_chunks(&self, atom_chunks: Vec<AtomChunk>) -> Vec<ChunkOutput> {
        struct AtomRoutingPlan {
            start_idx: usize,
            prev_plan_idx: usize,
            cost: usize,
            overlap_cost_base: usize,
        }
        type PrevPlanCandidate = (std::cmp::Reverse<usize>, usize);

        let mut plans = Vec::with_capacity(atom_chunks.len());
        plans.push(AtomRoutingPlan {
            start_idx: 0,
            prev_plan_idx: 0,
            cost: 0,
            overlap_cost_base: self.get_overlap_cost_base(0),
        });
        let mut prev_plan_candidates = std::collections::BinaryHeap::<PrevPlanCandidate>::new();

        let mut gap_cost_cache = vec![0];
        let mut syntax_level_gap_cost = |boundary: usize, internal: usize| -> usize {
            if boundary > internal {
                let gap = boundary - internal;
                for i in gap_cost_cache.len()..=gap {
                    gap_cost_cache.push(gap_cost_cache[i - 1] + SYNTAX_LEVEL_GAP_COST / i);
                }
                gap_cost_cache[gap]
            } else {
                0
            }
        };

        for (i, chunk) in atom_chunks[0..atom_chunks.len() - 1].iter().enumerate() {
            let mut min_cost = usize::MAX;
            let mut arg_min_start_idx: usize = 0;
            let mut arg_min_prev_plan_idx: usize = 0;
            let mut start_idx = i;

            let end_syntax_level = atom_chunks[i + 1].boundary_syntax_level;
            let end_lb_level = atom_chunks[i + 1].boundary_lb_level;
            let end_cut_cost = atom_chunks[i + 1].boundary_cut_cost;

            let mut internal_syntax_level = usize::MAX;
            let mut internal_lb_level = LineBreakLevel::Inline;

            fn lb_level_gap(boundary: LineBreakLevel, internal: LineBreakLevel) -> usize {
                if boundary.ord() < internal.ord() {
                    internal.ord() - boundary.ord()
                } else {
                    0
                }
            }
            loop {
                let start_chunk = &atom_chunks[start_idx];
                let chunk_size = chunk.range.end - start_chunk.range.start;

                let mut cost = 0;
                cost +=
                    syntax_level_gap_cost(start_chunk.boundary_syntax_level, internal_syntax_level);
                cost += syntax_level_gap_cost(end_syntax_level, internal_syntax_level);
                cost += (start_chunk.boundary_cut_cost + end_cut_cost) as usize;
                cost += (lb_level_gap(start_chunk.boundary_lb_level, internal_lb_level)
                    + lb_level_gap(end_lb_level, internal_lb_level))
                    * PER_LINE_BREAK_LEVEL_GAP_COST;
                cost += self.too_small_chunk_cost(chunk_size);

                if chunk_size > self.chunk_size {
                    if min_cost == usize::MAX {
                        min_cost = cost + plans[start_idx].cost;
                        arg_min_start_idx = start_idx;
                        arg_min_prev_plan_idx = start_idx;
                    }
                    break;
                }

                let prev_plan_idx = if self.chunk_overlap > 0 {
                    while let Some(top_prev_plan) = prev_plan_candidates.peek() {
                        let overlap_size =
                            atom_chunks[top_prev_plan.1].range.end - start_chunk.range.start;
                        if overlap_size <= self.chunk_overlap {
                            break;
                        }
                        prev_plan_candidates.pop();
                    }
                    prev_plan_candidates.push((
                        std::cmp::Reverse(
                            plans[start_idx].cost + plans[start_idx].overlap_cost_base,
                        ),
                        start_idx,
                    ));
                    prev_plan_candidates.peek().unwrap().1
                } else {
                    start_idx
                };
                let prev_plan = &plans[prev_plan_idx];
                cost += prev_plan.cost;
                if self.chunk_overlap == 0 {
                    cost += MISSING_OVERLAP_COST / 2;
                } else {
                    let start_cost_base = self.get_overlap_cost_base(start_chunk.range.start);
                    cost += if prev_plan.overlap_cost_base < start_cost_base {
                        MISSING_OVERLAP_COST + prev_plan.overlap_cost_base - start_cost_base
                    } else {
                        MISSING_OVERLAP_COST
                    };
                }
                if cost < min_cost {
                    min_cost = cost;
                    arg_min_start_idx = start_idx;
                    arg_min_prev_plan_idx = prev_plan_idx;
                }

                if start_idx == 0 {
                    break;
                }

                start_idx -= 1;
                internal_syntax_level =
                    internal_syntax_level.min(start_chunk.boundary_syntax_level);
                internal_lb_level = internal_lb_level.max(start_chunk.internal_lb_level);
            }
            plans.push(AtomRoutingPlan {
                start_idx: arg_min_start_idx,
                prev_plan_idx: arg_min_prev_plan_idx,
                cost: min_cost,
                overlap_cost_base: self.get_overlap_cost_base(chunk.range.end),
            });
            prev_plan_candidates.clear();
        }

        let mut output = Vec::new();
        let mut plan_idx = plans.len() - 1;
        while plan_idx > 0 {
            let plan = &plans[plan_idx];
            let start_chunk = &atom_chunks[plan.start_idx];
            let end_chunk = &atom_chunks[plan_idx - 1];
            output.push(ChunkOutput {
                start_pos: Position::new(start_chunk.range.start),
                end_pos: Position::new(end_chunk.range.end),
            });
            plan_idx = plan.prev_plan_idx;
        }
        output.reverse();
        output
    }

    fn split_root_chunk(&self, kind: ChunkKind<'t>) -> Vec<ChunkOutput> {
        let mut atom_collector = AtomChunksCollector {
            full_text: self.full_text,
            min_level: 0,
            curr_level: 0,
            atom_chunks: Vec::new(),
        };
        self.collect_atom_chunks(
            InternalChunk {
                full_text: self.full_text,
                range: TextRange::new(0, self.full_text.len()),
                kind,
            },
            &mut atom_collector,
        );
        let atom_chunks = atom_collector.into_atom_chunks();
        self.merge_atom_chunks(atom_chunks)
    }
}

/// A recursive text chunker with syntax awareness.
pub struct RecursiveChunker {
    custom_languages: HashMap<UniCase<String>, Arc<SimpleLanguageConfig>>,
}

impl RecursiveChunker {
    /// Create a new recursive chunker with the given configuration.
    ///
    /// Returns an error if any regex pattern is invalid or if there are duplicate language names.
    pub fn new(config: RecursiveSplitConfig) -> Result<Self, String> {
        let mut custom_languages = HashMap::new();
        for lang in config.custom_languages {
            let separator_regex = lang
                .separators_regex
                .iter()
                .map(|s| Regex::new(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    format!(
                        "failed in parsing regexp for language `{}`: {}",
                        lang.language_name, e
                    )
                })?;
            let language_config = Arc::new(SimpleLanguageConfig {
                name: lang.language_name,
                aliases: lang.aliases,
                separator_regex,
            });
            if custom_languages
                .insert(
                    UniCase::new(language_config.name.clone()),
                    language_config.clone(),
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate language name / alias: `{}`",
                    language_config.name
                ));
            }
            for alias in &language_config.aliases {
                if custom_languages
                    .insert(UniCase::new(alias.clone()), language_config.clone())
                    .is_some()
                {
                    return Err(format!("duplicate language name / alias: `{}`", alias));
                }
            }
        }
        Ok(Self { custom_languages })
    }

    /// Split the source into chunks according to the configuration.
    ///
    /// Routing: a custom (regex) language configured on this chunker takes
    /// priority (never parses); otherwise the source's tree-sitter parse is
    /// used when available, and everything else (unknown language, no grammar,
    /// parse failure) degrades to the default regex splitter. The parse is
    /// cached on the [`CodeSource`], so several consumers of the same source
    /// parse at most once.
    pub fn split(&self, source: &CodeSource<'_>, config: RecursiveChunkConfig) -> Vec<Chunk> {
        let text = source.text();
        let min_chunk_size = config.min_chunk_size.unwrap_or(config.chunk_size / 2);
        let chunk_overlap = std::cmp::min(config.chunk_overlap.unwrap_or(0), min_chunk_size);

        let language = UniCase::new(source.requested_language().unwrap_or_default().to_string());
        let custom_language = self.custom_languages.get(&language);
        // A custom language never parses.
        let tree = match custom_language {
            Some(_) => None,
            None => match source.tree() {
                ParseOutcome::Parsed(tree) => Some(tree),
                _ => None,
            },
        };

        let internal_chunker = InternalRecursiveChunker {
            full_text: text,
            chunk_size: config.chunk_size,
            chunk_overlap,
            min_chunk_size,
            too_small_chunk_cost_scale: u64::from(u32::MAX)
                .checked_div(min_chunk_size as u64)
                .unwrap_or(0),
            min_atom_chunk_size: if chunk_overlap > 0 {
                chunk_overlap
            } else {
                min_chunk_size
            },
            tiny_chunk_cost: if chunk_overlap > 0 {
                TINY_CHUNK_WITH_OVERLAP_COST
            } else {
                TINY_CHUNK_COST
            },
            body_field_ids: tree.map_or_else(Vec::new, body_field_ids),
        };

        let output = if let Some(lang_config) = custom_language {
            internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config,
                next_regexp_sep_id: 0,
            })
        } else if let Some(tree) = tree {
            let tree_sitter_info = source
                .treesitter_info()
                .expect("a parsed tree implies tree-sitter info");
            internal_chunker.split_root_chunk(ChunkKind::TreeSitterNode {
                tree_sitter_info,
                node: tree.root_node(),
            })
        } else {
            // Unknown language, no grammar, or parse failure: default regex split.
            internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config: &DEFAULT_LANGUAGE_CONFIG,
                next_regexp_sep_id: 0,
            })
        };

        finalize_chunks(text, output)
    }
}

/// Compute line/column positions on `output` and convert to the public `Chunk`s.
fn finalize_chunks(text: &str, mut output: Vec<ChunkOutput>) -> Vec<Chunk> {
    set_output_positions(
        text,
        output.iter_mut().flat_map(|chunk_output| {
            std::iter::once(&mut chunk_output.start_pos)
                .chain(std::iter::once(&mut chunk_output.end_pos))
        }),
    );
    output
        .into_iter()
        .map(|chunk_output| {
            let start = chunk_output.start_pos.output.unwrap();
            let end = chunk_output.end_pos.output.unwrap();
            Chunk {
                range: TextRange::new(
                    chunk_output.start_pos.byte_offset,
                    chunk_output.end_pos.byte_offset,
                ),
                start,
                end,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_keeps_text_past_the_end_of_the_root_node() {
        // YAML's root node stops before a stray byte order mark, and with an
        // overlap the root is descended into: the text after it must stay.
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "{日本語\u{feff}é```";
        let config = RecursiveChunkConfig {
            chunk_size: 373,
            min_chunk_size: None,
            chunk_overlap: Some(15),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "yaml"), config);
        let covered: String = chunks
            .iter()
            .map(|chunk| &text[chunk.range.start..chunk.range.end])
            .collect();
        assert_eq!(covered, text);
    }

    #[test]
    fn test_split_basic() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "Linea 1.\nLinea 2.\n\nLinea 3.";
        let config = RecursiveChunkConfig {
            chunk_size: 15,
            min_chunk_size: Some(5),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::new(text), config);

        assert_eq!(chunks.len(), 3);
        assert_eq!(
            &text[chunks[0].range.start..chunks[0].range.end],
            "Linea 1."
        );
        assert_eq!(
            &text[chunks[1].range.start..chunks[1].range.end],
            "Linea 2."
        );
        assert_eq!(
            &text[chunks[2].range.start..chunks[2].range.end],
            "Linea 3."
        );
    }

    #[test]
    fn test_split_long_text() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "A very very long text that needs to be split.";
        let config = RecursiveChunkConfig {
            chunk_size: 20,
            min_chunk_size: Some(12),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::new(text), config);

        assert!(chunks.len() > 1);
        for chunk in &chunks {
            let chunk_text = &text[chunk.range.start..chunk.range.end];
            assert!(chunk_text.len() <= 20);
        }
    }

    #[test]
    fn test_split_with_overlap() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "This is a test text that is a bit longer to see how the overlap works.";
        let config = RecursiveChunkConfig {
            chunk_size: 20,
            min_chunk_size: Some(10),
            chunk_overlap: Some(5),
        };
        let chunks = chunker.split(&CodeSource::new(text), config);

        assert!(chunks.len() > 1);
        for chunk in &chunks {
            let chunk_text = &text[chunk.range.start..chunk.range.end];
            assert!(
                chunk_text.len() <= 25,
                "Chunk was too long: '{}'",
                chunk_text
            );
        }
    }

    #[test]
    fn test_split_trims_whitespace() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "  \n First chunk  \n\n  Second chunk with spaces at the end    \n";
        let config = RecursiveChunkConfig {
            chunk_size: 30,
            min_chunk_size: Some(10),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::new(text), config);

        assert_eq!(chunks.len(), 3);
        // Verify chunks are trimmed appropriately
        let chunk_text = &text[chunks[0].range.start..chunks[0].range.end];
        assert!(!chunk_text.starts_with("  "));
    }

    const PYTHON_SAMPLE: &str = r#"class Inventory:
    """Stock levels for one warehouse."""

    def __init__(self, name):
        self.name = name
        self.items = {}

    def add_item(self, sku, qty):
        if qty <= 0:
            raise ValueError("qty must be positive")
        self.items[sku] = self.items.get(sku, 0) + qty

    def remove_item(self, sku, qty):
        have = self.items.get(sku, 0)
        if qty > have:
            raise ValueError(f"only {have} of {sku} left")
        self.items[sku] = have - qty

    class Report:
        def __init__(self, inventory):
            self.inventory = inventory

        def low_stock(self, limit=5):
            items = self.inventory.items
            return [k for k in items if items[k] < limit]

        def total(self):
            return sum(self.inventory.items.values())


def load(path):
    inv = Inventory(path.stem)
    for line in path.read_text().splitlines():
        sku, qty = line.split(",")
        inv.add_item(sku, int(qty))
    return inv
"#;

    /// A byte range's size in a chunk: a node on its own line counts its indentation.
    fn size_with_indent(text: &str, range: &TextRange) -> usize {
        let line_start = text[..range.start].rfind('\n').map_or(0, |p| p + 1);
        if text[line_start..range.start].trim().is_empty() {
            range.end - line_start
        } else {
            range.end - range.start
        }
    }

    fn python_nodes(tree: &tree_sitter::Tree, kinds: &[&str]) -> Vec<TextRange> {
        fn walk(node: tree_sitter::Node, kinds: &[&str], out: &mut Vec<TextRange>) {
            if kinds.contains(&node.kind()) {
                out.push(TextRange::new(node.start_byte(), node.end_byte()));
            }
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                walk(child, kinds, out);
            }
        }
        let mut out = Vec::new();
        walk(tree.root_node(), kinds, &mut out);
        out
    }

    /// A function or class that fits in `chunk_size` is never cut: the splitter
    /// leaves a chunk under `min_chunk_size` rather than split it. Here
    /// `class Report:` with its `__init__` is ~95 bytes, under the default
    /// floor at chunk sizes 200-250, and the old flat penalty cut `low_stock`
    /// from its signature to avoid it.
    #[test]
    fn test_split_python_keeps_fitting_functions_whole() {
        let text = PYTHON_SAMPLE;
        let source = CodeSource::with_language(text, "python");
        let ParseOutcome::Parsed(tree) = source.tree() else {
            panic!("python must parse");
        };
        let nodes = python_nodes(tree, &["function_definition", "class_definition"]);

        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        for chunk_size in (100..=1100).step_by(25) {
            let chunks = chunker.split(
                &source,
                RecursiveChunkConfig {
                    chunk_size,
                    min_chunk_size: None,
                    chunk_overlap: None,
                },
            );
            for chunk in &chunks {
                for node in nodes
                    .iter()
                    .filter(|n| size_with_indent(text, n) <= chunk_size)
                {
                    assert!(
                        !(node.start < chunk.range.start && chunk.range.start < node.end),
                        "chunk_size {chunk_size}: a chunk starts inside a function or class \
                         that fits, at line {}:\n{}",
                        chunk.start.line,
                        &text[chunk.range.start..chunk.range.end],
                    );
                }
            }
        }
    }

    const RUST_SAMPLE: &str = r#"use std::collections::HashMap;

/// Items in stock, by name.
pub struct Inventory {
    items: HashMap<String, u32>,
}

impl Inventory {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    pub fn add(&mut self, name: &str, count: u32) {
        *self.items.entry(name.to_string()).or_insert(0) += count;
    }

    pub fn remove(&mut self, name: &str, count: u32) -> Result<(), String> {
        let have = self.items.get_mut(name).ok_or_else(|| format!("no {name}"))?;
        if *have < count {
            return Err(format!("only {have} of {name}, asked for {count}"));
        }
        *have -= count;
        Ok(())
    }

    pub fn low_stock(&self, limit: u32) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .items
            .iter()
            .filter(|(_, count)| **count < limit)
            .map(|(name, _)| name.as_str())
            .collect();
        names.sort();
        names
    }
}

pub fn total(inventory: &Inventory) -> u32 {
    inventory.items.values().sum()
}
"#;

    const PYTHON_BRANCHES_SAMPLE: &str = r#"import functools


class Cache:
    """A small cache with a size limit and hit counters."""

    @functools.cached_property
    def capacity(self) -> int:
        return self.settings.get("capacity", 128)

    @staticmethod
    def key_of(request, *, include_headers: bool = False) -> str:
        if include_headers:
            return f"{request.method} {request.path} {sorted(request.headers.items())}"
        elif request.query:
            return f"{request.method} {request.path}?{request.query}"
        else:
            return f"{request.method} {request.path}"

    def get(self, request):
        key = self.key_of(request)
        if key in self.entries:
            self.hits += 1
            return self.entries[key]
        self.misses += 1
        return None
"#;

    const MARKDOWN_SAMPLE: &str = r#"# Inventory service

The inventory service keeps stock levels for every warehouse and answers
queries from the storefront and from the planning tools.

## Setup

Install the package, then point it at a database. The service reads its
settings from the environment and refuses to start without a database URL.

## Usage

Call `add_item` to record a delivery and `remove_item` to record a sale. A sale
that asks for more than the warehouse holds fails and changes nothing.

### Reports

`low_stock` lists the items under a limit. `total` counts every item in the
warehouse, across all of its locations.
"#;

    /// Where the first unit of `body` ends: the first child of a body that
    /// has no header of its own, else the first unit of what that header
    /// introduces (a decorated `def`'s first statement, not its name).
    fn first_unit_end(text: &str, ids: &[NonZeroU16], body: tree_sitter::Node) -> usize {
        if let Some(header) = node_header(text, ids, body) {
            let mut cursor = body.walk();
            let inner = body
                .children(&mut cursor)
                .find(|c| c.start_byte() == header.body.start && c.end_byte() == header.body.end);
            if let Some(inner) = inner {
                return first_unit_end(text, ids, inner);
            }
        }
        body.named_child(0)
            .map_or(body.end_byte(), |c| c.end_byte())
    }

    /// Asserts, at every chunk size from 100 to 1100, that no chunk boundary
    /// falls inside a node's header (see `node_header`), and that a header
    /// ends a chunk only where it and the first unit of its body don't fit in
    /// `chunk_size` together.
    fn assert_headers_kept_whole_with_body(text: &str, language: &str) {
        let source = CodeSource::with_language(text, language);
        let ParseOutcome::Parsed(tree) = source.tree() else {
            panic!("{language} must parse");
        };
        let ids = body_field_ids(tree);
        let mut headers = Vec::new();
        let mut stack = vec![tree.root_node()];
        while let Some(node) = stack.pop() {
            if let Some(header) = node_header(text, &ids, node) {
                let mut cursor = node.walk();
                let body = node
                    .children(&mut cursor)
                    .find(|c| {
                        c.start_byte() == header.body.start && c.end_byte() == header.body.end
                    })
                    .unwrap();
                let line_start = text[..header.range.start].rfind('\n').map_or(0, |p| p + 1);
                headers.push((header.range, first_unit_end(text, &ids, body) - line_start));
            }
            let mut cursor = node.walk();
            stack.extend(node.children(&mut cursor));
        }
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        for chunk_size in (100..=1100).step_by(25) {
            let chunks = chunker.split(
                &source,
                RecursiveChunkConfig {
                    chunk_size,
                    min_chunk_size: None,
                    chunk_overlap: None,
                },
            );
            for chunk in &chunks[1..] {
                let chunk_text = &text[chunk.range.start..chunk.range.end];
                let start = chunk.range.end - chunk_text.trim_start().len();
                for &(header, with_first_unit) in &headers {
                    let header_text = &text[header.start..header.end];
                    assert!(
                        !(header.start < start && start < header.end),
                        "chunk_size {chunk_size}: a chunk splits the header {header_text:?}, \
                         at line {}:\n{chunk_text}",
                        chunk.start.line,
                    );
                    assert!(
                        !(start == header.end && with_first_unit <= chunk_size),
                        "chunk_size {chunk_size}: the header {header_text:?} ends a chunk, \
                         though it fits with its body's first {with_first_unit} bytes",
                    );
                }
            }
        }
    }

    /// A cut never splits a header, as in `def f(x) -> int` | `:` or
    /// `fn f()` | `{`, and leaves a header without its body only where the
    /// two can't share a chunk.
    #[test]
    fn test_split_keeps_headers_whole_with_body() {
        assert_headers_kept_whole_with_body(PYTHON_SAMPLE, "python");
        assert_headers_kept_whole_with_body(PYTHON_BRANCHES_SAMPLE, "python");
        assert_headers_kept_whole_with_body(RUST_SAMPLE, "rust");
        assert_headers_kept_whole_with_body(MARKDOWN_SAMPLE, "markdown");
    }

    /// A Markdown heading stays with the text of its section.
    #[test]
    fn test_split_markdown_keeps_heading_with_section() {
        let text = MARKDOWN_SAMPLE;
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        for chunk_size in (100..=1100).step_by(25) {
            let chunks = chunker.split(
                &CodeSource::with_language(text, "markdown"),
                RecursiveChunkConfig {
                    chunk_size,
                    min_chunk_size: None,
                    chunk_overlap: None,
                },
            );
            for chunk in &chunks[..chunks.len() - 1] {
                let chunk_text = text[chunk.range.start..chunk.range.end].trim_end();
                let last_line = chunk_text.rsplit('\n').next().unwrap();
                assert!(
                    !last_line.starts_with('#'),
                    "chunk_size {chunk_size}: a chunk ends on a heading, at line {}:\n{chunk_text}",
                    chunk.end.line,
                );
            }
        }
    }

    /// A node of a terminal kind is split as text, but a cut inside it still
    /// cuts the node: a doc comment that fits stays whole.
    #[test]
    fn test_split_javascript_keeps_fitting_comment_whole() {
        let comment = format!(
            "/**\n{} */",
            " * Adds two numbers and returns their sum, for the totals.\n".repeat(4)
        );
        let text = format!(
            "{comment}\nfunction add(a, b) {{\n  const sum = a + b;\n  return sum;\n}}\n\
             function sub(a, b) {{\n  return a - b;\n}}\n"
        );
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let chunks = chunker.split(
            &CodeSource::with_language(&text, "javascript"),
            RecursiveChunkConfig {
                chunk_size: 300,
                min_chunk_size: None,
                chunk_overlap: None,
            },
        );
        assert_eq!(
            &text[chunks[0].range.start..chunks[0].range.end],
            comment,
            "chunks: {:?}",
            chunks.iter().map(|c| c.range).collect::<Vec<_>>(),
        );
    }

    /// The fixed-point too-small cost stays in range at any `min_chunk_size`,
    /// including one larger than 2³² bytes, where its scale is zero.
    #[test]
    fn test_split_too_small_cost_in_range_at_extreme_sizes() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "Linea 1.\nLinea 2.\n\nLinea 3.";
        for (chunk_size, min_chunk_size) in [
            (15, Some(1)),
            (15, Some(14)),
            (15, Some(15)),
            (1 << 20, Some((1 << 20) - 1)),
            (1 << 40, None),
            (usize::MAX, Some(usize::MAX)),
        ] {
            let chunks = chunker.split(
                &CodeSource::new(text),
                RecursiveChunkConfig {
                    chunk_size,
                    min_chunk_size,
                    chunk_overlap: None,
                },
            );
            assert!(!chunks.is_empty(), "chunk_size {chunk_size}");
        }
    }

    #[test]
    fn test_split_with_rust_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"
fn main() {
    println!("Hello");
}

fn other() {
    let x = 1;
}
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 50,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "rust"), config);

        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_with_svelte_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"<script lang="ts">
  let count = 0;
  function increment() { count += 1; }
</script>

<button on:click={increment}>
  Clicked {count} times
</button>

<style>
  button { color: red; }
</style>
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 80,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "svelte"), config);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_with_julia_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"
function foo(x)
    return x + 1
end

struct Point
    x::Int
    y::Int
end

module MyModule
    export hello
    hello() = println("hi")
end
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 60,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "julia"), config);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_with_vue_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"<template>
  <div class="hello">
    <h1>{{ msg }}</h1>
    <button @click="increment">Count is: {{ count }}</button>
  </div>
</template>

<script>
export default {
  data() { return { msg: 'Hello', count: 0 } },
  methods: { increment() { this.count += 1 } },
}
</script>

<style scoped>
.hello { color: blue; }
</style>
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 80,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "vue"), config);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_with_dart_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"
void main() {
  print('Hello');
}

class Point {
  final int x;
  final int y;
  Point(this.x, this.y);

  int sum() => x + y;
}
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 60,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "dart"), config);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_with_lua_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        // One line, so line/whitespace separators give the fallback path no
        // function boundaries at all. Only the grammar can find them.
        let text = "local function f(a) return a+1 end \
                    local function g(b) return b*2 end \
                    local function h(c) return c-3 end";
        let config = RecursiveChunkConfig {
            chunk_size: 45,
            min_chunk_size: Some(10),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "lua"), config);
        assert!(
            chunks.len() > 1,
            "sample must split for the check below to mean anything"
        );
        for chunk in &chunks {
            let chunk_text = text[chunk.range.start..chunk.range.end].trim();
            assert!(
                chunk_text.starts_with("local function") && chunk_text.ends_with("end"),
                "chunk straddles a function boundary: {chunk_text:?}"
            );
        }
    }

    #[test]
    fn test_split_lua_keeps_table_constructor_whole() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        // Asserts one thing: the argument list of `setmetatable` stays whole.
        // Separator fallback cuts it at the comma between `x = x` and `y = y`,
        // because a comma is a separator to it and a table field to the
        // grammar. A constructor longer than `chunk_size` is still split, here
        // as in every other grammar-backed language.
        let text = "local function clamp(v, lo, hi) return math.min(math.max(v, lo), hi) end\n\
                    local Point = {}\n\
                    Point.__index = Point\n\
                    function Point.new(x, y) return setmetatable({ x = x, y = y }, Point) end\n\
                    function Point:norm() return math.sqrt(self.x * self.x + self.y * self.y) end\n\
                    return Point\n";
        let config = RecursiveChunkConfig {
            chunk_size: 60,
            min_chunk_size: Some(10),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "lua"), config);
        assert!(
            chunks.len() > 1,
            "sample must split for the check below to mean anything"
        );
        assert!(
            chunks.iter().any(|c| text[c.range.start..c.range.end]
                .contains("setmetatable({ x = x, y = y }, Point)")),
            "table constructor was severed across chunks"
        );
    }

    #[test]
    fn test_split_with_nix_language() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = r#"{ pkgs, ... }:
let
  package-name = pkgs.hello;
  greeting = "hello ${package-name}";
in {
  inherit package-name;
  script = ''
    echo "${greeting}"
  '';
}
"#;
        let config = RecursiveChunkConfig {
            chunk_size: 70,
            min_chunk_size: Some(20),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "nix"), config);
        let chunk_texts: Vec<&str> = chunks
            .iter()
            .map(|chunk| &text[chunk.range.start..chunk.range.end])
            .collect();
        // The 18-byte header falls a little short of `min_chunk_size`, which
        // costs less than splitting the `let` bindings apart.
        assert_eq!(chunk_texts.len(), 3);
        assert_eq!(chunk_texts[0], "{ pkgs, ... }:\nlet");
        assert_eq!(
            chunk_texts[1],
            "  package-name = pkgs.hello;\n  greeting = \"hello ${package-name}\";\nin"
        );
        assert!(chunk_texts[2].starts_with("{\n  inherit package-name;"));
    }

    #[test]
    fn test_split_positions() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "Chunk1\n\nChunk2";
        let config = RecursiveChunkConfig {
            chunk_size: 10,
            min_chunk_size: Some(5),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::new(text), config);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].start.line, 1);
        assert_eq!(chunks[0].start.column, 1);
        assert_eq!(chunks[1].start.line, 3);
        assert_eq!(chunks[1].start.column, 1);
    }

    #[test]
    fn test_custom_language() {
        let config = RecursiveSplitConfig {
            custom_languages: vec![CustomLanguageConfig {
                language_name: "myformat".to_string(),
                aliases: vec!["mf".to_string()],
                separators_regex: vec![r"---".to_string()],
            }],
        };
        let chunker = RecursiveChunker::new(config).unwrap();
        let text = "Part1---Part2---Part3";
        let chunk_config = RecursiveChunkConfig {
            chunk_size: 10,
            min_chunk_size: Some(4),
            chunk_overlap: Some(0),
        };
        let chunks = chunker.split(&CodeSource::with_language(text, "myformat"), chunk_config);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "Part1");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "Part2");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "Part3");
    }
}
