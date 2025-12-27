//! Recursive text chunking with syntax awareness.

use regex::{Matches, Regex};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use unicase::UniCase;

use super::output_positions::{Position, set_output_positions};
use super::{Chunk, TextRange};
use crate::prog_langs::{self, TreeSitterLanguageInfo};

const SYNTAX_LEVEL_GAP_COST: usize = 512;
const MISSING_OVERLAP_COST: usize = 512;
const PER_LINE_BREAK_LEVEL_GAP_COST: usize = 64;
const TOO_SMALL_CHUNK_COST: usize = 1048576;

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
#[derive(Debug, Clone)]
pub struct RecursiveSplitConfig {
    /// Custom language configurations.
    pub custom_languages: Vec<CustomLanguageConfig>,
}

impl Default for RecursiveSplitConfig {
    fn default() -> Self {
        Self {
            custom_languages: vec![],
        }
    }
}

/// Configuration for a single chunking operation.
#[derive(Debug, Clone)]
pub struct RecursiveChunkConfig {
    /// Target chunk size in bytes.
    pub chunk_size: usize,
    /// Minimum chunk size in bytes. Defaults to chunk_size / 2.
    pub min_chunk_size: Option<usize>,
    /// Overlap between consecutive chunks in bytes.
    pub chunk_overlap: Option<usize>,
    /// Language name or file extension for syntax-aware splitting.
    pub language: Option<String>,
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
}

struct AtomChunksCollector<'s> {
    full_text: &'s str,
    curr_level: usize,
    min_level: usize,
    atom_chunks: Vec<AtomChunk>,
}

impl<'s> AtomChunksCollector<'s> {
    fn collect(&mut self, range: TextRange) {
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
        });
        self.min_level = self.curr_level;
    }

    fn into_atom_chunks(mut self) -> Vec<AtomChunk> {
        self.atom_chunks.push(AtomChunk {
            range: TextRange::new(self.full_text.len(), self.full_text.len()),
            boundary_syntax_level: self.min_level,
            internal_lb_level: LineBreakLevel::Inline,
            boundary_lb_level: LineBreakLevel::DoubleNewline,
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
    min_atom_chunk_size: usize,
}

impl<'t, 's: 't> InternalRecursiveChunker<'s> {
    fn collect_atom_chunks(
        &self,
        chunk: InternalChunk<'t, 's>,
        atom_collector: &mut AtomChunksCollector<'s>,
    ) {
        let mut iter_stack: Vec<ChunkIterator<'t, 's>> =
            vec![ChunkIterator::Once(std::iter::once(chunk))];

        while !iter_stack.is_empty() {
            atom_collector.curr_level = iter_stack.len();

            if let Some(current_chunk) = iter_stack.last_mut().unwrap().next() {
                if current_chunk.range.len() <= self.min_atom_chunk_size {
                    atom_collector.collect(current_chunk.range);
                } else {
                    match current_chunk.kind {
                        ChunkKind::TreeSitterNode {
                            tree_sitter_info: lang_config,
                            node,
                        } => {
                            if !lang_config.terminal_node_kind_ids.contains(&node.kind_id()) {
                                let mut cursor = node.walk();
                                if cursor.goto_first_child() {
                                    iter_stack.push(ChunkIterator::TreeSitter(
                                        TreeSitterNodeIter {
                                            lang_config,
                                            full_text: self.full_text,
                                            cursor: Some(cursor),
                                            next_start_pos: node.start_byte(),
                                            end_pos: node.end_byte(),
                                        },
                                    ));
                                    continue;
                                }
                            }
                            iter_stack.push(ChunkIterator::Once(std::iter::once(InternalChunk {
                                full_text: self.full_text,
                                range: current_chunk.range,
                                kind: ChunkKind::RegexpSepChunk {
                                    lang_config: &DEFAULT_LANGUAGE_CONFIG,
                                    next_regexp_sep_id: 0,
                                },
                            })));
                        }
                        ChunkKind::RegexpSepChunk {
                            lang_config,
                            next_regexp_sep_id,
                        } => {
                            if next_regexp_sep_id >= lang_config.separator_regex.len() {
                                atom_collector.collect(current_chunk.range);
                            } else {
                                iter_stack.push(ChunkIterator::Text(TextChunksIter::new(
                                    lang_config,
                                    current_chunk.full_text,
                                    current_chunk.range,
                                    next_regexp_sep_id,
                                )));
                            }
                        }
                    }
                }
            } else {
                iter_stack.pop();
                let level_after_pop = iter_stack.len();
                atom_collector.curr_level = level_after_pop;
                if level_after_pop < atom_collector.min_level {
                    atom_collector.min_level = level_after_pop;
                }
            }
        }
        atom_collector.curr_level = 0;
    }

    fn get_overlap_cost_base(&self, offset: usize) -> usize {
        if self.chunk_overlap == 0 {
            0
        } else {
            (self.full_text.len() - offset) * MISSING_OVERLAP_COST / self.chunk_overlap
        }
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
                cost += (lb_level_gap(start_chunk.boundary_lb_level, internal_lb_level)
                    + lb_level_gap(end_lb_level, internal_lb_level))
                    * PER_LINE_BREAK_LEVEL_GAP_COST;
                if chunk_size < self.min_chunk_size {
                    cost += TOO_SMALL_CHUNK_COST;
                }

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

    /// Split the text into chunks according to the configuration.
    pub fn split(&self, text: &str, config: RecursiveChunkConfig) -> Vec<Chunk> {
        let min_chunk_size = config.min_chunk_size.unwrap_or(config.chunk_size / 2);
        let chunk_overlap = std::cmp::min(config.chunk_overlap.unwrap_or(0), min_chunk_size);

        let internal_chunker = InternalRecursiveChunker {
            full_text: text,
            chunk_size: config.chunk_size,
            chunk_overlap,
            min_chunk_size,
            min_atom_chunk_size: if chunk_overlap > 0 {
                chunk_overlap
            } else {
                min_chunk_size
            },
        };

        let language = UniCase::new(config.language.unwrap_or_default());
        let mut output = if let Some(lang_config) = self.custom_languages.get(&language) {
            internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config,
                next_regexp_sep_id: 0,
            })
        } else if let Some(lang_info) = prog_langs::get_language_info(&language)
            && let Some(tree_sitter_info) = lang_info.treesitter_info.as_ref()
        {
            let mut parser = tree_sitter::Parser::new();
            if parser
                .set_language(&tree_sitter_info.tree_sitter_lang)
                .is_err()
            {
                // Fall back to default if language setup fails
                internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                    lang_config: &DEFAULT_LANGUAGE_CONFIG,
                    next_regexp_sep_id: 0,
                })
            } else if let Some(tree) = parser.parse(text, None) {
                internal_chunker.split_root_chunk(ChunkKind::TreeSitterNode {
                    tree_sitter_info,
                    node: tree.root_node(),
                })
            } else {
                // Fall back to default if parsing fails
                internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                    lang_config: &DEFAULT_LANGUAGE_CONFIG,
                    next_regexp_sep_id: 0,
                })
            }
        } else {
            internal_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config: &DEFAULT_LANGUAGE_CONFIG,
                next_regexp_sep_id: 0,
            })
        };

        // Compute positions
        set_output_positions(
            text,
            output.iter_mut().flat_map(|chunk_output| {
                std::iter::once(&mut chunk_output.start_pos)
                    .chain(std::iter::once(&mut chunk_output.end_pos))
            }),
        );

        // Convert to final output
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_basic() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "Linea 1.\nLinea 2.\n\nLinea 3.";
        let config = RecursiveChunkConfig {
            chunk_size: 15,
            min_chunk_size: Some(5),
            chunk_overlap: Some(0),
            language: None,
        };
        let chunks = chunker.split(text, config);

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
            language: None,
        };
        let chunks = chunker.split(text, config);

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
            language: None,
        };
        let chunks = chunker.split(text, config);

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
            language: None,
        };
        let chunks = chunker.split(text, config);

        assert_eq!(chunks.len(), 3);
        // Verify chunks are trimmed appropriately
        let chunk_text = &text[chunks[0].range.start..chunks[0].range.end];
        assert!(!chunk_text.starts_with("  "));
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
            language: Some("rust".to_string()),
        };
        let chunks = chunker.split(text, config);

        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_split_positions() {
        let chunker = RecursiveChunker::new(RecursiveSplitConfig::default()).unwrap();
        let text = "Chunk1\n\nChunk2";
        let config = RecursiveChunkConfig {
            chunk_size: 10,
            min_chunk_size: Some(5),
            chunk_overlap: Some(0),
            language: None,
        };
        let chunks = chunker.split(text, config);

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
            language: Some("myformat".to_string()),
        };
        let chunks = chunker.split(text, chunk_config);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "Part1");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "Part2");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "Part3");
    }
}
