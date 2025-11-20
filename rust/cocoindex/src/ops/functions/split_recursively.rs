use anyhow::{Context, anyhow};
use regex::{Matches, Regex};
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;

use crate::ops::sdk::RangeValue;
use crate::ops::shared::program_langs;
use crate::ops::shared::split::{Position, set_output_positions};
use crate::{fields_value, ops::sdk::*};
#[derive(Serialize, Deserialize)]
struct CustomLanguageSpec {
    language_name: String,
    #[serde(default)]
    aliases: Vec<String>,
    separators_regex: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Spec {
    #[serde(default)]
    custom_languages: Vec<CustomLanguageSpec>,
}

const SYNTAX_LEVEL_GAP_COST: usize = 512;
const MISSING_OVERLAP_COST: usize = 512;
const PER_LINE_BREAK_LEVEL_GAP_COST: usize = 64;
const TOO_SMALL_CHUNK_COST: usize = 1048576;

pub struct Args {
    text: ResolvedOpArg,
    chunk_size: ResolvedOpArg,
    min_chunk_size: Option<ResolvedOpArg>,
    chunk_overlap: Option<ResolvedOpArg>,
    language: Option<ResolvedOpArg>,
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
            r"[;:—\-]\s+|；|：|—+",
            r",\s+|，",
            r"\s+",
        ]
        .into_iter()
        .map(|s| Regex::new(s).unwrap())
        .collect(),
    });

enum ChunkKind<'t> {
    TreeSitterNode {
        tree_sitter_info: &'t program_langs::TreeSitterLanguageInfo,
        node: tree_sitter::Node<'t>,
    },
    RegexpSepChunk {
        lang_config: &'t SimpleLanguageConfig,
        next_regexp_sep_id: usize,
    },
}

struct Chunk<'t, 's: 't> {
    full_text: &'s str,
    range: RangeValue,
    kind: ChunkKind<'t>,
}

struct TextChunksIter<'t, 's: 't> {
    lang_config: &'t SimpleLanguageConfig,
    full_text: &'s str,
    range: RangeValue,
    matches_iter: Matches<'t, 's>,
    regexp_sep_id: usize,
    next_start_pos: Option<usize>,
}

impl<'t, 's: 't> TextChunksIter<'t, 's> {
    fn new(
        lang_config: &'t SimpleLanguageConfig,
        full_text: &'s str,
        range: RangeValue,
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
    type Item = Chunk<'t, 's>;

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
        Some(Chunk {
            full_text: self.full_text,
            range: RangeValue::new(start_pos, end_pos),
            kind: ChunkKind::RegexpSepChunk {
                lang_config: self.lang_config,
                next_regexp_sep_id: self.regexp_sep_id + 1,
            },
        })
    }
}

struct TreeSitterNodeIter<'t, 's: 't> {
    lang_config: &'t program_langs::TreeSitterLanguageInfo,
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
    ) -> Option<Chunk<'t, 's>> {
        let start_pos = *next_start_pos;
        if start_pos < gap_end_pos {
            *next_start_pos = gap_end_pos;
            Some(Chunk {
                full_text,
                range: RangeValue::new(start_pos, gap_end_pos),
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
    type Item = Chunk<'t, 's>;

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
        Some(Chunk {
            full_text: self.full_text,
            range: RangeValue::new(node.start_byte(), node.end_byte()),
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
    Once(std::iter::Once<Chunk<'t, 's>>),
}

impl<'t, 's: 't> Iterator for ChunkIterator<'t, 's> {
    type Item = Chunk<'t, 's>;

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
    range: RangeValue,
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
    fn collect(&mut self, range: RangeValue) {
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
            RangeValue::new(prev_end + trimmed_gap.len(), new_end)
        } else {
            RangeValue::new(new_start, new_end)
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
            range: RangeValue::new(self.full_text.len(), self.full_text.len()),
            boundary_syntax_level: self.min_level,
            internal_lb_level: LineBreakLevel::Inline,
            boundary_lb_level: LineBreakLevel::DoubleNewline,
        });
        self.atom_chunks
    }
}

struct ChunkOutput<'s> {
    start_pos: Position,
    end_pos: Position,
    text: &'s str,
}

struct RecursiveChunker<'s> {
    full_text: &'s str,
    chunk_size: usize,
    chunk_overlap: usize,
    min_chunk_size: usize,
    min_atom_chunk_size: usize,
}

impl<'t, 's: 't> RecursiveChunker<'s> {
    fn collect_atom_chunks(
        &self,
        chunk: Chunk<'t, 's>,
        atom_collector: &mut AtomChunksCollector<'s>,
    ) -> Result<()> {
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
                            iter_stack.push(ChunkIterator::Once(std::iter::once(Chunk {
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
        Ok(())
    }

    fn get_overlap_cost_base(&self, offset: usize) -> usize {
        if self.chunk_overlap == 0 {
            0
        } else {
            (self.full_text.len() - offset) * MISSING_OVERLAP_COST / self.chunk_overlap
        }
    }

    fn merge_atom_chunks(&self, atom_chunks: Vec<AtomChunk>) -> Vec<ChunkOutput<'s>> {
        struct AtomRoutingPlan {
            start_idx: usize,     // index of `atom_chunks` for the start chunk
            prev_plan_idx: usize, // index of `plans` for the previous plan
            cost: usize,
            overlap_cost_base: usize,
        }
        type PrevPlanCandidate = (std::cmp::Reverse<usize>, usize); // (cost, start_idx)

        let mut plans = Vec::with_capacity(atom_chunks.len());
        // Janitor
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
                text: &self.full_text[start_chunk.range.start..end_chunk.range.end],
            });
            plan_idx = plan.prev_plan_idx;
        }
        output.reverse();
        output
    }

    fn split_root_chunk(&self, kind: ChunkKind<'t>) -> Result<Vec<ChunkOutput<'s>>> {
        let mut atom_collector = AtomChunksCollector {
            full_text: self.full_text,
            min_level: 0,
            curr_level: 0,
            atom_chunks: Vec::new(),
        };
        self.collect_atom_chunks(
            Chunk {
                full_text: self.full_text,
                range: RangeValue::new(0, self.full_text.len()),
                kind,
            },
            &mut atom_collector,
        )?;
        let atom_chunks = atom_collector.into_atom_chunks();
        let output = self.merge_atom_chunks(atom_chunks);
        Ok(output)
    }
}

struct Executor {
    args: Args,
    custom_languages: HashMap<UniCase<String>, Arc<SimpleLanguageConfig>>,
}

impl Executor {
    fn new(args: Args, spec: Spec) -> Result<Self> {
        let mut custom_languages = HashMap::new();
        for lang in spec.custom_languages {
            let separator_regex = lang
                .separators_regex
                .iter()
                .map(|s| Regex::new(s))
                .collect::<Result<_, _>>()
                .with_context(|| {
                    format!(
                        "failed in parsing regexp for language `{}`",
                        lang.language_name
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
                api_bail!(
                    "duplicate language name / alias: `{}`",
                    language_config.name
                );
            }
            for alias in &language_config.aliases {
                if custom_languages
                    .insert(UniCase::new(alias.clone()), language_config.clone())
                    .is_some()
                {
                    api_bail!("duplicate language name / alias: `{}`", alias);
                }
            }
        }
        Ok(Self {
            args,
            custom_languages,
        })
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let full_text = self.args.text.value(&input)?.as_str()?;
        let chunk_size = self.args.chunk_size.value(&input)?.as_int64()?;
        let min_chunk_size = (self.args.min_chunk_size.value(&input)?)
            .optional()
            .map(|v| v.as_int64())
            .transpose()?
            .unwrap_or(chunk_size / 2) as usize;
        let chunk_overlap = std::cmp::min(
            (self.args.chunk_overlap.value(&input)?)
                .optional()
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(0) as usize,
            min_chunk_size,
        );
        let recursive_chunker = RecursiveChunker {
            full_text,
            chunk_size: chunk_size as usize,
            chunk_overlap,
            min_chunk_size,
            min_atom_chunk_size: if chunk_overlap > 0 {
                chunk_overlap
            } else {
                min_chunk_size
            },
        };

        let language = UniCase::new(
            (if let Some(language) = self.args.language.value(&input)?.optional() {
                language.as_str()?
            } else {
                ""
            })
            .to_string(),
        );
        let mut output = if let Some(lang_config) = self.custom_languages.get(&language) {
            recursive_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config,
                next_regexp_sep_id: 0,
            })?
        } else if let Some(lang_info) = program_langs::get_language_info(&language)
            && let Some(tree_sitter_info) = lang_info.treesitter_info.as_ref()
        {
            let mut parser = tree_sitter::Parser::new();
            parser.set_language(&tree_sitter_info.tree_sitter_lang)?;
            let tree = parser
                .parse(full_text.as_ref(), None)
                .ok_or_else(|| anyhow!("failed in parsing text in language: {}", lang_info.name))?;
            recursive_chunker.split_root_chunk(ChunkKind::TreeSitterNode {
                tree_sitter_info,
                node: tree.root_node(),
            })?
        } else {
            recursive_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config: &DEFAULT_LANGUAGE_CONFIG,
                next_regexp_sep_id: 0,
            })?
        };

        set_output_positions(
            full_text,
            output.iter_mut().flat_map(|chunk_output| {
                std::iter::once(&mut chunk_output.start_pos)
                    .chain(std::iter::once(&mut chunk_output.end_pos))
            }),
        );

        let table = output
            .into_iter()
            .map(|chunk_output| {
                let output_start = chunk_output.start_pos.output.unwrap();
                let output_end = chunk_output.end_pos.output.unwrap();
                (
                    KeyValue::from_single_part(RangeValue::new(
                        output_start.char_offset,
                        output_end.char_offset,
                    )),
                    fields_value!(
                        Arc::<str>::from(chunk_output.text),
                        output_start.into_output(),
                        output_end.into_output()
                    )
                    .into(),
                )
            })
            .collect();

        Ok(Value::KTable(table))
    }
}

struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = Spec;
    type ResolvedArgs = Args;

    fn name(&self) -> &str {
        "SplitRecursively"
    }

    async fn analyze<'a>(
        &'a self,
        _spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<SimpleFunctionAnalysisOutput<Args>> {
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?
                .required()?,
            chunk_size: args_resolver
                .next_arg("chunk_size")?
                .expect_type(&ValueType::Basic(BasicValueType::Int64))?
                .required()?,
            min_chunk_size: args_resolver
                .next_arg("min_chunk_size")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Int64))?
                .optional(),
            chunk_overlap: args_resolver
                .next_arg("chunk_overlap")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Int64))?
                .optional(),
            language: args_resolver
                .next_arg("language")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Str))?
                .optional(),
        };

        let output_schema =
            crate::ops::shared::split::make_common_chunk_schema(args_resolver, &args.text)?;
        Ok(SimpleFunctionAnalysisOutput {
            resolved_args: args,
            output_schema,
            behavior_version: None,
        })
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<impl SimpleFunctionExecutor> {
        Executor::new(args, spec)
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory.register(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::functions::test_utils::test_flow_function;
    use crate::ops::sdk::{BasicValueType, KeyPart, KeyValue, make_output_type};
    use crate::ops::shared::split::OutputPosition;

    fn build_split_recursively_arg_schemas() -> Vec<(Option<&'static str>, EnrichedValueType)> {
        vec![
            (
                Some("text"),
                make_output_type(BasicValueType::Str).with_nullable(true),
            ),
            (
                Some("chunk_size"),
                make_output_type(BasicValueType::Int64).with_nullable(true),
            ),
            (
                Some("min_chunk_size"),
                make_output_type(BasicValueType::Int64).with_nullable(true),
            ),
            (
                Some("chunk_overlap"),
                make_output_type(BasicValueType::Int64).with_nullable(true),
            ),
            (
                Some("language"),
                make_output_type(BasicValueType::Str).with_nullable(true),
            ),
        ]
    }

    #[tokio::test]
    async fn test_split_recursively() {
        let spec = Spec {
            custom_languages: vec![],
        };
        let factory = Arc::new(Factory);
        let text_content = "Linea 1.\nLinea 2.\n\nLinea 3.";
        let input_arg_schemas = &build_split_recursively_arg_schemas();

        {
            let result = test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text_content.to_string().into(),
                    (15i64).into(),
                    (5i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await;
            assert!(
                result.is_ok(),
                "test_flow_function failed: {:?}",
                result.err()
            );
            let value = result.unwrap();
            match value {
                Value::KTable(table) => {
                    let expected_chunks = vec![
                        (RangeValue::new(0, 8), "Linea 1."),
                        (RangeValue::new(9, 17), "Linea 2."),
                        (RangeValue::new(19, 27), "Linea 3."),
                    ];

                    for (range, expected_text) in expected_chunks {
                        let key = KeyValue::from_single_part(range);
                        match table.get(&key) {
                            Some(scope_value_ref) => {
                                let chunk_text =
                                    scope_value_ref.0.fields[0].as_str().unwrap_or_else(|_| {
                                        panic!("Chunk text not a string for key {key:?}")
                                    });
                                assert_eq!(*chunk_text, expected_text.into());
                            }
                            None => panic!("Expected row value for key {key:?}, not found"),
                        }
                    }
                }
                other => panic!("Expected Value::KTable, got {other:?}"),
            }
        }

        // Argument text is required
        assert_eq!(
            test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    Value::Null,
                    (15i64).into(),
                    (5i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await
            .unwrap(),
            Value::Null
        );

        // Argument chunk_size is required
        assert_eq!(
            test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text_content.to_string().into(),
                    Value::Null,
                    (5i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await
            .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_translate_bytes_to_chars_simple() {
        let text = "abc😄def";
        let mut start1 = Position::new(0);
        let mut end1 = Position::new(3);
        let mut start2 = Position::new(3);
        let mut end2 = Position::new(7);
        let mut start3 = Position::new(7);
        let mut end3 = Position::new(10);
        let mut end_full = Position::new(text.len());

        let offsets = vec![
            &mut start1,
            &mut end1,
            &mut start2,
            &mut end2,
            &mut start3,
            &mut end3,
            &mut end_full,
        ];

        set_output_positions(text, offsets.into_iter());

        assert_eq!(
            start1.output,
            Some(OutputPosition {
                char_offset: 0,
                line: 1,
                column: 1,
            })
        );
        assert_eq!(
            end1.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
        assert_eq!(
            start2.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
        assert_eq!(
            end2.output,
            Some(OutputPosition {
                char_offset: 4,
                line: 1,
                column: 5,
            })
        );
        assert_eq!(
            end3.output,
            Some(OutputPosition {
                char_offset: 7,
                line: 1,
                column: 8,
            })
        );
        assert_eq!(
            end_full.output,
            Some(OutputPosition {
                char_offset: 7,
                line: 1,
                column: 8,
            })
        );
    }

    #[tokio::test]
    async fn test_basic_split_no_overlap() {
        let spec = Spec {
            custom_languages: vec![],
        };
        let factory = Arc::new(Factory);
        let text = "Linea 1.\nLinea 2.\n\nLinea 3.";
        let input_arg_schemas = &build_split_recursively_arg_schemas();

        {
            let result = test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text.to_string().into(),
                    (15i64).into(),
                    (5i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await;
            let value = result.unwrap();
            match value {
                Value::KTable(table) => {
                    let expected_chunks = vec![
                        (RangeValue::new(0, 8), "Linea 1."),
                        (RangeValue::new(9, 17), "Linea 2."),
                        (RangeValue::new(19, 27), "Linea 3."),
                    ];

                    for (range, expected_text) in expected_chunks {
                        let key = KeyValue::from_single_part(range);
                        match table.get(&key) {
                            Some(scope_value_ref) => {
                                let chunk_text = scope_value_ref.0.fields[0].as_str().unwrap();
                                assert_eq!(*chunk_text, expected_text.into());
                            }
                            None => panic!("Expected row value for key {key:?}, not found"),
                        }
                    }
                }
                other => panic!("Expected Value::KTable, got {other:?}"),
            }
        }

        // Test splitting when chunk_size forces breaks within segments.
        let text2 = "A very very long text that needs to be split.";
        {
            let result = test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text2.to_string().into(),
                    (20i64).into(),
                    (12i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await;
            let value = result.unwrap();
            match value {
                Value::KTable(table) => {
                    assert!(table.len() > 1);

                    let key = KeyValue::from_single_part(RangeValue::new(0, 16));
                    match table.get(&key) {
                        Some(scope_value_ref) => {
                            let chunk_text = scope_value_ref.0.fields[0].as_str().unwrap();
                            assert_eq!(*chunk_text, "A very very long".into());
                            assert!(chunk_text.len() <= 20);
                        }
                        None => panic!("Expected row value for key {key:?}, not found"),
                    }
                }
                other => panic!("Expected Value::KTable, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_basic_split_with_overlap() {
        let spec = Spec {
            custom_languages: vec![],
        };
        let factory = Arc::new(Factory);
        let text = "This is a test text that is a bit longer to see how the overlap works.";
        let input_arg_schemas = &build_split_recursively_arg_schemas();

        {
            let result = test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text.to_string().into(),
                    (20i64).into(),
                    (10i64).into(),
                    (5i64).into(),
                    Value::Null,
                ],
            )
            .await;
            let value = result.unwrap();
            match value {
                Value::KTable(table) => {
                    assert!(table.len() > 1);

                    if table.len() >= 2 {
                        let first_key = table.keys().next().unwrap();
                        match table.get(first_key) {
                            Some(scope_value_ref) => {
                                let chunk_text = scope_value_ref.0.fields[0].as_str().unwrap();
                                assert!(
                                    chunk_text.len() <= 25,
                                    "Chunk was too long: '{}'",
                                    chunk_text
                                );
                            }
                            None => panic!("Expected row value for first key, not found"),
                        }
                    }
                }
                other => panic!("Expected Value::KTable, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_split_trims_whitespace() {
        let spec = Spec {
            custom_languages: vec![],
        };
        let factory = Arc::new(Factory);
        let text = "  \n First chunk  \n\n  Second chunk with spaces at the end    \n";
        let input_arg_schemas = &build_split_recursively_arg_schemas();

        {
            let result = test_flow_function(
                &factory,
                &spec,
                input_arg_schemas,
                vec![
                    text.to_string().into(),
                    (30i64).into(),
                    (10i64).into(),
                    (0i64).into(),
                    Value::Null,
                ],
            )
            .await;
            assert!(
                result.is_ok(),
                "test_flow_function failed: {:?}",
                result.err()
            );
            let value = result.unwrap();
            match value {
                Value::KTable(table) => {
                    assert_eq!(table.len(), 3);

                    let expected_chunks = vec![
                        (RangeValue::new(3, 15), " First chunk"),
                        (RangeValue::new(19, 45), "  Second chunk with spaces"),
                        (RangeValue::new(46, 56), "at the end"),
                    ];

                    for (range, expected_text) in expected_chunks {
                        let key = KeyValue::from_single_part(range);
                        match table.get(&key) {
                            Some(scope_value_ref) => {
                                let chunk_text =
                                    scope_value_ref.0.fields[0].as_str().unwrap_or_else(|_| {
                                        panic!("Chunk text not a string for key {key:?}")
                                    });
                                assert_eq!(**chunk_text, *expected_text);
                            }
                            None => panic!("Expected row value for key {key:?}, not found"),
                        }
                    }
                }
                other => panic!("Expected Value::KTable, got {other:?}"),
            }
        }
    }
}
