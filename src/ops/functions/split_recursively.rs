use anyhow::anyhow;
use log::{error, trace};
use regex::{Matches, Regex};
use std::collections::HashSet;
use std::sync::LazyLock;
use std::usize;
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;

use crate::base::field_attrs;
use crate::ops::registry::ExecutorFactoryRegistry;
use crate::{fields_value, ops::sdk::*};

#[derive(Deserialize)]
struct CustomLanguageSpec {
    language_name: String,
    #[serde(default)]
    aliases: Vec<String>,
    separators_regex: Vec<String>,
}

#[derive(Deserialize)]
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
        separator_regex: [r"\n\n+", r"\n", r"\s+"]
            .into_iter()
            .map(|s| Regex::new(s).unwrap())
            .collect(),
    });

struct TreesitterLanguageConfig {
    name: String,
    tree_sitter_lang: tree_sitter::Language,
    terminal_node_kind_ids: HashSet<u16>,
}

fn add_treesitter_language<'a>(
    output: &'a mut HashMap<UniCase<String>, Arc<TreesitterLanguageConfig>>,
    name: &'static str,
    aliases: impl IntoIterator<Item = &'static str>,
    lang_fn: impl Into<tree_sitter::Language>,
    terminal_node_kinds: impl IntoIterator<Item = &'a str>,
) {
    let tree_sitter_lang: tree_sitter::Language = lang_fn.into();
    let terminal_node_kind_ids = terminal_node_kinds
        .into_iter()
        .filter_map(|kind| {
            let id = tree_sitter_lang.id_for_node_kind(kind, true);
            if id != 0 {
                trace!("Got id for node kind: `{kind}` -> {id}");
                Some(id)
            } else {
                error!("Failed in getting id for node kind: `{kind}`");
                None
            }
        })
        .collect();

    let config = Arc::new(TreesitterLanguageConfig {
        name: name.to_string(),
        tree_sitter_lang,
        terminal_node_kind_ids,
    });
    for name in std::iter::once(name).chain(aliases.into_iter()) {
        if output.insert(name.into(), config.clone()).is_some() {
            panic!("Language `{name}` already exists");
        }
    }
}

static TREE_SITTER_LANGUAGE_BY_LANG: LazyLock<
    HashMap<UniCase<String>, Arc<TreesitterLanguageConfig>>,
> = LazyLock::new(|| {
    let mut map = HashMap::new();
    add_treesitter_language(&mut map, "C", [".c"], tree_sitter_c::LANGUAGE, []);
    add_treesitter_language(
        &mut map,
        "C++",
        [".cpp", ".cc", ".cxx", ".h", ".hpp", "cpp"],
        tree_sitter_c::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "C#",
        [".cs", "cs"],
        tree_sitter_c_sharp::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "CSS",
        [".css", ".scss"],
        tree_sitter_css::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "Fortran",
        [".f", ".f90", ".f95", ".f03", "f", "f90", "f95", "f03"],
        tree_sitter_fortran::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "Go",
        [".go", "golang"],
        tree_sitter_go::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "HTML",
        [".html", ".htm"],
        tree_sitter_html::LANGUAGE,
        [],
    );
    add_treesitter_language(&mut map, "Java", [".java"], tree_sitter_java::LANGUAGE, []);
    add_treesitter_language(
        &mut map,
        "JavaScript",
        [".js", "js"],
        tree_sitter_javascript::LANGUAGE,
        [],
    );
    add_treesitter_language(&mut map, "JSON", [".json"], tree_sitter_json::LANGUAGE, []);
    add_treesitter_language(
        &mut map,
        "Markdown",
        [".md", ".mdx", "md"],
        tree_sitter_md::LANGUAGE,
        ["inline"],
    );
    add_treesitter_language(
        &mut map,
        "Pascal",
        [".pas", "pas", ".dpr", "dpr", "Delphi"],
        tree_sitter_pascal::LANGUAGE,
        [],
    );
    add_treesitter_language(&mut map, "PHP", [".php"], tree_sitter_php::LANGUAGE_PHP, []);
    add_treesitter_language(
        &mut map,
        "Python",
        [".py"],
        tree_sitter_python::LANGUAGE,
        [],
    );
    add_treesitter_language(&mut map, "R", [".r"], tree_sitter_r::LANGUAGE, []);
    add_treesitter_language(&mut map, "Ruby", [".rb"], tree_sitter_ruby::LANGUAGE, []);
    add_treesitter_language(
        &mut map,
        "Rust",
        [".rs", "rs"],
        tree_sitter_rust::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "Scala",
        [".scala"],
        tree_sitter_scala::LANGUAGE,
        [],
    );
    add_treesitter_language(&mut map, "SQL", [".sql"], tree_sitter_sequel::LANGUAGE, []);
    add_treesitter_language(
        &mut map,
        "Swift",
        [".swift"],
        tree_sitter_swift::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "TOML",
        [".toml"],
        tree_sitter_toml_ng::LANGUAGE,
        [],
    );
    add_treesitter_language(
        &mut map,
        "TSX",
        [".tsx"],
        tree_sitter_typescript::LANGUAGE_TSX,
        [],
    );
    add_treesitter_language(
        &mut map,
        "TypeScript",
        [".ts", "ts"],
        tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
        [],
    );
    add_treesitter_language(&mut map, "XML", [".xml"], tree_sitter_xml::LANGUAGE_XML, []);
    add_treesitter_language(&mut map, "DTD", [".dtd"], tree_sitter_xml::LANGUAGE_DTD, []);
    add_treesitter_language(
        &mut map,
        "YAML",
        [".yaml", ".yml"],
        tree_sitter_yaml::LANGUAGE,
        [],
    );
    map
});

enum ChunkKind<'t> {
    TreeSitterNode {
        lang_config: &'t TreesitterLanguageConfig,
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

impl<'t, 's: 't> Chunk<'t, 's> {
    fn text(&self) -> &'s str {
        self.range.extract_str(self.full_text)
    }
}

struct TextChunksIter<'t, 's: 't> {
    lang_config: &'t SimpleLanguageConfig,
    parent: &'t Chunk<'t, 's>,
    matches_iter: Matches<'t, 's>,
    regexp_sep_id: usize,
    next_start_pos: Option<usize>,
}

impl<'t, 's: 't> TextChunksIter<'t, 's> {
    fn new(
        lang_config: &'t SimpleLanguageConfig,
        parent: &'t Chunk<'t, 's>,
        regexp_sep_id: usize,
    ) -> Self {
        Self {
            lang_config,
            parent,
            matches_iter: lang_config.separator_regex[regexp_sep_id].find_iter(parent.text()),
            regexp_sep_id,
            next_start_pos: Some(parent.range.start),
        }
    }
}

impl<'t, 's: 't> Iterator for TextChunksIter<'t, 's> {
    type Item = Chunk<'t, 's>;

    fn next(&mut self) -> Option<Self::Item> {
        let start_pos = self.next_start_pos?;
        let end_pos = match self.matches_iter.next() {
            Some(grp) => {
                self.next_start_pos = Some(self.parent.range.start + grp.end());
                self.parent.range.start + grp.start()
            }
            None => {
                self.next_start_pos = None;
                if start_pos >= self.parent.range.end {
                    return None;
                }
                self.parent.range.end
            }
        };
        Some(Chunk {
            full_text: self.parent.full_text,
            range: RangeValue::new(start_pos, end_pos),
            kind: ChunkKind::RegexpSepChunk {
                lang_config: self.lang_config,
                next_regexp_sep_id: self.regexp_sep_id + 1,
            },
        })
    }
}

struct TreeSitterNodeIter<'t, 's: 't> {
    lang_config: &'t TreesitterLanguageConfig,
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
                lang_config: self.lang_config,
                node,
            },
        })
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
            while let Some(c2) = iter.next() {
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

struct RecursiveChunker<'s> {
    full_text: &'s str,
    chunk_size: usize,
    chunk_overlap: usize,
    min_chunk_size: usize,
}

impl<'t, 's: 't> RecursiveChunker<'s> {
    fn collect_atom_chunks_from_iter(
        &self,
        sub_chunks_iter: impl Iterator<Item = Chunk<'t, 's>>,
        atom_collector: &mut AtomChunksCollector<'s>,
    ) -> Result<()> {
        atom_collector.curr_level += 1;
        for sub_chunk in sub_chunks_iter {
            let range = sub_chunk.range;
            if range.len() <= self.min_chunk_size {
                atom_collector.collect(range);
            } else {
                self.collect_atom_chunks(sub_chunk, atom_collector)?;
            }
        }
        atom_collector.curr_level -= 1;
        if atom_collector.curr_level < atom_collector.min_level {
            atom_collector.min_level = atom_collector.curr_level;
        }
        Ok(())
    }

    fn collect_atom_chunks(
        &self,
        chunk: Chunk<'t, 's>,
        atom_collector: &mut AtomChunksCollector<'s>,
    ) -> Result<()> {
        match chunk.kind {
            ChunkKind::TreeSitterNode { lang_config, node } => {
                if !lang_config.terminal_node_kind_ids.contains(&node.kind_id()) {
                    let mut cursor = node.walk();
                    if cursor.goto_first_child() {
                        return self.collect_atom_chunks_from_iter(
                            TreeSitterNodeIter {
                                lang_config,
                                full_text: self.full_text,
                                cursor: Some(cursor),
                                next_start_pos: node.start_byte(),
                                end_pos: node.end_byte(),
                            },
                            atom_collector,
                        );
                    }
                }
                self.collect_atom_chunks(
                    Chunk {
                        full_text: self.full_text,
                        range: chunk.range,
                        kind: ChunkKind::RegexpSepChunk {
                            lang_config: &DEFAULT_LANGUAGE_CONFIG,
                            next_regexp_sep_id: 0,
                        },
                    },
                    atom_collector,
                )
            }
            ChunkKind::RegexpSepChunk {
                lang_config,
                next_regexp_sep_id,
            } => {
                if next_regexp_sep_id >= lang_config.separator_regex.len() {
                    Ok(atom_collector.collect(chunk.range))
                } else {
                    self.collect_atom_chunks_from_iter(
                        TextChunksIter::new(lang_config, &chunk, next_regexp_sep_id),
                        atom_collector,
                    )
                }
            }
        }
    }

    fn get_overlap_cost_base(&self, offset: usize) -> usize {
        if self.chunk_overlap == 0 {
            0
        } else {
            (self.full_text.len() - offset) * MISSING_OVERLAP_COST / self.chunk_overlap
        }
    }

    fn merge_atom_chunks(&self, atom_chunks: Vec<AtomChunk>) -> Vec<(RangeValue, &'s str)> {
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

        for (i, chunk) in (&atom_chunks[0..atom_chunks.len() - 1]).iter().enumerate() {
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
            let range = RangeValue::new(start_chunk.range.start, end_chunk.range.end);
            output.push((range, &self.full_text[range.start..range.end]));
            plan_idx = plan.prev_plan_idx;
        }
        output.reverse();
        output
    }

    fn split_root_chunk(&self, kind: ChunkKind<'t>) -> Result<Vec<(RangeValue, &'s str)>> {
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

fn translate_bytes_to_chars<'a>(text: &str, offsets: impl Iterator<Item = &'a mut usize>) {
    let mut offsets = offsets.collect::<Vec<_>>();
    offsets.sort_by_key(|o| **o);

    let mut offsets_iter = offsets.iter_mut();
    let mut next_offset = if let Some(offset) = offsets_iter.next() {
        offset
    } else {
        return;
    };

    let mut char_idx = 0;
    for (bytes_idx, _) in text.char_indices() {
        while **next_offset == bytes_idx {
            **next_offset = char_idx;
            next_offset = if let Some(offset) = offsets_iter.next() {
                offset
            } else {
                return;
            }
        }
        char_idx += 1;
    }

    // Offsets after the last char.
    **next_offset = char_idx;
    for offset in offsets_iter {
        **offset = char_idx;
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let full_text = self.args.text.value(&input)?.as_str()?;
        let chunk_size = self.args.chunk_size.value(&input)?.as_int64()?;
        let recursive_chunker = RecursiveChunker {
            full_text,
            chunk_size: chunk_size as usize,
            chunk_overlap: (self.args.chunk_overlap.value(&input)?)
                .optional()
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(0) as usize,
            min_chunk_size: (self.args.min_chunk_size.value(&input)?)
                .optional()
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(chunk_size / 2) as usize,
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
        } else if let Some(lang_config) = TREE_SITTER_LANGUAGE_BY_LANG.get(&language) {
            let mut parser = tree_sitter::Parser::new();
            parser.set_language(&lang_config.tree_sitter_lang)?;
            let tree = parser.parse(full_text.as_ref(), None).ok_or_else(|| {
                anyhow!("failed in parsing text in language: {}", lang_config.name)
            })?;
            recursive_chunker.split_root_chunk(ChunkKind::TreeSitterNode {
                lang_config,
                node: tree.root_node(),
            })?
        } else {
            recursive_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
                lang_config: &DEFAULT_LANGUAGE_CONFIG,
                next_regexp_sep_id: 0,
            })?
        };

        translate_bytes_to_chars(
            full_text,
            output.iter_mut().flat_map(|(range, _)| {
                std::iter::once(&mut range.start).chain(std::iter::once(&mut range.end))
            }),
        );

        let table = output
            .into_iter()
            .map(|(range, text)| (range.into(), fields_value!(Arc::<str>::from(text)).into()))
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

    fn resolve_schema(
        &self,
        _spec: &Spec,
        args_resolver: &mut OpArgsResolver<'_>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?,
            chunk_size: args_resolver
                .next_arg("chunk_size")?
                .expect_type(&ValueType::Basic(BasicValueType::Int64))?,
            min_chunk_size: args_resolver
                .next_optional_arg("min_chunk_size")?
                .expect_type(&ValueType::Basic(BasicValueType::Int64))?,
            chunk_overlap: args_resolver
                .next_optional_arg("chunk_overlap")?
                .expect_type(&ValueType::Basic(BasicValueType::Int64))?,
            language: args_resolver
                .next_optional_arg("language")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?,
        };

        let mut struct_schema = StructSchema::default();
        let mut schema_builder = StructSchemaBuilder::new(&mut struct_schema);
        schema_builder.add_field(FieldSchema::new(
            "location",
            make_output_type(BasicValueType::Range),
        ));
        schema_builder.add_field(FieldSchema::new(
            "text",
            make_output_type(BasicValueType::Str),
        ));
        let output_schema = make_output_type(TableSchema::new(TableKind::KTable, struct_schema))
            .with_attr(
                field_attrs::CHUNK_BASE_TEXT,
                serde_json::to_value(args_resolver.get_analyze_value(&args.text))?,
            );
        Ok((args, output_schema))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor::new(args, spec)?))
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory.register(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to assert chunk text and its consistency with the range within the original text.
    fn assert_chunk_text_consistency(
        full_text: &str, // Added full text
        actual_chunk: &(RangeValue, &str),
        expected_text: &str,
        context: &str,
    ) {
        // Extract text using the chunk's range from the original full text.
        let extracted_text = actual_chunk.0.extract_str(full_text);
        // Assert that the expected text matches the text provided in the chunk.
        assert_eq!(
            actual_chunk.1, expected_text,
            "Provided chunk text mismatch - {}",
            context
        );
        // Assert that the expected text also matches the text extracted using the chunk's range.
        assert_eq!(
            extracted_text, expected_text,
            "Range inconsistency: extracted text mismatch - {}",
            context
        );
    }

    // Creates a default RecursiveChunker for testing, assuming no language-specific parsing.
    fn create_test_chunker(
        text: &str,
        chunk_size: usize,
        min_chunk_size: usize,
        chunk_overlap: usize,
    ) -> RecursiveChunker {
        RecursiveChunker {
            full_text: text,
            chunk_size,
            chunk_overlap,
            min_chunk_size,
        }
    }

    #[test]
    fn test_translate_bytes_to_chars_simple() {
        let text = "abc😄def";
        let mut start1 = 0;
        let mut end1 = 3;
        let mut start2 = 3;
        let mut end2 = 7;
        let mut start3 = 7;
        let mut end3 = 10;
        let mut end_full = text.len();

        let offsets = vec![
            &mut start1,
            &mut end1,
            &mut start2,
            &mut end2,
            &mut start3,
            &mut end3,
            &mut end_full,
        ];

        translate_bytes_to_chars(text, offsets.into_iter());

        assert_eq!(start1, 0);
        assert_eq!(end1, 3);
        assert_eq!(start2, 3);
        assert_eq!(end2, 4);
        assert_eq!(start3, 4);
        assert_eq!(end3, 7);
        assert_eq!(end_full, 7);
    }

    #[test]
    fn test_basic_split_no_overlap() {
        let text = "Linea 1.\nLinea 2.\n\nLinea 3.";
        let chunker = create_test_chunker(text, 15, 5, 0);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
            lang_config: &DEFAULT_LANGUAGE_CONFIG,
            next_regexp_sep_id: 0,
        });

        assert!(result.is_ok());
        let chunks = result.unwrap();

        assert_eq!(chunks.len(), 3);
        assert_chunk_text_consistency(text, &chunks[0], "Linea 1.", "Test 1, Chunk 0");
        assert_chunk_text_consistency(text, &chunks[1], "Linea 2.", "Test 1, Chunk 1");
        assert_chunk_text_consistency(text, &chunks[2], "Linea 3.", "Test 1, Chunk 2");

        // Test splitting when chunk_size forces breaks within segments.
        let text2 = "A very very long text that needs to be split.";
        let chunker2 = create_test_chunker(text2, 20, 12, 0);
        let result2 = chunker2.split_root_chunk(ChunkKind::RegexpSepChunk {
            lang_config: &DEFAULT_LANGUAGE_CONFIG,
            next_regexp_sep_id: 0,
        });

        assert!(result2.is_ok());
        let chunks2 = result2.unwrap();

        // Expect multiple chunks, likely split by spaces due to chunk_size.
        assert!(chunks2.len() > 1);
        assert_chunk_text_consistency(text2, &chunks2[0], "A very very long", "Test 2, Chunk 0");
        assert!(chunks2[0].1.len() <= 20);
    }
    #[test]
    fn test_basic_split_with_overlap() {
        let text = "This is a test text that is a bit longer to see how the overlap works.";
        let chunker = create_test_chunker(text, 20, 10, 5);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
            lang_config: &DEFAULT_LANGUAGE_CONFIG,
            next_regexp_sep_id: 0,
        });

        assert!(result.is_ok());
        let chunks = result.unwrap();

        assert!(chunks.len() > 1);

        if chunks.len() >= 2 {
            let _chunk1_text = chunks[0].1;
            let _chunk2_text = chunks[1].1;

            assert!(chunks[0].1.len() <= 25);
        }
    }
    #[test]
    fn test_split_trims_whitespace() {
        let text = "  \n First chunk. \n\n  Second chunk with spaces at the end.   \n";
        let chunker = create_test_chunker(text, 30, 10, 0);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
            lang_config: &DEFAULT_LANGUAGE_CONFIG,
            next_regexp_sep_id: 0,
        });

        assert!(result.is_ok());
        let chunks = result.unwrap();

        assert_eq!(chunks.len(), 3);

        assert_chunk_text_consistency(
            text,
            &chunks[0],
            " First chunk.",
            "Whitespace Test, Chunk 0",
        );
        assert_chunk_text_consistency(
            text,
            &chunks[1],
            "  Second chunk with spaces",
            "Whitespace Test, Chunk 1",
        );
        assert_chunk_text_consistency(text, &chunks[2], "at the end.", "Whitespace Test, Chunk 2");
    }
}
