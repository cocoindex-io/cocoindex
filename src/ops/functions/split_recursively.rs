use anyhow::anyhow;
use log::{error, trace};
use regex::{Matches, Regex};
use std::collections::HashSet;
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc};
use unicase::UniCase;

use crate::base::field_attrs;
use crate::{fields_value, ops::sdk::*};

type Spec = EmptySpec;

pub struct Args {
    text: ResolvedOpArg,
    chunk_size: ResolvedOpArg,
    chunk_overlap: Option<ResolvedOpArg>,
    language: Option<ResolvedOpArg>,
}

static TEXT_SEPARATOR: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [r"\n\n+", r"\n", r"\s+"]
        .into_iter()
        .map(|s| Regex::new(s).unwrap())
        .collect()
});

struct LanguageConfig {
    name: &'static str,
    tree_sitter_lang: tree_sitter::Language,
    terminal_node_kind_ids: HashSet<u16>,
}

fn add_language<'a>(
    output: &'a mut HashMap<UniCase<&'static str>, Arc<LanguageConfig>>,
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

    let config = Arc::new(LanguageConfig {
        name,
        tree_sitter_lang,
        terminal_node_kind_ids,
    });
    for name in std::iter::once(name).chain(aliases.into_iter()) {
        if output.insert(name.into(), config.clone()).is_some() {
            panic!("Language `{name}` already exists");
        }
    }
}

static TREE_SITTER_LANGUAGE_BY_LANG: LazyLock<HashMap<UniCase<&'static str>, Arc<LanguageConfig>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        add_language(&mut map, "C", [".c"], tree_sitter_c::LANGUAGE, []);
        add_language(
            &mut map,
            "C++",
            [".cpp", ".cc", ".cxx", ".h", ".hpp", "cpp"],
            tree_sitter_c::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "C#",
            [".cs", "cs"],
            tree_sitter_c_sharp::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "CSS",
            [".css", ".scss"],
            tree_sitter_css::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "Fortran",
            [".f", ".f90", ".f95", ".f03", "f", "f90", "f95", "f03"],
            tree_sitter_fortran::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "Go",
            [".go", "golang"],
            tree_sitter_go::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "HTML",
            [".html", ".htm"],
            tree_sitter_html::LANGUAGE,
            [],
        );
        add_language(&mut map, "Java", [".java"], tree_sitter_java::LANGUAGE, []);
        add_language(
            &mut map,
            "JavaScript",
            [".js", "js"],
            tree_sitter_javascript::LANGUAGE,
            [],
        );
        add_language(&mut map, "JSON", [".json"], tree_sitter_json::LANGUAGE, []);
        add_language(
            &mut map,
            "Markdown",
            [".md", ".mdx", "md"],
            tree_sitter_md::LANGUAGE,
            ["inline"],
        );
        add_language(
            &mut map,
            "Pascal",
            [".pas", "pas", ".dpr", "dpr", "Delphi"],
            tree_sitter_pascal::LANGUAGE,
            [],
        );
        add_language(&mut map, "PHP", [".php"], tree_sitter_php::LANGUAGE_PHP, []);
        add_language(
            &mut map,
            "Python",
            [".py"],
            tree_sitter_python::LANGUAGE,
            [],
        );
        add_language(&mut map, "R", [".r"], tree_sitter_r::LANGUAGE, []);
        add_language(&mut map, "Ruby", [".rb"], tree_sitter_ruby::LANGUAGE, []);
        add_language(
            &mut map,
            "Rust",
            [".rs", "rs"],
            tree_sitter_rust::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "Scala",
            [".scala"],
            tree_sitter_scala::LANGUAGE,
            [],
        );
        add_language(&mut map, "SQL", [".sql"], tree_sitter_sequel::LANGUAGE, []);
        add_language(
            &mut map,
            "Swift",
            [".swift"],
            tree_sitter_swift::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "TOML",
            [".toml"],
            tree_sitter_toml_ng::LANGUAGE,
            [],
        );
        add_language(
            &mut map,
            "TSX",
            [".tsx"],
            tree_sitter_typescript::LANGUAGE_TSX,
            [],
        );
        add_language(
            &mut map,
            "TypeScript",
            [".ts", "ts"],
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
            [],
        );
        add_language(&mut map, "XML", [".xml"], tree_sitter_xml::LANGUAGE_XML, []);
        add_language(&mut map, "DTD", [".dtd"], tree_sitter_xml::LANGUAGE_DTD, []);
        add_language(
            &mut map,
            "YAML",
            [".yaml", ".yml"],
            tree_sitter_yaml::LANGUAGE,
            [],
        );
        map
    });

enum ChunkKind<'t> {
    TreeSitterNode { node: tree_sitter::Node<'t> },
    RegexpSepChunk { next_regexp_sep_id: usize },
    LeafText,
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
    parent: &'t Chunk<'t, 's>,
    matches_iter: Matches<'static, 's>,
    regexp_sep_id: usize,
    next_start_pos: Option<usize>,
}

impl<'t, 's: 't> TextChunksIter<'t, 's> {
    fn new(parent: &'t Chunk<'t, 's>, regexp_sep_id: usize) -> Self {
        Self {
            parent,
            matches_iter: TEXT_SEPARATOR[regexp_sep_id].find_iter(parent.text()),
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
                next_regexp_sep_id: self.regexp_sep_id + 1,
            },
        })
    }
}

struct TreeSitterNodeIter<'t, 's: 't> {
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
                kind: ChunkKind::LeafText,
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
            kind: ChunkKind::TreeSitterNode { node },
        })
    }
}

struct RecursiveChunker<'s> {
    full_text: &'s str,
    lang_config: Option<&'s LanguageConfig>,
    chunk_size: usize,
    chunk_overlap: usize,
}

impl<'t, 's: 't> RecursiveChunker<'s> {
    fn flush_small_chunks(&self, chunks: &[RangeValue], output: &mut Vec<(RangeValue, &'s str)>) {
        if chunks.is_empty() {
            return;
        }
        let mut start_pos = chunks[0].start;
        for i in 0..chunks.len() - 1 {
            let next_chunk = &chunks[i + 1];
            if next_chunk.end - start_pos > self.chunk_size {
                let chunk = &chunks[i];
                self.add_output(RangeValue::new(start_pos, chunk.end), output);

                // Find the new start position, allowing overlap within the threshold.
                let mut new_start_idx = i + 1;
                while new_start_idx > 0 {
                    let prev_pos = chunks[new_start_idx - 1].start;
                    if prev_pos <= start_pos
                        || chunk.end - prev_pos > self.chunk_overlap
                        || next_chunk.end - prev_pos > self.chunk_size
                    {
                        break;
                    }
                    new_start_idx -= 1;
                }
                start_pos = chunks[new_start_idx].start;
            }
        }

        let last_chunk = &chunks[chunks.len() - 1];
        self.add_output(RangeValue::new(start_pos, last_chunk.end), output);
    }

    fn process_sub_chunks(
        &self,
        sub_chunks_iter: impl Iterator<Item = Chunk<'t, 's>>,
        output: &mut Vec<(RangeValue, &'s str)>,
    ) -> Result<()> {
        let mut small_chunks = Vec::new();
        for sub_chunk in sub_chunks_iter {
            let sub_range = sub_chunk.range;
            if sub_range.len() <= self.chunk_size {
                small_chunks.push(sub_range);
            } else {
                self.flush_small_chunks(&small_chunks, output);
                small_chunks.clear();
                self.split_substring(sub_chunk, output)?;
            }
        }
        self.flush_small_chunks(&small_chunks, output);
        Ok(())
    }

    fn split_substring(
        &self,
        chunk: Chunk<'t, 's>,
        output: &mut Vec<(RangeValue, &'s str)>,
    ) -> Result<()> {
        match chunk.kind {
            ChunkKind::TreeSitterNode { node } => {
                if !self
                    .lang_config
                    .ok_or_else(|| anyhow!("Language not set."))?
                    .terminal_node_kind_ids
                    .contains(&node.kind_id())
                {
                    let mut cursor = node.walk();
                    if cursor.goto_first_child() {
                        self.process_sub_chunks(
                            TreeSitterNodeIter {
                                full_text: self.full_text,
                                cursor: Some(cursor),
                                next_start_pos: node.start_byte(),
                                end_pos: node.end_byte(),
                            },
                            output,
                        )?;
                        return Ok(());
                    }
                }
                self.add_output(chunk.range, output);
            }
            ChunkKind::RegexpSepChunk { next_regexp_sep_id } => {
                if next_regexp_sep_id >= TEXT_SEPARATOR.len() {
                    self.add_output(chunk.range, output);
                } else {
                    self.process_sub_chunks(
                        TextChunksIter::new(&chunk, next_regexp_sep_id),
                        output,
                    )?;
                }
            }
            ChunkKind::LeafText => {
                self.add_output(chunk.range, output);
            }
        }
        Ok(())
    }

    fn split_root_chunk(&self, kind: ChunkKind<'t>) -> Result<Vec<(RangeValue, &'s str)>> {
        let mut output = Vec::new();
        self.split_substring(
            Chunk {
                full_text: self.full_text,
                range: RangeValue::new(0, self.full_text.len()),
                kind,
            },
            &mut output,
        )?;
        Ok(output)
    }

    fn add_output(&self, range: RangeValue, output: &mut Vec<(RangeValue, &'s str)>) {
        let text = range.extract_str(self.full_text);

        // Trim leading new lines.
        let trimmed_text = text.trim_start_matches(['\n', '\r']);
        let adjusted_start = range.start + (text.len() - trimmed_text.len());

        // Trim trailing whitespaces
        let trimmed_text = trimmed_text.trim_end();

        // Only record chunks with alphanumeric characters.
        if trimmed_text.chars().any(|ch| ch.is_alphanumeric()) {
            output.push((
                RangeValue::new(adjusted_start, adjusted_start + trimmed_text.len()),
                trimmed_text,
            ));
        }
    }
}

struct Executor {
    args: Args,
}

impl Executor {
    fn new(args: Args) -> Result<Self> {
        Ok(Self { args })
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
        let lang_config = {
            let language = self.args.language.value(&input)?;
            language
                .optional()
                .map(|v| anyhow::Ok(v.as_str()?.as_ref()))
                .transpose()?
                .and_then(|lang| TREE_SITTER_LANGUAGE_BY_LANG.get(&UniCase::new(lang)))
        };

        let recursive_chunker = RecursiveChunker {
            full_text,
            lang_config: lang_config.map(|c| c.as_ref()),
            chunk_size: self.args.chunk_size.value(&input)?.as_int64()? as usize,
            chunk_overlap: self
                .args
                .chunk_overlap
                .value(&input)?
                .optional()
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(0) as usize,
        };

        let mut output = if let Some(lang_config) = lang_config {
            let mut parser = tree_sitter::Parser::new();
            parser.set_language(&lang_config.tree_sitter_lang)?;
            let tree = parser.parse(full_text.as_ref(), None).ok_or_else(|| {
                anyhow!("failed in parsing text in language: {}", lang_config.name)
            })?;
            recursive_chunker.split_root_chunk(ChunkKind::TreeSitterNode {
                node: tree.root_node(),
            })?
        } else {
            recursive_chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
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

pub struct Factory;

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
        _spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor::new(args)?))
    }
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
        chunk_overlap: usize,
    ) -> RecursiveChunker {
        RecursiveChunker {
            full_text: text,
            lang_config: None,
            chunk_size,
            chunk_overlap,
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
        let chunker = create_test_chunker(text, 15, 0);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
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
        let chunker2 = create_test_chunker(text2, 20, 0);
        let result2 = chunker2.split_root_chunk(ChunkKind::RegexpSepChunk {
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
        let chunker = create_test_chunker(text, 20, 5);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
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
        let chunker = create_test_chunker(text, 30, 0);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
            next_regexp_sep_id: 0,
        });

        assert!(result.is_ok());
        let chunks = result.unwrap();

        assert_eq!(chunks.len(), 3);

        assert_chunk_text_consistency(
            text,
            &chunks[0],
            "  \n First chunk.",
            "Whitespace Test, Chunk 0",
        );
        assert_chunk_text_consistency(
            text,
            &chunks[1],
            "  Second chunk with spaces at",
            "Whitespace Test, Chunk 1",
        );
        assert_chunk_text_consistency(text, &chunks[2], "the end.", "Whitespace Test, Chunk 2");
    }
    #[test]
    fn test_split_discards_empty_chunks() {
        let text = "Chunk 1.\n\n   \n\nChunk 2.\n\n------\n\nChunk 3.";
        let chunker = create_test_chunker(text, 10, 0);

        let result = chunker.split_root_chunk(ChunkKind::RegexpSepChunk {
            next_regexp_sep_id: 0,
        });

        assert!(result.is_ok());
        let chunks = result.unwrap();

        assert_eq!(chunks.len(), 3);

        // Expect only the chunks with actual alphanumeric content.
        assert_chunk_text_consistency(text, &chunks[0], "Chunk 1.", "Discard Test, Chunk 0");
        assert_chunk_text_consistency(text, &chunks[1], "Chunk 2.", "Discard Test, Chunk 1");
        assert_chunk_text_consistency(text, &chunks[2], "Chunk 3.", "Discard Test, Chunk 2");
    }
}
