use regex::{Matches, Regex};
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc};

use crate::base::field_attrs;
use crate::{fields_value, ops::sdk::*};

type Spec = EmptySpec;

pub struct Args {
    text: ResolvedOpArg,
    chunk_size: ResolvedOpArg,
    chunk_overlap: Option<ResolvedOpArg>,
    language: Option<ResolvedOpArg>,
}

static DEFAULT_SEPARATORS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [r"\n\n+", r"\n", r"\s+"]
        .into_iter()
        .map(|s| Regex::new(s).unwrap())
        .collect()
});
static SEPARATORS_BY_LANG: LazyLock<HashMap<&'static str, Vec<Regex>>> = LazyLock::new(|| {
    [
        (
            "markdown",
            vec![
                r"(^|\n)\n*# ",
                r"(^|\n)\n*## ",
                r"(^|\n)\n*### ",
                r"(^|\n)\n*#### ",
                r"(^|\n)\n*##### ",
                r"(^|\n)\n*###### ",
                // Code block
                r"(^|\n\n)\n*```\S*\n|\n\s*```\n*(\n\n|$)",
                // Horizontal lines
                r"(^|\n\n)\n*(\*\*\*+|---+|___+)\n*(\n\n|$)",
                r"\n\n+",
                r"(\.|!|\?)\s*(\s|$)",
                r":\s*(\s|$)",
                r";\s*(\s|$)",
                r"\n",
                r"\s+",
            ],
        ),
        (
            "python",
            vec![
                // First, try to split along class definitions
                r"\nclass ",
                r"\n  def ",
                r"\n    def ",
                r"\n      def ",
                r"\n        def ",
                // Now split by the normal type of lines
                r"\n\n",
                r"\n",
                r"\s+",
            ],
        ),
        (
            "javascript",
            vec![
                // Split along function definitions
                r"\nfunction ",
                r"\nconst ",
                r"\nlet ",
                r"\nvar ",
                r"\nclass ",
                // Split along control flow statements
                r"\n\s*if ",
                r"\n\s*for ",
                r"\n\s*while ",
                r"\n\s*switch ",
                r"\n\s*case ",
                r"\n\s*default ",
                // Split by the normal type of lines
                r"\n\n",
                r"\n",
            ],
        ),
    ]
    .into_iter()
    .map(|(lang, separators)| {
        let regexs = separators
            .into_iter()
            .map(|s| Regex::new(s).unwrap())
            .collect();
        (lang, regexs)
    })
    .collect()
});

trait NestedChunk {
    fn range(&self) -> &RangeValue;

    fn sub_chunks(&self) -> Option<impl Iterator<Item = Self>>;
}

struct SplitTarget<'s> {
    separators: &'static [Regex],
    text: &'s str,
}

struct Chunk<'s> {
    target: &'s SplitTarget<'s>,
    range: RangeValue,
    next_sep_id: usize,
}

struct SubChunksIter<'a, 's: 'a> {
    parent: &'a Chunk<'s>,
    matches_iter: Matches<'static, 's>,
    next_start_pos: Option<usize>,
}

impl<'a, 's: 'a> SubChunksIter<'a, 's> {
    fn new(parent: &'a Chunk<'s>, matches_iter: Matches<'static, 's>) -> Self {
        Self {
            parent,
            matches_iter,
            next_start_pos: Some(parent.range.start),
        }
    }
}

impl<'a, 's: 'a> Iterator for SubChunksIter<'a, 's> {
    type Item = Chunk<'s>;

    fn next(&mut self) -> Option<Self::Item> {
        let start_pos = if let Some(start_pos) = self.next_start_pos {
            start_pos
        } else {
            return None;
        };
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
            target: self.parent.target,
            range: RangeValue::new(start_pos, end_pos),
            next_sep_id: self.parent.next_sep_id + 1,
        })
    }
}

impl<'s> NestedChunk for Chunk<'s> {
    fn range(&self) -> &RangeValue {
        &self.range
    }

    fn sub_chunks(&self) -> Option<impl Iterator<Item = Self>> {
        if self.next_sep_id >= self.target.separators.len() {
            None
        } else {
            let sub_text = self.range.extract_str(&self.target.text);
            Some(SubChunksIter::new(
                self,
                self.target.separators[self.next_sep_id].find_iter(sub_text),
            ))
        }
    }
}

struct RecursiveChunker<'s> {
    text: &'s str,
    chunk_size: usize,
    chunk_overlap: usize,
}

impl<'s> RecursiveChunker<'s> {
    fn split_substring<Chk>(&self, chunk: Chk, output: &mut Vec<(RangeValue, &'s str)>)
    where
        Chk: NestedChunk + Sized,
    {
        let sub_chunks_iter = if let Some(sub_chunks_iter) = chunk.sub_chunks() {
            sub_chunks_iter
        } else {
            self.add_output(*chunk.range(), output);
            return;
        };

        let flush_small_chunks =
            |chunks: &[RangeValue], output: &mut Vec<(RangeValue, &'s str)>| {
                if chunks.is_empty() {
                    return;
                }
                let mut start_pos = chunks[0].start;
                for i in 1..chunks.len() - 1 {
                    let chunk = &chunks[i];
                    if chunk.end - start_pos > self.chunk_size {
                        self.add_output(RangeValue::new(start_pos, chunk.end), output);

                        // Find the new start position, allowing overlap within the threshold.
                        let mut new_start_idx = i + 1;
                        let next_chunk = &chunks[i + 1];
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
            };

        let mut small_chunks = Vec::new();
        for sub_chunk in sub_chunks_iter {
            let sub_range = sub_chunk.range();
            if sub_range.len() <= self.chunk_size {
                small_chunks.push(*sub_chunk.range());
            } else {
                flush_small_chunks(&small_chunks, output);
                small_chunks.clear();
                self.split_substring(sub_chunk, output);
            }
        }
        flush_small_chunks(&small_chunks, output);
    }

    fn add_output(&self, range: RangeValue, output: &mut Vec<(RangeValue, &'s str)>) {
        let text = range.extract_str(self.text);
        if !text.trim().is_empty() {
            output.push((range, text));
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
    for offset in offsets_iter {
        **offset = char_idx;
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let text = self.args.text.value(&input)?.as_str()?;
        let recursive_chunker = RecursiveChunker {
            text,
            chunk_size: self.args.chunk_size.value(&input)?.as_int64()? as usize,
            chunk_overlap: self
                .args
                .chunk_overlap
                .value(&input)?
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(0) as usize,
        };

        let separators = self
            .args
            .language
            .value(&input)?
            .map(|v| v.as_str())
            .transpose()?
            .and_then(|lang| {
                SEPARATORS_BY_LANG
                    .get(lang.to_lowercase().as_str())
                    .map(|v| v.as_slice())
            })
            .unwrap_or(DEFAULT_SEPARATORS.as_slice());

        let mut output = Vec::new();
        recursive_chunker.split_substring(
            Chunk {
                target: &SplitTarget { separators, text },
                range: RangeValue::new(0, text.len()),
                next_sep_id: 0,
            },
            &mut output,
        );

        translate_bytes_to_chars(
            text,
            output
                .iter_mut()
                .map(|(range, _)| [&mut range.start, &mut range.end].into_iter())
                .flatten(),
        );

        let table = output
            .into_iter()
            .map(|(range, text)| (range.into(), fields_value!(Arc::<str>::from(text)).into()))
            .collect();

        Ok(Value::Table(table))
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
        let output_schema = make_output_type(CollectionSchema::new(
            CollectionKind::Table,
            vec![
                FieldSchema::new("location", make_output_type(BasicValueType::Range)),
                FieldSchema::new("text", make_output_type(BasicValueType::Str)),
            ],
        ))
        .with_attr(
            field_attrs::CHUNK_BASE_TEXT,
            serde_json::to_value(&args_resolver.get_analyze_value(&args.text))?,
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
