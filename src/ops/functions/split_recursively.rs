use regex::Regex;
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

struct SplitTask {
    separators: &'static [Regex],
    chunk_size: usize,
    chunk_overlap: usize,
}

impl SplitTask {
    fn split_substring<'s>(
        &self,
        s: &'s str,
        base_pos: usize,
        next_sep_id: usize,
        output: &mut Vec<(RangeValue, &'s str)>,
    ) {
        if next_sep_id >= self.separators.len() {
            self.add_output(base_pos, s, output);
            return;
        }

        let flush_small_chunks =
            |chunks: &[RangeValue], output: &mut Vec<(RangeValue, &'s str)>| {
                if chunks.is_empty() {
                    return;
                }
                let mut start_pos = chunks[0].start;
                for i in 1..chunks.len() - 1 {
                    let chunk = &chunks[i];
                    if chunk.end - start_pos > self.chunk_size {
                        self.add_output(base_pos + start_pos, &s[start_pos..chunk.end], output);

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
                self.add_output(base_pos + start_pos, &s[start_pos..last_chunk.end], output);
            };

        let mut small_chunks = Vec::new();
        let mut process_chunk =
            |start: usize, end: usize, output: &mut Vec<(RangeValue, &'s str)>| {
                let chunk = &s[start..end];
                if chunk.len() <= self.chunk_size {
                    small_chunks.push(RangeValue::new(start, start + chunk.len()));
                } else {
                    flush_small_chunks(&small_chunks, output);
                    small_chunks.clear();
                    self.split_substring(chunk, base_pos + start, next_sep_id + 1, output);
                }
            };

        let mut next_start_pos = 0;
        for cap in self.separators[next_sep_id].find_iter(s) {
            process_chunk(next_start_pos, cap.start(), output);
            next_start_pos = cap.end();
        }
        if next_start_pos < s.len() {
            process_chunk(next_start_pos, s.len(), output);
        }

        flush_small_chunks(&small_chunks, output);
    }

    fn add_output<'s>(&self, pos: usize, text: &'s str, output: &mut Vec<(RangeValue, &'s str)>) {
        if !text.trim().is_empty() {
            output.push((RangeValue::new(pos, pos + text.len()), text));
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
        let task = SplitTask {
            separators: self
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
                .unwrap_or(DEFAULT_SEPARATORS.as_slice()),
            chunk_size: self.args.chunk_size.value(&input)?.as_int64()? as usize,
            chunk_overlap: self
                .args
                .chunk_overlap
                .value(&input)?
                .map(|v| v.as_int64())
                .transpose()?
                .unwrap_or(0) as usize,
        };

        let text = self.args.text.value(&input)?.as_str()?;
        let mut output = Vec::new();
        task.split_substring(text, 0, 0, &mut output);

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
