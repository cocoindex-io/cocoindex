use anyhow::{Context, Result};
use regex::Regex;
use std::sync::Arc;

use crate::ops::registry::ExecutorFactoryRegistry;
use crate::ops::shared::split::{Position, make_common_chunk_schema, set_output_positions};
use crate::{fields_value, ops::sdk::*};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum KeepSep {
    Left,
    Right,
}

#[derive(Serialize, Deserialize)]
struct Spec {
    // Python SDK provides defaults/values.
    separators_regex: Vec<String>,
    keep_separator: Option<KeepSep>,
    include_empty: bool,
    trim: bool,
}

struct Args {
    text: ResolvedOpArg,
}

struct Executor {
    spec: Spec,
    regex: Option<Regex>,
    args: Args,
}

impl Executor {
    fn new(args: Args, spec: Spec) -> Result<Self> {
        let regex = if spec.separators_regex.is_empty() {
            None
        } else {
            // OR-join all separators, multiline
            let pattern = format!(
                "(?m){}",
                spec.separators_regex
                    .iter()
                    .map(|s| format!("(?:{s})"))
                    .collect::<Vec<_>>()
                    .join("|")
            );
            Some(Regex::new(&pattern).context("failed to compile separators_regex")?)
        };
        Ok(Self { args, spec, regex })
    }
}

struct ChunkOutput<'s> {
    start_pos: Position,
    end_pos: Position,
    text: &'s str,
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let full_text = self.args.text.value(&input)?.as_str()?;
        let bytes = full_text.as_bytes();

        // add_range applies trim/include_empty and records the text slice
        let mut chunks: Vec<ChunkOutput<'_>> = Vec::new();
        let mut add_range = |mut s: usize, mut e: usize| {
            if self.spec.trim {
                while s < e && bytes[s].is_ascii_whitespace() {
                    s += 1;
                }
                while e > s && bytes[e - 1].is_ascii_whitespace() {
                    e -= 1;
                }
            }
            if self.spec.include_empty || e > s {
                chunks.push(ChunkOutput {
                    start_pos: Position::new(s),
                    end_pos: Position::new(e),
                    text: &full_text[s..e],
                });
            }
        };

        if let Some(re) = &self.regex {
            let mut start = 0usize;
            for m in re.find_iter(full_text) {
                let end = match self.spec.keep_separator {
                    Some(KeepSep::Left) => m.end(),
                    Some(KeepSep::Right) | None => m.start(),
                };
                add_range(start, end);
                start = match self.spec.keep_separator {
                    Some(KeepSep::Right) => m.start(),
                    _ => m.end(),
                };
            }
            add_range(start, full_text.len());
        } else {
            // No separators: emit whole text
            add_range(0, full_text.len());
        }

        set_output_positions(
            full_text,
            chunks.iter_mut().flat_map(|c| {
                std::iter::once(&mut c.start_pos).chain(std::iter::once(&mut c.end_pos))
            }),
        );

        let table = chunks
            .into_iter()
            .map(|c| {
                let s = c.start_pos.output.unwrap();
                let e = c.end_pos.output.unwrap();
                (
                    KeyValue::from_single_part(RangeValue::new(s.char_offset, e.char_offset)),
                    fields_value!(Arc::<str>::from(c.text), s.into_output(), e.into_output())
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
        "SplitBySeparators"
    }

    async fn analyze<'a>(
        &'a self,
        _spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<SimpleFunctionAnalysisOutput<Args>> {
        // one required arg: text: Str
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?
                .required()?,
        };

        let output_schema = make_common_chunk_schema(args_resolver, &args.text)?;
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

    #[tokio::test]
    async fn test_split_by_separators_paragraphs() {
        let spec = Spec {
            separators_regex: vec![r"\n\n+".to_string()],
            keep_separator: None,
            include_empty: false,
            trim: true,
        };
        let factory = Arc::new(Factory);
        let text = "Para1\n\nPara2\n\n\nPara3";

        let input_arg_schemas = &[(
            Some("text"),
            make_output_type(BasicValueType::Str).with_nullable(true),
        )];

        let result = test_flow_function(
            &factory,
            &spec,
            input_arg_schemas,
            vec![text.to_string().into()],
        )
        .await
        .unwrap();

        match result {
            Value::KTable(table) => {
                // Expected ranges after trimming whitespace:
                let expected = vec![
                    (RangeValue::new(0, 5), "Para1"),
                    (RangeValue::new(7, 12), "Para2"),
                    (RangeValue::new(15, 20), "Para3"),
                ];
                for (range, expected_text) in expected {
                    let key = KeyValue::from_single_part(range);
                    let row = table.get(&key).unwrap();
                    let chunk_text = row.0.fields[0].as_str().unwrap();
                    assert_eq!(**chunk_text, *expected_text);
                }
            }
            other => panic!("Expected KTable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_split_by_separators_keep_right() {
        let spec = Spec {
            separators_regex: vec![r"\.".to_string()],
            keep_separator: Some(KeepSep::Right),
            include_empty: false,
            trim: true,
        };
        let factory = Arc::new(Factory);
        let text = "A. B. C.";

        let input_arg_schemas = &[(
            Some("text"),
            make_output_type(BasicValueType::Str).with_nullable(true),
        )];

        let result = test_flow_function(
            &factory,
            &spec,
            input_arg_schemas,
            vec![text.to_string().into()],
        )
        .await
        .unwrap();

        match result {
            Value::KTable(table) => {
                assert!(table.len() >= 3);
            }
            _ => panic!("KTable expected"),
        }
    }
}
