use std::sync::Arc;

use crate::ops::registry::ExecutorFactoryRegistry;
use crate::ops::shared::split::{
    KeepSeparator, SeparatorSplitConfig, SeparatorSplitter, make_common_chunk_schema,
    output_position_to_value,
};
use crate::{fields_value, ops::sdk::*};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum KeepSep {
    Left,
    Right,
}

impl From<KeepSep> for KeepSeparator {
    fn from(value: KeepSep) -> Self {
        match value {
            KeepSep::Left => KeepSeparator::Left,
            KeepSep::Right => KeepSeparator::Right,
        }
    }
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
    splitter: SeparatorSplitter,
    args: Args,
}

impl Executor {
    fn new(args: Args, spec: Spec) -> Result<Self> {
        let config = SeparatorSplitConfig {
            separators_regex: spec.separators_regex,
            keep_separator: spec.keep_separator.map(Into::into),
            include_empty: spec.include_empty,
            trim: spec.trim,
        };
        let splitter =
            SeparatorSplitter::new(config).with_context(|| "failed to compile separators_regex")?;
        Ok(Self { args, splitter })
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let full_text = self.args.text.value(&input)?.as_str()?;

        // Use the extra_text splitter
        let chunks = self.splitter.split(&full_text);

        // Convert chunks to cocoindex table format
        let table = chunks
            .into_iter()
            .map(|c| {
                let chunk_text = &full_text[c.range.start..c.range.end];
                (
                    KeyValue::from_single_part(RangeValue::new(
                        c.start.char_offset,
                        c.end.char_offset,
                    )),
                    fields_value!(
                        Arc::<str>::from(chunk_text),
                        output_position_to_value(c.start),
                        output_position_to_value(c.end)
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
