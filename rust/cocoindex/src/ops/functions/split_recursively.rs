use std::sync::Arc;

use crate::ops::shared::split::{
    CustomLanguageConfig, RecursiveChunkConfig, RecursiveChunker, RecursiveSplitConfig,
    make_common_chunk_schema, output_position_to_value,
};
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

pub struct Args {
    text: ResolvedOpArg,
    chunk_size: ResolvedOpArg,
    min_chunk_size: Option<ResolvedOpArg>,
    chunk_overlap: Option<ResolvedOpArg>,
    language: Option<ResolvedOpArg>,
}

struct Executor {
    args: Args,
    chunker: RecursiveChunker,
}

impl Executor {
    fn new(args: Args, spec: Spec) -> Result<Self> {
        let config = RecursiveSplitConfig {
            custom_languages: spec
                .custom_languages
                .into_iter()
                .map(|lang| CustomLanguageConfig {
                    language_name: lang.language_name,
                    aliases: lang.aliases,
                    separators_regex: lang.separators_regex,
                })
                .collect(),
        };
        let chunker = RecursiveChunker::new(config).map_err(|e| api_error!("{}", e))?;
        Ok(Self { args, chunker })
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
            .map(|v| v as usize);
        let chunk_overlap = (self.args.chunk_overlap.value(&input)?)
            .optional()
            .map(|v| v.as_int64())
            .transpose()?
            .map(|v| v as usize);
        let language = if let Some(language) = self.args.language.value(&input)?.optional() {
            Some(language.as_str()?.to_string())
        } else {
            None
        };

        let config = RecursiveChunkConfig {
            chunk_size: chunk_size as usize,
            min_chunk_size,
            chunk_overlap,
            language,
        };

        let chunks = self.chunker.split(&full_text, config);

        let table = chunks
            .into_iter()
            .map(|chunk| {
                let chunk_text = &full_text[chunk.range.start..chunk.range.end];
                (
                    KeyValue::from_single_part(RangeValue::new(
                        chunk.start.char_offset,
                        chunk.end.char_offset,
                    )),
                    fields_value!(
                        Arc::<str>::from(chunk_text),
                        output_position_to_value(chunk.start),
                        output_position_to_value(chunk.end)
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
