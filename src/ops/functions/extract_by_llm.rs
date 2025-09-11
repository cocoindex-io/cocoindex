use crate::llm::{
    LlmGenerateRequest, LlmGenerationClient, LlmSpec, OutputFormat, new_llm_generation_client,
};
use crate::ops::sdk::*;
use crate::prelude::*;
use base::json_schema::build_json_schema;
use schemars::schema::SchemaObject;
use std::borrow::Cow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    llm_spec: LlmSpec,
    output_type: EnrichedValueType,
    instruction: Option<String>,
}

pub struct Args {
    text: Option<ResolvedOpArg>,
    image: Option<ResolvedOpArg>,
}

struct Executor {
    args: Args,
    client: Box<dyn LlmGenerationClient>,
    model: String,
    output_json_schema: SchemaObject,
    system_prompt: String,
    value_extractor: base::json_schema::ValueExtractor,
}

fn get_system_prompt(instructions: &Option<String>, extra_instructions: Option<String>) -> String {
    let mut message =
        "You are a helpful assistant that processes user-provided inputs (text, images, or both) to produce structured outputs. \
Your task is to follow the provided instructions to generate or extract information and output valid JSON matching the specified schema. \
Base your response solely on the content of the input. \
For generative tasks, respond accurately and relevantly based on what is provided. \
Unless explicitly instructed otherwise, output only the JSON. DO NOT include explanations, descriptions, or formatting outside the JSON."
            .to_string();

    if let Some(custom_instructions) = instructions {
        message.push_str("\n\n");
        message.push_str(custom_instructions);
    }

    if let Some(extra_instructions) = extra_instructions {
        message.push_str("\n\n");
        message.push_str(&extra_instructions);
    }

    message
}

impl Executor {
    async fn new(spec: Spec, args: Args) -> Result<Self> {
        let client = new_llm_generation_client(
            spec.llm_spec.api_type,
            spec.llm_spec.address,
            spec.llm_spec.api_config,
        )
        .await?;
        let schema_output = build_json_schema(spec.output_type, client.json_schema_options())?;
        Ok(Self {
            args,
            client,
            model: spec.llm_spec.model,
            output_json_schema: schema_output.schema,
            system_prompt: get_system_prompt(&spec.instruction, schema_output.extra_instructions),
            value_extractor: schema_output.value_extractor,
        })
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    fn behavior_version(&self) -> Option<u32> {
        Some(1)
    }

    fn enable_cache(&self) -> bool {
        true
    }

    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let image_bytes: Option<Cow<'_, [u8]>> = if let Some(arg) = self.args.image.as_ref()
            && let Some(value) = arg.value(&input)?.optional()
        {
            Some(Cow::Borrowed(value.as_bytes()?))
        } else {
            None
        };

        let text = if let Some(arg) = self.args.text.as_ref()
            && let Some(value) = arg.value(&input)?.optional()
        {
            Some(value.as_str()?)
        } else {
            None
        };

        if text.is_none() && image_bytes.is_none() {
            return Ok(Value::Null);
        }

        let user_prompt = text.map_or("", |v| v);
        let req = LlmGenerateRequest {
            model: &self.model,
            system_prompt: Some(Cow::Borrowed(&self.system_prompt)),
            user_prompt: Cow::Borrowed(user_prompt),
            image: image_bytes,
            output_format: Some(OutputFormat::JsonSchema {
                name: Cow::Borrowed("ExtractedData"),
                schema: Cow::Borrowed(&self.output_json_schema),
            }),
        };
        let res = self.client.generate(req).await?;
        let json_value: serde_json::Value = utils::deser::from_json_str(res.text.as_str())?;
        let value = self.value_extractor.extract_value(json_value)?;
        Ok(value)
    }
}

pub struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = Spec;
    type ResolvedArgs = Args;

    fn name(&self) -> &str {
        "ExtractByLlm"
    }

    async fn resolve_schema<'a>(
        &'a self,
        spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Str))?
                .optional(),
            image: args_resolver
                .next_arg("image")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Bytes))?
                .optional(),
        };

        if args.text.is_none() && args.image.is_none() {
            api_bail!("At least one of 'text' or 'image' must be provided");
        }

        let mut output_type = spec.output_type.clone();
        if args.text.as_ref().map_or(true, |arg| arg.typ.nullable)
            && args.image.as_ref().map_or(true, |arg| arg.typ.nullable)
        {
            output_type.nullable = true;
        }
        Ok((args, output_type))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        resolved_input_schema: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<impl SimpleFunctionExecutor> {
        Executor::new(spec, resolved_input_schema).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::functions::test_utils::{build_arg_schema, test_flow_function};

    #[tokio::test]
    #[ignore = "This test requires an OpenAI API key or a configured local LLM and may make network calls."]
    async fn test_extract_by_llm() {
        // Define the expected output structure
        let target_output_schema = StructSchema {
            fields: Arc::new(vec![
                FieldSchema::new(
                    "extracted_field_name",
                    make_output_type(BasicValueType::Str),
                ),
                FieldSchema::new(
                    "extracted_field_value",
                    make_output_type(BasicValueType::Int64),
                ),
            ]),
            description: Some("A test structure for extraction".into()),
        };

        let output_type_spec = EnrichedValueType {
            typ: ValueType::Struct(target_output_schema.clone()),
            nullable: false,
            attrs: Arc::new(BTreeMap::new()),
        };

        let spec = Spec {
            llm_spec: LlmSpec {
                api_type: crate::llm::LlmApiType::OpenAi,
                model: "gpt-4o".to_string(),
                address: None,
                api_config: None,
            },
            output_type: output_type_spec,
            instruction: Some("Extract the name and value from the text. The name is a string, the value is an integer.".to_string()),
        };

        let factory = Arc::new(Factory);
        let text_content = "The item is called 'CocoIndex Test' and its value is 42.";

        let input_args_values = vec![text_content.to_string().into()];

        let input_arg_schemas = &[build_arg_schema("text", BasicValueType::Str)];

        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;

        if result.is_err() {
            eprintln!(
                "test_extract_by_llm: test_flow_function returned error (potentially expected for evaluate): {:?}",
                result.as_ref().err()
            );
        }

        assert!(
            result.is_ok(),
            "test_flow_function failed. NOTE: This test may require network access/API keys for OpenAI. Error: {:?}",
            result.err()
        );

        let value = result.unwrap();

        match value {
            Value::Struct(field_values) => {
                assert_eq!(
                    field_values.fields.len(),
                    target_output_schema.fields.len(),
                    "Mismatched number of fields in output struct"
                );
                for (idx, field_schema) in target_output_schema.fields.iter().enumerate() {
                    match (&field_values.fields[idx], &field_schema.value_type.typ) {
                        (
                            Value::Basic(BasicValue::Str(_)),
                            ValueType::Basic(BasicValueType::Str),
                        ) => {}
                        (
                            Value::Basic(BasicValue::Int64(_)),
                            ValueType::Basic(BasicValueType::Int64),
                        ) => {}
                        (val, expected_type) => panic!(
                            "Field '{}' type mismatch. Got {:?}, expected type compatible with {:?}",
                            field_schema.name,
                            val.kind(),
                            expected_type
                        ),
                    }
                }
            }
            _ => panic!("Expected Value::Struct, got {value:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "This test requires an OpenAI API key or a configured local LLM and may make network calls."]
    async fn test_null_inputs() {
        let factory = Arc::new(Factory);
        let spec = Spec {
            llm_spec: LlmSpec {
                api_type: crate::llm::LlmApiType::OpenAi,
                model: "gpt-4o".to_string(),
                address: None,
                api_config: None,
            },
            output_type: make_output_type(BasicValueType::Str),
            instruction: None,
        };
        let input_arg_schemas = &[
            (
                Some("text"),
                make_output_type(BasicValueType::Str).with_nullable(true),
            ),
            (
                Some("image"),
                make_output_type(BasicValueType::Bytes).with_nullable(true),
            ),
        ];
        let input_args_values = vec![Value::Null, Value::Null];
        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;
        assert_eq!(result.unwrap(), Value::Null);
    }
}
