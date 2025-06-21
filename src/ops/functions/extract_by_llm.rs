use crate::prelude::*;

use crate::llm::{
    LlmGenerateRequest, LlmGenerationClient, LlmSpec, OutputFormat, new_llm_generation_client,
};
use crate::ops::sdk::*;
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
    text: ResolvedOpArg,
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
        "You are a helpful assistant that extracts structured information from text. \
Your task is to analyze the input text and output valid JSON that matches the specified schema. \
Be precise and only include information that is explicitly stated in the text. \
Output only the JSON without any additional messages or explanations."
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
        let client =
            new_llm_generation_client(spec.llm_spec.api_type, spec.llm_spec.address).await?;
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
        let text = self.args.text.value(&input)?.as_str()?;
        let req = LlmGenerateRequest {
            model: &self.model,
            system_prompt: Some(Cow::Borrowed(&self.system_prompt)),
            user_prompt: Cow::Borrowed(text),
            output_format: Some(OutputFormat::JsonSchema {
                name: Cow::Borrowed("ExtractedData"),
                schema: Cow::Borrowed(&self.output_json_schema),
            }),
        };
        let res = self.client.generate(req).await?;
        let json_value: serde_json::Value = serde_json::from_str(res.text.as_str())?;
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

    fn resolve_schema(
        &self,
        spec: &Spec,
        args_resolver: &mut OpArgsResolver<'_>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        Ok((
            Args {
                text: args_resolver
                    .next_arg("text")?
                    .expect_type(&ValueType::Basic(BasicValueType::Str))?,
            },
            spec.output_type.clone(),
        ))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        resolved_input_schema: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor::new(spec, resolved_input_schema).await?))
    }
}
