use std::borrow::Cow;
use std::sync::Arc;

use schemars::schema::SchemaObject;
use serde::Serialize;

use crate::base::json_schema::ToJsonSchema;
use crate::llm::{
    new_llm_generation_client, LlmGenerateRequest, LlmGenerationClient, LlmSpec, OutputFormat,
};
use crate::ops::sdk::*;

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
    output_json_schema: SchemaObject,
    output_type: EnrichedValueType,
    system_prompt: String,
}

fn get_system_prompt(instructions: &Option<String>) -> String {
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

    message
}

impl Executor {
    async fn new(spec: Spec, args: Args) -> Result<Self> {
        Ok(Self {
            args,
            client: new_llm_generation_client(spec.llm_spec).await?,
            output_json_schema: spec.output_type.to_json_schema(),
            output_type: spec.output_type,
            system_prompt: get_system_prompt(&spec.instruction),
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
            system_prompt: Some(Cow::Borrowed(&self.system_prompt)),
            user_prompt: Cow::Borrowed(text),
            output_format: Some(OutputFormat::JsonSchema {
                name: Cow::Borrowed("ExtractedData"),
                schema: Cow::Borrowed(&self.output_json_schema),
            }),
        };
        let res = self.client.generate(req).await?;
        let json_value: serde_json::Value = serde_json::from_str(res.text.as_str())?;
        let value = Value::from_json(json_value, &self.output_type.typ)?;
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
