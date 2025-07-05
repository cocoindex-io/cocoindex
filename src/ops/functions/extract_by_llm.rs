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
        let image_bytes: Option<Cow<'_, [u8]>> = self
            .args
            .image
            .as_ref()
            .map(|arg| arg.value(&input)?.as_bytes())
            .transpose()?
            .map(|bytes| Cow::Borrowed(bytes.as_ref()));
        let text = self
            .args
            .text
            .as_ref()
            .map(|arg| arg.value(&input)?.as_str())
            .transpose()?;

        if text.is_none() && image_bytes.is_none() {
            api_bail!("At least one of `text` or `image` must be provided");
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

    async fn resolve_schema<'a>(
        &'a self,
        spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        let args = Args {
            text: args_resolver
                .next_optional_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?,
            image: args_resolver
                .next_optional_arg("image")?
                .expect_type(&ValueType::Basic(BasicValueType::Bytes))?,
        };

        if args.text.is_none() && args.image.is_none() {
            api_bail!("At least one of 'text' or 'image' must be provided");
        }

        Ok((args, spec.output_type.clone()))
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
