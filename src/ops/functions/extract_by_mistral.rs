use std::sync::Arc;

use anyhow::anyhow;
use mistralrs::{self, TextMessageRole};
use serde::Serialize;

use crate::base::json_schema::ToJsonSchema;
use crate::ops::sdk::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MistralModelSpec {
    model_id: String,
    isq_type: mistralrs::IsqType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    model: MistralModelSpec,
    output_type: EnrichedValueType,
    instructions: Option<String>,
}

struct Executor {
    model: mistralrs::Model,
    output_type: EnrichedValueType,
    request_base: mistralrs::RequestBuilder,
}

fn get_system_message(instructions: &Option<String>) -> String {
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
    async fn new(spec: Spec) -> Result<Self> {
        let model = mistralrs::TextModelBuilder::new(spec.model.model_id)
            .with_isq(spec.model.isq_type)
            .with_paged_attn(|| mistralrs::PagedAttentionMetaBuilder::default().build())?
            .build()
            .await?;
        let request_base = mistralrs::RequestBuilder::new()
            .set_constraint(mistralrs::Constraint::JsonSchema(serde_json::to_value(
                spec.output_type.to_json_schema(),
            )?))
            .set_deterministic_sampler()
            .add_message(
                TextMessageRole::System,
                get_system_message(&spec.instructions),
            );
        Ok(Self {
            model,
            output_type: spec.output_type,
            request_base,
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
        let text = input.iter().next().unwrap().as_str()?;
        let request = self
            .request_base
            .clone()
            .add_message(TextMessageRole::User, text);
        let response = self.model.send_chat_request(request).await?;
        let response_text = response.choices[0]
            .message
            .content
            .as_ref()
            .ok_or_else(|| anyhow!("No content in response"))?;
        let json_value: serde_json::Value = serde_json::from_str(response_text)?;
        let value = Value::from_json(json_value, &self.output_type.typ)?;
        Ok(value)
    }
}

pub struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "ExtractByMistral"
    }

    fn get_output_schema(
        &self,
        spec: &Spec,
        input_schema: &Vec<OpArgSchema>,
        _context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType> {
        match &expect_input_1(input_schema)?.value_type.typ {
            ValueType::Basic(BasicValueType::Str) => {}
            t => {
                api_bail!("Expect String as input type, got {}", t)
            }
        }
        Ok(spec.output_type.clone())
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _input_schema: Vec<OpArgSchema>,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor::new(spec).await?))
    }
}
