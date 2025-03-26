use std::borrow::Cow;

use anyhow::Result;
use async_trait::async_trait;
use schemars::schema::SchemaObject;
use serde::{Deserialize, Serialize};

use crate::base::json_schema::ToJsonSchemaOptions;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LlmApiType {
    Ollama,
    OpenAi,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSpec {
    api_type: LlmApiType,
    address: Option<String>,
    model: String,
}

#[derive(Debug)]
pub enum OutputFormat<'a> {
    JsonSchema {
        name: Cow<'a, str>,
        schema: Cow<'a, SchemaObject>,
    },
}

#[derive(Debug)]
pub struct LlmGenerateRequest<'a> {
    pub system_prompt: Option<Cow<'a, str>>,
    pub user_prompt: Cow<'a, str>,
    pub output_format: Option<OutputFormat<'a>>,
}

#[derive(Debug)]
pub struct LlmGenerateResponse {
    pub text: String,
}

#[async_trait]
pub trait LlmGenerationClient: Send + Sync {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse>;

    /// If true, the LLM only accepts a JSON schema with all fields required.
    /// This is a limitation of LLM models such as OpenAI.
    /// Otherwise, the LLM will accept a JSON schema with optional fields.
    fn json_schema_fields_always_required(&self) -> bool {
        false
    }

    fn to_json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: self.json_schema_fields_always_required(),
        }
    }
}

mod ollama;
mod openai;

pub async fn new_llm_generation_client(spec: LlmSpec) -> Result<Box<dyn LlmGenerationClient>> {
    let client = match spec.api_type {
        LlmApiType::Ollama => {
            Box::new(ollama::Client::new(spec).await?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::OpenAi => {
            Box::new(openai::Client::new(spec).await?) as Box<dyn LlmGenerationClient>
        }
    };
    Ok(client)
}
