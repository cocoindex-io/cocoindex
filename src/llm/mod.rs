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
    Gemini,
    Anthropic,
    LiteLlm,
    OpenRouter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSpec {
    pub api_type: LlmApiType,
    pub address: Option<String>,
    pub model: String,
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
    pub model: &'a str,
    pub system_prompt: Option<Cow<'a, str>>,
    pub user_prompt: Cow<'a, str>,
    pub output_format: Option<OutputFormat<'a>>,
}

#[derive(Debug)]
pub struct LlmGenerateResponse {
    pub text: String,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse>;

    fn json_schema_options(&self) -> ToJsonSchemaOptions;
}

mod anthropic;
mod gemini;
mod litellm;
mod ollama;
mod openai;
mod openrouter;

pub async fn new_llm_generation_client(
    api_type: LlmApiType,
    address: Option<String>,
) -> Result<Box<dyn LlmClient>> {
    let client = match api_type {
        LlmApiType::Ollama => Box::new(ollama::Client::new(address).await?) as Box<dyn LlmClient>,
        LlmApiType::OpenAi => Box::new(openai::Client::new(address).await?) as Box<dyn LlmClient>,
        LlmApiType::Gemini => Box::new(gemini::Client::new(address).await?) as Box<dyn LlmClient>,
        LlmApiType::Anthropic => {
            Box::new(anthropic::Client::new(address).await?) as Box<dyn LlmClient>
        }
        LlmApiType::LiteLlm => {
            Box::new(litellm::Client::new_litellm(address).await?) as Box<dyn LlmClient>
        }
        LlmApiType::OpenRouter => {
            Box::new(openrouter::Client::new_openrouter(address).await?) as Box<dyn LlmClient>
        }
    };
    Ok(client)
}
