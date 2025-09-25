use crate::prelude::*;

use crate::base::json_schema::ToJsonSchemaOptions;
use infer::Infer;
use schemars::schema::SchemaObject;
use std::borrow::Cow;

static INFER: LazyLock<Infer> = LazyLock::new(Infer::new);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LlmApiType {
    Ollama,
    OpenAi,
    Gemini,
    Anthropic,
    LiteLlm,
    OpenRouter,
    Voyage,
    Vllm,
    VertexAi,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VertexAiConfig {
    pub project: String,
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenAiConfig {
    pub org_id: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum LlmApiConfig {
    VertexAi(VertexAiConfig),
    OpenAi(OpenAiConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSpec {
    pub api_type: LlmApiType,
    pub address: Option<String>,
    pub model: String,
    pub api_config: Option<LlmApiConfig>,
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
    pub image: Option<Cow<'a, [u8]>>,
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

    fn json_schema_options(&self) -> ToJsonSchemaOptions;
}

#[derive(Debug)]
pub struct LlmEmbeddingRequest<'a> {
    pub model: &'a str,
    pub text: Cow<'a, str>,
    pub output_dimension: Option<u32>,
    pub task_type: Option<Cow<'a, str>>,
}

pub struct LlmEmbeddingResponse {
    pub embedding: Vec<f32>,
}

#[async_trait]
pub trait LlmEmbeddingClient: Send + Sync {
    async fn embed_text<'req>(
        &self,
        request: LlmEmbeddingRequest<'req>,
    ) -> Result<LlmEmbeddingResponse>;

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32>;

    fn behavior_version(&self) -> Option<u32> {
        Some(1)
    }
}

mod anthropic;
mod gemini;
mod litellm;
mod ollama;
mod openai;
mod openrouter;
mod vllm;
mod voyage;

pub async fn new_llm_generation_client(
    api_type: LlmApiType,
    address: Option<String>,
    api_config: Option<LlmApiConfig>,
) -> Result<Box<dyn LlmGenerationClient>> {
    let client = match api_type {
        LlmApiType::Ollama => {
            Box::new(ollama::Client::new(address).await?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::OpenAi => {
            Box::new(openai::Client::new(address, api_config)?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::Gemini => {
            Box::new(gemini::AiStudioClient::new(address)?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::VertexAi => Box::new(gemini::VertexAiClient::new(address, api_config).await?)
            as Box<dyn LlmGenerationClient>,
        LlmApiType::Anthropic => {
            Box::new(anthropic::Client::new(address).await?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::LiteLlm => {
            Box::new(litellm::Client::new_litellm(address).await?) as Box<dyn LlmGenerationClient>
        }
        LlmApiType::OpenRouter => Box::new(openrouter::Client::new_openrouter(address).await?)
            as Box<dyn LlmGenerationClient>,
        LlmApiType::Voyage => {
            api_bail!("Voyage is not supported for generation")
        }
        LlmApiType::Vllm => {
            Box::new(vllm::Client::new_vllm(address).await?) as Box<dyn LlmGenerationClient>
        }
    };
    Ok(client)
}

pub async fn new_llm_embedding_client(
    api_type: LlmApiType,
    address: Option<String>,
    api_config: Option<LlmApiConfig>,
) -> Result<Box<dyn LlmEmbeddingClient>> {
    let client = match api_type {
        LlmApiType::Ollama => {
            Box::new(ollama::Client::new(address).await?) as Box<dyn LlmEmbeddingClient>
        }
        LlmApiType::Gemini => {
            Box::new(gemini::AiStudioClient::new(address)?) as Box<dyn LlmEmbeddingClient>
        }
        LlmApiType::OpenAi => {
            Box::new(openai::Client::new(address, api_config)?) as Box<dyn LlmEmbeddingClient>
        }
        LlmApiType::Voyage => {
            Box::new(voyage::Client::new(address)?) as Box<dyn LlmEmbeddingClient>
        }
        LlmApiType::VertexAi => Box::new(gemini::VertexAiClient::new(address, api_config).await?)
            as Box<dyn LlmEmbeddingClient>,
        LlmApiType::OpenRouter | LlmApiType::LiteLlm | LlmApiType::Vllm | LlmApiType::Anthropic => {
            api_bail!("Embedding is not supported for API type {:?}", api_type)
        }
    };
    Ok(client)
}

pub fn detect_image_mime_type(bytes: &[u8]) -> Result<&'static str> {
    let infer = &*INFER;
    match infer.get(bytes) {
        Some(info) if info.mime_type().starts_with("image/") => Ok(info.mime_type()),
        _ => bail!("Unknown or unsupported image format"),
    }
}
