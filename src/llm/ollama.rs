use crate::prelude::*;

use super::{LlmEmbeddingClient, LlmGenerationClient};
use schemars::schema::SchemaObject;
use serde_with::{base64::Base64, serde_as};

fn get_embedding_dimension(model: &str) -> Option<u32> {
    match model.to_ascii_lowercase().as_str() {
        "mxbai-embed-large"
        | "bge-m3"
        | "bge-large"
        | "snowflake-arctic-embed"
        | "snowflake-arctic-embed2" => Some(1024),

        "nomic-embed-text"
        | "paraphrase-multilingual"
        | "snowflake-arctic-embed:110m"
        | "snowflake-arctic-embed:137m"
        | "granite-embedding:278m" => Some(768),

        "all-minilm"
        | "snowflake-arctic-embed:22m"
        | "snowflake-arctic-embed:33m"
        | "granite-embedding" => Some(384),

        _ => None,
    }
}

pub struct Client {
    generate_url: String,
    embed_url: String,
    reqwest_client: reqwest::Client,
}

#[derive(Debug, Serialize)]
enum OllamaFormat<'a> {
    #[serde(untagged)]
    JsonSchema(&'a SchemaObject),
}

#[serde_as]
#[derive(Debug, Serialize)]
struct OllamaRequest<'a> {
    pub model: &'a str,
    pub prompt: &'a str,
    #[serde_as(as = "Option<Vec<Base64>>")]
    pub images: Option<Vec<&'a [u8]>>,
    pub format: Option<OllamaFormat<'a>>,
    pub system: Option<&'a str>,
    pub stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    pub response: String,
}

#[derive(Debug, Serialize)]
struct OllamaEmbeddingRequest<'a> {
    pub model: &'a str,
    pub input: &'a str,
}

#[derive(Debug, Deserialize)]
struct OllamaEmbeddingResponse {
    pub embedding: Vec<f32>,
}

const OLLAMA_DEFAULT_ADDRESS: &str = "http://localhost:11434";

impl Client {
    pub async fn new(address: Option<String>) -> Result<Self> {
        let address = match &address {
            Some(addr) => addr.trim_end_matches('/'),
            None => OLLAMA_DEFAULT_ADDRESS,
        };
        Ok(Self {
            generate_url: format!("{address}/api/generate"),
            embed_url: format!("{address}/api/embed"),
            reqwest_client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: super::LlmGenerateRequest<'req>,
    ) -> Result<super::LlmGenerateResponse> {
        let req = OllamaRequest {
            model: request.model,
            prompt: request.user_prompt.as_ref(),
            images: request.image.as_deref().map(|img| vec![img]),
            format: request.output_format.as_ref().map(
                |super::OutputFormat::JsonSchema { schema, .. }| {
                    OllamaFormat::JsonSchema(schema.as_ref())
                },
            ),
            system: request.system_prompt.as_ref().map(|s| s.as_ref()),
            stream: Some(false),
        };
        let res = retryable::run(
            || async {
                self.reqwest_client
                    .post(self.generate_url.as_str())
                    .json(&req)
                    .send()
                    .await?
                    .error_for_status()
            },
            &retryable::HEAVY_LOADED_OPTIONS,
        )
        .await
        .context("Ollama API error")?;
        let json: OllamaResponse = res.json().await?;
        Ok(super::LlmGenerateResponse {
            text: json.response,
        })
    }

    fn json_schema_options(&self) -> super::ToJsonSchemaOptions {
        super::ToJsonSchemaOptions {
            fields_always_required: false,
            supports_format: true,
            extract_descriptions: true,
            top_level_must_be_object: false,
        }
    }
}

#[async_trait]
impl LlmEmbeddingClient for Client {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let req = OllamaEmbeddingRequest {
            model: request.model,
            input: request.text.as_ref(),
        };
        let resp = retryable::run(
            || async {
                self.reqwest_client
                    .post(self.embed_url.as_str())
                    .json(&req)
                    .send()
                    .await?
                    .error_for_status()
            },
            &retryable::HEAVY_LOADED_OPTIONS,
        )
        .await
        .context("Ollama API error")?;
        let embedding_resp: OllamaEmbeddingResponse = resp.json().await.context("Invalid JSON")?;
        Ok(super::LlmEmbeddingResponse {
            embedding: embedding_resp.embedding,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        get_embedding_dimension(model)
    }
}
