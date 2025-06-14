use super::LlmGenerationClient;
use anyhow::Result;
use async_trait::async_trait;
use crate::llm::prompt_utils::STRICT_JSON_PROMPT;
use schemars::schema::SchemaObject;
use serde::{Deserialize, Serialize};

pub struct Client {
    generate_url: String,
    model: String,
    reqwest_client: reqwest::Client,
}

#[derive(Debug, Serialize)]
enum OllamaFormat<'a> {
    #[serde(untagged)]
    JsonSchema(&'a SchemaObject),
}

#[derive(Debug, Serialize)]
struct OllamaRequest<'a> {
    pub model: &'a str,
    pub prompt: &'a str,
    pub format: Option<OllamaFormat<'a>>,
    pub system: Option<&'a str>,
    pub stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    pub response: String,
}

const OLLAMA_DEFAULT_ADDRESS: &str = "http://localhost:11434";

impl Client {
    pub async fn new(spec: super::LlmSpec) -> Result<Self> {
        let address = match &spec.address {
            Some(addr) => addr.trim_end_matches('/'),
            None => OLLAMA_DEFAULT_ADDRESS,
        };
        Ok(Self {
            generate_url: format!("{}/api/generate", address),
            model: spec.model,
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
        let mut system_prompt = request.system_prompt.unwrap_or_default();
        if matches!(request.output_format, Some(super::OutputFormat::JsonSchema { .. })) {
            system_prompt = format!("{STRICT_JSON_PROMPT}\n\n{system_prompt}").into();
        }
        let req = OllamaRequest {
            model: &self.model,
            prompt: request.user_prompt.as_ref(),
            format: request.output_format.as_ref().map(
                |super::OutputFormat::JsonSchema { schema, .. }| {
                    OllamaFormat::JsonSchema(schema.as_ref())
                },
            ),
            system: Some(&system_prompt),
            stream: Some(false),
        };
        let res = self
            .reqwest_client
            .post(self.generate_url.as_str())
            .json(&req)
            .send()
            .await?;
        let body = res.text().await?;
        let json: OllamaResponse = serde_json::from_str(&body)?;
        // Check if output_format is JsonSchema, try to parse as JSON
        if let Some(super::OutputFormat::JsonSchema { .. }) = request.output_format {
            match serde_json::from_str::<serde_json::Value>(&json.response) {
                Ok(val) => Ok(super::LlmGenerateResponse::Json(val)),
                Err(_) => Ok(super::LlmGenerateResponse::Text(json.response)),
            }
        } else {
            Ok(super::LlmGenerateResponse::Text(json.response))
        }
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
