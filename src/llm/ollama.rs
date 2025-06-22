use crate::prelude::*;

use super::LlmGenerationClient;
use schemars::schema::SchemaObject;

pub struct Client {
    generate_url: String,
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
    pub async fn new(address: Option<String>) -> Result<Self> {
        let address = match &address {
            Some(addr) => addr.trim_end_matches('/'),
            None => OLLAMA_DEFAULT_ADDRESS,
        };
        Ok(Self {
            generate_url: format!("{}/api/generate", address),
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
            format: request.output_format.as_ref().map(
                |super::OutputFormat::JsonSchema { schema, .. }| {
                    OllamaFormat::JsonSchema(schema.as_ref())
                },
            ),
            system: request.system_prompt.as_ref().map(|s| s.as_ref()),
            stream: Some(false),
        };
        let res = self
            .reqwest_client
            .post(self.generate_url.as_str())
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            bail!(
                "Ollama API error: {:?}\n{}\n",
                res.status(),
                res.text().await?
            );
        }
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
