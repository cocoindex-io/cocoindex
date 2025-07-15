use crate::prelude::*;

use super::LlmGenerationClient;
use schemars::schema::SchemaObject;
use serde_with::{base64::Base64, serde_as};

pub struct Client {
    generate_url: String,
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

const OLLAMA_DEFAULT_ADDRESS: &str = "http://localhost:11434";

impl Client {
    pub async fn new(address: Option<String>) -> Result<Self> {
        let address = match &address {
            Some(addr) => addr.trim_end_matches('/'),
            None => OLLAMA_DEFAULT_ADDRESS,
        };
        Ok(Self {
            generate_url: format!("{address}/api/generate"),
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
            || {
                self.reqwest_client
                    .post(self.generate_url.as_str())
                    .json(&req)
                    .send()
            },
            &retryable::HEAVY_LOADED_OPTIONS,
        )
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
