use anyhow::Result;
use schemars::schema::SchemaObject;
use serde::{Deserialize, Serialize};

pub struct LlmClient {
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

impl LlmClient {
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

    pub async fn generate<'a>(
        &self,
        request: super::LlmGenerateRequest<'a>,
    ) -> Result<super::LlmGenerateResponse> {
        let req = OllamaRequest {
            model: &self.model,
            prompt: request.user_prompt.as_ref(),
            format: match &request.output_format {
                Some(super::OutputFormat::JsonSchema(schema)) => {
                    Some(OllamaFormat::JsonSchema(schema.as_ref()))
                }
                None => None,
            },
            system: request.system_prompt.as_ref().map(|s| s.as_ref()),
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
        Ok(super::LlmGenerateResponse {
            text: json.response,
        })
    }
}
