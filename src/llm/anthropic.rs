use async_trait::async_trait;
use crate::llm::{LlmGenerationClient, LlmSpec, LlmGenerateRequest, LlmGenerateResponse, ToJsonSchemaOptions, OutputFormat};
use anyhow::{Result, bail, Context};
use crate::llm::prompt_utils::STRICT_JSON_PROMPT;
use serde_json::Value;

use crate::api_bail;
use urlencoding::encode;

pub struct Client {
    model: String,
    api_key: String,
    client: reqwest::Client,
}

impl Client {
    pub async fn new(spec: LlmSpec) -> Result<Self> {
        let api_key = match std::env::var("ANTHROPIC_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("ANTHROPIC_API_KEY environment variable must be set"),
        };
        Ok(Self {
            model: spec.model,
            api_key,
            client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse> {
        let messages = vec![serde_json::json!({
            "role": "user",
            "content": request.user_prompt
        })];

        let mut payload = serde_json::json!({
            "model": self.model,
            "messages": messages,
            "max_tokens": 4096
        });

        // Add system prompt as top-level field if present (required)
        let mut system_prompt = request.system_prompt.unwrap_or_default();
        if matches!(request.output_format, Some(OutputFormat::JsonSchema { .. })) {
            system_prompt = format!("{STRICT_JSON_PROMPT}\n\n{system_prompt}").into();
        }
        payload["system"] = serde_json::json!(system_prompt);

        // Extract schema from output_format, error if not JsonSchema
        let schema = match request.output_format.as_ref() {
            Some(OutputFormat::JsonSchema { schema, .. }) => schema,
            _ => api_bail!("Anthropic client expects OutputFormat::JsonSchema for all requests"),
        };

        let schema_json = serde_json::to_value(schema)?;
        payload["tools"] = serde_json::json!([
            { "type": "custom", "name": "report_result", "input_schema": schema_json }
        ]);

        let url = "https://api.anthropic.com/v1/messages";

        let encoded_api_key = encode(&self.api_key);

        let resp = self.client
            .post(url)
            .header("x-api-key", encoded_api_key.as_ref())
            .header("anthropic-version", "2023-06-01")
            .json(&payload)
            .send()
            .await
            .context("HTTP error")?;
        let resp_json: Value = resp.json().await.context("Invalid JSON")?;
        if let Some(error) = resp_json.get("error") {
            bail!("Anthropic API error: {:?}", error);
        }

        // Extract the text response
        let text = match resp_json["content"][0]["text"].as_str() {
            Some(s) => s.to_string(),
            None => bail!("No text in response"),
        };

        // Try to parse as JSON
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(val) => Ok(LlmGenerateResponse::Json(val)),
            Err(_) => Ok(LlmGenerateResponse::Text(text)),
        }
    }

    fn json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: false,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
        }
    }
}
