use crate::prelude::*;
use base64::prelude::*;

use crate::llm::{
    LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, OutputFormat,
    ToJsonSchemaOptions, detect_image_mime_type,
};
use anyhow::Context;
use urlencoding::encode;

pub struct Client {
    api_key: String,
    client: reqwest::Client,
}

impl Client {
    pub async fn new(address: Option<String>) -> Result<Self> {
        if address.is_some() {
            api_bail!("Anthropic doesn't support custom API address");
        }
        let api_key = match std::env::var("ANTHROPIC_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("ANTHROPIC_API_KEY environment variable must be set"),
        };
        Ok(Self {
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
        let mut user_content_parts: Vec<serde_json::Value> = Vec::new();

        // Add image part if present
        if let Some(image_bytes) = &request.image {
            let base64_image = BASE64_STANDARD.encode(image_bytes.as_ref());
            let mime_type = detect_image_mime_type(image_bytes.as_ref())?;
            user_content_parts.push(serde_json::json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": mime_type,
                    "data": base64_image,
                }
            }));
        }

        // Add text part
        user_content_parts.push(serde_json::json!({
            "type": "text",
            "text": request.user_prompt
        }));

        let messages = vec![serde_json::json!({
            "role": "user",
            "content": user_content_parts
        })];

        let mut payload = serde_json::json!({
            "model": request.model,
            "messages": messages,
            "max_tokens": 4096
        });

        // Add system prompt as top-level field if present (required)
        if let Some(system) = request.system_prompt {
            payload["system"] = serde_json::json!(system);
        }

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

        let resp = retryable::run(
            || async {
                self.client
                    .post(url)
                    .header("x-api-key", encoded_api_key.as_ref())
                    .header("anthropic-version", "2023-06-01")
                    .json(&payload)
                    .send()
                    .await?
                    .error_for_status()
            },
            &retryable::HEAVY_LOADED_OPTIONS,
        )
        .await
        .context("Anthropic API error")?;

        let mut resp_json: serde_json::Value = resp.json().await.context("Invalid JSON")?;
        if let Some(error) = resp_json.get("error") {
            bail!("Anthropic API error: {:?}", error);
        }

        // Debug print full response
        // println!("Anthropic API full response: {resp_json:?}");

        let resp_content = &resp_json["content"];
        let tool_name = "report_result";
        let mut extracted_json: Option<serde_json::Value> = None;
        if let Some(array) = resp_content.as_array() {
            for item in array {
                if item.get("type") == Some(&serde_json::Value::String("tool_use".to_string()))
                    && item.get("name") == Some(&serde_json::Value::String(tool_name.to_string()))
                {
                    if let Some(input) = item.get("input") {
                        extracted_json = Some(input.clone());
                        break;
                    }
                }
            }
        }
        let text = if let Some(json) = extracted_json {
            // Try strict JSON serialization first
            serde_json::to_string(&json)?
        } else {
            // Fallback: try text if no tool output found
            match &mut resp_json["content"][0]["text"] {
                serde_json::Value::String(s) => {
                    // Try strict JSON parsing first
                    match utils::deser::from_json_str::<serde_json::Value>(s) {
                        Ok(_) => std::mem::take(s),
                        Err(e) => {
                            // Try permissive json5 parsing as fallback
                            match json5::from_str::<serde_json::Value>(s) {
                                Ok(value) => {
                                    println!("[Anthropic] Used permissive JSON5 parser for output");
                                    serde_json::to_string(&value)?
                                }
                                Err(e2) => {
                                    return Err(anyhow::anyhow!(format!(
                                        "No structured tool output or text found in response, and permissive JSON5 parsing also failed: {e}; {e2}"
                                    )));
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "No structured tool output or text found in response"
                    ));
                }
            }
        };

        Ok(LlmGenerateResponse { text })
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
