use crate::prelude::*;
use base64::prelude::*;

use crate::llm::{
    GeneratedOutput, LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, OutputFormat,
    ToJsonSchemaOptions, detect_image_mime_type,
};
use urlencoding::encode;

pub struct Client {
    api_key: String,
    region: String,
    client: reqwest::Client,
}

impl Client {
    pub async fn new(address: Option<String>) -> Result<Self> {
        if address.is_some() {
            api_bail!("Bedrock doesn't support custom API address");
        }

        let api_key = match std::env::var("BEDROCK_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("BEDROCK_API_KEY environment variable must be set"),
        };

        // Default to us-east-1 if no region specified
        let region = std::env::var("BEDROCK_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        Ok(Self {
            api_key,
            region,
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
                "image": {
                    "format": mime_type.split('/').nth(1).unwrap_or("png"),
                    "source": {
                        "bytes": base64_image,
                    }
                }
            }));
        }

        // Add text part
        user_content_parts.push(serde_json::json!({
            "text": request.user_prompt
        }));

        let messages = vec![serde_json::json!({
            "role": "user",
            "content": user_content_parts
        })];

        let mut payload = serde_json::json!({
            "messages": messages,
            "inferenceConfig": {
                "maxTokens": 4096
            }
        });

        // Add system prompt if present
        if let Some(system) = request.system_prompt {
            payload["system"] = serde_json::json!([{
                "text": system
            }]);
        }

        // Handle structured output using tool schema
        let has_json_schema = request.output_format.is_some();
        if let Some(OutputFormat::JsonSchema { schema, name }) = request.output_format.as_ref() {
            let schema_json = serde_json::to_value(schema)?;
            payload["toolConfig"] = serde_json::json!({
                "tools": [{
                    "toolSpec": {
                        "name": name,
                        "description": format!("Extract structured data according to the schema"),
                        "inputSchema": {
                            "json": schema_json
                        }
                    }
                }]
            });
        }

        // Construct the Bedrock Runtime API URL
        let url = format!(
            "https://bedrock-runtime.{}.amazonaws.com/model/{}/converse",
            self.region, request.model
        );

        let encoded_api_key = encode(&self.api_key);

        let resp = http::request(|| {
            self.client
                .post(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", encoded_api_key.as_ref()),
                )
                .header("Content-Type", "application/json")
                .json(&payload)
        })
        .await
        .with_context(|| "Bedrock API error")?;

        let resp_json: serde_json::Value = resp.json().await.with_context(|| "Invalid JSON")?;

        // Check for errors in the response
        if let Some(error) = resp_json.get("error") {
            client_bail!("Bedrock API error: {:?}", error);
        }

        // Debug print full response (uncomment for debugging)
        // println!("Bedrock API full response: {resp_json:?}");

        // Extract the response content
        let output = &resp_json["output"];
        let message = &output["message"];
        let content = &message["content"];

        let generated_output = if let Some(content_array) = content.as_array() {
            // Look for tool use first (structured output)
            let mut extracted_json: Option<serde_json::Value> = None;
            for item in content_array {
                if let Some(tool_use) = item.get("toolUse") {
                    if let Some(input) = tool_use.get("input") {
                        extracted_json = Some(input.clone());
                        break;
                    }
                }
            }

            if let Some(json) = extracted_json {
                // Return the structured output as JSON
                GeneratedOutput::Json(json)
            } else if has_json_schema {
                // If JSON schema was requested but no tool output found, try parsing text as JSON
                let mut text_parts = Vec::new();
                for item in content_array {
                    if let Some(text) = item.get("text") {
                        if let Some(text_str) = text.as_str() {
                            text_parts.push(text_str);
                        }
                    }
                }
                let text = text_parts.join("");
                GeneratedOutput::Json(serde_json::from_str(&text)?)
            } else {
                // Fall back to text content
                let mut text_parts = Vec::new();
                for item in content_array {
                    if let Some(text) = item.get("text") {
                        if let Some(text_str) = text.as_str() {
                            text_parts.push(text_str);
                        }
                    }
                }
                GeneratedOutput::Text(text_parts.join(""))
            }
        } else {
            return Err(client_error!("No content found in Bedrock response"));
        };

        Ok(LlmGenerateResponse {
            output: generated_output,
        })
    }

    fn json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: false,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
            supports_additional_properties: true,
        }
    }
}
