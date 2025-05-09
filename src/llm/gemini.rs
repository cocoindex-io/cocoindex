use crate::api_bail;
use crate::llm::{
    LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, LlmSpec, OutputFormat,
    ToJsonSchemaOptions,
};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use urlencoding::encode;

pub struct Client {
    model: String,
    api_key: String,
    client: reqwest::Client,
}

impl Client {
    pub async fn new(spec: LlmSpec) -> Result<Self> {
        let api_key = match std::env::var("GEMINI_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("GEMINI_API_KEY environment variable must be set"),
        };
        Ok(Self {
            model: spec.model,
            api_key,
            client: reqwest::Client::new(),
        })
    }
}

// Recursively remove all `additionalProperties` fields from a JSON value
fn remove_additional_properties(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("additionalProperties");
            for v in map.values_mut() {
                remove_additional_properties(v);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                remove_additional_properties(v);
            }
        }
        _ => {}
    }
}

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse> {
        // Compose the prompt/messages
        let contents = vec![serde_json::json!({
            "role": "user",
            "parts": [{ "text": request.user_prompt }]
        })];

        // Prepare payload
        let mut payload = serde_json::json!({ "contents": contents });
        if let Some(system) = request.system_prompt {
            payload["systemInstruction"] = serde_json::json!({
                "parts": [ { "text": system } ]
            });
        }

        // If structured output is requested, add schema and responseMimeType
        if let Some(OutputFormat::JsonSchema { schema, .. }) = &request.output_format {
            let mut schema_json = serde_json::to_value(schema)?;
            remove_additional_properties(&mut schema_json);
            payload["generationConfig"] = serde_json::json!({
                "responseMimeType": "application/json",
                "responseSchema": schema_json
            });
        }

        let api_key = &self.api_key;
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            encode(&self.model),
            encode(api_key)
        );

        let resp = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("HTTP error")?;

        let resp_json: Value = resp.json().await.context("Invalid JSON")?;

        if let Some(error) = resp_json.get("error") {
            bail!("Gemini API error: {:?}", error);
        }
        let mut resp_json = resp_json;
        let text = match &mut resp_json["candidates"][0]["content"]["parts"][0]["text"] {
            Value::String(s) => std::mem::take(s),
            _ => bail!("No text in response"),
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
