use crate::prelude::*;

use crate::llm::{
    LlmEmbeddingClient, LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, OutputFormat,
    ToJsonSchemaOptions,
};
use phf::phf_map;
use serde_json::Value;
use urlencoding::encode;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    "gemini-embedding-exp-03-07" => 3072,
    "text-embedding-004" => 768,
    "embedding-001" => 768,
};

pub struct Client {
    api_key: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(address: Option<String>) -> Result<Self> {
        if address.is_some() {
            api_bail!("Gemini doesn't support custom API address");
        }
        let api_key = match std::env::var("GEMINI_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("GEMINI_API_KEY environment variable must be set"),
        };
        Ok(Self {
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

impl Client {
    fn get_api_url(&self, model: &str, api_name: &str) -> String {
        format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:{}?key={}",
            encode(model),
            api_name,
            encode(&self.api_key)
        )
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

        let url = self.get_api_url(request.model, "generateContent");
        let resp = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("HTTP error")?;
        if !resp.status().is_success() {
            bail!(
                "Gemini API error: {:?}\n{}\n",
                resp.status(),
                resp.text().await?
            );
        }
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

#[derive(Deserialize)]
struct ContentEmbedding {
    values: Vec<f32>,
}
#[derive(Deserialize)]
struct EmbedContentResponse {
    embedding: ContentEmbedding,
}

#[async_trait]
impl LlmEmbeddingClient for Client {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let url = self.get_api_url(request.model, "embedContent");
        let mut payload = serde_json::json!({
            "model": request.model,
            "content": { "parts": [{ "text": request.text }] },
        });
        if let Some(task_type) = request.task_type {
            payload["taskType"] = serde_json::Value::String(task_type.into());
        }
        let resp = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("HTTP error")?;
        if !resp.status().is_success() {
            bail!(
                "Gemini API error: {:?}\n{}\n",
                resp.status(),
                resp.text().await?
            );
        }
        let embedding_resp: EmbedContentResponse = resp.json().await.context("Invalid JSON")?;
        Ok(super::LlmEmbeddingResponse {
            embedding: embedding_resp.embedding.values,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
