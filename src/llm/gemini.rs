use crate::prelude::*;

use crate::llm::{
    LlmEmbeddingClient, LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, OutputFormat,
    ToJsonSchemaOptions, detect_image_mime_type,
};
use base64::prelude::*;
use google_cloud_aiplatform_v1 as vertexai;
use serde_json::Value;
use urlencoding::encode;

fn get_embedding_dimension(model: &str) -> Option<u32> {
    let model = model.to_ascii_lowercase();
    if model.starts_with("gemini-embedding-") {
        Some(3072)
    } else if model.starts_with("text-embedding-") {
        Some(768)
    } else if model.starts_with("embedding-") {
        Some(768)
    } else if model.starts_with("text-multilingual-embedding-") {
        Some(768)
    } else {
        None
    }
}

pub struct AiStudioClient {
    api_key: String,
    client: reqwest::Client,
}

impl AiStudioClient {
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

impl AiStudioClient {
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
impl LlmGenerationClient for AiStudioClient {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse> {
        let mut user_parts: Vec<serde_json::Value> = Vec::new();

        // Add text part first
        user_parts.push(serde_json::json!({ "text": request.user_prompt }));

        // Add image part if present
        if let Some(image_bytes) = &request.image {
            let base64_image = BASE64_STANDARD.encode(image_bytes.as_ref());
            let mime_type = detect_image_mime_type(image_bytes.as_ref())?;
            user_parts.push(serde_json::json!({
                "inlineData": {
                    "mimeType": mime_type,
                    "data": base64_image
                }
            }));
        }

        // Compose the contents
        let contents = vec![serde_json::json!({
            "role": "user",
            "parts": user_parts
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
        let resp = retryable::run(
            || self.client.post(&url).json(&payload).send(),
            &retryable::HEAVY_LOADED_OPTIONS,
        )
        .await?;
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
impl LlmEmbeddingClient for AiStudioClient {
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
        if let Some(output_dimension) = request.output_dimension {
            payload["outputDimensionality"] = serde_json::Value::Number(output_dimension.into());
        }
        let resp = retryable::run(
            || async {
                self.client
                    .post(&url)
                    .json(&payload)
                    .send()
                    .await?
                    .error_for_status()
            },
            &retryable::HEAVY_LOADED_OPTIONS,
        )
        .await
        .context("Gemini API error")?;
        let embedding_resp: EmbedContentResponse = resp.json().await.context("Invalid JSON")?;
        Ok(super::LlmEmbeddingResponse {
            embedding: embedding_resp.embedding.values,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        get_embedding_dimension(model)
    }

    fn behavior_version(&self) -> Option<u32> {
        Some(2)
    }
}

pub struct VertexAiClient {
    client: vertexai::client::PredictionService,
    config: super::VertexAiConfig,
}

impl VertexAiClient {
    pub async fn new(
        address: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> Result<Self> {
        if address.is_some() {
            api_bail!("VertexAi API address is not supported for VertexAi API type");
        }
        let Some(super::LlmApiConfig::VertexAi(config)) = api_config else {
            api_bail!("VertexAi API config is required for VertexAi API type");
        };
        let client = vertexai::client::PredictionService::builder()
            .build()
            .await?;
        Ok(Self { client, config })
    }

    fn get_model_path(&self, model: &str) -> String {
        format!(
            "projects/{}/locations/{}/publishers/google/models/{}",
            self.config.project,
            self.config.region.as_deref().unwrap_or("global"),
            model
        )
    }
}

#[async_trait]
impl LlmGenerationClient for VertexAiClient {
    async fn generate<'req>(
        &self,
        request: super::LlmGenerateRequest<'req>,
    ) -> Result<super::LlmGenerateResponse> {
        use vertexai::model::{Blob, Content, GenerationConfig, Part, Schema, part::Data};

        // Compose parts
        let mut parts = Vec::new();
        // Add text part
        parts.push(Part::new().set_text(request.user_prompt.to_string()));
        // Add image part if present
        if let Some(image_bytes) = request.image {
            let mime_type = detect_image_mime_type(image_bytes.as_ref())?;
            parts.push(
                Part::new().set_inline_data(
                    Blob::new()
                        .set_data(image_bytes.into_owned())
                        .set_mime_type(mime_type.to_string()),
                ),
            );
        }
        // Compose content
        let mut contents = Vec::new();
        contents.push(Content::new().set_role("user".to_string()).set_parts(parts));
        // Compose system instruction if present
        let system_instruction = request.system_prompt.as_ref().map(|sys| {
            Content::new()
                .set_role("system".to_string())
                .set_parts(vec![Part::new().set_text(sys.to_string())])
        });

        // Compose generation config
        let mut generation_config = None;
        if let Some(OutputFormat::JsonSchema { schema, .. }) = &request.output_format {
            let schema_json = serde_json::to_value(schema)?;
            generation_config = Some(
                GenerationConfig::new()
                    .set_response_mime_type("application/json".to_string())
                    .set_response_schema(utils::deser::from_json_value::<Schema>(schema_json)?),
            );
        }

        let mut req = self
            .client
            .generate_content()
            .set_model(self.get_model_path(request.model))
            .set_contents(contents);
        if let Some(sys) = system_instruction {
            req = req.set_system_instruction(sys);
        }
        if let Some(config) = generation_config {
            req = req.set_generation_config(config);
        }

        // Call the API
        let resp = req.send().await?;
        // Extract text from response
        let Some(Data::Text(text)) = resp
            .candidates
            .into_iter()
            .next()
            .and_then(|c| c.content)
            .and_then(|content| content.parts.into_iter().next())
            .and_then(|part| part.data)
        else {
            bail!("No text in response");
        };
        Ok(super::LlmGenerateResponse { text })
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

#[async_trait]
impl LlmEmbeddingClient for VertexAiClient {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        // Create the instances for the request
        let mut instance = serde_json::json!({
            "content": request.text
        });
        // Add task type if specified
        if let Some(task_type) = &request.task_type {
            instance["task_type"] = serde_json::Value::String(task_type.to_string());
        }

        let instances = vec![instance];

        // Prepare the request parameters
        let mut parameters = serde_json::json!({});
        if let Some(output_dimension) = request.output_dimension {
            parameters["outputDimensionality"] = serde_json::Value::Number(output_dimension.into());
        }

        // Build the prediction request using the raw predict builder
        let response = self
            .client
            .predict()
            .set_endpoint(self.get_model_path(request.model))
            .set_instances(instances)
            .set_parameters(parameters)
            .send()
            .await?;

        // Extract the embedding from the response
        let embeddings = response
            .predictions
            .into_iter()
            .next()
            .and_then(|mut e| e.get_mut("embeddings").map(|v| v.take()))
            .ok_or_else(|| anyhow::anyhow!("No embeddings in response"))?;
        let embedding: ContentEmbedding = utils::deser::from_json_value(embeddings)?;
        Ok(super::LlmEmbeddingResponse {
            embedding: embedding.values,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        get_embedding_dimension(model)
    }
}
