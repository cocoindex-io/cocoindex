use crate::prelude::*;

use crate::llm::{
    GeneratedOutput, LlmEmbeddingClient, LlmGenerateRequest, LlmGenerateResponse,
    LlmGenerationClient, OutputFormat, ToJsonSchemaOptions, detect_image_mime_type,
};
use base64::prelude::*;
use google_cloud_aiplatform_v1 as vertexai;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_gax::retry_throttler::{AdaptiveThrottler, SharedRetryThrottler};
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
    pub fn new(address: Option<String>, api_key: Option<String>) -> Result<Self> {
        if address.is_some() {
            api_bail!("Gemini doesn't support custom API address");
        }

        let api_key = if let Some(key) = api_key {
            key
        } else {
            std::env::var("GEMINI_API_KEY")
                .map_err(|_| client_error!("GEMINI_API_KEY environment variable must be set"))?
        };

        Ok(Self {
            api_key,
            client: reqwest::Client::new(),
        })
    }
}

impl AiStudioClient {
    fn get_api_url(&self, model: &str, api_name: &str) -> String {
        format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:{}",
            encode(model),
            api_name
        )
    }
}

fn build_embed_payload(
    model: &str,
    texts: &[&str],
    task_type: Option<&str>,
    output_dimension: Option<u32>,
) -> serde_json::Value {
    let requests: Vec<_> = texts
        .iter()
        .map(|text| {
            let mut req = serde_json::json!({
                "model": format!("models/{}", model),
                "content": { "parts": [{ "text": text }] },
            });
            if let Some(task_type) = task_type {
                req["taskType"] = serde_json::Value::String(task_type.to_string());
            }
            if let Some(output_dimension) = output_dimension {
                req["outputDimensionality"] = serde_json::json!(output_dimension);
                if model.starts_with("gemini-embedding-") {
                    req["config"] = serde_json::json!({
                        "outputDimensionality": output_dimension,
                    });
                }
            }
            req
        })
        .collect();

    serde_json::json!({
        "requests": requests,
    })
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
        let has_json_schema = request.output_format.is_some();
        if let Some(OutputFormat::JsonSchema { schema, .. }) = &request.output_format {
            let schema_json = serde_json::to_value(schema)?;
            payload["generationConfig"] = serde_json::json!({
                "responseMimeType": "application/json",
                "responseSchema": schema_json
            });
        }

        let url = self.get_api_url(request.model, "generateContent");
        let resp = http::request(|| {
            self.client
                .post(&url)
                .header("x-goog-api-key", &self.api_key)
                .json(&payload)
        })
        .await
        .map_err(Error::from)
        .with_context(|| "Gemini API error")?;
        let resp_json: Value = resp.json().await.with_context(|| "Invalid JSON")?;

        if let Some(error) = resp_json.get("error") {
            client_bail!("Gemini API error: {:?}", error);
        }
        let mut resp_json = resp_json;
        let text = match &mut resp_json["candidates"][0]["content"]["parts"][0]["text"] {
            Value::String(s) => std::mem::take(s),
            _ => client_bail!("No text in response"),
        };

        let output = if has_json_schema {
            GeneratedOutput::Json(serde_json::from_str(&text)?)
        } else {
            GeneratedOutput::Text(text)
        };

        Ok(LlmGenerateResponse { output })
    }

    fn json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: false,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
            supports_additional_properties: false,
        }
    }
}

#[derive(Deserialize)]
struct ContentEmbedding {
    values: Vec<f32>,
}
#[derive(Deserialize)]
struct BatchEmbedContentResponse {
    embeddings: Vec<ContentEmbedding>,
}

#[async_trait]
impl LlmEmbeddingClient for AiStudioClient {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let url = self.get_api_url(request.model, "batchEmbedContents");
        let texts: Vec<&str> = request.texts.iter().map(|t| t.as_ref()).collect();
        let payload = build_embed_payload(
            request.model,
            &texts,
            request.task_type.as_deref(),
            request.output_dimension,
        );
        let resp = http::request(|| {
            self.client
                .post(&url)
                .header("x-goog-api-key", &self.api_key)
                .json(&payload)
        })
        .await
        .map_err(Error::from)
        .with_context(|| "Gemini API error")?;
        let embedding_resp: BatchEmbedContentResponse =
            resp.json().await.with_context(|| "Invalid JSON")?;
        Ok(super::LlmEmbeddingResponse {
            embeddings: embedding_resp
                .embeddings
                .into_iter()
                .map(|e| e.values)
                .collect(),
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

#[derive(Debug)]
struct CustomizedGoogleCloudRetryPolicy;

impl google_cloud_gax::retry_policy::RetryPolicy for CustomizedGoogleCloudRetryPolicy {
    fn on_error(
        &self,
        state: &google_cloud_gax::retry_state::RetryState,
        error: google_cloud_gax::error::Error,
    ) -> google_cloud_gax::retry_result::RetryResult {
        use google_cloud_gax::retry_result::RetryResult;

        if let Some(status) = error.status() {
            if status.code == google_cloud_gax::error::rpc::Code::ResourceExhausted {
                return RetryResult::Continue(error);
            }
        } else if let Some(code) = error.http_status_code()
            && code == reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16()
        {
            return RetryResult::Continue(error);
        }
        Aip194Strict.on_error(state, error)
    }
}

static SHARED_RETRY_THROTTLER: LazyLock<SharedRetryThrottler> =
    LazyLock::new(|| Arc::new(Mutex::new(AdaptiveThrottler::new(2.0).unwrap())));

impl VertexAiClient {
    pub async fn new(
        address: Option<String>,
        api_key: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> Result<Self> {
        if address.is_some() {
            api_bail!("VertexAi API address is not supported for VertexAi API type");
        }
        if api_key.is_some() {
            api_bail!(
                "VertexAi API key is not supported for VertexAi API type. Vertex AI uses Application Default Credentials (ADC) for authentication. Please set up ADC using 'gcloud auth application-default login' instead."
            );
        }
        let Some(super::LlmApiConfig::VertexAi(config)) = api_config else {
            api_bail!("VertexAi API config is required for VertexAi API type");
        };
        let client = vertexai::client::PredictionService::builder()
            .with_retry_policy(
                CustomizedGoogleCloudRetryPolicy.with_time_limit(retryable::DEFAULT_RETRY_TIMEOUT),
            )
            .with_backoff_policy(ExponentialBackoff::default())
            .with_retry_throttler(SHARED_RETRY_THROTTLER.clone())
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
        let has_json_schema = request.output_format.is_some();
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
            .set_contents(contents)
            .with_idempotency(true);
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
            client_bail!("No text in response");
        };

        let output = if has_json_schema {
            super::GeneratedOutput::Json(serde_json::from_str(&text)?)
        } else {
            super::GeneratedOutput::Text(text)
        };

        Ok(super::LlmGenerateResponse { output })
    }

    fn json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: false,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
            supports_additional_properties: false,
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
        let instances: Vec<_> = request
            .texts
            .iter()
            .map(|text| {
                let mut instance = serde_json::json!({
                    "content": text
                });
                // Add task type if specified
                if let Some(task_type) = &request.task_type {
                    instance["task_type"] = serde_json::Value::String(task_type.to_string());
                }
                instance
            })
            .collect();

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
            .with_idempotency(true)
            .send()
            .await?;

        // Extract the embeddings from the response
        let embeddings: Vec<Vec<f32>> = response
            .predictions
            .into_iter()
            .map(|mut prediction| {
                let embeddings = prediction
                    .get_mut("embeddings")
                    .map(|v| v.take())
                    .ok_or_else(|| client_error!("No embeddings in prediction"))?;
                let embedding: ContentEmbedding = utils::deser::from_json_value(embeddings)?;
                Ok(embedding.values)
            })
            .collect::<Result<_>>()?;
        Ok(super::LlmEmbeddingResponse { embeddings })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        get_embedding_dimension(model)
    }
}
