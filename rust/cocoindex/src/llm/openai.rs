use crate::prelude::*;
use base64::prelude::*;

use super::{LlmEmbeddingClient, LlmGenerationClient, detect_image_mime_type};
use async_openai::{
    Client as OpenAIClient,
    config::{AzureConfig, OpenAIConfig},
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartImage,
        ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
        CreateChatCompletionRequest, CreateEmbeddingRequest, EmbeddingInput, ImageDetail,
        ResponseFormat, ResponseFormatJsonSchema,
    },
};
use phf::phf_map;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    "text-embedding-3-small" => 1536,
    "text-embedding-3-large" => 3072,
    "text-embedding-ada-002" => 1536,
};

pub struct Client<C: async_openai::config::Config = OpenAIConfig> {
    client: async_openai::Client<C>,
}

impl Client {
    pub(crate) fn from_parts<C: async_openai::config::Config>(
        client: async_openai::Client<C>,
    ) -> Client<C> {
        Client { client }
    }

    pub fn new(
        address: Option<String>,
        api_key: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> Result<Self> {
        let config = match api_config {
            Some(super::LlmApiConfig::OpenAi(config)) => config,
            Some(_) => api_bail!("unexpected config type, expected OpenAiConfig"),
            None => super::OpenAiConfig::default(),
        };

        let mut openai_config = OpenAIConfig::new();
        if let Some(address) = address {
            openai_config = openai_config.with_api_base(address);
        }
        if let Some(org_id) = config.org_id {
            openai_config = openai_config.with_org_id(org_id);
        }
        if let Some(project_id) = config.project_id {
            openai_config = openai_config.with_project_id(project_id);
        }
        if let Some(key) = api_key {
            openai_config = openai_config.with_api_key(key);
        } else {
            // Verify API key is set in environment if not provided in config
            if std::env::var("OPENAI_API_KEY").is_err() {
                api_bail!("OPENAI_API_KEY environment variable must be set");
            }
        }

        Ok(Self {
            client: OpenAIClient::with_config(openai_config),
        })
    }
}

impl Client<AzureConfig> {
    pub async fn new_azure(
        address: Option<String>,
        api_key: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> Result<Self> {
        let config = match api_config {
            Some(super::LlmApiConfig::AzureOpenAi(config)) => config,
            Some(_) => api_bail!("unexpected config type, expected AzureOpenAiConfig"),
            None => api_bail!("AzureOpenAiConfig is required for Azure OpenAI"),
        };

        let api_base =
            address.ok_or_else(|| client_error!("address is required for Azure OpenAI"))?;

        // Default to API version that supports structured outputs (json_schema).
        let api_version = config
            .api_version
            .unwrap_or_else(|| "2024-08-01-preview".to_string());

        let api_key = api_key
            .or_else(|| std::env::var("AZURE_OPENAI_API_KEY").ok())
            .ok_or_else(|| client_error!(
                "AZURE_OPENAI_API_KEY must be set either via api_key parameter or environment variable"
            ))?;

        let azure_config = AzureConfig::new()
            .with_api_base(api_base)
            .with_api_version(api_version)
            .with_deployment_id(config.deployment_id)
            .with_api_key(api_key);

        Ok(Self {
            client: OpenAIClient::with_config(azure_config),
        })
    }
}

pub(super) fn create_llm_generation_request(
    request: &super::LlmGenerateRequest,
) -> Result<CreateChatCompletionRequest> {
    let mut messages = Vec::new();

    // Add system prompt if provided
    if let Some(system) = &request.system_prompt {
        messages.push(ChatCompletionRequestMessage::System(
            ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(system.to_string()),
                ..Default::default()
            },
        ));
    }

    // Add user message
    let user_message_content = match &request.image {
        Some(img_bytes) => {
            let base64_image = BASE64_STANDARD.encode(img_bytes.as_ref());
            let mime_type = detect_image_mime_type(img_bytes.as_ref())?;
            let image_url = format!("data:{mime_type};base64,{base64_image}");
            ChatCompletionRequestUserMessageContent::Array(vec![
                ChatCompletionRequestUserMessageContentPart::Text(
                    ChatCompletionRequestMessageContentPartText {
                        text: request.user_prompt.to_string(),
                    },
                ),
                ChatCompletionRequestUserMessageContentPart::ImageUrl(
                    ChatCompletionRequestMessageContentPartImage {
                        image_url: async_openai::types::ImageUrl {
                            url: image_url,
                            detail: Some(ImageDetail::Auto),
                        },
                    },
                ),
            ])
        }
        None => ChatCompletionRequestUserMessageContent::Text(request.user_prompt.to_string()),
    };
    messages.push(ChatCompletionRequestMessage::User(
        ChatCompletionRequestUserMessage {
            content: user_message_content,
            ..Default::default()
        },
    ));
    // Create the chat completion request
    let request = CreateChatCompletionRequest {
        model: request.model.to_string(),
        messages,
        response_format: match &request.output_format {
            Some(super::OutputFormat::JsonSchema { name, schema }) => {
                Some(ResponseFormat::JsonSchema {
                    json_schema: ResponseFormatJsonSchema {
                        name: name.to_string(),
                        description: None,
                        schema: Some(serde_json::to_value(&schema)?),
                        strict: Some(true),
                    },
                })
            }
            None => None,
        },
        ..Default::default()
    };

    Ok(request)
}

#[async_trait]
impl<C> LlmGenerationClient for Client<C>
where
    C: async_openai::config::Config + Send + Sync,
{
    async fn generate<'req>(
        &self,
        request: super::LlmGenerateRequest<'req>,
    ) -> Result<super::LlmGenerateResponse> {
        let has_json_schema = request.output_format.is_some();
        let request = &request;
        let response = retryable::run(
            || async {
                let req = create_llm_generation_request(request)?;
                let response = self.client.chat().create(req).await?;
                retryable::Ok(response)
            },
            &retryable::RetryOptions::default(),
        )
        .await?;

        // Extract the response text from the first choice
        let text = response
            .choices
            .into_iter()
            .next()
            .and_then(|choice| choice.message.content)
            .ok_or_else(|| client_error!("No response from OpenAI"))?;

        let output = if has_json_schema {
            super::GeneratedOutput::Json(serde_json::from_str(&text)?)
        } else {
            super::GeneratedOutput::Text(text)
        };

        Ok(super::LlmGenerateResponse { output })
    }

    fn json_schema_options(&self) -> super::ToJsonSchemaOptions {
        super::ToJsonSchemaOptions {
            fields_always_required: true,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
            supports_additional_properties: true,
        }
    }
}

#[async_trait]
impl<C> LlmEmbeddingClient for Client<C>
where
    C: async_openai::config::Config + Send + Sync,
{
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let response = retryable::run(
            || async {
                let texts: Vec<String> = request.texts.iter().map(|t| t.to_string()).collect();
                let response = self
                    .client
                    .embeddings()
                    .create(CreateEmbeddingRequest {
                        model: request.model.to_string(),
                        input: EmbeddingInput::StringArray(texts),
                        dimensions: request.output_dimension,
                        ..Default::default()
                    })
                    .await?;
                retryable::Ok(response)
            },
            &retryable::RetryOptions::default(),
        )
        .await
        .map_err(Error::from)?;
        Ok(super::LlmEmbeddingResponse {
            embeddings: response.data.into_iter().map(|e| e.embedding).collect(),
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
