use crate::api_bail;

use super::{LlmEmbeddingClient, LlmGenerationClient, detect_image_mime_type};
use anyhow::Result;
use async_openai::{
    Client as OpenAIClient,
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartImage,
        ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
        CreateChatCompletionRequest, CreateEmbeddingRequest, EmbeddingInput, ImageDetail,
        ResponseFormat, ResponseFormatJsonSchema,
    },
};
use async_trait::async_trait;
use base64::prelude::*;
use phf::phf_map;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    "text-embedding-3-small" => 1536,
    "text-embedding-3-large" => 3072,
    "text-embedding-ada-002" => 1536,
};

pub struct Client {
    client: async_openai::Client<OpenAIConfig>,
}

impl Client {
    pub(crate) fn from_parts(client: async_openai::Client<OpenAIConfig>) -> Self {
        Self { client }
    }

    pub fn new(address: Option<String>, api_config: Option<super::LlmApiConfig>) -> Result<Self> {
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

        // Verify API key is set
        if std::env::var("OPENAI_API_KEY").is_err() {
            api_bail!("OPENAI_API_KEY environment variable must be set");
        }
        Ok(Self {
            // OpenAI client will use OPENAI_API_KEY and OPENAI_API_BASE env variables by default
            client: OpenAIClient::with_config(openai_config),
        })
    }
}

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: super::LlmGenerateRequest<'req>,
    ) -> Result<super::LlmGenerateResponse> {
        let mut messages = Vec::new();

        // Add system prompt if provided
        if let Some(system) = request.system_prompt {
            messages.push(ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Text(system.into_owned()),
                    ..Default::default()
                },
            ));
        }

        // Add user message
        let user_message_content = match request.image {
            Some(img_bytes) => {
                let base64_image = BASE64_STANDARD.encode(img_bytes.as_ref());
                let mime_type = detect_image_mime_type(img_bytes.as_ref())?;
                let image_url = format!("data:{mime_type};base64,{base64_image}");
                ChatCompletionRequestUserMessageContent::Array(vec![
                    ChatCompletionRequestUserMessageContentPart::Text(
                        ChatCompletionRequestMessageContentPartText {
                            text: request.user_prompt.into_owned(),
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
            None => ChatCompletionRequestUserMessageContent::Text(request.user_prompt.into_owned()),
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
            response_format: match request.output_format {
                Some(super::OutputFormat::JsonSchema { name, schema }) => {
                    Some(ResponseFormat::JsonSchema {
                        json_schema: ResponseFormatJsonSchema {
                            name: name.into_owned(),
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

        // Send request and get response
        let response = self.client.chat().create(request).await?;

        // Extract the response text from the first choice
        let text = response
            .choices
            .into_iter()
            .next()
            .and_then(|choice| choice.message.content)
            .ok_or_else(|| anyhow::anyhow!("No response from OpenAI"))?;

        Ok(super::LlmGenerateResponse { text })
    }

    fn json_schema_options(&self) -> super::ToJsonSchemaOptions {
        super::ToJsonSchemaOptions {
            fields_always_required: true,
            supports_format: false,
            extract_descriptions: false,
            top_level_must_be_object: true,
        }
    }
}

#[async_trait]
impl LlmEmbeddingClient for Client {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let response = self
            .client
            .embeddings()
            .create(CreateEmbeddingRequest {
                model: request.model.to_string(),
                input: EmbeddingInput::String(request.text.to_string()),
                dimensions: request.output_dimension,
                ..Default::default()
            })
            .await?;
        Ok(super::LlmEmbeddingResponse {
            embedding: response
                .data
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("No embedding returned from OpenAI"))?
                .embedding,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
