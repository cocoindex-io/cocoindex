use crate::api_bail;

use super::LlmGenerationClient;
use anyhow::Result;
use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, CreateChatCompletionRequest, ResponseFormat,
        ResponseFormatJsonSchema,
    },
    Client as OpenAIClient,
};
use async_trait::async_trait;

pub struct Client {
    client: async_openai::Client<OpenAIConfig>,
    model: String,
}

impl Client {
    pub async fn new(spec: super::LlmSpec) -> Result<Self> {
        if let Some(address) = spec.address {
            api_bail!("OpenAI doesn't support custom API address: {address}");
        }
        // Verify API key is set
        if std::env::var("OPENAI_API_KEY").is_err() {
            api_bail!("OPENAI_API_KEY environment variable must be set");
        }
        Ok(Self {
            // OpenAI client will use OPENAI_API_KEY env variable by default
            client: OpenAIClient::new(),
            model: spec.model,
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
        messages.push(ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Text(
                    request.user_prompt.into_owned(),
                ),
                ..Default::default()
            },
        ));

        // Create the chat completion request
        let request = CreateChatCompletionRequest {
            model: self.model.clone(),
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
