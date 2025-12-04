use crate::prelude::*;

use super::LlmEmbeddingClient;
use super::LlmGenerationClient;
use async_openai::{Client as OpenAIClient, config::AzureConfig};
use phf::phf_map;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    "text-embedding-3-small" => 1536,
    "text-embedding-3-large" => 3072,
    "text-embedding-ada-002" => 1536,
};

pub struct Client {
    client: async_openai::Client<AzureConfig>,
}

impl Client {
    pub async fn new_azure_openai(
        address: Option<String>,
        api_key: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> anyhow::Result<Self> {
        let config = match api_config {
            Some(super::LlmApiConfig::AzureOpenAi(config)) => config,
            Some(_) => anyhow::bail!("unexpected config type, expected AzureOpenAiConfig"),
            None => anyhow::bail!("AzureOpenAiConfig is required for Azure OpenAI"),
        };

        let api_base =
            address.ok_or_else(|| anyhow::anyhow!("address is required for Azure OpenAI"))?;

        // Default to API version that supports structured outputs (json_schema).
        // See: https://learn.microsoft.com/en-us/azure/ai-foundry/openai/api-version-lifecycle
        let api_version = config
            .api_version
            .unwrap_or_else(|| "2024-08-01-preview".to_string());

        let api_key = api_key.or_else(|| std::env::var("AZURE_OPENAI_API_KEY").ok())
            .ok_or_else(|| anyhow::anyhow!("AZURE_OPENAI_API_KEY must be set either via api_key parameter or environment variable"))?;

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

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: super::LlmGenerateRequest<'req>,
    ) -> Result<super::LlmGenerateResponse> {
        let request = &request;
        let response = retryable::run(
            || async {
                let req = super::openai::create_llm_generation_request(request)?;
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
            .ok_or_else(|| anyhow::anyhow!("No response from Azure OpenAI"))?;

        Ok(super::LlmGenerateResponse { text })
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
impl LlmEmbeddingClient for Client {
    async fn embed_text<'req>(
        &self,
        request: super::LlmEmbeddingRequest<'req>,
    ) -> Result<super::LlmEmbeddingResponse> {
        let response = retryable::run(
            || async {
                let texts: Vec<String> = request.texts.iter().map(|t| t.to_string()).collect();
                self.client
                    .embeddings()
                    .create(async_openai::types::CreateEmbeddingRequest {
                        model: request.model.to_string(),
                        input: async_openai::types::EmbeddingInput::StringArray(texts),
                        dimensions: request.output_dimension,
                        ..Default::default()
                    })
                    .await
            },
            &retryable::RetryOptions::default(),
        )
        .await?;
        Ok(super::LlmEmbeddingResponse {
            embeddings: response.data.into_iter().map(|e| e.embedding).collect(),
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
