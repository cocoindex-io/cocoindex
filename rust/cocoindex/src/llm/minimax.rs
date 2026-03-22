use crate::prelude::*;

use crate::llm::{LlmEmbeddingClient, LlmEmbeddingRequest, LlmEmbeddingResponse};
use async_openai::Client as OpenAIClient;
use async_openai::config::OpenAIConfig;
use phf::phf_map;

pub use super::openai::Client;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    "embo-01" => 1536,
};

impl Client {
    pub async fn new_minimax(
        address: Option<String>,
        api_key: Option<String>,
    ) -> Result<Self> {
        let address = address.unwrap_or_else(|| "https://api.minimax.io/v1".to_string());

        let api_key = api_key
            .or_else(|| std::env::var("MINIMAX_API_KEY").ok())
            .ok_or_else(|| {
                client_error!("MINIMAX_API_KEY environment variable must be set")
            })?;

        let config = OpenAIConfig::new()
            .with_api_base(address)
            .with_api_key(api_key);
        Ok(Client::from_parts(OpenAIClient::with_config(config)))
    }
}

pub struct MiniMaxEmbeddingClient {
    api_key: String,
    address: String,
    client: reqwest::Client,
}

impl MiniMaxEmbeddingClient {
    pub fn new(address: Option<String>, api_key: Option<String>) -> Result<Self> {
        let address =
            address.unwrap_or_else(|| "https://api.minimax.io/v1/embeddings".to_string());

        let api_key = api_key
            .or_else(|| std::env::var("MINIMAX_API_KEY").ok())
            .ok_or_else(|| {
                client_error!("MINIMAX_API_KEY environment variable must be set")
            })?;

        Ok(Self {
            api_key,
            address,
            client: reqwest::Client::new(),
        })
    }
}

#[derive(Deserialize)]
struct EmbedResponse {
    vectors: Vec<Vec<f32>>,
}

#[async_trait]
impl LlmEmbeddingClient for MiniMaxEmbeddingClient {
    async fn embed_text<'req>(
        &self,
        request: LlmEmbeddingRequest<'req>,
    ) -> Result<LlmEmbeddingResponse> {
        let texts: Vec<String> = request.texts.iter().map(|t| t.to_string()).collect();

        // MiniMax uses "db" for storage and "query" for search queries.
        let embed_type = match request.task_type.as_deref() {
            Some("query") => "query",
            _ => "db",
        };

        let payload = serde_json::json!({
            "model": request.model,
            "texts": texts,
            "type": embed_type,
        });

        let resp = http::request(&self.client, |client| {
            client
                .post(&self.address)
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(&payload)
        })
        .await
        .map_err(Error::from)
        .with_context(|| "MiniMax Embedding API error")?;

        let embedding_resp: EmbedResponse =
            resp.json().await.with_context(|| "Invalid JSON from MiniMax Embedding API")?;

        Ok(LlmEmbeddingResponse {
            embeddings: embedding_resp.vectors,
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
