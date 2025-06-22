use crate::prelude::*;

use crate::llm::{LlmEmbeddingClient, LlmEmbeddingRequest, LlmEmbeddingResponse};
use phf::phf_map;

static DEFAULT_EMBEDDING_DIMENSIONS: phf::Map<&str, u32> = phf_map! {
    // Current models
    "voyage-3-large" => 1024,
    "voyage-3.5" => 1024,
    "voyage-3.5-lite" => 1024,
    "voyage-code-3" => 1024,
    "voyage-finance-2" => 1024,
    "voyage-law-2" => 1024,
    "voyage-code-2" => 1536,

    // Legacy models
    "voyage-3" => 1024,
    "voyage-3-lite" => 512,
    "voyage-multilingual-2" => 1024,
    "voyage-large-2-instruct" => 1024,
    "voyage-large-2" => 1536,
    "voyage-2" => 1024,
    "voyage-lite-02-instruct" => 1024,
    "voyage-02" => 1024,
    "voyage-01" => 1024,
    "voyage-lite-01" => 1024,
    "voyage-lite-01-instruct" => 1024,
};

pub struct Client {
    api_key: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(address: Option<String>) -> Result<Self> {
        if address.is_some() {
            api_bail!("Voyage AI doesn't support custom API address");
        }
        let api_key = match std::env::var("VOYAGE_API_KEY") {
            Ok(val) => val,
            Err(_) => api_bail!("VOYAGE_API_KEY environment variable must be set"),
        };
        Ok(Self {
            api_key,
            client: reqwest::Client::new(),
        })
    }
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbeddingData>,
}

#[async_trait]
impl LlmEmbeddingClient for Client {
    async fn embed_text<'req>(
        &self,
        request: LlmEmbeddingRequest<'req>,
    ) -> Result<LlmEmbeddingResponse> {
        let url = "https://api.voyageai.com/v1/embeddings";

        let mut payload = serde_json::json!({
            "input": request.text,
            "model": request.model,
        });

        if let Some(task_type) = request.task_type {
            payload["input_type"] = serde_json::Value::String(task_type.into());
        }

        let resp = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&payload)
            .send()
            .await
            .context("HTTP error")?;

        if !resp.status().is_success() {
            bail!(
                "Voyage AI API error: {:?}\n{}\n",
                resp.status(),
                resp.text().await?
            );
        }

        let embedding_resp: EmbedResponse = resp.json().await.context("Invalid JSON")?;

        if embedding_resp.data.is_empty() {
            bail!("No embedding data in response");
        }

        Ok(LlmEmbeddingResponse {
            embedding: embedding_resp.data[0].embedding.clone(),
        })
    }

    fn get_default_embedding_dimension(&self, model: &str) -> Option<u32> {
        DEFAULT_EMBEDDING_DIMENSIONS.get(model).copied()
    }
}
