use async_openai::Client as OpenAIClient;
use async_openai::config::OpenAIConfig;

pub use super::openai::Client;

impl Client {
    pub async fn new_vllm(address: Option<String>) -> anyhow::Result<Self> {
        let address = address.unwrap_or_else(|| "http://127.0.0.1:8000/v1".to_string());
        let api_key = std::env::var("VLLM_API_KEY").ok();
        let mut config = OpenAIConfig::new().with_api_base(address);
        if let Some(api_key) = api_key {
            config = config.with_api_key(api_key);
        }
        Ok(Client::from_parts(OpenAIClient::with_config(config)))
    }
}
