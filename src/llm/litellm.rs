use async_openai::config::OpenAIConfig;
use async_openai::Client as OpenAIClient;

pub use super::openai::Client;

impl Client {
    pub async fn new_litellm(spec: super::LlmSpec) -> anyhow::Result<Self> {
        let address = spec.address.clone().unwrap_or_else(|| "http://127.0.0.1:4000".to_string());
        let api_key = std::env::var("LITELLM_API_KEY").ok();
        let mut config = OpenAIConfig::new().with_api_base(address);
        if let Some(api_key) = api_key {
            config = config.with_api_key(api_key);
        }
        Ok(Client::from_parts(OpenAIClient::with_config(config), spec.model))
    }
}
