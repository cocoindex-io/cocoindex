use crate::prelude::*;
use async_openai::config::AzureConfig;

// Re-export the generic client from openai module
pub use super::openai::Client;

impl Client<AzureConfig> {
    pub async fn new_azure_openai(
        address: Option<String>,
        api_key: Option<String>,
        api_config: Option<super::LlmApiConfig>,
    ) -> Result<Self> {
        Self::new_azure(address, api_key, api_config).await
    }
}
