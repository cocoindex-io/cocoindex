use std::borrow::Cow;

use schemars::schema::SchemaObject;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LlmApiType {
    Ollama,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSpec {
    api_type: LlmApiType,
    address: Option<String>,
    model: String,
}

#[derive(Debug)]
pub enum OutputFormat<'a> {
    JsonSchema(Cow<'a, SchemaObject>),
}

#[derive(Debug)]
pub struct LlmGenerateRequest<'a> {
    pub system_prompt: Option<Cow<'a, str>>,
    pub user_prompt: Cow<'a, str>,
    pub output_format: Option<OutputFormat<'a>>,
}

#[derive(Debug)]
pub struct LlmGenerateResponse {
    pub text: String,
}

mod client;
pub use client::LlmClient;
