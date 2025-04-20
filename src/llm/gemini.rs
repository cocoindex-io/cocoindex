use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use super::{LlmGenerateRequest, LlmGenerateResponse, LlmGenerationClient, ToJsonSchemaOptions};

#[derive(Debug)]
pub struct Client {
    client: reqwest::Client,
    model: String,
    api_key: String,
}

// Request/Response Structs based on Gemini API documentation
#[derive(Serialize)]
struct GeminiRequest<'a> {
    contents: Vec<Content<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<Content<'a>>,
    // TODO: Add generationConfig, safetySettings if needed
}

#[derive(Serialize)]
struct Content<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>, // "user" or "model"
    parts: Vec<Part<'a>>,
}

#[derive(Serialize)]
struct Part<'a> {
    text: Cow<'a, str>,
    // TODO: Add support for other part types if needed (e.g., inline_data for images)
}

#[derive(Deserialize)]
struct GeminiResponse {
    candidates: Vec<Candidate>,
    // TODO: Add promptFeedback if needed
}

#[derive(Deserialize)]
struct Candidate {
    content: ResponseContent,
    // TODO: Add finishReason, safetyRatings if needed
}

#[derive(Deserialize)]
struct ResponseContent {
    parts: Vec<ResponsePart>,
    // role is usually "model"
}

#[derive(Deserialize)]
struct ResponsePart {
    text: String,
}

impl Client {
    pub async fn new(spec: super::LlmSpec) -> Result<Self> {
        // Gemini doesn't have a specific address parameter like Ollama
        // API Key is expected via env var
        let api_key = std::env::var("GEMINI_API_KEY")
            .map_err(|_| anyhow::anyhow!("GEMINI_API_KEY environment variable must be set"))?;

        Ok(Self {
            client: reqwest::Client::new(),
            model: spec.model,
            api_key,
        })
    }
}

#[async_trait]
impl LlmGenerationClient for Client {
    async fn generate<'req>(
        &self,
        request: LlmGenerateRequest<'req>,
    ) -> Result<LlmGenerateResponse> {
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model,
            self.api_key
        );

        let mut contents = Vec::new();

        // Currently, CocoIndex LlmGenerateRequest doesn't explicitly support chat history.
        // We'll map system prompt and user prompt accordingly.
        // If chat history is needed later, LlmGenerateRequest needs modification.

        // Add user message
        contents.push(Content {
            role: Some("user"), // Explicitly set role
            parts: vec![Part { text: request.user_prompt }],
        });

        let gemini_request = GeminiRequest {
            contents,
            system_instruction: request.system_prompt.map(|sp| Content {
                role: None, // System instruction doesn't have a role in the Gemini API struct
                parts: vec![Part { text: sp }],
            }),
        };

        let response = self.client.post(&url)
            .json(&gemini_request)
            .send()
            .await?
            .error_for_status()? // Ensure we handle HTTP errors
            .json::<GeminiResponse>()
            .await?;

        // Extract the response text
        let text = response
            .candidates
            .into_iter()
            .next()
            .and_then(|c| c.content.parts.into_iter().next())
            .map(|p| p.text)
            .ok_or_else(|| anyhow::anyhow!("No response text found in Gemini response"))?;

        Ok(LlmGenerateResponse { text })
    }

    // Define how Gemini handles JSON schema constraints.
    // This might need refinement based on actual Gemini capabilities for JSON mode.
    // For now, using defaults similar to OpenAI but noting differences.
    fn json_schema_options(&self) -> ToJsonSchemaOptions {
        ToJsonSchemaOptions {
            fields_always_required: true, // Assuming similar behavior to OpenAI's strict mode
            supports_format: false,      // Unsure if Gemini supports 'format' keyword well
            extract_descriptions: false, // Unsure if Gemini uses descriptions
            top_level_must_be_object: true, // Assuming similar JSON mode constraints
        }
    }
} 