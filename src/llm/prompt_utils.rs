// Shared prompt utilities for LLM clients
// Only import this in clients that require strict JSON output instructions (e.g., Anthropic, Gemini, Ollama)

pub const STRICT_JSON_PROMPT: &str = "IMPORTANT: Output ONLY valid JSON that matches the schema. Do NOT say anything else. Do NOT explain. Do NOT preface. Do NOT add comments. If you cannot answer, output an empty JSON object: {}.";
