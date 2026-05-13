//! Pydantic-equivalent models for multi-codebase summarization.

use serde::{Deserialize, Serialize};

/// Information about a public function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionInfo {
    /// Function name.
    pub name: String,
    /// Function signature, e.g. `async def foo(x: int) -> str`.
    pub signature: String,
    /// Whether decorated with @coco.function or @cocoindex.function.
    pub is_coco_function: bool,
    /// Brief summary of what the function does.
    pub summary: String,
}

/// Information about a public class.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassInfo {
    /// Class name.
    pub name: String,
    /// Brief summary of what the class represents/does.
    pub summary: String,
}

/// Extracted information from Python code (file or project level).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CodebaseInfo {
    /// File path (for files) or project name (for projects).
    pub name: String,
    /// Brief summary of purpose and functionality.
    pub summary: String,
    /// Public classes (not starting with _).
    #[serde(default)]
    pub public_classes: Vec<ClassInfo>,
    /// Public functions (not starting with _).
    #[serde(default)]
    pub public_functions: Vec<FunctionInfo>,
    /// Mermaid graphs showing CocoIndex function call relationships.
    #[serde(default)]
    pub mermaid_graphs: Vec<String>,
}

impl CodebaseInfo {
    /// JSON Schema for structured LLM output.
    pub fn json_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "summary": { "type": "string" },
                "public_classes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "summary": { "type": "string" }
                        },
                        "required": ["name", "summary"],
                        "additionalProperties": false
                    }
                },
                "public_functions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "signature": { "type": "string" },
                            "is_coco_function": { "type": "boolean" },
                            "summary": { "type": "string" }
                        },
                        "required": ["name", "signature", "is_coco_function", "summary"],
                        "additionalProperties": false
                    }
                },
                "mermaid_graphs": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            },
            "required": ["name", "summary", "public_classes", "public_functions", "mermaid_graphs"],
            "additionalProperties": false
        })
    }
}
