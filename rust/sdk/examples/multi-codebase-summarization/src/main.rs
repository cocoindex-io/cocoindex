//! Multi-Codebase Summarization — Rust equivalent of the Python example.
//!
//! Scans subdirectories of a root directory (each a project), extracts:
//! - Public classes/functions with summaries (via LLM)
//! - Mermaid graphs for function call relationships
//! - File-level summaries
//!
//! Aggregates per-file extractions into a project summary and outputs
//! markdown documentation.
//!
//! Demonstrates full macro usage:
//! - `#[cocoindex::function(memo)]` — per-file LLM extraction cached by fingerprint
//! - `#[cocoindex::function(memo)]` — project aggregation cached by project fingerprint
//! - `#[cocoindex::function]` — markdown generation
//! - `ctx.mount_each(...)` — concurrent per-file processing
//! - `ctx.write_file(...)` — output file creation
//!
//! ## Usage
//!
//! ```sh
//! export LLM_API_KEY="your-api-key"
//! cargo run -- ../../../../examples ./output
//! ```

use cocoindex::prelude::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Component;
use std::path::PathBuf;
use std::sync::OnceLock;

mod models;
use models::CodebaseInfo;

// ---------------------------------------------------------------------------
// LLM client (module-level, like Python's _instructor_client)
// ---------------------------------------------------------------------------

struct LlmClient {
    api_key: String,
    model: String,
    http: reqwest::Client,
    base_url: String,
}

/// Module-level LLM client, initialized once (same pattern as Python).
/// This avoids needing `ctx.get_or_err()` inside `#[function(memo)]` bodies.
static LLM: OnceLock<LlmClient> = OnceLock::new();

fn llm() -> &'static LlmClient {
    LLM.get()
        .expect("LLM client not initialized — call init_llm() first")
}

fn init_llm() {
    dotenvy::dotenv().ok();
    LLM.set(LlmClient {
        api_key: std::env::var("LLM_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .expect("set LLM_API_KEY or OPENAI_API_KEY"),
        model: std::env::var("LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into()),
        base_url: std::env::var("LLM_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
        http: reqwest::Client::new(),
    })
    .ok();
}

impl LlmClient {
    async fn extract<T: for<'de> Deserialize<'de>>(
        &self,
        prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<T> {
        let body = serde_json::json!({
            "model": &self.model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "extraction",
                    "schema": schema,
                    "strict": true
                }
            }
        });

        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| cocoindex::Error::engine(format!("LLM request failed: {e}")))?;

        let resp_json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| cocoindex::Error::engine(format!("LLM response parse: {e}")))?;

        let content = resp_json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| cocoindex::Error::engine("no content in LLM response"))?;

        serde_json::from_str(content)
            .map_err(|e| cocoindex::Error::engine(format!("JSON decode: {e}")))
    }
}

fn should_skip_python_file(file: &FileEntry) -> bool {
    file.relative_path()
        .components()
        .any(|component| match component {
            Component::Normal(part) => {
                let part = part.to_string_lossy();
                part == "__pycache__" || part.starts_with('.')
            }
            _ => false,
        })
}

fn cleanup_stale_outputs(
    output_dir: &std::path::Path,
    active_projects: &HashSet<String>,
) -> Result<()> {
    if !output_dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(output_dir).map_err(cocoindex::Error::Io)? {
        let entry = entry.map_err(cocoindex::Error::Io)?;
        if !entry.file_type().map_err(cocoindex::Error::Io)?.is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }

        let Some(project_name) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        if !active_projects.contains(project_name) {
            std::fs::remove_file(&path).map_err(cocoindex::Error::Io)?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-file extraction — memoized (unchanged files skip the LLM call)
// ---------------------------------------------------------------------------

/// Extract structured info from a single Python file via LLM.
/// `memo` caches results by file fingerprint — unchanged files are skipped.
#[cocoindex::function(memo)]
async fn extract_file_info(ctx: &Ctx, file: &FileEntry) -> Result<CodebaseInfo> {
    let content = file.content_str()?;
    let file_path = file.key();

    let prompt = format!(
        "Analyze the following Python file and extract structured information.\n\n\
         File path: {file_path}\n\n\
         ```python\n{content}\n```\n\n\
         Instructions:\n\
         1. Identify all PUBLIC classes (not starting with _) and summarize their purpose\n\
         2. Identify all PUBLIC functions (not starting with _) and summarize their purpose\n\
         3. If this file contains CocoIndex apps (coco.App), create Mermaid graphs showing the\n\
            function call relationships (see the mermaid_graphs field description for format)\n\
         4. Provide a brief summary of the file's purpose"
    );

    llm().extract(&prompt, &CodebaseInfo::json_schema()).await
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Aggregate per-file summaries into a project-level summary via LLM.
/// `memo` caches the aggregation result until any file-level summary changes.
#[cocoindex::function(memo)]
async fn aggregate_project_info(
    _ctx: &Ctx,
    project_name: String,
    file_infos: Vec<CodebaseInfo>,
) -> Result<CodebaseInfo> {
    if file_infos.is_empty() {
        return Ok(CodebaseInfo {
            name: project_name,
            summary: "Empty project with no Python files.".to_string(),
            ..Default::default()
        });
    }

    if file_infos.len() == 1 {
        let info = &file_infos[0];
        return Ok(CodebaseInfo {
            name: project_name,
            summary: info.summary.clone(),
            public_classes: info.public_classes.clone(),
            public_functions: info.public_functions.clone(),
            mermaid_graphs: info.mermaid_graphs.clone(),
        });
    }

    let files_text: String = file_infos
        .iter()
        .map(|info| {
            let classes: String = info
                .public_classes
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let fns: String = info
                .public_functions
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "### {}\nSummary: {}\nClasses: {}\nFunctions: {}",
                info.name,
                info.summary,
                if classes.is_empty() { "None" } else { &classes },
                if fns.is_empty() { "None" } else { &fns },
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    let all_graphs: Vec<String> = file_infos
        .iter()
        .flat_map(|info| info.mermaid_graphs.iter().cloned())
        .collect();

    let prompt = format!(
        "Aggregate the following Python files into a project-level summary.\n\n\
         Project name: {project_name}\n\n\
         Files:\n{files_text}\n\n\
         Create a unified CodebaseInfo that:\n\
         1. Summarizes the overall project purpose (not individual files)\n\
         2. Lists the most important public classes across all files\n\
         3. Lists the most important public functions across all files\n\
         4. For mermaid_graphs: create a single unified graph showing how the CocoIndex\n\
            components connect across the project (if applicable)"
    );

    let mut result: CodebaseInfo = llm().extract(&prompt, &CodebaseInfo::json_schema()).await?;

    // Keep original file-level graphs if LLM didn't generate a unified one
    if result.mermaid_graphs.is_empty() && !all_graphs.is_empty() {
        result.mermaid_graphs = all_graphs;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Markdown generation
// ---------------------------------------------------------------------------

/// Generate markdown documentation from project info.
#[cocoindex::function]
async fn generate_markdown(
    _ctx: &Ctx,
    project_name: String,
    info: CodebaseInfo,
    file_infos: Vec<CodebaseInfo>,
) -> Result<String> {
    let mut lines = vec![
        format!("# {project_name}"),
        String::new(),
        "## Overview".into(),
        String::new(),
        info.summary.clone(),
        String::new(),
    ];

    if !info.public_classes.is_empty() || !info.public_functions.is_empty() {
        lines.push("## Components".into());
        lines.push(String::new());

        if !info.public_classes.is_empty() {
            lines.push("**Classes:**".into());
            for cls in &info.public_classes {
                lines.push(format!("- `{}`: {}", cls.name, cls.summary));
            }
            lines.push(String::new());
        }

        if !info.public_functions.is_empty() {
            lines.push("**Functions:**".into());
            for f in &info.public_functions {
                let marker = if f.is_coco_function { " ★" } else { "" };
                lines.push(format!("- `{}`{marker}: {}", f.signature, f.summary));
            }
            lines.push(String::new());
        }
    }

    if !info.mermaid_graphs.is_empty() {
        lines.push("## CocoIndex Pipeline".into());
        lines.push(String::new());
        for graph in &info.mermaid_graphs {
            let content = graph.trim();
            if content.starts_with("```") {
                lines.push(content.to_string());
            } else {
                lines.push("```mermaid".into());
                lines.push(content.to_string());
                lines.push("```".into());
            }
            lines.push(String::new());
        }
    }

    if file_infos.len() > 1 {
        lines.push("## File Details".into());
        lines.push(String::new());
        for fi in &file_infos {
            lines.push(format!("### {}", fi.name));
            lines.push(String::new());
            lines.push(fi.summary.clone());
            lines.push(String::new());
        }
    }

    lines.push("---".into());
    lines.push(String::new());
    lines.push("*★ = CocoIndex function*".into());

    Ok(lines.join("\n"))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    init_llm();

    let args: Vec<String> = std::env::args().collect();
    let root_dir = PathBuf::from(
        args.get(1)
            .map(|s| s.as_str())
            .unwrap_or("../../../../examples"),
    );
    let output_dir = PathBuf::from(args.get(2).map(|s| s.as_str()).unwrap_or("./output"));

    let app = cocoindex::App::open("multi_codebase_summarization", ".cocoindex_db")?;

    let stats = app
        .run(move |ctx| async move {
            // List subdirectories (each is a project)
            let mut entries: Vec<_> = std::fs::read_dir(&root_dir)
                .map_err(cocoindex::Error::Io)?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                        && !e.file_name().to_string_lossy().starts_with('.')
                })
                .collect();
            entries.sort_by_key(|e| e.file_name());
            std::fs::create_dir_all(&output_dir).map_err(cocoindex::Error::Io)?;
            let mut active_projects = HashSet::new();

            for entry in entries {
                let project_name = entry.file_name().to_string_lossy().to_string();
                let project_dir = entry.path();

                // Match both root-level and nested Python files.
                let files = cocoindex::fs::walk(&project_dir, &["*.py", "**/*.py"])?;

                let files: Vec<_> = files
                    .into_iter()
                    .filter(|f| !should_skip_python_file(f))
                    .collect();
                let mut files = files;
                files.sort_by_key(|f| f.key());

                if files.is_empty() {
                    continue;
                }
                active_projects.insert(project_name.clone());

                println!("Processing project: {project_name} ({} files)", files.len());

                // Extract per-file info (memoized — unchanged files skip LLM)
                let file_infos: Vec<CodebaseInfo> = ctx
                    .mount_each(
                        files,
                        |f| format!("{project_name}/{}", f.key()),
                        |child_ctx, file| async move { extract_file_info(&child_ctx, &file).await },
                    )
                    .await?;

                // Aggregate into project summary (memoized — unchanged projects skip LLM)
                let project_info =
                    aggregate_project_info(&ctx, project_name.clone(), file_infos.clone()).await?;

                // Generate and write markdown
                let markdown =
                    generate_markdown(&ctx, project_name.clone(), project_info, file_infos).await?;

                ctx.write_file(
                    output_dir.join(format!("{project_name}.md")),
                    markdown.as_bytes(),
                )?;
                println!("  Wrote {}/{project_name}.md", output_dir.display());
            }

            cleanup_stale_outputs(&output_dir, &active_projects)?;
            Ok(())
        })
        .await?;

    println!("{stats}");
    Ok(())
}
