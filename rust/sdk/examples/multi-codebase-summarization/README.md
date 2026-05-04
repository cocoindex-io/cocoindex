# Multi-Codebase Summarization 📝

Rust equivalent of the Python [`multi_codebase_summarization`](../../../examples/multi_codebase_summarization) example.

Scans subdirectories of a root folder (each treated as a Python project), uses an LLM to extract structured info (public classes, functions, CocoIndex pipeline graphs), aggregates into project-level summaries, and outputs markdown documentation.

## Prerequisites

- An OpenAI-compatible API key:

```sh
export LLM_API_KEY="sk-..."
# Optional overrides:
export LLM_MODEL="gpt-4o-mini"         # default
export LLM_BASE_URL="https://api.openai.com/v1"  # default
```

## Build

```sh
cd rust/sdk/examples/multi-codebase-summarization
cargo build --release
```

## Usage

**Summarize** all Python examples in this repo:

```sh
cargo run -- ../../../../examples ./output
```

This will:

1. Scan each subdirectory of `../../../../examples` as a project
2. Walk `*.py` and `**/*.py` files in each project
3. Extract per-file info via LLM (memoized — unchanged files skip the LLM)
4. Aggregate into a project-level summary via LLM (memoized — unchanged projects skip the LLM)
5. Write `output/<project_name>.md` for each project and remove stale markdown for deleted projects

**Re-run** — unchanged files and unchanged project summaries are cached (memoized in LMDB), so a fully warm rerun makes zero LLM calls:

```sh
cargo run -- ../../../../examples ./output
# Much faster — skips files and project summaries already analyzed
```

## Macro Showcase

This example demonstrates:

| Macro | Purpose |
|---|---|
| `#[cocoindex::function(memo)]` | `extract_file_info` — LLM call cached per file fingerprint |
| `#[cocoindex::function(memo)]` | `aggregate_project_info` — LLM call cached until any file summary changes |
| `#[cocoindex::function]` | `generate_markdown` — pure transform, no caching needed |
| `ctx.mount_each(...)` | Process all files concurrently within each project |
| `OnceLock<LlmClient>` | Access a process-wide shared LLM client |
