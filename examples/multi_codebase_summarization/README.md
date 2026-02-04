<p align="center">
    <img src="https://cocoindex.io/images/github.svg" alt="CocoIndex">
</p>

<h1 align="center">Multi Codebase Summarization </h1>

<div align="center">

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)
[![Documentation](https://img.shields.io/badge/Documentation-394e79?logo=readthedocs&logoColor=00B9FF)](https://cocoindex.io/docs/getting_started/quickstart)
[![License](https://img.shields.io/badge/license-Apache%202.0-5B5BD6?logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://img.shields.io/pypi/v/cocoindex?color=5B5BD6)](https://pypi.org/project/cocoindex/)
<!--[![PyPI - Downloads](https://img.shields.io/pypi/dm/cocoindex)](https://pypistats.org/packages/cocoindex) -->
[![PyPI Downloads](https://static.pepy.tech/badge/cocoindex/month)](https://pepy.tech/projects/cocoindex)
[![CI](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/CI.yml)
[![release](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml/badge.svg?event=push&color=5B5BD6)](https://github.com/cocoindex-io/cocoindex/actions/workflows/release.yml)
[![Link Check](https://github.com/cocoindex-io/cocoindex/actions/workflows/links.yml/badge.svg)](https://github.com/cocoindex-io/cocoindex/actions/workflows/links.yml)
[![Discord](https://img.shields.io/discord/1314801574169673738?logo=discord&color=5B5BD6&logoColor=white)](https://discord.com/invite/zpA9S2DR7s)

</div>

<div align="center">
  
[Step By Step Tutorial](https://cocoindex.io/examples-v1/multi-codebase-summarization)

</div>

<div align="center">
  
Star 🌟 [CocoIndex](https://github.com/cocoindex-io/cocoindex) if you like it!!

</div>

<img width="2732" height="1536" alt="image" src="https://github.com/user-attachments/assets/15feef96-f6d0-4f00-9a7a-b9bdb6a45ac5" />



This example shows how to use [instructor](https://github.com/jxnl/instructor) with Gemini to analyze multiple Python codebases and generate markdown documentation using CocoIndex v1.

<img width="1180" height="1190" alt="image" src="https://github.com/user-attachments/assets/0efbdf7f-8fd3-460c-afd3-417285d42c69" />


## What It Does

1. **Scans subdirectories** of a root directory (each expected to be a separate Python project)
2. **Per-file extraction** using LLM with a unified `CodebaseInfo` model:
   - Public classes and functions with functionality summaries
   - CocoIndex app call relationship graphs (Mermaid format)
   - File-level summaries
3. **Project aggregation** - combines file-level `CodebaseInfo` into a project-level summary
4. **Outputs markdown** documentation to `output/PROJECT_NAME.md`

## Key Features

- **Instructor Integration**: Uses instructor library for structured LLM outputs with Pydantic
- **Unified Data Model**: Same `CodebaseInfo` type for both file-level and project-level extraction
- **LLM-Generated Mermaid Graphs**: The LLM generates mermaid syntax directly with:
  - Bold text for `@coco.function` decorated functions
  - Thick arrows (`==>`) for `mount`/`mount_run` calls
- **Incremental Processing**: CocoIndex handles caching - only re-processes changed files
- **Multi-Project Support**: Processes multiple codebases in parallel

## Output Format

The generated markdown includes:

- **Overview** - High-level project description
- **Components** - Classes and functions with summaries
- **CocoIndex Pipeline** - Mermaid diagrams (if CocoIndex is used)
- **File Details** - Per-file summaries (for multi-file projects)

### Example Mermaid Graph

```mermaid
graph TD
    %% App: SampleApp
    app_main[<b>app_main</b>] ==> process_file[<b>process_file</b>]
    process_file --> helper_func[helper_func]
```

*Bold = `@coco.function`, thick arrows (`==>`) = `mount`/`mount_run` calls*

## Run

### 1. Install dependencies

```sh
pip install -e .
```

### 2. Set up environment variables

Create a `.env` file in the example directory:

```sh
echo "GEMINI_API_KEY=your_api_key_here" > .env
```

Replace `your_api_key_here` with your actual Gemini API key.

Optionally, set a different LLM model:

```sh
echo "LLM_MODEL=gemini/gemini-2.5-flash" >> .env
```

### 3. Prepare your projects

Create a `projects/` directory with subdirectories for each Python project:

```
projects/
├── my_project_1/
│   ├── main.py
│   └── utils.py
├── my_project_2/
│   └── app.py
└── ...
```

### 4. Run the application

```sh
cocoindex update main.py
```

This will:
1. Scan all subdirectories in `projects/`
2. Extract information from all `.py` files (excluding `.venv*` directories)
3. Generate markdown documentation in `output/`

### 5. Verify the output

```sh
ls -la output/
cat output/my_project_1.md
```

## Customization

### Change Input/Output Directories

Edit the `app` definition in `main.py`:

```python
app = coco.App(
    app_main,
    coco.AppConfig(name="MultiCodebaseSummarization"),
    root_dir=pathlib.Path("./your_projects_dir"),
    output_dir=pathlib.Path("./your_output_dir"),
)
```

### Use a Different LLM

Set the `LLM_MODEL` environment variable to any LiteLLM-supported model:

```sh
# OpenAI
export LLM_MODEL=gpt-4o

# Anthropic
export LLM_MODEL=anthropic/claude-3-5-sonnet

# Local (Ollama)
export LLM_MODEL=ollama/llama3.2
```

## How It Works

```mermaid
graph TD
    %% App: MultiCodebaseSummarization
    app_main[<b>app_main</b>] ==> process_project[<b>process_project</b>]
    process_project ==> extract_file_info[<b>extract_file_info</b>]
    process_project ==> aggregate_project_info[<b>aggregate_project_info</b>]
    process_project --> generate_markdown[generate_markdown]
```

1. **app_main**: Lists subdirectories, sets up output target, mounts `process_project` for each
2. **process_project**: Extracts info from each file, aggregates, outputs markdown
3. **extract_file_info**: Uses instructor + LLM to extract `CodebaseInfo` from each file
4. **aggregate_project_info**: Combines file `CodebaseInfo` into project-level `CodebaseInfo`
5. **generate_markdown**: Converts `CodebaseInfo` to markdown and calls `declare_file`
