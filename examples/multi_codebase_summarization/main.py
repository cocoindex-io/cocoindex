"""
Multi-Codebase Summarization - CocoIndex Pipeline Example

This example demonstrates a CocoIndex pipeline that:
1. Scans subdirectories of a root directory (each expected to be a Python project)
2. For each project, extracts:
   - Public classes/functions with functionality summaries
   - Mermaid graphs for CocoIndex app/function call relationships
   - File-level summaries
3. Aggregates per-file extractions into a project summary
4. Outputs markdown documentation to output/PROJECT_NAME.md
"""

from __future__ import annotations

import asyncio
import os
import pathlib
from typing import Collection

import instructor
from litellm import acompletion

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

from models import CodebaseInfo


LLM_MODEL = os.environ.get("LLM_MODEL", "gemini/gemini-2.5-flash")

_instructor_client = instructor.from_litellm(acompletion, mode=instructor.Mode.JSON)


@coco.function(memo=True)
async def extract_file_info(file: FileLike) -> CodebaseInfo:
    """Extract structured information from a single Python file using LLM."""
    content = file.read_text()
    file_path = str(file.file_path.path)

    prompt = f"""Analyze the following Python file and extract structured information.

File path: {file_path}

```python
{content}
```

Instructions:
1. Identify all PUBLIC classes (not starting with _) and summarize their purpose
2. Identify all PUBLIC functions (not starting with _) and summarize their purpose
3. If this file contains CocoIndex apps (coco.App), create Mermaid graphs showing the
   function call relationships (see the mermaid_graphs field description for format)
4. Provide a brief summary of the file's purpose
"""

    result = await _instructor_client.chat.completions.create(
        model=LLM_MODEL,
        response_model=CodebaseInfo,
        messages=[{"role": "user", "content": prompt}],
    )
    return CodebaseInfo.model_validate(result.model_dump())


@coco.function
async def aggregate_project_info(
    project_name: str,
    file_infos: list[CodebaseInfo],
) -> CodebaseInfo:
    """Aggregate multiple file extractions into a project-level summary."""
    if not file_infos:
        return CodebaseInfo(
            name=project_name, summary="Empty project with no Python files."
        )

    # Single file - just update the name to be the project name
    if len(file_infos) == 1:
        info = file_infos[0]
        return CodebaseInfo(
            name=project_name,
            summary=info.summary,
            public_classes=info.public_classes,
            public_functions=info.public_functions,
            mermaid_graphs=info.mermaid_graphs,
        )

    # Multiple files - use LLM to create aggregated summary

    # Format file summaries for the prompt
    files_text = "\n\n".join(
        f"### {info.name}\n"
        f"Summary: {info.summary}\n"
        f"Classes: {', '.join(c.name for c in info.public_classes) or 'None'}\n"
        f"Functions: {', '.join(f.name for f in info.public_functions) or 'None'}"
        for info in file_infos
    )

    # Collect all mermaid graphs from files
    all_graphs = [g for info in file_infos for g in info.mermaid_graphs]

    prompt = f"""Aggregate the following Python files into a project-level summary.

Project name: {project_name}

Files:
{files_text}

Create a unified CodebaseInfo that:
1. Summarizes the overall project purpose (not individual files)
2. Lists the most important public classes across all files
3. Lists the most important public functions across all files
4. For mermaid_graphs: create a single unified graph showing how the CocoIndex
   components connect across the project (if applicable)
"""

    result = await _instructor_client.chat.completions.create(
        model=LLM_MODEL,
        response_model=CodebaseInfo,
        messages=[{"role": "user", "content": prompt}],
    )
    result = CodebaseInfo.model_validate(result.model_dump())

    # Keep original file-level graphs if LLM didn't generate a unified one
    if not result.mermaid_graphs and all_graphs:
        result.mermaid_graphs = all_graphs

    return result


@coco.function
def generate_markdown(
    project_name: str, info: CodebaseInfo, file_infos: list[CodebaseInfo]
) -> str:
    """Generate markdown documentation from project info."""
    lines = [
        f"# {project_name}",
        "",
        "## Overview",
        "",
        info.summary,
        "",
    ]

    # Components
    if info.public_classes or info.public_functions:
        lines.extend(["## Components", ""])

        if info.public_classes:
            lines.append("**Classes:**")
            for cls in info.public_classes:
                lines.append(f"- `{cls.name}`: {cls.summary}")
            lines.append("")

        if info.public_functions:
            lines.append("**Functions:**")
            for fn in info.public_functions:
                marker = " ★" if fn.is_coco_function else ""
                lines.append(f"- `{fn.signature}`{marker}: {fn.summary}")
            lines.append("")

    # Mermaid graphs
    if info.mermaid_graphs:
        lines.extend(["## CocoIndex Pipeline", ""])
        for graph in info.mermaid_graphs:
            # Ensure proper code fence wrapping (LLM may or may not include them)
            graph_content = graph.strip()
            if graph_content.startswith("```"):
                # Already has fences, use as-is
                lines.append(graph_content)
            else:
                # Add mermaid code fences
                lines.append("```mermaid")
                lines.append(graph_content)
                lines.append("```")
            lines.append("")

    # File details (if multiple files)
    if len(file_infos) > 1:
        lines.extend(["## File Details", ""])
        for fi in file_infos:
            lines.extend([f"### {fi.name}", "", fi.summary, ""])

    # Legend
    lines.extend(["---", "", "*★ = CocoIndex function*"])

    return "\n".join(lines)


@coco.function(memo=True)
async def process_project(
    project_name: str,
    files: Collection[localfs.File],
    output_dir: pathlib.Path,
) -> None:
    """Process a single project: extract info from all files, aggregate, and output markdown."""
    # Extract info from each file.
    file_infos = await asyncio.gather(*[extract_file_info(f) for f in files])

    # Aggregate into project-level summary
    project_info = await aggregate_project_info(project_name, file_infos)

    # Generate and output markdown
    markdown = generate_markdown(project_name, project_info, file_infos)
    localfs.declare_file(
        output_dir / f"{project_name}.md", markdown, create_parent_dirs=True
    )


@coco.function
def app_main(
    root_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> None:
    """
    Main application function.

    Scans subdirectories of root_dir, treating each as a Python project,
    and generates markdown documentation for each.
    """
    # List subdirectories (each is a project)
    for entry in root_dir.resolve().iterdir():
        # Skip non-directories and hidden directories
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        project_name = entry.name

        # Walk Python files in this project, excluding .venv directories
        files = list(
            localfs.walk_dir(
                entry,
                recursive=True,
                path_matcher=PatternFilePathMatcher(
                    included_patterns=["**/*.py"],
                    excluded_patterns=["**/.*", "**/__pycache__"],
                ),
            )
        )

        if files:
            # Mount a component to process this project
            coco.mount(
                coco.component_subpath("project", project_name),
                process_project,
                project_name,
                files,
                output_dir,
            )


app = coco.App(
    "MultiCodebaseSummarization",
    app_main,
    root_dir=pathlib.Path("../"),
    output_dir=pathlib.Path("./output"),
)
