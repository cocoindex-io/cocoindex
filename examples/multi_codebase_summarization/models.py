"""Pydantic models for multi-codebase summarization."""

from pydantic import BaseModel, Field


class FunctionInfo(BaseModel):
    """Information about a public function."""

    name: str = Field(description="Function name")
    signature: str = Field(
        description="Function signature, e.g. 'async def foo(x: int) -> str'"
    )
    is_coco_function: bool = Field(
        description="Whether decorated with @coco.function or @cocoindex.function"
    )
    summary: str = Field(description="Brief summary of what the function does")


class ClassInfo(BaseModel):
    """Information about a public class."""

    name: str = Field(description="Class name")
    summary: str = Field(description="Brief summary of what the class represents/does")


MERMAID_GRAPH_DESCRIPTION = """
Mermaid graph showing CocoIndex function call relationships. Requirements:
- Use 'graph TD' (top-down) layout
- Include ONLY functions defined in this codebase (not external libraries)
- Use **bold** text for @coco.function decorated functions
- Use thick arrows (==>) for coco.mount/coco.use_mount calls
- Use normal arrows (-->) for regular function calls
- Include a comment with the app name if this is for a coco.App

Example:
```mermaid
graph TD
    %% App: MyApp
    app_main[<b>app_main</b>] ==> process_file[<b>process_file</b>]
    process_file --> helper_func[helper_func]
```
"""


class CodebaseInfo(BaseModel):
    """Extracted information from Python code (file or project level)."""

    name: str = Field(
        description="File path (for files) or project name (for projects)"
    )
    summary: str = Field(description="Brief summary of purpose and functionality")
    public_classes: list[ClassInfo] = Field(
        default_factory=list,
        description="Public classes (not starting with _)",
    )
    public_functions: list[FunctionInfo] = Field(
        default_factory=list,
        description="Public functions (not starting with _)",
    )
    mermaid_graphs: list[str] = Field(
        default_factory=list,
        description=MERMAID_GRAPH_DESCRIPTION,
    )
