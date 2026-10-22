"""Per-file extraction shared by the Neo4j example and lifecycle tests."""

import cocoindex as coco
from cocoindex.ops.code import CodeSource
from cocoindex.ops.code_graph import CodeGraph, extract_code_graph
from cocoindex.resources.file import FileLike


# Bump on native extractor/grammar upgrades: Python tracking cannot inspect Rust.
@coco.fn(memo=True, version=1)
async def parse_file(file: FileLike, path: str) -> CodeGraph:
    language = "c" if file.file_path.path.suffix == ".c" else "cpp"
    graph = extract_code_graph(
        CodeSource(await file.read_text(), language), file_path=path
    )
    if graph.has_errors:
        raise ValueError(f"Parse recovery in {path}; keeping previous graph")
    return graph
