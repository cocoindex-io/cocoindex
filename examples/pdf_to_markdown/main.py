"""
PDF to Markdown (v1) - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Output markdown files to an output folder
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import AsyncIterator

from docling.document_converter import DocumentConverter

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


_converter: DocumentConverter | None = None


def get_converter() -> DocumentConverter:
    global _converter
    if _converter is None:
        _converter = DocumentConverter()
    return _converter


def pdf_to_markdown(pdf_path: str) -> str:
    converter = get_converter()
    result = converter.convert(pdf_path)
    return result.document.export_to_markdown()


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function(memo=True)
def process_file(
    scope: coco.Scope,
    file: FileLike,
    sourcedir: pathlib.Path,
    target: localfs.DirTarget,
) -> None:
    # Get absolute path of the PDF file
    pdf_path = str(sourcedir / file.relative_path)
    markdown = pdf_to_markdown(pdf_path)
    # Replace .pdf extension with .md
    outname = file.relative_path.stem + ".md"
    target.declare_file(scope, filename=outname, content=markdown)


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", outdir
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )
    for f in files:
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, sourcedir, target)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="PdfToMarkdown"),
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)


async def main() -> None:
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
