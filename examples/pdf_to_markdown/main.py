"""
PDF to Markdown (v1) - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Output markdown files to an output folder
"""

from __future__ import annotations

import pathlib
from typing import Iterator

from docling.document_converter import DocumentConverter

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher


_converter = DocumentConverter()


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function(memo=True)
def process_file(
    scope: coco.Scope,
    file: localfs.File,
    target: localfs.DirTarget,
) -> None:
    # Get absolute path of the PDF file
    markdown = _converter.convert(file.path).document.export_to_markdown()
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
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, target)


app = coco.App(
    app_main,
    coco.AppConfig(name="PdfToMarkdown"),
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
