"""
PDF to Markdown (v1) - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Output markdown files to an output folder
"""

from __future__ import annotations

import pathlib

from docling.document_converter import DocumentConverter

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher


_converter = DocumentConverter()


@coco.function(memo=True)
def process_file(
    file: localfs.File,
    outdir: pathlib.Path,
) -> None:
    # Get absolute path of the PDF file
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()
    # Replace .pdf extension with .md
    outname = file.file_path.path.stem + ".md"
    localfs.declare_file(outdir / outname, markdown, create_parent_dirs=True)


@coco.function
def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )
    for f in files:
        coco.mount(
            coco.component_subpath("process", str(f.file_path.path)),
            process_file,
            f,
            outdir,
        )


app = coco.App(
    "PdfToMarkdown",
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
