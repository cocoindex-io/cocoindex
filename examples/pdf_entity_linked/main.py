"""
PDF to Entity-Linked Markdown - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Resolve entities against a CocoIndex-managed entity table
- Output markdown files with hyperlinks to an output folder
"""

from __future__ import annotations

import os
import pathlib
import uuid
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg

from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.resources.file import PatternFilePathMatcher

DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
PG_SCHEMA = "entity_demo"
PG_DB = coco.ContextKey[asyncpg.Pool]("pdf_entity_linked_db")

_pipeline_options = PdfPipelineOptions(
    accelerator_options=AcceleratorOptions(device=AcceleratorDevice.CPU)
)
_converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=_pipeline_options)
    }
)

_ENTITIES_TO_EXTRACT = {"Albert Einstein"}


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield


@dataclass
class Entity:
    id: str
    name: str


@coco.fn(memo=True)
async def process_file(
    file: localfs.File,
    entity_table: postgres.TableTarget[Entity],
    outdir: pathlib.Path,
) -> None:
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()

    pool = coco.use_context(PG_DB)
    links: dict[str, str] = {}
    for name in _ENTITIES_TO_EXTRACT:
        if name not in markdown:
            continue

        # Query first: a concurrent component may have already written the entity
        # optimistically, making it immediately visible via a normal SELECT.
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT id FROM {PG_SCHEMA}.entities WHERE name = $1', name
            )

        if row:
            # Entity already exists — reuse its ID and declare ownership.
            entity_id = row["id"]
            entity_table.declare_row(row=Entity(id=entity_id, name=name))
        else:
            # Not found: write optimistically so concurrent sibling components
            # can discover it via SELECT without waiting for our submit phase.
            entity_id = str(uuid.uuid4())
            await entity_table.optimistic_declare_row(
                row=Entity(id=entity_id, name=name)
            )

        links[name] = entity_id

    md = markdown
    for name, eid in links.items():
        md = md.replace(name, f"[{name}](entities/{eid})")

    outname = file.file_path.path.stem + ".md"
    localfs.declare_file(outdir / outname, md, create_parent_dirs=True)


@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    entity_table = await postgres.mount_table_target(
        PG_DB,
        "entities",
        await postgres.TableSchema.from_class(Entity, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await coco.mount_each(process_file, files.items(), entity_table, outdir)


app = coco.App(
    "PdfEntityLinked",
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
