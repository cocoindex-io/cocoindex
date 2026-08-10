"""
PDF to Entity-Linked Markdown - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Resolve entities against a CocoIndex-managed entity table
- Output markdown files with hyperlinks to an output folder
"""

from __future__ import annotations

import asyncio
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
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@127.0.0.1:55432/cocoindex"
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
    """One canonical entity.

    ``name`` is the identity — it is the primary key, so two documents
    mentioning the same person converge on one row. ``id`` is the payload
    each document *proposes*; exactly one proposal survives.
    """

    name: str
    id: str


# How long to wait for the winning writer's row to become visible before
# giving up and retrying the whole lookup. Private to this example: the
# engine exposes no retry knobs, and none are needed for correctness.
_RESOLVE_ATTEMPTS = 20
_RESOLVE_BACKOFF_SECONDS = 0.05


async def _lookup_entity_id(pool: asyncpg.Pool, name: str) -> str | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT id FROM {PG_SCHEMA}.entities WHERE name = $1", name
        )


async def _resolve_entity_id(
    entity_table: postgres.TableTarget[Entity], name: str
) -> str:
    """Get-or-create the canonical id for ``name``.

    Every document that mentions the same name proposes its *own* random
    UUID, and they are processed concurrently — so this is a genuine race.
    Two CocoIndex features settle it:

    * ``optimistic_declare_row`` elects exactly one winner for the
      name, in CocoIndex's own store rather than in Postgres;
    * the winner's write is *optimistic*, so its row is visible to a plain
      ``SELECT`` from the sibling components while everyone is still
      processing — no one has to wait for a commit phase.

    A loser therefore just looks again: either the winner's row is already
    there, or the winner failed and released the name, in which case this
    component gets to try for it.
    """
    pool = coco.use_context(PG_DB)

    for _ in range(_RESOLVE_ATTEMPTS):
        existing = await _lookup_entity_id(pool, name)
        if existing is not None:
            # Keep the confirmed target state in this component's ordinary
            # declaration set. Optimistic CAS is only needed for creation.
            entity_table.declare_row(row=Entity(name=name, id=existing))
            return existing

        # Nothing there yet — propose an id and race for the name.
        proposed = str(uuid.uuid4())
        if await entity_table.optimistic_declare_row(
            row=Entity(name=name, id=proposed)
        ):
            # We won; the row is already written and visible to siblings.
            return proposed

        # Someone else won. Give their write a moment to land, then look
        # again. `asyncio.sleep` keeps this cancellation-friendly.
        await asyncio.sleep(_RESOLVE_BACKOFF_SECONDS)

    raise RuntimeError(f"could not resolve an entity id for {name!r}")


@coco.fn(memo=True)
async def process_file(
    file: localfs.File,
    entity_table: postgres.TableTarget[Entity],
    outdir: pathlib.Path,
) -> None:
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()

    links: dict[str, str] = {}
    for name in _ENTITIES_TO_EXTRACT:
        if name in markdown:
            links[name] = await _resolve_entity_id(entity_table, name)

    md = markdown
    for name, entity_id in links.items():
        md = md.replace(name, f"[{name}](entities/{entity_id})")

    outname = file.file_path.path.stem + ".md"
    localfs.declare_file(outdir / outname, md, create_parent_dirs=True)


@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    entity_table = await postgres.mount_table_target(
        PG_DB,
        "entities",
        await postgres.TableSchema.from_class(Entity, primary_key=["name"]),
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
