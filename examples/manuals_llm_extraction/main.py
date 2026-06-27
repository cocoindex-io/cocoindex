"""
Manuals LLM Extraction (v1) — CocoIndex pipeline example.

Turn a folder of PDF manuals into structured records: convert each PDF to
Markdown with docling, then LLM-extract a typed module summary (title,
description, classes, and methods with their arguments) and store it in
Postgres. The sample manuals are the reference docs for a few Python standard
library modules (array, base64, copy).

Index (use `-L` for live mode, omit for one-shot catch-up):
    cocoindex update main
"""

from __future__ import annotations

import functools
import io
import json
import os
import pathlib
from collections.abc import AsyncIterator
from dataclasses import dataclass

import asyncpg
import instructor
import litellm
import pydantic
from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

litellm.drop_params = True

TABLE_NAME = "modules_info"
PG_SCHEMA_NAME = "coco_examples"

PG_DB = coco.ContextKey[asyncpg.Pool]("manuals_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)


# ---------------------------------------------------------------------------
# PDF -> Markdown (docling, on a GPU runner)
# ---------------------------------------------------------------------------


@functools.cache
def pdf_converter() -> DocumentConverter:
    options = PdfPipelineOptions(
        accelerator_options=AcceleratorOptions(device=AcceleratorDevice.CPU)
    )
    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)}
    )


@coco.fn.as_async(runner=coco.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="manual.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()


# ---------------------------------------------------------------------------
# LLM extraction schema (Pydantic, for instructor)
# ---------------------------------------------------------------------------


class ArgInfo(pydantic.BaseModel):
    name: str
    description: str = ""


class MethodInfo(pydantic.BaseModel):
    name: str
    args: list[ArgInfo] = pydantic.Field(default_factory=list)
    description: str = ""


class ClassInfo(pydantic.BaseModel):
    name: str
    description: str = ""
    methods: list[MethodInfo] = pydantic.Field(default_factory=list)


class ModuleInfo(pydantic.BaseModel):
    title: str
    description: str
    classes: list[ClassInfo] = pydantic.Field(default_factory=list)
    methods: list[MethodInfo] = pydantic.Field(default_factory=list)


EXTRACT_PROMPT = (
    "Extract structured Python module information from the manual: a title, a "
    "one-paragraph description, the public classes (with their methods), and the "
    "module-level functions (with their arguments). Use only what the text supports."
)


@coco.fn(memo=True)
async def extract_module(markdown: str) -> ModuleInfo:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=ModuleInfo,
        messages=[
            {"role": "system", "content": EXTRACT_PROMPT},
            {"role": "user", "content": markdown},
        ],
    )
    return ModuleInfo.model_validate(result.model_dump())


# ---------------------------------------------------------------------------
# Postgres row
# ---------------------------------------------------------------------------


@dataclass
class ModuleRecord:
    filename: str  # primary key
    title: str
    description: str
    num_classes: int
    num_methods: int
    module_info: str  # the full ModuleInfo as JSON


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[ModuleRecord],
) -> None:
    markdown = await pdf_to_markdown(await file.read())
    info = await extract_module(markdown)
    table.declare_row(
        row=ModuleRecord(
            filename=file.file_path.path.name,
            title=info.title,
            description=info.description,
            num_classes=len(info.classes),
            num_methods=len(info.methods),
            module_info=json.dumps(info.model_dump()),
        )
    )


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(os.environ["POSTGRES_URL"]) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-4o"))
        yield


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            ModuleRecord, primary_key=["filename"]
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), table)


app = coco.App(
    coco.AppConfig(name="ManualsLlmExtraction"),
    app_main,
    sourcedir=pathlib.Path("./manuals"),
)
