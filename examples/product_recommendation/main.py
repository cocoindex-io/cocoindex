"""
Product Recommendation Graph (v1) — CocoIndex pipeline example, Neo4j.

For each product (a JSON file) an LLM extracts two kinds of taxonomy:
  - taxonomies: what the product *is* (e.g. "pen", "notebook")
  - complementary_taxonomies: what a buyer might also want (e.g. "ink refill")

These become a product knowledge graph in Neo4j that powers recommendations
("people who bought a pen often need ink refills"):

  Product  nodes — one per product (title, price)
  Taxonomy nodes — one per distinct taxonomy label
  Relationships:
    PRODUCT_TAXONOMY               Product -> Taxonomy   (what it is)
    PRODUCT_COMPLEMENTARY_TAXONOMY Product -> Taxonomy   (what pairs with it)

The pipeline runs in two phases:
  1. Per-product extraction declares each Product node and carries its
     taxonomy labels forward.
  2. A single graph-building pass declares the deduplicated Taxonomy nodes and
     the relationship edges across all products.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import instructor
import litellm
import pydantic
from jinja2 import Template

import cocoindex as coco
from cocoindex.connectors import localfs, neo4j
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

litellm.drop_params = True


# ---------------------------------------------------------------------------
# Context keys
# ---------------------------------------------------------------------------

KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(
        KG_DB,
        neo4j.ConnectionFactory(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ.get("NEO4J_PASSWORD", "cocoindex"),
            ),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        ),
    )
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-4.1"))
    yield


# ---------------------------------------------------------------------------
# Neo4j node / edge schemas
# ---------------------------------------------------------------------------


@dataclass
class Product:
    id: str  # primary key — the filename stem
    title: str
    price: float


@dataclass
class Taxonomy:
    value: str  # primary key — the taxonomy label


# PRODUCT_TAXONOMY and PRODUCT_COMPLEMENTARY_TAXONOMY carry no payload — the
# connector derives each edge's PK from (from_id, to_id): one edge per pair.


# ---------------------------------------------------------------------------
# LLM extraction schema (Pydantic, for instructor)
# ---------------------------------------------------------------------------


class ProductTaxonomy(pydantic.BaseModel):
    name: str = pydantic.Field(
        description=(
            "A concise noun (or short noun phrase) for the product's core "
            "functionality, without branding or style. Most common US English, "
            "lowercase, no punctuation unless a proper noun or acronym. Avoid "
            "broad categories like 'office supplies'; prefer specific ones like "
            "'pen' or 'printer'."
        )
    )


class ProductTaxonomyInfo(pydantic.BaseModel):
    taxonomies: list[ProductTaxonomy] = pydantic.Field(
        description="Taxonomies describing what this product is."
    )
    complementary_taxonomies: list[ProductTaxonomy] = pydantic.Field(
        description=(
            "Taxonomies for complementary products a buyer of this product "
            "might also need."
        )
    )


TAXONOMY_PROMPT = """\
You are an expert at categorizing retail products. Given the product details,
extract the taxonomies that describe what the product is, and the complementary
taxonomies a buyer might also need. Return only what the text supports.
"""

PRODUCT_TEMPLATE = Template(
    """\
# {{ title }}

## Highlights
{% for highlight in highlights %}
- {{ highlight }}
{% endfor %}

## Description
{{ description.header | default('') }}
{{ description.paragraph | default('') }}
{% for bullet in description.bullets %}
- {{ bullet }}
{% endfor %}
"""
)


@coco.fn(memo=True)
async def extract_taxonomy(detail: str) -> ProductTaxonomyInfo:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=ProductTaxonomyInfo,
        messages=[
            {"role": "system", "content": TAXONOMY_PROMPT},
            {"role": "user", "content": detail},
        ],
    )
    return ProductTaxonomyInfo.model_validate(result.model_dump())


# ---------------------------------------------------------------------------
# Internal transfer type (Phase 1 -> Phase 2)
# ---------------------------------------------------------------------------


@dataclass
class ProductTaxonomies:
    product_id: str
    taxonomies: list[str]
    complementary: list[str]


# ---------------------------------------------------------------------------
# Phase 1: per-product extraction — declare Product nodes, carry labels forward
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    product_table: neo4j.TableTarget[Product],
) -> ProductTaxonomies:
    raw = json.loads(await file.read_text())
    product_id = file.file_path.path.name.removesuffix(".json")
    price = float(str(raw["price"]).lstrip("$").replace(",", ""))

    product_table.declare_record(
        row=Product(id=product_id, title=raw["title"], price=price)
    )

    info = await extract_taxonomy(PRODUCT_TEMPLATE.render(**raw))
    return ProductTaxonomies(
        product_id=product_id,
        taxonomies=[t.name for t in info.taxonomies],
        complementary=[t.name for t in info.complementary_taxonomies],
    )


# ---------------------------------------------------------------------------
# Phase 2: build the graph — Taxonomy nodes + relationship edges
# ---------------------------------------------------------------------------


@coco.fn
async def build_graph(
    products: list[ProductTaxonomies],
    taxonomy_table: neo4j.TableTarget[Taxonomy],
    product_taxonomy_rel: neo4j.RelationTarget[Any],
    complementary_rel: neo4j.RelationTarget[Any],
) -> None:
    labels: set[str] = set()
    for p in products:
        labels.update(p.taxonomies)
        labels.update(p.complementary)
    for value in labels:
        taxonomy_table.declare_record(row=Taxonomy(value=value))

    for p in products:
        for t in set(p.taxonomies):
            product_taxonomy_rel.declare_relation(from_id=p.product_id, to_id=t)
        for t in set(p.complementary):
            complementary_rel.declare_relation(from_id=p.product_id, to_id=t)


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    product_table = await neo4j.mount_table_target(
        KG_DB,
        "Product",
        await neo4j.TableSchema.from_class(Product, primary_key="id"),
        primary_key="id",
    )
    taxonomy_table = await neo4j.mount_table_target(
        KG_DB,
        "Taxonomy",
        await neo4j.TableSchema.from_class(Taxonomy, primary_key="value"),
        primary_key="value",
    )
    product_taxonomy_rel = await neo4j.mount_relation_target(
        KG_DB, "PRODUCT_TAXONOMY", product_table, taxonomy_table
    )
    complementary_rel = await neo4j.mount_relation_target(
        KG_DB, "PRODUCT_COMPLEMENTARY_TAXONOMY", product_table, taxonomy_table
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.json"]),
    )
    file_coros = []
    async for path_key, file in files.items():
        file_coros.append(
            coco.use_mount(
                coco.component_subpath("file", path_key),
                process_file,
                file,
                product_table,
            )
        )
    products: list[ProductTaxonomies] = list(await asyncio.gather(*file_coros))

    await coco.mount(
        coco.component_subpath("build_graph"),
        build_graph,
        products,
        taxonomy_table,
        product_taxonomy_rel,
        complementary_rel,
    )


app = coco.App(
    coco.AppConfig(name="ProductRecommendation"),
    app_main,
    sourcedir=pathlib.Path("./products"),
)
