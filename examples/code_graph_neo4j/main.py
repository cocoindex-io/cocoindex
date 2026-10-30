"""Incrementally index syntactic C/C++ graph facts into Neo4j."""

import os
import pathlib
from collections.abc import AsyncIterator

import cocoindex as coco
from cocoindex.connectors import localfs, neo4j
from cocoindex.ops.code_graph import CodeEntity, CodeReference
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

from pipeline import parse_file

DB = coco.ContextKey[neo4j.ConnectionFactory]("code_graph_neo4j")


@coco.lifespan
async def lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    db = neo4j.ConnectionFactory(
        uri=os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]),
    )
    builder.provide(DB, db)
    try:
        yield
    finally:
        await db.close()


@coco.fn
async def index_file(
    file: FileLike,
    sourcedir: pathlib.Path,
    entities: neo4j.TableTarget[CodeEntity],
    references: neo4j.TableTarget[CodeReference],
    contains: neo4j.RelationTarget[None],
    calls: neo4j.RelationTarget[None],
    inherits: neo4j.RelationTarget[None],
) -> None:
    graph = await parse_file(
        file, file.file_path.path.relative_to(sourcedir).as_posix()
    )
    for entity in graph.entities:
        entities.declare_record(row=entity)
    for ref in graph.references:
        references.declare_record(row=ref)
    # Nodes must be synced before Neo4j MATCH can create edges to them.
    # The containing component's target-state sink orders node actions first.
    for entity in graph.entities:
        if entity.parent_id is not None:
            contains.declare_relation(from_id=entity.parent_id, to_id=entity.id)
    for ref in graph.references:
        if ref.source_id is not None:
            relation = calls if ref.kind == "call" else inherits
            relation.declare_relation(from_id=ref.source_id, to_id=ref.id)


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    entities = await neo4j.mount_table_target(
        DB,
        "CppEntity",
        await neo4j.TableSchema.from_class(CodeEntity, primary_key="id"),
    )
    references = await neo4j.mount_table_target(
        DB,
        "CppReference",
        await neo4j.TableSchema.from_class(CodeReference, primary_key="id"),
    )
    contains: neo4j.RelationTarget[None] = await neo4j.mount_relation_target(
        DB, "CPP_CONTAINS", entities, entities
    )
    calls: neo4j.RelationTarget[None] = await neo4j.mount_relation_target(
        DB, "CPP_CALLS_SPELLING", entities, references
    )
    inherits: neo4j.RelationTarget[None] = await neo4j.mount_relation_target(
        DB, "CPP_INHERITS_SPELLING", entities, references
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/*.c",
                "**/*.cc",
                "**/*.cpp",
                "**/*.cxx",
                "**/*.h",
                "**/*.hpp",
            ],
        ),
    )
    await coco.mount_each(
        index_file,
        files.items(),
        sourcedir,
        entities,
        references,
        contains,
        calls,
        inherits,
    )


app = coco.App(
    coco.AppConfig(name="CppSyntaxGraph"),
    app_main,
    sourcedir=pathlib.Path(os.environ.get("CODE_SOURCE_DIR", "code")),
)
