"""
Google Docs Link Graph (v1) — CocoIndex pipeline example.

Turn a Google Drive folder of Docs that link to each other into a graph in
Omnigraph:

  Doc      nodes — one per Google Doc, keyed by its Drive file ID
  LinksTo  edges — Doc -> Doc, one per other Doc it hyperlinks to

Index (one-shot catch-up; the google_drive source has no live mode):
    cocoindex update main

A link to a Doc outside the folder still lands: the connector writes a
key-only stub for the target, and the stub fills in once that Doc is synced.
"""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import cocoindex as coco
from cocoindex.connectors import google_drive, omnigraph

KG_DB = coco.ContextKey[omnigraph.ConnectionFactory]("doc_links_db")

# Matches a link to a Google Doc in the Markdown export, e.g.
# `https://docs.google.com/document/d/<file id>/edit?tab=t.0#heading=h.x`.
_DOC_LINK_RE = re.compile(r"docs\.google\.com/document/(?:u/\d+/)?d/([A-Za-z0-9_-]+)")


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    kg = omnigraph.ConnectionFactory(
        os.environ.get("OMNIGRAPH_URL", "http://127.0.0.1:8080"),
        graph_id=os.environ.get("OMNIGRAPH_GRAPH", "doc-links"),
        branch=os.environ.get("OMNIGRAPH_BRANCH", "main"),
        cluster=omnigraph.ClusterConfig(
            config_dir=os.environ.get("OMNIGRAPH_CLUSTER_DIR", "./cluster"),
            target=os.environ.get("OMNIGRAPH_CLUSTER_TARGET", "local"),
            schema_file=os.environ.get("OMNIGRAPH_SCHEMA_FILE", "doc-links.pg"),
        ),
    )
    builder.provide(KG_DB, kg)
    yield
    await kg.close()


@dataclass
class Doc:
    doc_id: str
    # Optional so a linked Doc outside the folder can exist as a key-only stub.
    title: str | None
    words: int | None


@coco.fn(memo=True)
async def process_doc(
    file: google_drive.DriveFile,
    docs: omnigraph.NodeTarget[Doc],
    links: omnigraph.EdgeTarget[Any],
) -> None:
    text = await file.read_text()
    doc_id = file.file_path.resolve()
    docs.declare_node(
        node=Doc(doc_id=doc_id, title=file.file_path.path.name, words=len(text.split()))
    )
    for target in sorted(set(_DOC_LINK_RE.findall(text)) - {doc_id}):
        links.declare_edge(from_id=doc_id, to_id=target)


@coco.fn
async def app_main() -> None:
    docs = await omnigraph.mount_node_target(
        KG_DB, "Doc", await omnigraph.NodeSchema.from_class(Doc, key="doc_id")
    )
    links = await omnigraph.mount_edge_target(KG_DB, "LinksTo", docs, docs)

    source = google_drive.GoogleDriveSource(
        service_account_credential_path=os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"],
        root_folder_ids=[
            folder.strip()
            for folder in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")
            if folder.strip()
        ],
        mime_types=["application/vnd.google-apps.document"],
    )
    await coco.mount_each(process_doc, source.items(), docs, links)


app = coco.App(coco.AppConfig(name="GDriveDocLinksOmnigraph"), app_main)
