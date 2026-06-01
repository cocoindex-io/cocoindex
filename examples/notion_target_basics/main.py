"""Minimal example for the cocoindex Notion target connector.

Declares three rows against a Notion database (data source). On the first
run, the rows are created. On subsequent runs with identical rows, nothing
happens. Modify the ``PEOPLE`` list below to see how reconciliation works:

- Edit a row's value -> CocoIndex PATCHes the corresponding Notion page.
- Remove a row -> CocoIndex archives the page (or hard-deletes, or leaves
  it untouched, depending on the ``on_delete`` strategy).
- Add a row -> CocoIndex creates a new page.

Setup
-----
1. Create a Notion database with the properties: ``Name`` (title),
   ``Email`` (email), ``Role`` (select), ``Active`` (checkbox).
2. Share it with your integration (top-right ··· -> Connections).
3. Grab the data source ID from the URL (or via
   ``GET /v1/databases/{id}/data_sources``).
4. Export ``NOTION_TOKEN`` and ``NOTION_DATA_SOURCE_ID``.
5. ``cocoindex update main.py:NotionTargetBasics``
"""

import os
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator

from typing_extensions import Annotated

import cocoindex as coco
from cocoindex.connectors import notion

notion_client = coco.ContextKey[notion.NotionClient]("notion")


@dataclass(frozen=True)
class Person:
    """One row in the Notion database."""

    name: Annotated[str, notion.TitleProp("Name")]
    email: Annotated[str, notion.EmailProp("Email")]
    role: Annotated[str, notion.SelectProp("Role")]
    active: Annotated[bool, notion.CheckboxProp("Active")]


PEOPLE: list[Person] = [
    Person(name="Ada Lovelace", email="ada@example.com", role="Engineer", active=True),
    Person(
        name="Grace Hopper", email="grace@example.com", role="Engineer", active=True
    ),
    Person(
        name="Alan Turing", email="alan@example.com", role="Researcher", active=False
    ),
]


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    async with notion.NotionClient(token=os.environ["NOTION_TOKEN"]) as client:
        builder.provide(notion_client, client)
        yield


@coco.fn
async def app_main() -> None:
    target = await notion.mount_database_target(
        notion_client,
        os.environ["NOTION_DATA_SOURCE_ID"],
        await notion.DatabaseSchema.from_class(Person, primary_key=["name"]),
    )
    for person in PEOPLE:
        target.declare_row(row=person)


app = coco.App(
    coco.AppConfig(name="NotionTargetBasics"),
    app_main,
)
