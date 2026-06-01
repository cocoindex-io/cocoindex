"""Minimal example for the cocoindex Notion target connector.

CocoIndex manages the database for you (``managed_by="system"``, the default):
on the first run it creates a database titled "People" under the parent page
you point it at, then keeps the rows in sync on every run. Modify the
``PEOPLE`` list below to see how reconciliation works:

- Edit a row's value -> CocoIndex PATCHes the corresponding Notion page.
- Remove a row -> CocoIndex archives the page (or hard-deletes, or leaves
  it untouched, depending on the ``on_delete`` strategy).
- Add a row -> CocoIndex creates a new page.
- Add a field to ``Person`` -> CocoIndex adds the property to the database.

Setup
-----
1. Create (or pick) a Notion page to hold the database.
2. Share it with your integration (top-right ··· -> Connections). Every parent
   page in the path must be shared, not just the database.
3. Export ``NOTION_TOKEN`` and ``NOTION_PARENT_PAGE`` (the page's ID, from its
   URL).
4. ``cocoindex update main.py:NotionTargetBasics``

To point at an existing database instead of creating one, pass
``managed_by="user"`` with a ``data_source_id`` — see the connector docs.
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
    # managed_by="system" is the default — CocoIndex creates the "People"
    # database under the parent page on first run and evolves it as Person grows.
    target = await notion.mount_database_target(
        notion_client,
        schema=await notion.DatabaseSchema.from_class(Person, primary_key=["name"]),
        parent_page_id=os.environ["NOTION_PARENT_PAGE"],
        title="People",
    )
    for person in PEOPLE:
        target.declare_row(row=person)


app = coco.App(
    coco.AppConfig(name="NotionTargetBasics"),
    app_main,
)
