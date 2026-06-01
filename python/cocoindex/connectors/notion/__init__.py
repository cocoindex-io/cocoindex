"""Notion target connector for CocoIndex.

Declarative target for Notion databases (data sources) with automatic
upsert + delete semantics, mirroring ``connectors.postgres``.

Quick start::

    from cocoindex.connectors import notion

    notion_client = coco.ContextKey[notion.NotionClient]("notion_main")

    @coco.lifespan
    async def lifespan(builder):
        async with notion.NotionClient(token=os.environ["NOTION_TOKEN"]) as c:
            builder.provide(notion_client, c)
            yield

    @dataclass(frozen=True)
    class AccountRow:
        domain: Annotated[str, notion.UrlProp("Domain")]
        name: Annotated[str, notion.TitleProp("Name")]

    @coco.fn
    async def app_main():
        accounts = await notion.mount_database_target(
            notion_client,
            "d7b662bc-3241-49d2-b41f-92aed710630e",
            await notion.DatabaseSchema.from_class(
                AccountRow, primary_key=["domain"]
            ),
        )
        accounts.declare_row(row=AccountRow(domain="anthropic.com", name="Anthropic"))
"""

from ._client import NotionClient, NOTION_API_VERSION
from ._target import (
    DatabaseTarget,
    ManagedBy,
    OnDelete,
    database_target,
    declare_database_target,
    mount_database_target,
)
from ._types import (
    CheckboxProp,
    DatabaseSchema,
    DateProp,
    EmailProp,
    MultiSelectProp,
    NumberProp,
    PropType,
    RelationProp,
    RichTextProp,
    SelectProp,
    TitleProp,
    UrlProp,
)

__all__ = [
    "CheckboxProp",
    "DatabaseSchema",
    "DatabaseTarget",
    "DateProp",
    "ManagedBy",
    "EmailProp",
    "MultiSelectProp",
    "NOTION_API_VERSION",
    "NotionClient",
    "NumberProp",
    "OnDelete",
    "PropType",
    "RelationProp",
    "RichTextProp",
    "SelectProp",
    "TitleProp",
    "UrlProp",
    "database_target",
    "declare_database_target",
    "mount_database_target",
]
