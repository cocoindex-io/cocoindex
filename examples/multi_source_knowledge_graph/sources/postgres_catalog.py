"""Rows of a catalog table in Postgres, one Entity per row."""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass

import asyncpg

import cocoindex as coco
from cocoindex.connectors import postgres

from records import Entity
from sources.base import EntitySource


@dataclass
class CatalogRef:
    """A listing entry: the key and the row's last-writer transaction id."""

    key: str
    xmin: int


@dataclass(frozen=True)
class PostgresCatalog(EntitySource[CatalogRef]):
    pool: asyncpg.Pool
    table: str

    def __coco_memo_key__(self) -> object:
        # The pool is a connection, not an input: memoize on what names the source.
        return (self.name, self.table)

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, CatalogRef]]:
        # Two columns per row. xmin changes whenever the row is updated, so it
        # is a free change token: no updated_at column to maintain.
        rows = postgres.PgTableSource(
            self.pool,
            table_name=self.table,
            columns=["key", "xmin"],
            row_type=CatalogRef,
        )
        return rows.fetch_rows().items(lambda ref: ref.key)

    @coco.fn.as_async(batching=True, max_batch_size=100)
    async def fetch(self, refs: list[CatalogRef]) -> list[Entity]:
        # Called with one ref at a time; concurrent misses across refs are
        # batched into one query. Results line up with the input order. The
        # batched body runs outside any component context, which is why the
        # pool lives on the adapter rather than being looked up here.
        rows = await self.pool.fetch(
            f"SELECT key, name, kind, area, depends_on FROM {self.table}"
            " WHERE key = ANY($1)",
            [ref.key for ref in refs],
        )
        by_key = {row["key"]: row for row in rows}
        return [
            Entity(
                key=ref.key,
                name=by_key[ref.key]["name"],
                kind=by_key[ref.key]["kind"],
                area=by_key[ref.key]["area"],
                depends_on=tuple(by_key[ref.key]["depends_on"]),
            )
            for ref in refs
        ]
