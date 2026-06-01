"""Property type system + schema for the Notion target.

Each PropType encodes a single Notion property: its Notion API type name,
how to encode a Python value into the property's JSON shape, and how to
decode it back (used by query-on-miss to extract the primary key from an
existing page returned by ``POST /v1/data_sources/{id}/query``).

The MVP supports the property types you'd reach for first:
title, rich_text, number, url, email, date, select, multi_select, checkbox.
Relation, people, and files can land in a follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Generic, get_args, get_origin, get_type_hints

from typing_extensions import Annotated, TypeVar

RowT = TypeVar("RowT")


# ---------------------------------------------------------------------------
# Property type base + concrete classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PropType:
    """Base class for Notion property type bindings.

    Subclasses set `notion_type` to the Notion API's property type string
    (e.g. ``"title"``, ``"rich_text"``, ``"select"``) and implement
    ``encode``/``decode``.
    """

    name: str  # The property's name as it appears in the Notion data source
    notion_type: str = ""

    def encode(self, value: Any) -> dict[str, Any]:
        """Return the Notion property JSON body for this value."""
        raise NotImplementedError

    def decode(self, prop_json: dict[str, Any]) -> Any:
        """Extract the Python value from a Notion property JSON body."""
        raise NotImplementedError


@dataclass(frozen=True)
class TitleProp(PropType):
    """The data source's single title property (every Notion DS has exactly one)."""

    notion_type: str = "title"

    def encode(self, value: Any) -> dict[str, Any]:
        text = "" if value is None else str(value)
        return {"title": [{"text": {"content": text[:2000]}}]}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        parts = prop_json.get("title") or []
        return "".join(p.get("plain_text", "") for p in parts)


@dataclass(frozen=True)
class RichTextProp(PropType):
    notion_type: str = "rich_text"

    def encode(self, value: Any) -> dict[str, Any]:
        text = "" if value is None else str(value)
        return {"rich_text": [{"text": {"content": text[:2000]}}]}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        parts = prop_json.get("rich_text") or []
        return "".join(p.get("plain_text", "") for p in parts)


@dataclass(frozen=True)
class NumberProp(PropType):
    notion_type: str = "number"

    def encode(self, value: Any) -> dict[str, Any]:
        return {"number": None if value is None else float(value)}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        return prop_json.get("number")


@dataclass(frozen=True)
class UrlProp(PropType):
    notion_type: str = "url"

    def encode(self, value: Any) -> dict[str, Any]:
        return {"url": None if value is None else str(value)}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        return prop_json.get("url")


@dataclass(frozen=True)
class EmailProp(PropType):
    notion_type: str = "email"

    def encode(self, value: Any) -> dict[str, Any]:
        return {"email": None if value is None else str(value)}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        return prop_json.get("email")


@dataclass(frozen=True)
class SelectProp(PropType):
    notion_type: str = "select"

    def encode(self, value: Any) -> dict[str, Any]:
        return {"select": None if value is None else {"name": str(value)}}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        sel = prop_json.get("select")
        return sel.get("name") if sel else None


@dataclass(frozen=True)
class MultiSelectProp(PropType):
    notion_type: str = "multi_select"

    def encode(self, value: Any) -> dict[str, Any]:
        items = list(value or [])
        return {"multi_select": [{"name": str(v)} for v in items]}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        return [opt.get("name") for opt in (prop_json.get("multi_select") or [])]


@dataclass(frozen=True)
class CheckboxProp(PropType):
    notion_type: str = "checkbox"

    def encode(self, value: Any) -> dict[str, Any]:
        return {"checkbox": bool(value)}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        return bool(prop_json.get("checkbox"))


@dataclass(frozen=True)
class DateProp(PropType):
    notion_type: str = "date"

    def encode(self, value: Any) -> dict[str, Any]:
        if value is None:
            return {"date": None}
        if isinstance(value, datetime):
            return {"date": {"start": value.isoformat()}}
        if isinstance(value, date):
            return {"date": {"start": value.isoformat()}}
        return {"date": {"start": str(value)}}

    def decode(self, prop_json: dict[str, Any]) -> Any:
        d = prop_json.get("date")
        if not d:
            return None
        return d.get("start")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseSchema(Generic[RowT]):
    """A bound mapping from a Python class's fields onto Notion properties.

    Built via :meth:`from_class` — the user either annotates each field with
    ``Annotated[T, SomePropType(...)]`` or passes a ``property_map`` override.
    """

    record_type: type[RowT]
    primary_key: tuple[str, ...]
    properties: tuple[tuple[str, PropType], ...]  # ordered: (field_name, prop)

    @property
    def properties_by_field(self) -> dict[str, PropType]:
        return {f: p for f, p in self.properties}

    @property
    def properties_by_notion_name(self) -> dict[str, PropType]:
        return {p.name: p for _, p in self.properties}

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        primary_key: list[str],
        *,
        property_map: dict[str, PropType] | None = None,
    ) -> "DatabaseSchema[RowT]":
        """Build a DatabaseSchema from a dataclass / NamedTuple / similar.

        Order of precedence for the property binding of each field:
        1. ``property_map[field_name]`` if provided
        2. ``Annotated[T, PropType(...)]`` metadata on the field

        Either source must yield exactly one ``PropType`` per field. Fields
        with neither are skipped (allowing the dataclass to carry transient
        state that doesn't land in Notion).
        """
        property_map = property_map or {}
        hints = get_type_hints(record_type, include_extras=True)

        # Catch property_map keys that don't name an actual field — almost
        # always a typo and silently swallowing it leads to "why are my rows
        # missing this column?" debugging.
        unknown_pm_keys = set(property_map) - set(hints)
        if unknown_pm_keys:
            raise ValueError(
                f"{record_type.__name__}: property_map keys {sorted(unknown_pm_keys)} "
                f"do not match any field. Known fields: {sorted(hints)}."
            )

        bindings: list[tuple[str, PropType]] = []
        for field_name, type_hint in hints.items():
            if field_name in property_map:
                bindings.append((field_name, property_map[field_name]))
                continue
            if get_origin(type_hint) is Annotated:
                for meta in get_args(type_hint)[1:]:
                    if isinstance(meta, PropType):
                        bindings.append((field_name, meta))
                        break

        if not bindings:
            raise ValueError(
                f"{record_type.__name__}: no Notion properties found. "
                "Annotate fields with Annotated[T, notion.SomeProp(...)] "
                "or pass property_map={...}."
            )

        # Sanity: at most one title property.
        titles = [f for f, p in bindings if p.notion_type == "title"]
        if len(titles) > 1:
            raise ValueError(
                f"{record_type.__name__}: only one TitleProp allowed; got {titles}"
            )

        # Sanity: every PK field must be in bindings.
        binding_fields = {f for f, _ in bindings}
        missing_pk = [pk for pk in primary_key if pk not in binding_fields]
        if missing_pk:
            raise ValueError(
                f"{record_type.__name__}: primary_key fields {missing_pk} "
                "have no Notion property binding."
            )

        return cls(
            record_type=record_type,
            primary_key=tuple(primary_key),
            properties=tuple(bindings),
        )

    def encode_row(self, row: Any) -> dict[str, dict[str, Any]]:
        """Encode a row instance into Notion's ``properties`` body."""
        out: dict[str, dict[str, Any]] = {}
        for field_name, prop in self.properties:
            if isinstance(row, dict):
                value = row.get(field_name)
            else:
                value = getattr(row, field_name, None)
            out[prop.name] = prop.encode(value)
        return out

    def extract_pk(self, page_properties: dict[str, Any]) -> tuple[Any, ...]:
        """Pull the primary-key tuple out of a Notion page's properties payload."""
        prop_by_field = self.properties_by_field
        return tuple(
            prop_by_field[pk].decode(page_properties.get(prop_by_field[pk].name, {}))
            for pk in self.primary_key
        )

    def validate_against(self, notion_schema: dict[str, dict[str, Any]]) -> None:
        """Check the declared schema against the live Notion data source.

        ``notion_schema`` is the ``properties`` map from
        ``GET /v1/data_sources/{id}`` — keys are property names, values look like
        ``{"id": ..., "name": ..., "type": "select", "select": {...}}``.

        Errors loudly on two cases that would otherwise fail silently at write
        time:

        - Declared property name doesn't exist in Notion.
        - Declared property name exists but with a different Notion type.

        Extra properties in Notion that the schema doesn't declare are left
        alone (extra-columns-on-target is fine — they aren't touched).
        """
        missing: list[str] = []
        type_mismatch: list[tuple[str, str, str]] = []
        for _, prop in self.properties:
            notion_prop = notion_schema.get(prop.name)
            if notion_prop is None:
                missing.append(prop.name)
                continue
            actual_type = notion_prop.get("type")
            if actual_type != prop.notion_type:
                type_mismatch.append((prop.name, prop.notion_type, str(actual_type)))

        if not missing and not type_mismatch:
            return

        parts: list[str] = []
        if missing:
            available = sorted(notion_schema)
            parts.append(
                f"missing properties {sorted(missing)} (data source has: {available})"
            )
        if type_mismatch:
            parts.append(
                "type mismatches: "
                + ", ".join(
                    f"{name!r} declared {declared!r} but Notion has {actual!r}"
                    for name, declared, actual in type_mismatch
                )
            )
        raise ValueError(
            f"{self.record_type.__name__} schema does not match the Notion data "
            f"source: " + "; ".join(parts)
        )
