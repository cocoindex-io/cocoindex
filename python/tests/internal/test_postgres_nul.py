"""Postgres value encoding checks that do not need a database server."""

import copy
import json
from dataclasses import dataclass
from typing import Any

import pytest
import cocoindex as coco

try:
    from cocoindex.connectors import postgres
except ImportError:
    pytest.skip("asyncpg is not installed", allow_module_level=True)

from tests.common.target_states import GlobalDictTarget
from tests import common


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("nesting", ["root", "dict", "list", "tuple"])
def test_nul_key_collision_is_rejected(reverse: bool, nesting: str) -> None:
    pairs = [("a\x00b", "first"), ("ab", "second")]
    payload: Any = dict(reversed(pairs) if reverse else pairs)
    if nesting == "dict":
        payload = {"nested": payload}
    elif nesting == "list":
        payload = [payload]
    elif nesting == "tuple":
        payload = (payload,)
    original = copy.deepcopy(payload)

    with pytest.raises(ValueError, match="NUL.*duplicate.*key"):
        postgres._target._sanitize_nul(payload)
    with pytest.raises(ValueError, match="NUL.*duplicate.*key"):
        postgres._target._json_encoder(payload)
    assert payload == original


def test_noncolliding_keys_and_values_still_strip_nul() -> None:
    payload = {
        "a\x00b": ["x\x00y", {"n\x00ested": "val\x00ue"}],
        "different": ("text\x00", None, 42),
    }
    original = copy.deepcopy(payload)
    sanitized = postgres._target._sanitize_nul(payload)
    assert sanitized == {
        "ab": ["xy", {"nested": "value"}],
        "different": ("text", None, 42),
    }
    assert isinstance(sanitized["different"], tuple)
    assert json.loads(postgres._target._json_encoder(payload)) == {
        "ab": ["xy", {"nested": "value"}],
        "different": ["text", None, 42],
    }
    assert payload == original


def test_empty_key_collision_is_rejected_even_when_values_match() -> None:
    with pytest.raises(ValueError, match="NUL.*duplicate.*key"):
        postgres._target._json_encoder({"": 1, "\x00": 1})


@dataclass
class _Row:
    id: int
    payload: dict[str, str]


@pytest.mark.asyncio
@pytest.mark.parametrize("as_record", [False, True])
async def test_declare_row_rejects_collision_before_declaration(
    as_record: bool,
) -> None:
    schema = await postgres.TableSchema.from_class(_Row, primary_key=["id"])
    target = postgres.TableTarget(GlobalDictTarget._provider, "nul_test", schema)
    payload = {"a\x00b": "first", "ab": "second"}
    row: Any = _Row(1, payload) if as_record else {"id": 1, "payload": payload}
    env = common.create_test_env(__file__, suffix=str(as_record))

    @coco.fn
    def main() -> None:
        target.declare_row(row=row)

    app = coco.App(coco.AppConfig(name="nul_collision", environment=env), main)
    try:
        with pytest.raises(ValueError, match="NUL.*duplicate.*key"):
            await app.update()
    finally:
        env.close()
