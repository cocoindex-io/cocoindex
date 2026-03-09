"""Tests for attachment providers on target states."""

import pytest

import cocoindex as coco

from typing import Any

from tests import common
from tests.common.target_states import (
    AttachmentDictsTarget,
    MultiAttachmentDictsTarget,
    DictsTarget,
    DictDataWithPrev,
)

coco_env = common.create_test_env(__file__)


_source_data: dict[str, dict[str, Any]] = {}


async def _declare_with_attachments() -> None:
    for name, att_data in _source_data.items():
        dict_provider = await coco.use_mount(
            coco.component_subpath(name),
            AttachmentDictsTarget.declare_dict_target,
            name,
        )
        att_provider = dict_provider.attachment("items")
        for key, value in att_data.items():
            coco.declare_target_state(att_provider.target_state(key, value))


def test_attachment_basic_lifecycle() -> None:
    """Verifies insert -> update -> delete lifecycle for states under an attachment provider."""
    AttachmentDictsTarget.store.clear()
    _source_data.clear()

    app = coco.App(
        coco.AppConfig(name="test_attachment_basic_lifecycle", environment=coco_env),
        _declare_with_attachments,
    )

    # Run 1: Insert items a, b under D1
    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert AttachmentDictsTarget.store.attachment_data == {
        "D1": {
            "items": {
                "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
                "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            },
        },
    }
    assert AttachmentDictsTarget.store.metrics.collect() == {"sink": 1, "insert": 1}
    assert AttachmentDictsTarget.store.collect_attachment_metrics("items") == {
        "sink": 1,
        "upsert": 2,
    }

    # Run 2: Remove a, add c
    del _source_data["D1"]["a"]
    _source_data["D1"]["c"] = 3
    app.update_blocking()
    assert AttachmentDictsTarget.store.attachment_data == {
        "D1": {
            "items": {
                "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
                "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            },
        },
    }
    assert AttachmentDictsTarget.store.metrics.collect() == {"sink": 1}
    assert AttachmentDictsTarget.store.collect_attachment_metrics("items") == {
        "sink": 1,
        "delete": 1,
        "upsert": 1,
    }

    # Run 3: Remove outer target entirely
    _source_data.clear()
    app.update_blocking()
    assert AttachmentDictsTarget.store.attachment_data == {}
    assert AttachmentDictsTarget.store.metrics.collect() == {"sink": 1, "delete": 1}


def test_attachment_idempotent_provider() -> None:
    """Verifies calling provider.attachment(att_type) twice returns same provider."""
    AttachmentDictsTarget.store.clear()
    _source_data.clear()

    async def _declare_idempotent() -> None:
        dict_provider = await coco.use_mount(
            coco.component_subpath("D1"),
            AttachmentDictsTarget.declare_dict_target,
            "D1",
        )
        att1 = dict_provider.attachment("items")
        att2 = dict_provider.attachment("items")
        coco.declare_target_state(att1.target_state("a", 1))
        coco.declare_target_state(att2.target_state("b", 2))

    app = coco.App(
        coco.AppConfig(
            name="test_attachment_idempotent_provider", environment=coco_env
        ),
        _declare_idempotent,
    )
    app.update_blocking()

    assert AttachmentDictsTarget.store.attachment_data == {
        "D1": {
            "items": {
                "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
                "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            },
        },
    }


_multi_source_items: dict[str, dict[str, Any]] = {}
_multi_source_tags: dict[str, dict[str, Any]] = {}


async def _declare_with_multi_attachments() -> None:
    for name in set(_multi_source_items) | set(_multi_source_tags):
        dict_provider = await coco.use_mount(
            coco.component_subpath(name),
            MultiAttachmentDictsTarget.declare_dict_target,
            name,
        )
        if name in _multi_source_items:
            items_provider = dict_provider.attachment("items")
            for key, value in _multi_source_items[name].items():
                coco.declare_target_state(items_provider.target_state(key, value))
        if name in _multi_source_tags:
            tags_provider = dict_provider.attachment("tags")
            for key, value in _multi_source_tags[name].items():
                coco.declare_target_state(tags_provider.target_state(key, value))


def test_attachment_independent_types() -> None:
    """Verifies two different attachment types under the same parent don't interfere."""
    MultiAttachmentDictsTarget.store.clear()
    _multi_source_items.clear()
    _multi_source_tags.clear()

    app = coco.App(
        coco.AppConfig(name="test_attachment_independent_types", environment=coco_env),
        _declare_with_multi_attachments,
    )

    _multi_source_items["D1"] = {"x": 10, "y": 20}
    _multi_source_tags["D1"] = {"t1": "tag1", "t2": "tag2"}
    app.update_blocking()

    assert MultiAttachmentDictsTarget.store.attachment_data == {
        "D1": {
            "items": {
                "x": DictDataWithPrev(data=10, prev=[], prev_may_be_missing=True),
                "y": DictDataWithPrev(data=20, prev=[], prev_may_be_missing=True),
            },
            "tags": {
                "t1": DictDataWithPrev(data="tag1", prev=[], prev_may_be_missing=True),
                "t2": DictDataWithPrev(data="tag2", prev=[], prev_may_be_missing=True),
            },
        },
    }
    assert MultiAttachmentDictsTarget.store.collect_attachment_metrics("items") == {
        "sink": 1,
        "upsert": 2,
    }
    assert MultiAttachmentDictsTarget.store.collect_attachment_metrics("tags") == {
        "sink": 1,
        "upsert": 2,
    }


def test_attachment_no_support_returns_none() -> None:
    """Verifies that calling attachment on a handler that doesn't support it raises an error."""
    DictsTarget.store.clear()

    async def _declare_unsupported() -> None:
        dict_provider = await coco.use_mount(
            coco.component_subpath("D1"),
            DictsTarget.declare_dict_target,
            "D1",
        )
        dict_provider.attachment("nonexistent")

    app = coco.App(
        coco.AppConfig(
            name="test_attachment_no_support_returns_none", environment=coco_env
        ),
        _declare_unsupported,
    )
    with pytest.raises(Exception, match="does not support attachment type"):
        app.update_blocking()
