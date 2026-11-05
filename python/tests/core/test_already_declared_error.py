"""``coco.AlreadyDeclaredError``: one typed error for the "declared twice in
one run" family, so a caller branches on the type instead of on message text.
It subclasses ``ValueError``, so an existing ``except ValueError`` still
catches it.
"""

from __future__ import annotations

import pytest

import cocoindex as coco
from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)


def test_already_declared_error_is_a_value_error() -> None:
    assert issubclass(coco.AlreadyDeclaredError, ValueError)


def test_duplicate_target_state_raises_already_declared() -> None:
    GlobalDictTarget.store.clear()

    @coco.fn
    async def _root() -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("dup", 1))
        coco.declare_target_state(GlobalDictTarget.target_state("dup", 2))

    app = coco.App(
        coco.AppConfig(name="already_declared_target_state", environment=coco_env),
        _root,
    )
    with pytest.raises(coco.AlreadyDeclaredError, match="already declared"):
        app.update_blocking()


def test_duplicate_use_state_key_raises_already_declared() -> None:
    raised: list[BaseException] = []

    @coco.fn
    async def _root() -> None:
        coco.use_state("counter", 0)
        try:
            coco.use_state("counter", 0)
        except ValueError as exc:  # the subclass is caught by its base too
            raised.append(exc)

    app = coco.App(
        coco.AppConfig(name="already_declared_use_state", environment=coco_env),
        _root,
    )
    app.update_blocking()
    assert len(raised) == 1
    assert isinstance(raised[0], coco.AlreadyDeclaredError)
    assert "declared more than once" in str(raised[0])


def test_duplicate_mount_each_key_raises_already_declared() -> None:
    GlobalDictTarget.store.clear()

    @coco.fn
    async def _child(value: int) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("k", value))

    @coco.fn
    async def _root() -> None:
        await coco.mount_each(_child, [("a", 1), ("a", 2)])

    app = coco.App(
        coco.AppConfig(name="already_declared_mount_each", environment=coco_env),
        _root,
    )
    with pytest.raises(coco.AlreadyDeclaredError, match="mounted more than once"):
        app.update_blocking()


def test_duplicate_context_key_raises_already_declared() -> None:
    coco.ContextKey[int]("already_declared_test_key")
    with pytest.raises(coco.AlreadyDeclaredError, match="already used"):
        coco.ContextKey[int]("already_declared_test_key")
