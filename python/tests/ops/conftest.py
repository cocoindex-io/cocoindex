"""Shared fixtures for the ``cocoindex.ops`` tests."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
from typing import Any

import pytest


@pytest.fixture
def litellm_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """``cocoindex.ops.litellm`` loaded against a stub ``litellm`` package.

    Lets tests exercise the module's own logic without the optional
    dependency — which CI does not install, so tests gated on the real
    ``litellm`` never run there.

    Each test gets a fresh module object, so decorator-level state (memo
    caches, batchers keyed by function identity) never leaks between tests,
    and none of it touches the real ``cocoindex.ops.litellm``. The name is
    made unique per test only so the ``sys.modules`` entry cannot collide
    with a concurrently-registered one.
    """
    fake_litellm: Any = types.ModuleType("litellm")
    fake_litellm.aembedding = object()
    fake_litellm.atranscription = object()
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)

    module_path = pathlib.Path(__file__).parents[2] / "cocoindex" / "ops" / "litellm.py"
    module_name = f"_test_litellm_{id(monkeypatch)}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module
