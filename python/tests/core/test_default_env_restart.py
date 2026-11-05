from collections.abc import AsyncIterator, Iterator
import gc
import pathlib
import weakref

import pytest

import cocoindex as coco
from cocoindex._internal.environment import (
    reset_default_env_for_tests,
)
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget, Metrics


class _Resource:
    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.closed = False


_RESOURCE_KEY = coco.ContextKey[_Resource]("test_default_env_restart/resource")
_PATH_KEY = coco.ContextKey[str]("test_default_env_restart/path")
_MEMO_METRICS = Metrics()


@coco.fn(memo=True)
def _memoized_transform(value: str) -> str:
    _MEMO_METRICS.increment("transform")
    return value.upper()


@coco.fn
def _memoized_target_main() -> None:
    value = _memoized_transform("retained")
    coco.declare_target_state(GlobalDictTarget.target_state("restart", value))


@pytest.fixture
def _clean_default_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    reset_default_env_for_tests()
    monkeypatch.delenv("COCOINDEX_DB", raising=False)
    try:
        yield
    finally:
        reset_default_env_for_tests()


def test_stop_releases_lifespan_values_of_retained_app(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    resources: list[weakref.ref[_Resource]] = []

    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        resource = _Resource(len(resources) + 1)
        resources.append(weakref.ref(resource))
        builder.settings.db_path = tmp_path / "release_on_stop.db"
        builder.provide(_RESOURCE_KEY, resource)
        yield

    def resource_generation() -> int:
        return coco.use_context(_RESOURCE_KEY).generation

    app = coco.App("release_on_stop", resource_generation)
    assert app.update_blocking() == 1
    coco.stop_blocking()
    gc.collect()
    assert resources[0]() is None


def test_restart_uses_updated_database_path(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    paths = [tmp_path / "first.db", tmp_path / "second.db"]
    generation = 0

    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        nonlocal generation
        path = paths[generation]
        generation += 1
        builder.settings.db_path = path
        builder.provide(_PATH_KEY, str(path))
        yield

    def current_path() -> str:
        return coco.use_context(_PATH_KEY)

    app = coco.App("changing_database_path", current_path)

    with coco.runtime():
        assert app.update_blocking() == str(paths[0])
    with coco.runtime():
        assert app.update_blocking() == str(paths[1])

    assert all(path.exists() for path in paths)


def test_restart_recovers_after_teardown_failure(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    generation = 0

    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        nonlocal generation
        generation += 1
        resource = _Resource(generation)
        builder.settings.db_path = tmp_path / f"teardown_failure_{generation}.db"
        builder.provide(_RESOURCE_KEY, resource)
        try:
            yield
        finally:
            resource.closed = True
            if generation == 1:
                raise RuntimeError("teardown failed")

    def resource_generation() -> int:
        resource = coco.use_context(_RESOURCE_KEY)
        assert not resource.closed
        return resource.generation

    app = coco.App("teardown_failure", resource_generation)
    assert app.update_blocking() == 1
    with pytest.raises(RuntimeError, match="teardown failed"):
        coco.stop_blocking()

    try:
        assert app.update_blocking() == 2
    finally:
        coco.stop_blocking()


def test_restart_recovers_after_startup_validation_failure(
    _clean_default_env: None,
    tmp_path: pathlib.Path,
) -> None:
    settings_are_valid = False

    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        if settings_are_valid:
            builder.settings.db_path = tmp_path / "startup_recovery.db"
        yield

    app = coco.App("startup_recovery", lambda: "ready")
    with pytest.raises(ValueError, match="Settings.db_path"):
        app.update_blocking()

    settings_are_valid = True
    try:
        assert app.update_blocking() == "ready"
    finally:
        coco.stop_blocking()


def test_default_stop_does_not_change_explicit_environment_app(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = tmp_path / "default.db"
        yield

    explicit_env = coco.Environment(
        coco.Settings(db_path=tmp_path / "explicit.db"), name="explicit"
    )
    calls = 0

    def count_calls() -> int:
        nonlocal calls
        calls += 1
        return calls

    app = coco.App(
        coco.AppConfig(name="explicit_environment", environment=explicit_env),
        count_calls,
    )
    assert app.update_blocking() == 1

    with coco.runtime():
        pass

    assert app.update_blocking() == 2


def test_restart_preserves_memo_and_target_then_drop_uses_fresh_runtime(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    @coco.lifespan
    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = tmp_path / "preserved_state.db"
        yield

    GlobalDictTarget.store.clear()
    _MEMO_METRICS.clear()
    app = coco.App("preserved_state_sync", _memoized_target_main)

    with coco.runtime():
        app.update_blocking()
    assert _MEMO_METRICS.collect() == {"transform": 1}
    assert GlobalDictTarget.store.data == {
        "restart": DictDataWithPrev(data="RETAINED", prev=[], prev_may_be_missing=True)
    }

    with coco.runtime():
        app.update_blocking()
    assert _MEMO_METRICS.collect() == {}
    assert GlobalDictTarget.store.data == {
        "restart": DictDataWithPrev(data="RETAINED", prev=[], prev_may_be_missing=True)
    }

    try:
        app.drop_blocking()
        assert GlobalDictTarget.store.data == {}
    finally:
        coco.stop_blocking()


@pytest.mark.asyncio
async def test_async_drop_after_stop_uses_fresh_runtime(
    _clean_default_env: None, tmp_path: pathlib.Path
) -> None:
    @coco.lifespan
    async def lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
        builder.settings.db_path = tmp_path / "async_drop.db"
        yield

    GlobalDictTarget.store.clear()
    _MEMO_METRICS.clear()
    app = coco.App("preserved_state_async", _memoized_target_main)

    async with coco.runtime():
        await app.update()
    assert GlobalDictTarget.store.data

    try:
        await app.drop()
        assert GlobalDictTarget.store.data == {}
    finally:
        await coco.stop()
