import asyncio
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
import pathlib
import threading

import pytest

import cocoindex as coco
from cocoindex._internal.environment import (
    LazyEnvironment,
    default_env_loop,
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
    try:
        coco.stop_blocking()
    except RuntimeError as error:
        assert str(error) == "teardown failed"
    else:
        pytest.fail("stop should propagate the lifespan teardown failure")

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


class _AsyncPausedLazyEnvironment(LazyEnvironment):
    def __init__(self) -> None:
        super().__init__("paused-async")
        self.env_returning = asyncio.Event()
        self.allow_return = asyncio.Event()
        self.pause_next_get = True

    async def _get_env(self) -> coco.Environment:
        env = await super()._get_env()
        if self.pause_next_get:
            self.pause_next_get = False
            self.env_returning.set()
            await self.allow_return.wait()
        return env


@pytest.mark.asyncio
async def test_stop_during_async_app_cache_install_retries_with_fresh_env(
    tmp_path: pathlib.Path,
) -> None:
    lazy_env = _AsyncPausedLazyEnvironment()
    generation = 0

    @asynccontextmanager
    async def resource() -> AsyncIterator[_Resource]:
        nonlocal generation
        generation += 1
        value = _Resource(generation)
        try:
            yield value
        finally:
            value.closed = True

    async def lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
        builder.settings.db_path = tmp_path / "async_race.db"
        await builder.provide_async_with(_RESOURCE_KEY, resource())
        yield

    lazy_env.lifespan(lifespan)

    async def resource_generation() -> int:
        value = coco.use_context(_RESOURCE_KEY)
        assert not value.closed
        return value.generation

    app = coco.App(
        coco.AppConfig(name="async_cache_install_race", environment=lazy_env),
        resource_generation,
    )
    update = asyncio.create_task(app.update().result())
    await lazy_env.env_returning.wait()
    await lazy_env.stop()
    lazy_env.allow_return.set()

    try:
        assert await update == 2
    finally:
        await lazy_env.stop()


class _ThreadPausedLazyEnvironment(LazyEnvironment):
    def __init__(self) -> None:
        super().__init__("paused-sync")
        self.env_returning = threading.Event()
        self.allow_return = threading.Event()
        self.pause_next_get = True

    async def _get_env(self) -> coco.Environment:
        env = await super()._get_env()
        if self.pause_next_get:
            self.pause_next_get = False
            self.env_returning.set()
            await asyncio.to_thread(self.allow_return.wait)
        return env


def test_stop_during_sync_app_cache_install_retries_with_fresh_env(
    tmp_path: pathlib.Path,
) -> None:
    lazy_env = _ThreadPausedLazyEnvironment()
    generation = 0

    @contextmanager
    def resource() -> Iterator[_Resource]:
        nonlocal generation
        generation += 1
        value = _Resource(generation)
        try:
            yield value
        finally:
            value.closed = True

    def lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = tmp_path / "sync_race.db"
        builder.provide_with(_RESOURCE_KEY, resource())
        yield

    lazy_env.lifespan(lifespan)

    def resource_generation() -> int:
        value = coco.use_context(_RESOURCE_KEY)
        assert not value.closed
        return value.generation

    app = coco.App(
        coco.AppConfig(name="sync_cache_install_race", environment=lazy_env),
        resource_generation,
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        update = executor.submit(app.update_blocking)
        assert lazy_env.env_returning.wait(timeout=5)
        stop = asyncio.run_coroutine_threadsafe(lazy_env.stop(), default_env_loop())
        try:
            stop.result(timeout=5)
        finally:
            lazy_env.allow_return.set()
        try:
            assert update.result(timeout=5) == 2
        finally:
            final_stop = asyncio.run_coroutine_threadsafe(
                lazy_env.stop(), default_env_loop()
            )
            final_stop.result(timeout=5)
