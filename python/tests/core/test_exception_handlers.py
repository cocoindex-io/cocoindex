import traceback
from typing import Iterator

import cocoindex as coco

from cocoindex._internal import environment as envmod

from tests import common
from tests.common.target_states import GlobalDictTarget, DictDataWithPrev


def test_global_exception_handler_invoked_for_background_mount() -> None:
    envmod.reset_default_env_for_tests()

    seen: list[tuple[str, str]] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_global"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            seen.append((type(exc).__name__, ctx.mount_kind))

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _child() -> None:
        raise ValueError("boom")

    @coco.fn
    async def _root() -> None:
        await coco.mount(coco.component_subpath("child"), _child)

    app = coco.App("test_exception_handlers_global", _root)
    app.update_blocking()

    assert seen == [("ValueError", "mount")]


def test_scoped_handler_overrides_global_and_fallback_on_handler_error() -> None:
    envmod.reset_default_env_for_tests()

    calls: list[str] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_scoped"
        )

        def global_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            calls.append(f"global:{ctx.source}:{type(exc).__name__}")

        builder.set_exception_handler(global_handler)
        yield

    @coco.fn
    async def _child() -> None:
        raise ValueError("boom")

    @coco.fn
    async def _root() -> None:
        def inner_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            calls.append(f"inner:{ctx.source}:{type(exc).__name__}")
            raise RuntimeError("handler failed")

        async with coco.exception_handler(inner_handler):
            await coco.mount(coco.component_subpath("child"), _child)

    app = coco.App("test_exception_handlers_scoped", _root)
    app.update_blocking()

    # Inner sees component exception, then raises; global receives handler exception.
    assert calls == [
        "inner:component:ValueError",
        "global:handler:RuntimeError",
    ]


def _raise_for_trace_test() -> None:
    raise ValueError("traceful boom")


_orphan_source: dict[str, int] = {}


def test_orphan_delete_failure_routes_through_parent_handler() -> None:
    """Build-mode cascade: when a parent's commit-phase GC sweep deletes a
    child component that's no longer mounted (orphan) and the cleanup
    sink fails, the parent's exception handler chain should see the
    failure — not the framework's default `error!` log.

    Without §4.2 (storing `on_error` on `ComponentBuildContext` and
    reading it from `processing_action_on_error()` in
    `launch_child_component_gc`), the orphan-delete failure would
    silently log + swallow, and `seen` would stay empty.
    """
    envmod.reset_default_env_for_tests()
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False

    seen: list[tuple[str, str]] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_orphan_cascade"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            seen.append((type(exc).__name__, ctx.mount_kind))

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _child(value: int) -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("k", value))

    @coco.fn
    async def _parent() -> None:
        for name, value in _orphan_source.items():
            await coco.mount(coco.component_subpath(name), _child, value)

    @coco.fn
    async def _root() -> None:
        await coco.mount(coco.component_subpath("parent"), _parent)

    app = coco.App("test_exception_handlers_orphan_cascade", _root)

    # First update: mount child "A", child declares target state. Sink healthy.
    _orphan_source.clear()
    _orphan_source["A"] = 1
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "k": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert seen == []

    # Second update: source empty, child "A" is now an orphan. The parent's
    # commit-phase GC sweep deletes A; the cleanup sink fails. With §4.2,
    # the cascaded on_error fires the parent's handler.
    _orphan_source.clear()
    GlobalDictTarget.store.sink_exception = True
    try:
        app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False

    # Exactly one failure surfaces — the orphan-delete of "A". The handler's
    # mount_kind is "mount" because the on_error was wired through
    # `coco.mount(..., parent_fn)` at the parent's mount time.
    assert len(seen) == 1, f"expected one handler call; got {seen}"
    exc_name, mount_kind = seen[0]
    # The sink raises a Python ValueError; it reaches the handler as-is.
    assert exc_name == "ValueError"
    assert mount_kind == "mount"


def test_background_mount_failure_preserves_exception_and_traceback() -> None:
    """The handler receives the component's original Python exception, so it
    can route by type (``isinstance``) and still recover the full traceback via
    ``traceback.format_exception`` / ``logging(..., exc_info=exc)``."""
    envmod.reset_default_env_for_tests()

    seen: list[BaseException] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_trace"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            seen.append(exc)

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _failing() -> None:
        _raise_for_trace_test()

    @coco.fn
    async def _root() -> None:
        await coco.mount(coco.component_subpath("child"), _failing)

    app = coco.App("test_exception_handlers_trace", _root)
    app.update_blocking()

    assert len(seen) == 1
    exc = seen[0]
    assert isinstance(exc, ValueError)
    assert str(exc) == "traceful boom"
    assert exc.__traceback__ is not None
    formatted = "".join(traceback.format_exception(exc))
    assert "Traceback (most recent call last)" in formatted
    assert "_raise_for_trace_test" in formatted


def test_engine_native_failure_keeps_cerror_mapping() -> None:
    """An engine-side client error (here: declaring the same target state key
    twice) reaches the handler as the ``ValueError`` that ``cerror_to_pyerr``
    maps it to, not as a flattened ``RuntimeError``."""
    envmod.reset_default_env_for_tests()

    seen: list[BaseException] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_engine_native"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            seen.append(exc)

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _child() -> None:
        coco.declare_target_state(GlobalDictTarget.target_state("dup", 1))
        coco.declare_target_state(GlobalDictTarget.target_state("dup", 2))

    @coco.fn
    async def _root() -> None:
        await coco.mount(coco.component_subpath("child"), _child)

    app = coco.App("test_exception_handlers_engine_native", _root)
    app.update_blocking()

    assert len(seen) == 1
    assert isinstance(seen[0], ValueError)
    assert "Target state already declared" in str(seen[0])


class _DeadLetterError(Exception):
    pass


def test_handler_raise_preserves_type_through_ready() -> None:
    """An exception raised by a handler propagates through ``handle.ready()``
    with its Python type (and ``__cause__``) intact, so callers can catch it
    by type rather than string-matching a flattened RuntimeError."""
    envmod.reset_default_env_for_tests()

    caught: list[BaseException] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_typed_raise"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            if isinstance(exc, ValueError):
                raise _DeadLetterError("dead-letter") from exc
            raise exc

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _child() -> None:
        raise ValueError("boom")

    @coco.fn
    async def _root() -> None:
        handle = await coco.mount(coco.component_subpath("child"), _child)
        try:
            await handle.ready()
        except BaseException as exc:
            caught.append(exc)

    app = coco.App("test_exception_handlers_typed_raise", _root)
    app.update_blocking()

    assert len(caught) == 1
    assert isinstance(caught[0], _DeadLetterError)
    assert isinstance(caught[0].__cause__, ValueError)


def test_handler_raised_deadline_exceeded_propagates_through_ready() -> None:
    """A handler that raises ``coco.DeadlineExceededError`` surfaces it through
    ``handle.ready()`` as that type (still a ``TimeoutError``), not as a
    flattened ``RuntimeError``."""
    envmod.reset_default_env_for_tests()

    caught: list[BaseException] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_deadline_raise"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            raise coco.DeadlineExceededError("handler deadline")

        builder.set_exception_handler(handler)
        yield

    @coco.fn
    async def _child() -> None:
        raise ValueError("boom")

    @coco.fn
    async def _root() -> None:
        handle = await coco.mount(coco.component_subpath("child"), _child)
        try:
            await handle.ready()
        except BaseException as exc:
            caught.append(exc)

    app = coco.App("test_exception_handlers_deadline_raise", _root)
    app.update_blocking()

    assert len(caught) == 1
    assert isinstance(caught[0], coco.DeadlineExceededError)
    assert isinstance(caught[0], TimeoutError)
    assert str(caught[0]) == "handler deadline"
