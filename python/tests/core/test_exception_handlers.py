from typing import Iterator

import cocoindex as coco

from cocoindex._internal import environment as envmod

from tests import common


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

    assert seen == [("RuntimeError", "mount")]


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
        "inner:component:RuntimeError",
        "global:handler:RuntimeError",
    ]


def _raise_for_trace_test() -> None:
    raise ValueError("traceful boom")


def test_background_mount_failure_surfaces_python_traceback() -> None:
    """The handler should see the full Python traceback for a background mount failure,
    not just the exception message — the trace is what makes the error actionable."""
    envmod.reset_default_env_for_tests()

    seen_messages: list[str] = []

    @coco.lifespan
    def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_trace"
        )

        def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
            seen_messages.append(str(exc))

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

    assert len(seen_messages) == 1
    msg = seen_messages[0]
    assert "ValueError" in msg
    assert "traceful boom" in msg
    assert "Traceback (most recent call last)" in msg
    assert "_raise_for_trace_test" in msg
