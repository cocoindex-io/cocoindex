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
