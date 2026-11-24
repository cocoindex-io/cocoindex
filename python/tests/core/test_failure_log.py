"""How a component failure with no exception handler reaches the log while a
progress display runs. The log is the complete record: a display lists only the
first few failures of the stats it draws."""

from __future__ import annotations

import logging
import sys

import pytest

import cocoindex as coco
from tests.common import create_test_env

coco_env = create_test_env(__file__)

# More than the engine records for the display (`MAX_RECORDED_FAILURES`).
_FAILURES = 8


@coco.fn()
async def _fail(i: int) -> None:
    raise ValueError(f"boom {i}")


@coco.fn
async def _main_failures() -> None:
    await coco.mount_each(_fail, [(f"k{i}", i) for i in range(_FAILURES)])


@coco.fn
async def _main_failure_in_group() -> None:
    with coco.stats_group("Docs"):
        await coco.mount_each(_fail, [("k0", 0)])


def _failure_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == "cocoindex" and r.levelno == logging.ERROR
    ]


def test_scrolling_output_logs_each_failure_with_its_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = coco.App(
        coco.AppConfig(name="test_failure_log_plain", environment=coco_env),
        _main_failures,
    )
    with caplog.at_level(logging.DEBUG, logger="cocoindex"):
        app.update_blocking(report_to_stdout=True)
    records = _failure_records(caplog)
    assert len(records) == _FAILURES
    assert all(r.exc_info for r in records)
    assert any("ValueError: boom 7" in r.getMessage() for r in records)


def test_terminal_logs_each_failure_past_what_the_display_lists(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    app = coco.App(
        coco.AppConfig(name="test_failure_log_terminal", environment=coco_env),
        _main_failures,
    )
    with caplog.at_level(logging.DEBUG, logger="cocoindex"):
        app.update_blocking(report_to_stdout=True)
    records = _failure_records(caplog)
    assert len(records) == _FAILURES
    # A traceback per failure would push the display off screen: it is at DEBUG.
    assert not any(r.exc_info for r in records)
    tracebacks = [
        r for r in caplog.records if r.levelno == logging.DEBUG and r.exc_info
    ]
    assert len(tracebacks) == _FAILURES


def test_terminal_logs_a_failure_in_a_group_the_display_does_not_draw(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    app = coco.App(
        coco.AppConfig(name="test_failure_log_group", environment=coco_env),
        _main_failure_in_group,
    )
    with caplog.at_level(logging.DEBUG, logger="cocoindex"):
        app.update_blocking(report_to_stdout=True)
    assert len(_failure_records(caplog)) == 1
