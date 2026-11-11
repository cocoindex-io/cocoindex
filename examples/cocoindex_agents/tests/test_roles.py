"""The default roles, and the generic ``call`` tool's checks (DESIGN.md §3).

The orchestrator's recipes mount workers with ``for_each`` and reducers with
``reduce``, so those roles must be closed. A handle method passed an argument
it does not take reports it instead of defaulting, so a planner that writes
``sample(count=8)`` learns the name ``k`` rather than silently getting five
heads.
"""

from __future__ import annotations

import pathlib

import cocoindex as coco

from cocoindex_agents import (
    AGENTS_CONFIG,
    MODEL_CLIENT,
    AgentsConfig,
    RoleSpec,
    SessionResult,
    ToolRegistry,
    run_open_session,
)
from cocoindex_agents.app import default_config
from cocoindex_agents.hints import RunHints
from cocoindex_agents.spec import SessionSpec
from cocoindex_agents.testing import ScriptedModel
from cocoindex_agents.tools import sources
from cocoindex_agents.transcript import ModelOutput, ToolCall, Transcript


def test_default_roles_fit_the_recipes() -> None:
    """The recipes fan out with for_each and combine with reduce: those roles must be
    closed, or the mounting tool refuses them. Only the planner may observe freely."""
    config = default_config("m")
    assert config.open_tools(config.role("worker")) == []
    assert config.open_tools(config.role("reducer")) == []
    assert config.role("planner").volatile_ok
    assert not config.role("orchestrator").volatile_ok


def test_source_methods_reject_unknown_arguments(
    coco_env: coco.Environment, tmp_path: pathlib.Path
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(3):
        (docs / f"{i}.md").write_text(f"doc {i}")

    def script(spec: SessionSpec, prefix: Transcript, hints: RunHints) -> ModelOutput:
        n = len(prefix.steps)
        if n == 0:
            return ModelOutput(
                calls=(
                    ToolCall(
                        "open_source",
                        {"kind": "localfs", "name": "docs", "path": str(docs)},
                    ),
                )
            )
        if n == 1:
            return ModelOutput(
                calls=(
                    ToolCall(
                        "call",
                        {
                            "handle": "src:docs",
                            "method": "sample",
                            "args": {"count": 8},
                        },
                    ),
                    ToolCall(
                        "call",
                        {"handle": "src:docs", "method": "sample", "args": {"k": 2}},
                    ),
                    ToolCall(
                        "call",
                        {"handle": "src:docs", "method": "stats", "args": {"k": 2}},
                    ),
                )
            )
        return ModelOutput(
            answer=str([(r.ok, r.value) for r in prefix.steps[1].results])
        )

    config = AgentsConfig(
        roles={
            "planner": RoleSpec(
                "planner", "test", ("open_source", "call"), volatile_ok=True
            )
        },
        tools=ToolRegistry([sources.open_source, sources.call_method]),
        default_model="m",
    )
    coco_env.context_provider.provide(MODEL_CLIENT, ScriptedModel(script))
    coco_env.context_provider.provide(AGENTS_CONFIG, config)
    app = coco.App(
        coco.AppConfig(name="roles", environment=coco_env),
        run_open_session,
        config.spec("planner", "look"),
    )
    result = app.update_blocking()
    assert isinstance(result, SessionResult)
    assert result.answer == str(
        [
            (False, "sample does not take ['count']; it takes ['k']"),
            (
                True,
                [{"head": "doc 1", "key": "1.md"}, {"head": "doc 2", "key": "2.md"}],
            ),
            (False, "stats does not take ['k']; it takes no arguments"),
        ]
    )
