"""Assemble an app: default roles + built-in tools, and build a ``coco.App`` for a flow prompt.

Usage, from the folder the prompt talks about (README.md, *Running*):

    python -m cocoindex_agents.app "For every .md under ./docs, ..."

``$LLM_MODEL`` picks the model, and the run's state is kept in ``$COCOINDEX_DB``,
else ``./.coco_db``.

The sample flows under ``flows/`` drive this with checked-in data.
"""

from __future__ import annotations

import os
import pathlib
import sys

import cocoindex as coco

from .litellm_client import LiteLLMClient
from .model import MODEL_CLIENT
from .roles import default_roles
from .session import run_open_session
from .spec import SessionResult
from .tools import (
    AGENTS_CONFIG,
    AgentsConfig,
    Tool,
    ToolRegistry,
    agents,
    sources,
    targets,
)
from .trace import SessionScratch

# The LiteLLM model id used when ``$LLM_MODEL`` is unset. The sample flows
# were measured with it (flows/README.md).
DEFAULT_MODEL = "openai/gpt-6.1-terra"


def builtin_tools() -> list[Tool]:
    return [
        sources.open_source,
        sources.call_method,
        sources.read_input,
        targets.open_target,
        targets.declare,
        agents.call_agent,
        agents.start_agent,
        agents.observe,
        agents.for_each,
        agents.reduce,
    ]


def default_config(
    default_model: str, extra_tools: list[Tool] | None = None
) -> AgentsConfig:
    return AgentsConfig(
        roles=default_roles(),
        tools=ToolRegistry([*builtin_tools(), *(extra_tools or [])]),
        default_model=default_model,
    )


def build_app(
    name: str,
    flow_prompt: str,
    *,
    env: coco.Environment,
    config: AgentsConfig,
    scratch: SessionScratch | None = None,
) -> coco.App[..., SessionResult]:
    """One app per flow. The orchestrator is an open session: skipped while what it read is unchanged.

    ``scratch`` is marked when the orchestrator's body runs; left untouched,
    the whole run was served from the store (``report.RunReport``).
    """
    spec = config.spec("orchestrator", flow_prompt)
    return coco.App(
        coco.AppConfig(name=name, environment=env),
        run_open_session,
        spec,
        scratch=scratch,
    )


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print("usage: python -m cocoindex_agents.app <flow prompt>", file=sys.stderr)
        return 2
    import litellm  # optional dependency, needed only to run

    # LiteLLM's aiohttp transport leaves a pending close task at interpreter
    # exit ("Task was destroyed but it is pending"); httpx does not.
    litellm.disable_aiohttp_transport = True
    # ``Settings.from_env`` does not read ``COCOINDEX_DB``; the default state
    # directory sits beside the data, as the sample flows keep theirs.
    db_path = pathlib.Path(os.environ.get("COCOINDEX_DB") or ".coco_db")
    env = coco.Environment(coco.Settings.from_env(db_path=db_path))
    config = default_config(os.environ.get("LLM_MODEL", DEFAULT_MODEL))
    env.context_provider.provide(MODEL_CLIENT, LiteLLMClient())
    env.context_provider.provide(AGENTS_CONFIG, config)
    app = build_app("cocoindex_agents", " ".join(args), env=env, config=config)
    result = app.update_blocking(report_to_stdout=True)
    # A list only in preview mode, which this run is not.
    assert isinstance(result, SessionResult)
    print(result.answer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
