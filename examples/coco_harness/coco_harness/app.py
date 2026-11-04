"""Assemble a harness: default roles + built-in tools, and build a ``coco.App`` for a flow prompt.

Usage (sketch):

    LLM_MODEL=gemini/gemini-2.5-flash python -m coco_harness.app "For every .md under ./docs, ..."
"""

from __future__ import annotations

import os
import sys

import cocoindex as coco

from .litellm_client import LiteLLMClient
from .model import MODEL_CLIENT
from .roles import default_roles
from .session import run_open_session
from .tools import HARNESS, HarnessConfig, Tool, ToolRegistry, agents, sources, targets


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
) -> HarnessConfig:
    return HarnessConfig(
        roles=default_roles(),
        tools=ToolRegistry([*builtin_tools(), *(extra_tools or [])]),
        default_model=default_model,
    )


def build_app(
    name: str,
    flow_prompt: str,
    *,
    env: coco.Environment,
    config: HarnessConfig,
) -> coco.App[..., object]:
    """One app per flow. The orchestrator is an open session: skipped while what it read is unchanged."""
    spec = config.spec("orchestrator", flow_prompt)
    return coco.App(coco.AppConfig(name=name, environment=env), run_open_session, spec)


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print("usage: python -m coco_harness.app <flow prompt>", file=sys.stderr)
        return 2
    env = coco.Environment(coco.Settings.from_env())
    config = default_config(os.environ.get("LLM_MODEL", "gemini/gemini-2.5-flash"))
    env.context_provider.provide(MODEL_CLIENT, LiteLLMClient())
    env.context_provider.provide(HARNESS, config)
    app = build_app("coco_harness", " ".join(args), env=env, config=config)
    result = app.update_blocking(report_to_stdout=True)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
