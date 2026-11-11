"""Run one of the sample flows with a real model and report its model calls.

    uv run python examples/cocoindex_agents/flows/run.py summaries   # from the repository root
    LLM_MODEL=anthropic/claude-sonnet-5-5 uv run python examples/cocoindex_agents/flows/run.py report

A flow is a directory with a ``prompt.md`` and its sample data; outputs go to
``<flow>/out`` and the CocoIndex state to ``<flow>/.coco_db``. The prompt's
paths are relative to the flow directory. Run a flow twice unchanged, then
edit one input and run again, to see the model calls it saves
(``README.md`` records what was measured). ``--stats`` prints one line of
cache usage and ``--plan`` the plan it chose, with what each node
reused; both go to stderr, the answer to stdout.
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import re
import sys
import time

import cocoindex as coco
import litellm

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from cocoindex_agents.app import DEFAULT_MODEL, build_app, default_config  # noqa: E402
from cocoindex_agents.litellm_client import LiteLLMClient  # noqa: E402
from cocoindex_agents.model import MODEL_CLIENT  # noqa: E402
from cocoindex_agents.report import RunReport, render_plan, render_stats_line  # noqa: E402
from cocoindex_agents.tools import AGENTS_CONFIG  # noqa: E402
from cocoindex_agents.trace import SessionScratch  # noqa: E402

FLOWS = pathlib.Path(__file__).resolve().parent


def _expand(prompt_path: pathlib.Path) -> str:
    """The prompt, with ``$NAME`` references filled from the environment.

    A flow over data that is not checked in (``drive``: a Google Drive folder
    of the user's) names it this way; an unset name is an error, not an empty
    string in the prompt.
    """
    raw = prompt_path.read_text()
    missing = sorted(
        {v for v in re.findall(r"\$([A-Z][A-Z0-9_]*)", raw) if v not in os.environ}
    )
    if missing:
        raise SystemExit(
            f"{prompt_path} refers to {', '.join('$' + v for v in missing)}; "
            "set it in the environment (see flows/README.md)."
        )
    return os.path.expandvars(raw)


def main(argv: list[str] | None = None) -> int:
    flows = sorted(p.parent.name for p in FLOWS.glob("*/prompt.md"))
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("flow", choices=flows)
    parser.add_argument(
        "--model",
        default=os.environ.get("LLM_MODEL", DEFAULT_MODEL),
        help="LiteLLM model id (default: $LLM_MODEL, else %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="log every model turn's action; twice to log every request as the model saw it",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="one line of cache usage: what the run would have cost with no cache, "
        "and the share reused (stderr)",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="the plan chosen, as a tree, with what each node reused "
        "or recomputed and why (stderr)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    logging.getLogger("cocoindex_agents").setLevel(
        logging.DEBUG if args.verbose else logging.INFO
    )
    if args.verbose < 2:
        logging.getLogger("cocoindex_agents.litellm_client.messages").setLevel(
            logging.INFO
        )

    flow_dir = FLOWS / args.flow
    prompt = _expand(flow_dir / "prompt.md")
    os.chdir(flow_dir)
    # LiteLLM's aiohttp transport leaves a pending close task at interpreter
    # exit ("Task was destroyed but it is pending"); httpx does not.
    litellm.disable_aiohttp_transport = True
    env = coco.Environment(coco.Settings.from_env(db_path=flow_dir / ".coco_db"))
    client = LiteLLMClient()
    config = default_config(args.model)
    env.context_provider.provide(MODEL_CLIENT, client)
    env.context_provider.provide(AGENTS_CONFIG, config)
    scratch = SessionScratch()
    app = build_app(
        f"cocoindex_agents_{args.flow}", prompt, env=env, config=config, scratch=scratch
    )

    started = time.monotonic()
    result = app.update_blocking()
    report = RunReport.from_result(
        args.flow,
        result.trace,
        ran=scratch.ran,
        model=args.model,
        wall_seconds=time.monotonic() - started,
        usage=client.usage,
    )
    print(f"answer: {result.answer}")
    if args.plan:
        print(render_plan(report), file=sys.stderr)
    if args.stats:
        print(render_stats_line(report), file=sys.stderr)
    else:
        by_role = ", ".join(f"{r}={n}" for r, n in report.model_calls_by_role().items())
        print(
            f"model calls: {report.spent.model_calls} ({by_role or 'none'})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
