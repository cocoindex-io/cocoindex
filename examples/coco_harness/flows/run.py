"""Run one of the sample flows with a real model and report its model calls.

    uv run python examples/coco_harness/flows/run.py summaries   # from the repository root
    LLM_MODEL=anthropic/claude-sonnet-5-5 uv run python examples/coco_harness/flows/run.py report

A flow is a directory with a ``prompt.md`` and its sample data; outputs go to
``<flow>/out`` and the CocoIndex state to ``<flow>/.coco_db``. The prompt's
paths are relative to the flow directory. Run a flow twice unchanged, then
edit one input and run again, to see the model calls the harness saves
(``README.md`` records what was measured).
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import sys

import cocoindex as coco
import litellm

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from coco_harness.app import DEFAULT_MODEL, build_app, default_config  # noqa: E402
from coco_harness.litellm_client import LiteLLMClient  # noqa: E402
from coco_harness.model import MODEL_CLIENT  # noqa: E402
from coco_harness.tools import HARNESS  # noqa: E402

FLOWS = pathlib.Path(__file__).resolve().parent


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
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    logging.getLogger("coco_harness").setLevel(
        logging.DEBUG if args.verbose else logging.INFO
    )
    if args.verbose < 2:
        logging.getLogger("coco_harness.litellm_client.messages").setLevel(logging.INFO)

    flow_dir = FLOWS / args.flow
    prompt = (flow_dir / "prompt.md").read_text()
    os.chdir(flow_dir)
    # LiteLLM's aiohttp transport leaves a pending close task at interpreter
    # exit ("Task was destroyed but it is pending"); httpx does not.
    litellm.disable_aiohttp_transport = True
    env = coco.Environment(coco.Settings.from_env(db_path=flow_dir / ".coco_db"))
    client = LiteLLMClient()
    config = default_config(args.model)
    env.context_provider.provide(MODEL_CLIENT, client)
    env.context_provider.provide(HARNESS, config)
    app = build_app(f"coco_harness_{args.flow}", prompt, env=env, config=config)

    result = app.update_blocking()
    print(f"answer: {result.answer}")
    by_role = ", ".join(f"{role}={n}" for role, n in sorted(client.calls.items()))
    print(f"model calls: {sum(client.calls.values())} ({by_role or 'none'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
