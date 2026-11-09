# CocoIndex Agents

**One prompt, many files, kept current.**

Incremental agents over your data, on top of CocoIndex. You describe an outcome
in a prompt ("turn the invoices under `./invoices` into a table of JSON files in
`./out`"); an orchestrator agent decides how to split it into subagents over sources and
targets, following the recipes in its system prompt; CocoIndex makes the whole
thing incremental, so when the inputs change only the affected subagents,
reducers and model turns run again.

**Status: design frozen 2026-11-04; implementation driven with a real model.**
Read [`DESIGN.md`](DESIGN.md) first. The tests drive it with a
scripted model and assert on inference counts; the sample flows under `flows/`
drive it with a real model through LiteLLM (`openai/gpt-6.1-terra`) and record
what they measured: the same decomposition in three drives of each flow, no
model call on an unchanged rerun, and two or three calls after one edited
input.

## Running

It needs CocoIndex 1.2.0 or later. The steps below run it from a clone of
this repository, on the CocoIndex built from it. Once, from the repository
root:

```sh
uv run maturin develop                                   # build CocoIndex from this checkout
uv sync --extra litellm --inexact                        # the model client
uv pip install --no-deps -e examples/cocoindex_agents    # this package, into the same environment
```

Keep `--inexact` on later syncs: a plain `uv sync` removes the last install.

The model is `openai/gpt-6.1-terra`, or the LiteLLM model named in
`$LLM_MODEL`. Put that provider's key in the environment, for example
`export OPENAI_API_KEY=...`.

Then go to the folder your prompt is about and describe the outcome you want.
Paths in the prompt are relative to that folder:

```sh
cd ~/my-project
uv run --project ~/src/cocoindex python -m cocoindex_agents.app \
  "Summarize every markdown file under ./docs. Write one summary per file into ./out,
   named after the source file with .md replaced by .summary.md.
   Answer with one line saying what was written."
```

Replace `~/src/cocoindex` with your clone. The command prints CocoIndex's
update report, then the answer. It keeps its state in `./.coco_db`, or in
`$COCOINDEX_DB` when that is set. Run the same command again: when nothing
changed it makes no model call, and after you edit one file only that file's
work runs again. Delete `.coco_db` to start from scratch.

Two more ways to run it:

- **The sample flows** under `flows/` come with their data, and their runner
  adds `--stats` for cache usage and `--plan` for the plan it chose:
  `uv run python examples/cocoindex_agents/flows/run.py summaries --stats`.
  See [`flows/README.md`](flows/README.md).
- **A Google Drive folder** of your own, read as your account after a one-time
  sign-in: see *The `drive` flow* in [`flows/README.md`](flows/README.md).

## Layout

- `cocoindex_agents/` — the package (see DESIGN.md §9 for the module map, §8 for
  what is implemented and tested)
- `tests/` — scripted-model tests; no API keys needed
- `flows/` — five sample flows with their data and a runner: one per shape
  of DESIGN.md §2, one over PDFs the model reads natively, and one over a
  Google Drive folder read as the signed-in account; see
  [`flows/README.md`](flows/README.md) for how to run them and the measured
  model calls (`--stats` for cache usage, `--plan` for the plan it chose and
  what it reused)

## Running the tests

From the repository root, after `uv run maturin develop`:

```sh
uv run pytest examples/cocoindex_agents/tests
```

The tests need no API key. They run on this repository's CocoIndex, in the
same environment as the steps under [Running](#running).
