# coco-harness

An agent harness on top of CocoIndex. You describe a flow in a prompt; an
orchestrator agent runs it with subagents, sources and targets; CocoIndex makes
the whole thing incremental, so when the inputs change only the affected
subagents, reducers and model turns run again.

**Status: design frozen 2026-11-04; skeleton under construction.** Read
[`DESIGN.md`](DESIGN.md) first. The tests drive the harness with a scripted
model and assert on inference counts; the sample flows under `flows/` drive it
with a real model through LiteLLM and record what they measured.

## Layout

- `coco_harness/` — the package (see DESIGN.md §9 for the module map, §8 for
  what is implemented and tested)
- `tests/` — scripted-model tests; no API keys needed
- `flows/` — three sample flows with their data and a runner, one per shape
  of DESIGN.md §2; see [`flows/README.md`](flows/README.md) for how to run
  them and the measured model calls

## Running the tests

From the repository root, after `uv run maturin develop`:

```sh
uv run pytest examples/coco_harness/tests
```

Or as a standalone project:

```sh
cd examples/coco_harness
uv sync --group dev
uv run pytest
```
