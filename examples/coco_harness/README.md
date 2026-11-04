# coco-harness

An agent harness on top of CocoIndex. You describe a flow in a prompt; an
orchestrator agent runs it with subagents, sources and targets; CocoIndex makes
the whole thing incremental, so when the inputs change only the affected
subagents, reducers and model turns run again.

**Status: design frozen 2026-11-04; skeleton under construction.** Read
[`DESIGN.md`](DESIGN.md) first. A LiteLLM client exists but is untested; the
tests drive the harness with a scripted model and assert on inference counts.

## Layout

- `coco_harness/` — the package (see DESIGN.md §9 for the module map, §8 for
  what is implemented and tested)
- `tests/` — scripted-model tests; no API keys needed

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
