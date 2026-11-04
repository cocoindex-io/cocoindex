# Sample flows

Three small flows, one per shape of DESIGN.md §2, each a directory with a
`prompt.md` and a few short input files. The prompts are written as a user
would write them: the outcome, where the data is, where the output goes, the
file naming when it matters, and what to answer. They do not mention workers,
planners or reducers; the orchestrator decomposes the task itself, following
the recipes in its system prompt (`coco_harness/roles.py`). `run.py` runs one
flow with a real model through `coco_harness.app.build_app` and prints how
many model calls the run made, per role.

| Flow | Shape the orchestrator picks | Prompt, in short |
|---|---|---|
| `summaries` | A. map → targets | one summary file per markdown doc under `docs/`, `a.md` → `a.summary.md` |
| `report` | B. map → reduce → answer | the decisions and open questions in `notes/`, as one `report.md` |
| `table` | C. observe → plan → map | one JSON object per invoice under `invoices/`, columns chosen from the data |

## Running

From the repository root, with the LiteLLM extra installed
(`uv sync --extra litellm --inexact`) and the provider's key in the environment:

```sh
uv run python examples/coco_harness/flows/run.py summaries
uv run python examples/coco_harness/flows/run.py report -v     # log every turn: hit, replayed, picked, inferred
uv run python examples/coco_harness/flows/run.py table -vv    # also log every request as the model saw it
uv run python examples/coco_harness/flows/run.py table --stats --plan   # cache usage, and the plan with what it reused
```

The answer goes to stdout; everything else to stderr. By default the runner
prints the model calls made, per role. `--stats` replaces that with one line:
per count, what the run would have cost with no cache and the share reused,
then this run's tokens with the provider's prompt-cache share, the files
unchanged, and the wall time. `--plan` prints the plan the harness chose as a
tree — the orchestrator turn by turn, each child with whether it was reused
or recomputed and why, a planner's plan verbatim, a fan-out as counts with
only the workers that re-ran itemized. On an unchanged rerun it shows the
stored plan at no model call (DESIGN.md §7, *Observability*).

The model is `$LLM_MODEL`, else `openai/gpt-6.1-terra` (`coco_harness.app.DEFAULT_MODEL`;
LiteLLM routes the bare `gpt-6.1-terra` to OpenAI too, the prefix just makes
it explicit). Outputs go to `<flow>/out/` and the CocoIndex state to
`<flow>/.coco_db/`, both ignored by git; delete `.coco_db` to start a flow from
scratch. The prompts use paths relative to the flow directory, which `run.py`
changes into.

To see the incrementality, run a flow twice unchanged (the second run makes no
model call), then edit one input file and run again.

## Measured: stability of the decomposition

Measured 2026-11-04 with `openai/gpt-6.1-terra` through LiteLLM at
`temperature=0`, three drives per flow, each from an empty `.coco_db` and an
empty `out/`. Every drive of a flow made the same orchestrator actions with
the same handle names and child keys, used the same roles, wrote the same set
of files with the same structure, and made the same number of model calls.

| Flow | Orchestrator actions (identical in 3/3 drives) | Output structure (identical in 3/3) | Model calls |
|---|---|---|---|
| `summaries` | open `src:docs` + `tgt:out`; `for_each(key=items, inputs=[tgt:out])`; answer | three `*.summary.md`, one paragraph each | 12 (orchestrator 3, worker 9) |
| `report` | open `src:notes` + `tgt:out`; `for_each(key=items, collect)`; `reduce(key=combined)`; `declare(report.md, res:combined)`; answer | `report.md` with `## Decisions` and `## Open questions` | 15 (orchestrator 5, reducer 2, worker 8) |
| `table` | open `src:invoices` + `tgt:out`; `observe(key=plan, inputs=[src:invoices])`; `for_each(key=items, inputs=[tgt:out])`; answer with the column list | four `inv-*.txt.json` with the keys `invoice, vendor, date, due, items, subtotal, tax, total, status` | 18 (orchestrator 4, planner 2, worker 12) |

What varies between drives is the wording of the child tasks and of the
outputs' prose, never the decomposition or the structure (one `report` drive
spelled `collect=true` where the others left the default: the same action).

## Measured: incrementality

Same model and date, continuing from the third drive above. "One edit" is one
line appended to `docs/targets.md`, one line appended to
`notes/2026-10-13.md`, and the status of `invoices/inv-1044.txt` flipped,
respectively.

| Flow | Unchanged | One edit |
|---|---|---|
| `summaries` | 0 | 2 (the edited file's worker: declare, answer) |
| `report` | 0 | 2 (that note's worker 1, the reducer 1) |
| `table` | 0 | 3 (planner 1, that invoice's worker 2) |

What the counts show:

- An unchanged `summaries` or `report` run is skipped whole: no session's
  body runs, no turn is probed (DESIGN.md §7, *Open vs closed*). An unchanged
  `table` run re-runs the orchestrator's body without inference — its
  planner records the sample it reads, so the engine cannot skip the parent
  (`cannot_skip`) — while the planner and the workers are skipped;
  `--stats` shows it as `sessions 6 (skipped 5 / 83%)`.
- A worker's first turn (`read_input`) hits even for the edited file: its memo
  key is the spec and an empty transcript. Only the turns after the content
  entered the transcript run again.
- The orchestrator never re-infers after an edit: its turns hit from their
  per-turn memos, because the cards it saw (widened counts, handles) are
  unchanged. In `report`, `declare` passes the reducer's result by handle, so
  the new report reaches the target with no orchestrator inference.
- In `table`, the planner's `sample` covers all four invoices (there are
  fewer than the sample size), so the edit re-runs the planner's session. It
  sees the sample diff and its previous plan in the `[replay]` block and
  reissues the plan with `replay(from_step=1)`: one call, a byte-identical
  plan, and the orchestrator and the three other workers are untouched. With
  a realistic folder the sample rarely moves and the planner is skipped
  outright.

What `--stats` reports for these runs, from the scripted drives of the same
shapes in `tests/test_observability.py` (the counts are the ones above; the
tokens and the wall time come from the real run):

| Flow | Run | `stats:` |
|---|---|---|
| `summaries` | unchanged | `model calls 12 (reused 12 / 100%), sessions 4 (skipped 4 / 100%), tool calls 9 (reused 9 / 100%), files 3 (unchanged 3 / 100%)` |
| `summaries` | one edit | `model calls 12 (reused 10 / 83%), sessions 4 (skipped 2 / 50%), tool calls 9 (reused 4 / 44%), spent tokens …, files 3 (unchanged 2 / 67%)` |
| `table` | unchanged | `model calls 18 (reused 18 / 100%), sessions 6 (skipped 5 / 83%), tool calls 13 (reused 9 / 69%), files 4 (unchanged 4 / 100%)` |
| `table` | one edit | `model calls 18 (reused 15 / 83%), sessions 6 (skipped 3 / 50%), tool calls 13 (reused 6 / 46%), spent tokens …, files 4 (unchanged 3 / 75%)` |

The first drive of these flows (2026-11-04, `anthropic/claude-sonnet-5-5`,
prompts that still named workers and planners) measured 15/15/18 calls on a
first run, 0 unchanged and 2/2/3 after one edit; its findings are in the
next section.

## What the drives changed in the harness

Second drive (`gpt-6.1-terra`, user-level prompts):

- The orchestrator's system prompt carries recipes: open every source and
  target in the first turn, named after their folders; `for_each(key=items)`
  for one output per file, with the target in `inputs` and `collect` left at
  its default; `for_each` + `reduce(key=combined)` + `declare(res:combined)`
  for one combined output; `observe(key=plan)` first for a choice the data
  decides. Fixing the handle names and keys removes the free choices that
  varied between drives; with the old one-line role prompt, one drive in
  three chose `collect=false`, got no completion card, and fanned out again
  under a new key.
- The planner is told to observe in one turn, to name things after the
  data's own labels rather than synonyms it invents, to cover each labelled
  field once without splitting or deriving, and to answer as a compact list.
  Before the naming rule the column names drifted between drives
  (`invoice_id` / `invoice_number` / `invoice_no`, `tax` / `tax_amount`);
  with it the names came from the labels every time, but one drive in six
  still added two split-off columns (`tax_rate`, `currency`). With the
  one-field-one-column rule as well, the three drives in the table above
  chose the same nine columns.
- Tool cards say what the recipes rely on: `observe` is for a choice the data
  decides and its plan goes verbatim into later tasks; `for_each` names the
  target in `inputs`, and `collect=false` returns no card; `reduce`'s result
  is written with `declare(value="res:<key>")`.
- A source method passed an argument it does not take reports it
  (`sample does not take ['count']; it takes ['k']`) instead of defaulting:
  the planner wrote `sample(count=8)` three drives in a row and silently got
  five heads.
- Exact counts are not available to the orchestrator by design (every count
  is widened, DESIGN.md §5). A prompt that asks for "the number of files"
  made the orchestrator spend a reducer and an agent to count; the
  orchestrator is now told to report counts as shown, and the `summaries`
  prompt asks for a one-line status instead.

First drive (Claude Sonnet 5.5):

- The transcript is rendered as native tool calls (`tool_calls` and `tool`
  messages). Rendered as JSON prose, the model re-issued actions whose results
  it had already been shown.
- Literal payloads in the `[replay]` block (final answers, `declare` values)
  are shown in full. Elided to 160 characters, a planner could not confirm its
  previous plan and reworded it, which recomputed every worker; a worker
  reissued a stale `declare` it could not review.
- The planner is told its answer is the plan and nothing else. A plan that
  narrates what changed since last run is unstable by construction.
- Workers are told a declaration is final for the run, and a repeat is
  reported in those words.
- `temperature=0` is dropped where the provider refuses it (Claude 5.5).
