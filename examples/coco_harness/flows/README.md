# Sample flows

Three small flows, one per shape of DESIGN.md §2, each a directory with a
`prompt.md` and a few short input files. `run.py` runs one flow with a real
model through `coco_harness.app.build_app` and prints how many model calls the
run made, per role.

| Flow | Shape | Prompt, in short |
|---|---|---|
| `summaries` | A. map → targets | one summary file per markdown doc under `docs/` |
| `report` | B. map → reduce → answer | per-note decisions and questions, reduced into one `report.md` |
| `table` | C. observe → plan → map | a planner proposes columns from a sample, one JSON row per invoice |

## Running

From the repository root, with the LiteLLM extra installed
(`uv sync --extra litellm --inexact`) and the provider's key in the environment:

```sh
uv run python examples/coco_harness/flows/run.py summaries
uv run python examples/coco_harness/flows/run.py report -v     # log every turn: hit, replayed, picked, inferred
uv run python examples/coco_harness/flows/run.py table -vv    # also log every request as the model saw it
```

The model is `$LLM_MODEL`, else `anthropic/claude-sonnet-5-5`. Outputs go to
`<flow>/out/` and the CocoIndex state to `<flow>/.coco_db/`, both ignored by
git; delete `.coco_db` to start a flow from scratch. The prompts use paths
relative to the flow directory, which `run.py` changes into.

To see the incrementality, run a flow twice unchanged (the second run makes no
model call), then edit one input file and run again.

## Measured model calls

Measured 2026-11-04 with `anthropic/claude-sonnet-5-5` through LiteLLM, from
an empty `.coco_db`. "One edit" is one line appended to `docs/targets.md`, one
line appended to `notes/2026-10-13.md`, and the status of
`invoices/inv-1044.txt` flipped, respectively. Claude 5.5 only accepts its
default temperature, so counts on the first run vary by a few calls between
drives; the unchanged and one-edit counts were stable across every drive.

| Flow | First run | Unchanged | One edit |
|---|---|---|---|
| `summaries` | 15 (orchestrator 4, planner 2, worker 9) | 0 | 2 (the edited file's worker: declare, answer) |
| `report` | 15 (orchestrator 5, reducer 2, worker 8) | 0 | 2 (that note's worker 1, the reducer 1) |
| `table` | 18 (orchestrator 4, planner 2, worker 12) | 0 | 3 (planner 1, that invoice's worker 2) |

What the counts show:

- A worker's first turn (`read_input`) hits even for the edited file: its memo
  key is the spec and an empty transcript. Only the turns after the content
  entered the transcript run again.
- In `report`, the reducer's `read_input` turn hits too; its combine turn runs
  again because a member changed. The orchestrator's `declare` passes the
  result by handle, so the new report reaches the target with no orchestrator
  inference.
- In `table`, the planner's `sample` covers all four invoices (there are
  fewer than the sample size), so any edit re-runs the planner's session. It
  sees the sample diff and its previous plan in the `[replay]` block and
  reissues the plan with `replay(from_step=1)`: one call, a byte-identical
  plan, and the orchestrator and the three other workers are untouched. With
  a realistic folder the sample rarely moves and the planner is skipped
  outright.
- In `summaries`, the orchestrator spends a planner session on the exact file
  count the prompt asks for, because a `for_each` card shows a widened range
  (`2–4`). That session is skipped on later runs while the listing is
  unchanged.

## What the drive changed in the harness

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
