# coco-harness — design

An agent harness on top of CocoIndex. A user describes a flow in a prompt
("for every doc under `docs/`, write a review note against `AGENTS.md`";
"what do these 2,000 interview notes say about pricing?"). An orchestrator
agent runs the flow with subagents, sources and targets. CocoIndex makes the
whole thing **incremental**: when the inputs change, only the affected
subagents, reducers and model turns run again.

**Status: frozen 2026-11-04.** The design is agreed. It was reviewed
externally in three rounds and swept once by a fresh reader; the findings are
folded into the owning sections, and the history is in git. Further findings
go to implementation and tests. §8 is the delta index between this design and
the code. The harness lives under `examples/` for now and is expected to
become its own repository, so it is more elaborate than the other examples.

---

## 0. Scope and altitude

This document is **normative** for:

- the contracts the model sees: tools, handles, cards, input manifests, the
  replay block (§3, §4);
- the invariants the harness keeps: what enters a memo key and what does
  not, what is stored, when a session is skipped, how failures are
  classified (§4, §6, §7);
- the acceptance cases listed under *Acceptance:* lines, which the tests in
  `tests/` implement.

It is **constraint-only** for operational mechanics (how the loop is coded,
which asyncio primitive runs a batch, buffer sizes): the implementation owns
those as long as the stated invariants hold. Measurements quoted here are
one-off observations, labelled as such, not acceptance evidence.

It says nothing about deployment, model providers' pricing, or how a flow
prompt should be written.

### Vocabulary

- **Session**: one agent conversation, run as a CocoIndex component.
- **Closed session**: everything it can read is an argument of its component,
  so the component is memoized whole (§7). **Open session**: it can read
  beyond its arguments, so only its turns are memoized.
- **Turn**: one model call; a memoized child component of the session.
- **Slot**: the component path a child is mounted at (`turn/<n>`,
  `child/<key>`).
- **Handle**: a stable name for a live object (`src:docs`), with a **card**
  listing its methods.
- **Transcript**: the canonical sequence of (model output, tool results) of
  one session; **`prev`** is the one stored by the last successful run.
- **Scratch**: a run-local channel from a turn back to the session loop (§4).

## 1. The central idea: control plane vs. data plane

The harness is incremental only to the extent that the orchestrator's
transcript stays the same when the data changes. Everything below follows from
keeping two things apart:

- **Control plane — the model transcripts.** Small, canonical, insensitive to
  data changes. Each model turn is a memoized component keyed on the transcript
  so far; an identical prefix replays last run's action with no inference.
- **Data plane — file contents, per-item results, reduce outputs.** These move
  between components as CocoIndex values (component arguments, return values,
  target states). Transcripts refer to them by **handle** and see contents only
  when the model explicitly reads them.

One file changes in a 5,000-file folder: the orchestrator's transcript still
reads `for_each(src:docs, …) → coll:notes (4k–8k items)` in both runs, so the
orchestrator's turns replay without inference; that file's worker reruns its
content-dependent turn; the reduce tree reruns the groups whose members
changed (§6). The orchestrator needs inference again only where a value it
actually read has changed. Tool execution, source scans and memo validation
still run every time: what scales with change is inference, not wall time.

Two engine facts shape the design:

- A memoized function called inline cannot mount children, but a component body
  can. So the agent loop lives in a component body (which runs tools: mount,
  declare), and only the model call is memoized.
- Memo keys are computed on values, not on dirty flags. A planner or reducer
  that reruns but produces the same output lets its dependents hit (§7, reuse
  model).

## 2. Use cases for v1

| Shape | Example prompt | Tools |
|---|---|---|
| **A. Map → targets** | "Translate every doc under `docs/` into ja and de; keep code blocks." | `open_source`, `open_target`, `for_each(inputs=[tgt])`, `declare` |
| **B. Map → reduce → answer** | "What do 2,000 interview notes say about pricing? Write report.md with quotes." | `for_each(collect)`, `reduce`, `declare(value=res:…)` |
| **C. Observe → plan → map** | "Turn this folder of invoices into a table; figure out the columns." | `observe(inputs=[src])`, `for_each` |
| **D. Group-by reduce** | "Per person, list open action items across all meeting notes." | `for_each`, `reduce` with a grouping step (target, §8) |

What reruns after a change is defined once, in §7 (reuse model) and §6
(reduce locality); the shapes above only pick tools.

Directions kept in view, not in v1:

- **Cheap prompt iteration.** Editing the orchestrator prompt misses the cache
  at turn 0, but the model sees last run's actions as numbered picks (§4).
  Picking reissues a byte-identical child spec at the same slot, so the child
  hits. A demo to build: count inference calls per run across five prompt
  edits.
- **Always-on agents** (target). The same flow under a live source would be a
  reactive agent. It needs a live `for_each` (`mount_each` over a live
  `items()`), and collect/reduce need a refresh design; nothing in the skeleton
  watches anything yet.
- **Freeze a flow into a pipeline.** A settled transcript is effectively a
  program (source → per-item prompt → reduce → target); exporting it as plain
  CocoIndex Python removes orchestrator inference entirely.
- **Dogfooding.** Coding-agent session logs → lessons per session → reduced
  into proposed `AGENTS.md` edits.

## 3. What the model sees

**Sessions** have four roles: orchestrator, worker, planner (`observe`) and
reducer. A role has a system prompt, an allowed tool set, a model, and a turn
budget (`RoleSpec`). A session's model-visible contract is its `SessionSpec`:
the role, the task, the role's **tool specs**, the **input manifest** (the
handles it was given, with their kinds and methods), and the harness
**version**. Build it with `HarnessConfig.spec()`, which resolves a role's
`model="default"` to the configured model. A spec with an unresolved model, or
whose tool specs do not match the registry, is a programming error and fails
the session. The spec is part of every turn's memo key and of a closed
session's component key (§7).

**Handles** are stable names for live objects: `src:docs`, `tgt:wiki`,
`coll:notes`, `res:report`, `job:audit`. Opening an object returns a **card**
listing its methods, each tagged with how volatile its output is:

```
src:docs  localfs dir · 1k–2k files · *.md, *.mdx
  stats()        widened counts                     stable
  sample(k=5)    hash-stable representative items   stable
  read(key)      full text of one item              per-item
  keys(prefix)   every key                          volatile — planners only
```

Methods are called through one generic tool, `call(handle, method, args)`,
not through tools generated per object. Tool definitions are fixed for a whole
session, which keeps every turn's memo key stable and keeps the provider's
prompt cache valid (tools sit first in the cached prefix). Actions are JSON
tool calls, not a "code mode" in which the model writes Python against the
harness API: JSON calls are canonical, comparable and pickable (§4).

### Tool set (fixed)

| Tool | What it does | CocoIndex primitive | In the transcript | Closed? |
|---|---|---|---|---|
| `open_source(kind, name, path, patterns?)` | localfs in v1 | connector → `FileLike`… | card | no |
| `open_target(kind, name, path)` | a directory of files in v1 | `mount_target` | card | no |
| `call_agent(key, task, role?, inputs?)` | run an open subagent and wait | `use_mount` at `child/<key>` | `{result: res:key, chars}` | no |
| `start_agent(key, task, role?, inputs?)` | run in the background; effects only through targets in `inputs` | `mount` at `child/<key>` | `{job: job:key}` | no |
| `for_each(source, key, task, role?, inputs?, collect?)` | one closed worker per item | `use_mount` per item under `map`; `mount_each` when `collect=false` | `coll:` card (widened count); `{mounted}` when `collect=false` | no |
| `reduce(collection, key, task, threshold_tokens?)` | hierarchical combine (§6) | `use_mount` per reducer call | `{result: res:key, chars}` | no |
| `declare(target, key, value)` | declare a target state; `value` is text or a `res:` handle | `declare_file`, owned by the calling session's component | acknowledgement | yes |
| `observe(key, question, inputs?)` | an open planner that may use volatile methods | `use_mount` at `child/<key>` | `{plan}` | no |
| `call(handle, method, args)` | call an object method | — | the method's result | yes |
| `read_input()` | a closed session's input: `{key, text}` for a file, the member list for a reduce | — | the content | yes |
| `replay(from_step, steps?)` | harness meta tool, §4 | — | never stored | — |
| finish (`answer`) | end the session | the component's return value | — | — |

"Closed?" is the tool's `closed_ok` flag: whether a closed session's role may
have it (see "Inputs and closed children" below).

### Rules that keep it incremental

**Child keys are identity.** Every model-named child lives under one reserved
namespace at a fixed depth: `child/<key>` for `call_agent`, `start_agent` and
`observe`; `child/<key>/<item key>` for `for_each` workers;
`child/<key>/<group prefix>/<attempt>` for reducer calls (`root` for the empty
prefix). Turns live under `turn/<n>`. Renaming a key deletes the old subtree's
target states and recomputes it. A key may be used once per session — a second
use, in the same turn or a later one, is a tool error, and a call that is
rejected before it mounts does not consume its key. Because every child path
starts with its key, distinct keys cannot alias one engine path, so model
input does not reach the engine's duplicate-path check.

**No polling, no time.** Every tool result is a deterministic function of its
arguments and the data: no `status(job)`, no `wait_any`, no timestamps, JSON
with sorted keys. One timing-dependent byte breaks the memo for every later
turn.

**Calls in one turn are independent.** They run concurrently against a
snapshot of the handle table taken before the turn:

- a call does not see a handle a sibling registers in the same turn;
- two siblings registering the same name both fail and publish nothing;
- two siblings mounting under the same key both fail before anything runs;
- results keep call order; a call that depends on another belongs in the next
  turn;
- every call is drained before an infrastructure failure propagates.

**Arguments are validated at the boundary.** Shape (an object), required and
unknown names, declared primitive types and enums are checked before a handler
runs; a failure is the model's (`ok=False`). Arguments that are not an object
are reported the same way; §4 says how they are stored. Inside a handler, only
`ToolError` is the model's; any other exception is infrastructure (§7).

**Inputs and closed children.** `inputs` lists the parent's handles the child
may use. The child's table is seeded with them under the same names, and the
child's spec carries their cards, rendered under the task
(`tgt:out (localfs_dir): declare_file`), so its model knows what it has.

- A **closed** child (`for_each`; `reduce` passes no inputs) accepts only
  objects whose contents are covered by its memo key — targets and results —
  and only a role whose tools are all `closed_ok`. A role with any tool marked
  *no* in the table is refused for closed work.
- Sources and collections go only to **open** children (`call_agent`,
  `observe`, `start_agent`).
- Closedness is checked by the mounting tool and again by the loop; it is
  never inferred from the inputs alone.

**Results are handles.** `call_agent` and `reduce` return a `res:` handle and
a widened size, never the text. Later actions pass the handle
(`declare(tgt, key, value="res:report")`), so a replayed action uses *this*
run's value. A model that wants the text reads it (`call(res:x, "read")`), and
from then on its transcript depends on that content — which is the correct
dependency. A handle-shaped `value` that does not resolve, or resolves to
another kind of handle, is an error; it is never written as literal text.

**Volatile methods are role-gated.** `keys()` is available only to planners.

*Acceptance:* `test_for_each.py::test_workers_write_to_the_parents_target_named_by_their_input_cards`
(the worker finds the target in its input cards),
`::test_for_each_without_collect_mounts_workers_in_the_background`,
`::test_a_source_cannot_be_an_input_of_closed_workers`,
`::test_a_role_with_open_tools_cannot_run_closed_workers`,
`::test_two_calls_registering_one_name_in_a_turn_both_fail`,
`::test_distinct_child_keys_never_alias_one_engine_path`;
`test_agents.py::test_result_handles_cross_the_mount_and_resolve_or_fail_in_declare`,
`::test_duplicate_child_keys_are_tool_errors_not_engine_errors`,
`::test_background_agents_act_through_inputs_and_planners_return_plans`;
`test_replay.py::test_argument_validation_is_the_models_error_and_internal_lookups_are_not`,
`::test_non_object_arguments_are_the_models_error_and_survive_the_run`.

## 4. Replay policy: "dump once"

### The turn and its key

Each model turn is a memoized component:

```python
await coco.use_mount(coco.component_subpath("turn", k), llm_step, spec, tools, prefix, hints)

@coco.fn(memo=True, memo_key={"hints": None})
async def llm_step(spec, tools, prefix, hints) -> ModelOutput: ...
```

The key is `(spec, tools, prefix)`: the session contract (§3) and the
transcript so far. `hints` is excluded from the key and never stored; it
carries run-time context (below) and a mutable `TurnScratch` through which the
turn body tells the loop what happened: nothing (a memo hit), an inference, a
harness replay, a pick with its step number, and any notes it showed. The loop
never reads engine internals to learn hit from miss.

The transcript is `(output, results)` pairs and nothing else. `ModelOutput`
has `calls` and `answer`; replay bookkeeping lives in the scratch, not in the
output, because every dataclass field is part of the fingerprint. The output
is made storable before it is memoized: a call whose arguments are not a JSON
object is kept as `{"$invalid": <the raw value>}`, so the transcript and the
turn memo stay dict-typed and load on the next run, and the registry reports
the call as the model's error (§3).

### What "previous run" means

`prev` is the transcript stored by this session component's last
**successful** run (`use_state("transcript")`). Turn memos are committed per
turn (§7), so a failed run leaves turn entries that may differ from `prev`.
Two consequences, both intended:

- A hit on such an entry shows up as a divergence against `prev`, and the
  model sees `prev`'s remaining actions.
- A fast-forward *request* can be answered by such an entry instead of the
  replay body. That entry is the session's own earlier decision for this exact
  state, and **it wins**: the loop executes it, re-aligns it like a composed
  action, stops fast-forwarding, and adds a note for the next inference, if
  there is one. The memo may itself be a final answer, and later turns may hit
  too; a note does not force a model call. Requested provenance is never
  assumed to be actual provenance: the scratch's `replayed` flag, or an equal
  action, decides (alignment rule 2).

### Before divergence

`cursor` is the index in `prev` of the step expected next; it starts at 0 and
follows alignment. While each turn hits and each result equals `prev`'s, no
model is called. Until the first inference, the dump passed to a turn is
"`prev` from `cursor` on", positioned at the current turn: unused on a hit,
and what the model needs on a miss (a prompt edit, a lost entry).

### At divergence: the dump is shown once and stays put

Two events freeze the dump for the rest of the run:

- the first inference: the dump is frozen at the position the model saw it;
- a result that differs from `prev`'s for an aligned action: the diff is
  recorded and the dump is frozen one position later, holding `prev` from the
  step after the changed one.

Once frozen it is never moved or edited, so the provider's prompt cache keeps
the prefix. Later notes and diffs are appended after the step they refer to —
including a note the model saw while being asked to correct an invalid pick,
so the rendered history matches what the model saw.

Illustrative rendering (the exact text is in `litellm_client.py`):

```
[replay] result differs from last run for call(src:docs, "stats"):
  before: {"files":"1k–2k"}   after: {"files":"2k–4k"}
[replay] Last run continued from here with:
  [4] for_each({"source":"src:docs","key":"notes",…})
  [5] reduce({"collection":"coll:notes",…})
  [6] finish: …                    <- literal content; review before reuse
Call replay(from_step=N) to reissue [N] and the following until a result differs;
replay(from_step=N, steps=K) for at most K; or act on your own.
```

Numbers are `prev`'s step numbers. Arguments are truncated to 160 characters
in the listing; a pick reissues the exact arguments. Lines carrying literal
payloads (a final answer, a `declare` whose value is not a handle, a call with
`$invalid` arguments) are marked, because reissuing them repeats old content.

### Alignment

Alignment is by action identity, not by position; `cursor` only supplies the
candidate for rule 3. For each executed turn the loop aligns it with at most
one step of `prev`, in this order:

1. a pick → exactly the picked step (the scratch carries its number);
2. a harness replay that ran, or a hit that returned the same action → the
   replayed step;
3. otherwise `prev[cursor]` if it is the same action;
4. otherwise the first step after the last aligned one with the same action —
   best effort; with repeated identical actions it pairs the first unmatched
   occurrence, so the resulting diff is advisory.

Action identity is per turn: same calls in the same order, or the same
answer. A parallel batch with one changed call is a different action and gets
no comparison. An aligned step sets `cursor` to the aligned step plus one and
has its results compared per call; differences become diffs.

### Fast-forward

After a pick, or after a harness replay whose results matched, the loop
replays `prev`'s next step itself: `llm_step` runs with `hints.replay` set,
returns that output without inference, and **still writes its memo entry**,
so the next run hits on it. Fast-forward stops when a result differs, when a
pick's `steps` budget is spent (`steps=1` is a single pick), when an existing
memo answers with a different action (above), or when the next step is a final
answer — then a note asks the model to confirm or revise. A pick *of* a final
answer is honoured as the model's own answer.

`replay` is the only call allowed in its turn. An unresolvable pick (bad step
number, no dump, arguments that are not an object or not integers) is shown
back to the model once as a note. A second unresolvable pick in the same turn
fails the session as an infrastructure failure (§7). Meta calls therefore
never reach the transcript.

### Why picks matter beyond convenience

Picking a number reissues a byte-identical action. For a child mount that
means the same spec at the same slot, so the child hits (§7). A paraphrase
with one word changed would miss for the whole subtree.

### Downsides and mitigations

| Downside | Mitigation |
|---|---|
| Stale literals in picked actions | results travel by handle (§3); literal lines are marked in the dump; `finish` is never fast-forwarded |
| Stale handles in picked actions | a handle that does not resolve is a tool error the model reads next turn |
| Dump size | model outputs only, arguments truncated |
| Anchoring on last run's plan | the diff is shown first; planner and reducer prompts frame it as keep-or-revise |
| Transcript growth | hints are never stored; the transcript is flat `(output, results)` pairs |
| Advisory alignment for repeated composed actions | picks are exact; only composed duplicates are best effort (rule 4) |
| A failed run's memo overriding a requested replay | the memo is executed, the model is told, fast-forward stops |

*Acceptance:* `test_replay.py` — `test_unchanged_run_makes_no_model_calls`,
`test_changed_result_shows_dump_once_then_fast_forwards`,
`test_task_edit_dumps_at_turn_zero_and_fast_forwards_everything`,
`test_dump_is_frozen_where_the_model_first_saw_it`,
`test_model_may_pick_last_runs_final_answer`,
`test_pick_keeps_its_occurrence_among_repeated_actions`,
`test_bounded_pick_stops_after_its_steps`,
`test_invalid_pick_is_shown_back_once_and_the_note_stays`,
`test_cached_decision_from_a_failed_run_wins_over_a_requested_replay`,
`test_result_values_are_normalized_json`; `test_transcript.py`;
`test_rendering.py`.

## 5. Containing volatility

### Observation and planning

`observe()` runs an open planner whose transcript may be as volatile as it
likes; only its answer (the plan) reaches the caller, and the plan feeds every
child's memo key, so plan stability matters most. Three layers protect it:
stable observations (below), a keep-or-revise prompt, and value-based reuse
(an unchanged plan lets its dependents hit).

### Widened counts

Counts are reported as log₂ buckets everywhere a count appears: source cards,
`for_each` cards, result sizes. Powers of two render with `k`/`M`
(`1024–2048` is shown as `1k–2k`); smaller buckets render as plain numbers
(`4–8`).

Target: hysteresis — keep the previous bucket, held in `use_state`, while the
value stays within ±10% of a boundary, so a count hovering near 1,024 does not
flip back and forth. The helper exists (`widen_count(n, prev)`); nothing
stores `prev` yet (§8).

### Stable sampling

`sample(k)` returns the `k` items with the smallest stable hash of their key
(bottom-k) and the head of each. A new item changes the sample with
probability about k/N.

### Stable hash

Every hash used for grouping or sampling — bisection (§6), bottom-k — is a
keyed blake2b with a fixed key (`stable_hash.py`), never Python's randomized
`hash()`. Changing it reshuffles every group and sample, so treat it as a
version bump.

*Acceptance:* `test_stats.py`, `test_stable_hash.py`.

## 6. Hierarchical reduce: size-bounded bisection at every level

Fixed-size chunking works against incrementality: one insert shifts every
group boundary. Content-defined cuts give only an *expected* group size and a
position-dependent fallback. The reduce here uses **recursive hash bisection
with one size rule at every level**, and bounds the model's inputs by a size
*estimate* (characters / 4) summed over members, not by the rendered request.

### Items, identities, groups

Every item has an identity (64 bits, most significant bit first) and a size.
Raw items use the stable hash of their key; a reducer's output uses its group's
trie prefix (identity = prefix, depth = its length). `bisect_groups` splits a
set on successive bits until each group fits the threshold T; a single item is
always its own group.

### The frontier invariant

Every item on the frontier has `size ≤ T/2`.

- Admission: a raw item above T cannot be processed by any call and is an
  error. A raw item in (T/2, T] is condensed by a single-item reducer call
  before combining.
- Every reducer output is checked against T/2; a larger one is re-asked once
  with a shorter-answer instruction at its own attempt slot; a second
  oversize output fails the reduce.
- With the invariant, any two items fit together, so every level with two or
  more items has at least one multi-member group and the item count strictly
  decreases. That is the termination argument; the prompt instruction to keep
  answers short is not relied on.
- A single input gets exactly one reducer call, so the task is applied to it
  rather than bypassed. An empty collection is a tool error.

Splitting on bit *i* is always defined: a multi-member set under prefix *p* has
every member strictly deeper than |p|. Distinct keys whose 64-bit identities
collide and do not fit together fail loudly (`HashExhausted`) rather than
producing an over-budget call; with keys unique per collection this needs a
hash collision, which the harness does not try to handle.

### Slots and reuse

Each reducer call — a multi-member group, a condensing call, or the
single-input call — is one `use_mount` at slot
`child/<key>/<prefix>/<attempt>` (`root` for the empty prefix) with the
members' texts as the argument. Prefixes of multi-member groups are unique
within one reduce (a collapsed prefix never recurs at a later level), so the
prefix identifies the slot. Reuse follows the engine's rule (§7): the latest
memo at the same slot, with the same members. When the tree reshapes — two
groups merge because a sibling shrank, or a group splits because a member
grew — the affected groups are new calls even where their member sets did not
change.

### Locality

A change to one item re-runs the group containing it and the groups above it
along one root-to-leaf path, **provided the tree shape is unchanged**.

When a size change makes a group overflow (or lets an ancestor fit), the
subtree under that group's prefix is re-partitioned **at that level**: sibling
groups at the same level outside the subtree are untouched, but inside it the
recursion may peel off several groups along a shared prefix, so one former
call can become many. Groups at higher levels that contain the changed region
re-run or re-partition in turn, as they would for any changed member. There is
no constant bound on the number of calls inside the affected subtree; it
depends on the hash trie's shape around the change.

### Roadmap for reduce

- **Hysteresis**: keep last run's grouping in `use_state`, resplit only above
  T, remerge only below ~0.5·T, to damp oscillation near a boundary.
- **Prefix partitioning** (`partition="prefix"`): partition by key prefix
  (directory) first and fall back to hash bisection inside oversized prefixes,
  because sixteen files from one directory summarize better than sixteen
  random files.
- **Delta prompting**: give a reducer its previous output and the list of
  changed members and ask for a minimal edit, which raises the chance of a
  byte-identical output; the output then depends on history, so a rebuild
  flag must recompute from scratch.

### Comparison

| | Content-defined cuts | Fixed fan-in on a hash trie | **Size-bounded bisection everywhere** |
|---|---|---|---|
| Bound per call | expected only | estimate at leaves, none above | **estimate at every call, frontier ≤ T/2** |
| Unchanged shape | ~log_F N calls | one call per L levels | one call per level |
| Shape change | position-dependent cascades | — | re-partition of one subtree per level |
| Knobs | F, min/max | T, L | **T** |

*Acceptance:* `test_reduce_grouping.py` — every multi-member group fits;
a size change is confined to its group's subtree at that level; prefixes
unique; two size-6 items at T=10 are condensed then combined; an item above T
is an error; an oversize output is an error for the caller; a single input
gets one call; a non-fitting collision fails loudly; empty input is rejected.
The `reduce` tool's glue (slots, attempt retry) is not yet covered (§8).

## 7. Integration with CocoIndex

| Harness concept | CocoIndex |
|---|---|
| A run of the flow | `coco.App(run_open_session, config.spec("orchestrator", prompt))` |
| An open session | a component; body re-runs each time |
| A closed session | a `memo=True` component on `(item, spec, handles)`; a reducer call on `(members, spec)` |
| A model turn | `use_mount(subpath("turn", k), llm_step, …)` — a memoized component per turn |
| Previous transcript | `coco.use_state("transcript")` on the session component |
| A tool handler | `coco.fn`-wrapped, so its own code is tracked |
| Per-run report | `UpdateStats` |

### Reuse model

The engine keeps **one memo per component path**: the latest successful run's
inputs fingerprint and return value. A mount is skipped when the same path is
mounted with matching inputs and its logic and memo-state dependencies
validate. There is no content-addressed cache across paths and no history at
a path: identical inputs at a different slot miss, and A→B→A at one slot
recomputes A. Everything in this document that says "hits" means this.

### What invalidates a session

A session's inputs — and so what invalidates it — are:

- its `SessionSpec`: role name, system prompt (including the protocol text),
  concrete model id, turn budget, task, the role's **tool specs** (an edited
  description or schema misses every turn *and* every closed session with
  that tool), the **input manifest**, and the harness **version**;
- for a closed session: the item (file identity, then mtime/content memo
  state), and the memo keys of its `inputs` handles (a target's key, a
  result's content hash);
- tool **logic**: a handler's own source is tracked because handlers are
  `coco.fn`-wrapped; editing one invalidates the closed sessions that ran it
  (their turns still hit where the transcript is unchanged, so this costs tool
  execution, not inference).

Tracked logic stops at the handler: the engine strips decorators from logic
fingerprints and does not follow undecorated helpers, so a handle object's
method (`LocalDirSource.call`, `LocalDirTargetHandle.call`) or any other
helper is **not** tracked. Bump `HarnessConfig.version` when such code changes
behaviour; it is in every spec.

Not tracked, by design: the model client object and the harness config
(context keys without change detection), and external resources a session was
not given as inputs — which is exactly why a closed session may only have
`closed_ok` tools (§3).

### Open vs closed

- A **closed** session's component is `memo=True`: unchanged item, spec and
  input handles → skipped entirely, children and target states carried over.
  Its role has only `closed_ok` tools and its inputs only `closed_input_ok`
  objects; both are checked by the mounting tool and again by the loop.
- An **open** session reads beyond its arguments (a source, a collection);
  memoizing it would give wrong hits. Its body reruns; its turns hit the
  per-turn memo; its tools re-execute. Cost per run: tool execution plus one
  component-memo read per turn and per child.

### Failures

Two classes, decided at the tool boundary:

- **Model-facing**: argument validation failures (§3) and `ToolError` raised
  by a handler — unknown handle, bad reference, invalid inputs, a reused key.
  Becomes an `ok=False` result the model reads next turn. The session may still
  finish; a finish after an unrepaired `declare` error leaves that target state
  undeclared, which the engine then treats as removed. That is the declarative
  contract, and it is the model's responsibility; the harness does not
  second-guess a finish.
- **Infrastructure**: any other exception — I/O, connector or engine errors, a
  handler's own bug such as an internal `KeyError`, a second unresolvable
  replay pick in one turn (§4). It propagates, so the session component fails.
  The other calls of the turn are drained first. Nothing of that session is
  committed — no transcript, no memo for the session, no target changes — and
  the next run retries it. Turns already completed keep their own entries;
  children already committed keep theirs.

*Acceptance:* `test_replay.py::test_model_facing_errors_are_results_and_infrastructure_errors_fail_the_run`,
`::test_argument_validation_is_the_models_error_and_internal_lookups_are_not`.

### Why one `use_mount` per turn

A component that raises drops every fn-memo entry it made: the error returns
before `submit`, the only place the flush plan is built
(`rust/core/src/engine/component.rs`, `let ret = ret?;` before `submit`). A
session that dies at turn 40 would lose turns 0–39 if turns were inline
memoized calls. Children mounted with `use_mount` commit on their own, and
nothing on the parent's error path touches them; the failure test above
exercises exactly this (the turn before the failing tool is reused after the
failure).

One-off measurement (debug build, 200 turns, not acceptance evidence):
~4 ms cold, ~1.3 ms warm, ~290 B of DB growth per turn, against ~0.7 ms /
~40 B for a direct memo call. The turn index is only the slot; the memo
decision uses the prefix argument.

A `@coco.fn(memo=True)` passed to `use_mount` uses the **component** memo
layer (one row per path); `memo_key` applies there too.

### Framework roadmap

| Capability | Status | Harness v1 | Later |
|---|---|---|---|
| Progress survives failure | solved by `use_mount` per turn | ✅ | partial fn-memo flush on failure (S–M; pure entries only) |
| Skip unchanged **open** sessions | being built: `coco.DataDep` + `use_data_dep` + `cannot_skip` | replay | first slice: `recollect_after_run` for components (S) |
| Memo probe ("would this hit?") | deferred | — | S |
| A component hit keeps its `use_state` values | engine behaviour | ✅ | — |

Engine terms in this table: `LogicDep` is the engine's import-time logic
dependency; `recollect_after_run` lets a memo state be collected after the
body runs; `DataDep`/`use_data_dep` is the planned runtime data dependency.

`LogicDep` cannot carry data deps: it is a set-membership check against
fingerprints registered in the process now, must be constructed at import,
and is unioned into every ancestor's entry. The chosen engine design stores
deps only in the component that read them, validates them from stored bytes
at lookup, and propagates an O(1) `cannot_skip` flag so a memoized ancestor
re-runs its body while its closed children still hit. With it, "open" is
decided by what a run actually read rather than by the entry point, which
would let the harness drop the closed/open split.

## 8. Status: implemented, covered, target

**Implemented and covered** — each item names the test that exercises it
(scripted model, no API keys):

| Behaviour | Test |
|---|---|
| Replay contract of §4: hits, dump freezing, picks, bounded steps, invalid picks, cached-decision precedence, notes retained | `test_replay.py` (see §4 *Acceptance*) |
| Closed workers over a localfs source; one worker's content turn per edit | `test_for_each.py::test_edit_one_file_reruns_one_workers_content_turn_only` |
| `for_each(collect=false)`: background workers, their declarations, cleanup on delete | `test_for_each.py::test_for_each_without_collect_mounts_workers_in_the_background` |
| Tool description edit invalidates closed workers | `test_for_each.py::test_a_tool_description_edit_invalidates_closed_workers` |
| Version bump invalidates turns | `test_replay.py::test_version_bump_invalidates_every_turn` |
| Inputs across mounts, input cards, closed-input and closed-role checks | `test_for_each.py` (§3 *Acceptance*) |
| `call_agent` result handles; `declare` with resolving / missing / wrong-kind references; `call(res:…)` | `test_agents.py::test_result_handles_cross_the_mount_and_resolve_or_fail_in_declare` |
| Duplicate child keys within and across turns; a rejected call keeps its key free | `test_agents.py::test_duplicate_child_keys_are_tool_errors_not_engine_errors` |
| `start_agent` acting through an input target; `observe` returning a plan | `test_agents.py::test_background_agents_act_through_inputs_and_planners_return_plans` |
| Failure classes; completed turns survive a later failure | `test_replay.py` (§7 *Acceptance*) |
| Non-object arguments: canonical form, model-facing error, next run loads the transcript | `test_replay.py::test_non_object_arguments_are_the_models_error_and_survive_the_run` |
| Distinct child keys across tools never share an engine path | `test_for_each.py::test_distinct_child_keys_never_alias_one_engine_path` |
| Message rendering: task + input manifest, dump with literal marks, diffs, notes, canonical invalid calls; provider response parsing, malformed arguments kept as the model's error | `test_rendering.py` |
| Reduce algorithm (pure): admission, bisection, output check, termination, collision | `test_reduce_grouping.py` |
| Widened counts, bottom-k, stable hash, canonical JSON | `test_stats.py`, `test_stable_hash.py`, `test_transcript.py` |

**Implemented, not covered by a test:**

| Item | Owning section |
|---|---|
| `reduce` tool glue: group slots, attempt retry, `run_reduce_session` | §6 |
| LiteLLM client: the API call itself (rendering and response parsing are covered) | §4 |

**Target** (contract settled where stated, not implemented):

| Item | Owning section | Note |
|---|---|---|
| `group_by` reduce (shape D) | §2, §6 | no API yet |
| Stats hysteresis via `use_state` | §5 | helper exists, state not stored |
| Table targets (sqlite/postgres), other sources | §3 | only `localfs` / `localfs_dir` |
| Always-on / live mode | §2 | no watcher, no refresh design |
| Reduce roadmap: hysteresis, prefix partitioning, delta prompting | §6 | — |
| Output schemas for agents/planners | — | dropped from v1 (minimal surface); plans are text |

## 9. Package layout and testing

```
coco_harness/
  __init__.py      the public surface: HarnessConfig, HARNESS, MODEL_CLIENT, ModelClient,
                   RoleSpec, SessionSpec, SessionResult, ToolSpec, Tool, tool, ToolRegistry,
                   ToolError, run_open_session. Everything else is implementation.
  spec.py          RoleSpec, SessionSpec, InputCard, SessionResult, ToolSpec, Item
  errors.py        ToolError
  transcript.py    ToolCall, ModelOutput, ToolResult, Step, Transcript; canonical JSON;
                   canonical_output ($invalid arguments)
  hints.py         RunHints (Dump, Diff, Note, Replay, TurnScratch); the replay tool spec
  replay.py        llm_step; resolve_replay; alignment by action identity
  session.py       the loop: run_open_session / run_closed_session / run_reduce_session
  handles.py       HandleTable (seed, fork/merge), HandleObject, cards, ResultValue
  model.py         ModelClient protocol, MODEL_CLIENT
  stable_hash.py   keyed blake2b
  tools/           Tool (closed_ok; key_arg = the argument that names the child slot),
                   ToolRegistry (validate_args), HarnessConfig (spec, version);
                   sources, targets, agents, reduce, stats
  roles.py         default roles and the protocol text
  litellm_client.py  renders a transcript to messages and calls LiteLLM (API call untested)
  testing.py       ScriptedModel and probe_tool
  app.py           default config; build a coco.App from a flow prompt; CLI
tests/             see the Acceptance lines in §3–§7 and the table in §8
```

Tests use a scripted model that counts its calls, so assertions are about
inference ("edit 1 of 5 files → 1 worker call, 0 orchestrator calls"). Tools
in replay tests are fakes whose results the test mutates between runs to force
a divergence. Child scripts derive handle names from the spec's input cards,
not from the test's knowledge.

## 10. Decisions index

Pointers only; the substance is in the owning section.

- Control plane / data plane split, handles, fixed tool set, JSON tool calls
  rather than a code mode — §1, §3
- Results as handles; inputs across mounts with cards; closed = checked
  capability (`closed_ok` tools, `closed_input_ok` inputs) — §3, §7
- Child path layout `child/<key>`; one key per session — §3
- Replay: dump once, frozen at first inference; scratch channel; exact pick
  provenance; bounded `steps`; meta calls never stored; an existing memo wins
  over a requested replay; `$invalid` canonical form — §4
- Reduce: bisection with the frontier invariant; prefix slots; honest
  locality; roadmap — §6
- Reuse model: one memo per path, same inputs; the spec carries tool specs,
  manifest and version; handler logic tracked, helpers via version — §7
- Failure classes decided at the tool boundary; drain before propagating — §7
- One `use_mount` per turn — §7
- Engine work: `DataDep` / `use_data_dep` + `cannot_skip` to be built; probe
  deferred — §7
- Stable keyed hash; `k`/`M` rendering of power-of-two buckets — §5
- Model client: LiteLLM behind a protocol — §3, §9
