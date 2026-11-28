# Sample flows

Seven small flows: one per shape of DESIGN.md §2, one over PDFs, one over a
Google Drive folder and two over the web, each a directory with a
`prompt.md` and (except `drive` and the two `research` flows) a few short
input files. The prompts are written as a user
would write them: the outcome, where the data is, where the output goes, the
file naming when it matters, and what to answer. They do not mention workers,
planners or reducers; the orchestrator decomposes the task itself, following
the recipes in its system prompt (`cocoindex_agents/roles.py`). `run.py` runs one
flow with a real model through `cocoindex_agents.app.build_app` and prints how
many model calls the run made, per role.

| Flow | Shape the orchestrator picks | Prompt, in short |
|---|---|---|
| `summaries` | A. map → targets | one summary file per markdown doc under `docs/`, `a.md` → `a.summary.md` |
| `report` | B. map → reduce → answer | the decisions and open questions in `notes/`, as one `report.md` |
| `table` | C. observe → plan → map | one JSON object per invoice under `invoices/`, columns chosen from the data |
| `orders` | A. map → targets, over PDFs | one JSON object per purchase order under `orders/`, with the fields the prompt names, including which box of the Approval section is ticked |
| `drive` | A. map → targets, over a Google Drive folder | one summary file per file in the Drive folder `$DRIVE_FOLDER`, `Meeting notes` → `Meeting notes.summary.md` |
| `research` | E. search → read → follow → report, over the web | what incremental computation is and where it is used; search the web, read what is found, follow the leads worth following, write `out/report.md` with citations |
| `research_seeded` | E, starting from a page the prompt names | the same topic, starting from the page `$RESEARCH_SEED` (a page the user controls, so that a changed source can be staged) |

The purchase orders are PDFs the model reads natively (DESIGN.md §3,
*Documents*): one page each, a ruled line-item table, totals, and three
drawn boxes of which one is ticked with two strokes, so a text extractor
cannot tell which. `orders/make_orders.py` wrote them, with no dependency;
run it again to regenerate them.

## Running

From the repository root, with the LiteLLM extra installed
(`uv sync --extra litellm --inexact`) and the provider's key in the environment:

```sh
uv run python examples/cocoindex_agents/flows/run.py summaries
uv run python examples/cocoindex_agents/flows/run.py report -v     # log every turn: hit, replayed, picked, inferred
uv run python examples/cocoindex_agents/flows/run.py table -vv    # also log every request as the model saw it
uv run python examples/cocoindex_agents/flows/run.py table --stats --plan   # cache usage, and the plan with what it reused
```

The answer goes to stdout; everything else to stderr. By default the runner
prints the model calls made, per role. `--stats` replaces that with one line:
per count, what the run would have cost with no cache and the share reused,
then this run's tokens with the provider's prompt-cache share, the files
unchanged, and the wall time. `--plan` prints the plan it chose as a
tree — the orchestrator turn by turn, each child with whether it was reused
or recomputed and why, a planner's plan verbatim, a fan-out as counts with
only the workers that re-ran itemized. On an unchanged rerun it shows the
stored plan at no model call (DESIGN.md §7, *Observability*).

The model is `$LLM_MODEL`, else `openai/gpt-6.1-terra` (`cocoindex_agents.app.DEFAULT_MODEL`;
LiteLLM routes the bare `gpt-6.1-terra` to OpenAI too, the prefix just makes
it explicit). Outputs go to `<flow>/out/` and the CocoIndex state to
`<flow>/.coco_db/`, both ignored by git; delete `.coco_db` to start a flow from
scratch. The prompts use paths relative to the flow directory, which `run.py`
changes into.

To see the incrementality, run a flow twice unchanged (the second run makes no
model call), then edit one input file and run again.

### The `research` flow: the web as a source

The `research` flow searches the web, reads the pages it finds, follows the
leads worth following and writes a report with citations (DESIGN.md §3,
*Sources: the web*). Without a search key it searches **Wikipedia** (no key,
no cost) and reaches the open web through the articles' references; with
`TAVILY_API_KEY` set it searches with Tavily (a free tier of 1,000 credits a
month). The engine's identity is part of every record, so switching engines
re-runs the searches and the pages they find.

The agent fetches pages itself: it identifies itself as `cocoindex-agents`
with this repository's address, obeys `robots.txt` and `Crawl-delay`, sends
one request at a time per host, follows no sign-in, posts nothing, and
reads at most 12 pages from the searches and 6 from the leads. Pages are
normalized locally (title and main content as Markdown, by trafilatura); a
PDF is read by the model natively. The normalized pages are kept in the
flow's state (`.coco_db`) so that a `304 Not Modified` can reuse them;
delete `.coco_db` to start from scratch. The flow needs `trafilatura`
installed beside CocoIndex (`uv pip install trafilatura`, or install the
example with its dependencies).

A rerun reuses every step whose inputs did not change: an unchanged page
costs one conditional request and no model call; a page that changed only in
a timestamp, an ad or a comment count costs nothing either, because a
worker is memoized on the page's normalized content; only a real change
reaches the model, and only that page's worker and the reducers on its path
run again. To see that with a page you can change, run `research_seeded`
with `$RESEARCH_SEED` set to a page you control (the raw URL of a gist, for
example); the prompt names it as the page to start from, and the recipe
reads it beside the search results. The `--plan` tree lists each source's pages with their digest and
when the current content was first fetched, and its unavailable pages with
the reason; citations in the report carry the URL only (DESIGN.md §3: no
time in a tool result).

### The `drive` flow: a Google Drive folder of your own

The `drive` flow reads a folder in your Google Drive as you, through the
connector's OAuth sign-in (experimental; the connector docs describe the
setup). Once:

1. In a Google Cloud project of yours, enable the Google Drive API, configure
   the OAuth consent screen (external, Testing status, your account as a test
   user) and create an OAuth client of type **Desktop app**; download its
   JSON.
2. Sign in on the machine that runs the flow:

   ```sh
   uv run cocoindex auth sign-in google-drive ~/Downloads/client_secret_XXXX.json
   ```

   On a host with no browser the command prints a link to open elsewhere and
   takes the address the browser lands on, pasted back. The sign-in is kept
   in a file readable by your user only; `cocoindex auth sign-out google-drive`
   ends it.
3. Put a few files in a Drive folder: a Google Doc, a Google Sheet, a text
   file, a PDF. The model reads a Doc as Markdown, a Sheet as CSV and a
   Slides deck as text (the connector exports them), and a PDF natively.

Then run it with the folder's link (the address bar in Drive, or *Copy link*)
in `$DRIVE_FOLDER`; `run.py` fills the prompt's `$DRIVE_FOLDER` from the
environment and refuses to run with it unset:

```sh
DRIVE_FOLDER='https://drive.google.com/drive/folders/1AbC...' uv run python examples/cocoindex_agents/flows/run.py drive --stats
```

The prompt names the folder and nothing else: the orchestrator opens it as a
source of kind `google_drive`, read with the account signed in on this
machine, and never sees or names a credential (DESIGN.md §3, *Sources: a
Google Drive folder*). To see the incrementality, run it twice unchanged,
then edit one of the files in Drive and run again.

## Measured: stability of the decomposition

Measured 2026-11-04 (`orders`: 2026-11-05) with `openai/gpt-6.1-terra`
through LiteLLM at `temperature=0`, three drives per flow, each from an
empty `.coco_db` and an empty `out/`. Every drive of a flow made the same
orchestrator actions with the same handle names and child keys, used the
same roles, wrote the same set of files with the same structure, and made
the same number of model calls.

| Flow | Orchestrator actions (identical in 3/3 drives) | Output structure (identical in 3/3) | Model calls |
|---|---|---|---|
| `summaries` | open `src:docs` + `tgt:out`; `for_each(key=items, inputs=[tgt:out])`; answer | three `*.summary.md`, one paragraph each | 12 (orchestrator 3, worker 9) |
| `report` | open `src:notes` + `tgt:out`; `for_each(key=items, collect)`; `reduce(key=combined)`; `declare(report.md, res:combined)`; answer | `report.md` with `## Decisions` and `## Open questions` | 15 (orchestrator 5, reducer 2, worker 8) |
| `table` | open `src:invoices` + `tgt:out`; `observe(key=plan, inputs=[src:invoices])`; `for_each(key=items, inputs=[tgt:out])`; answer with the column list | four `inv-*.txt.json` with the keys `invoice, vendor, date, due, items, subtotal, tax, total, status` | 18 (orchestrator 4, planner 2, worker 12) |
| `orders` | open `src:orders` + `tgt:out`; `for_each(key=items, inputs=[tgt:out])`; answer | four `po-*.pdf.json` with the nine keys the prompt names, the same line items, amounts and ticked box in every drive | 15 (orchestrator 3, worker 12) |

What varies between drives is the wording of the child tasks and of the
outputs' prose, never the decomposition or the structure (one `report` drive
spelled `collect=true` where the others left the default: the same action).

## Measured: incrementality

Same model and date, continuing from the third drive above. "One edit" is one
line appended to `docs/targets.md`, one line appended to
`notes/2026-10-13.md`, the status of `invoices/inv-1044.txt` flipped, and
`orders/po-2026-1043.pdf` regenerated with its tick moved from Rejected to
Approved, respectively.

| Flow | Unchanged | One edit |
|---|---|---|
| `summaries` | 0 | 2 (the edited file's worker: declare, answer) |
| `report` | 0 | 2 (that note's worker 1, the reducer 1) |
| `table` | 0 | 3 (planner 1, that invoice's worker 2) |
| `orders` | 0 | 2 (the replaced order's worker: declare, answer) |

What the counts show:

- An unchanged run is skipped whole: no session's body runs, no turn is
  probed (DESIGN.md §7, *Open vs closed*). For `table` this holds since the
  engine validates the planner's recorded sample from the planner's own entry
  (transitive validation, engine PR #2671); before it, the orchestrator's
  body re-ran without inference and `--stats` showed `sessions 6 (skipped 5
  / 83%)`.
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
- In `orders`, a worker's `read_input` returns a reference to its PDF (key,
  media type, size, content digest) and the PDF itself is attached to the
  request after that result; the transcript holds the reference only. The
  regenerated order has a new digest, so the two turns that saw it re-infer
  and the worker wrote `"approval": "approved"` where it had written
  `"rejected"`; restoring the file re-ran the same two turns (8.2k input
  tokens, 93% of them provider-cached: the request up to the document is
  the same bytes as before). The three other workers are skipped on their
  file's fingerprint. Three fresh drives read every order's ticked box as
  drawn. The attached page is most of what the tokens pay for: 41k input
  tokens over the 15 calls of a first run, 8.2k over the 2 calls of the edit.

What `--stats` reports for these runs, from the scripted drives of the same
shapes in `tests/test_observability.py` (the counts are the ones above; the
tokens and the wall time come from the real run):

| Flow | Run | `stats:` |
|---|---|---|
| `summaries` | unchanged | `model calls 12 (reused 12 / 100%), sessions 4 (skipped 4 / 100%), tool calls 9 (reused 9 / 100%), files 3 (unchanged 3 / 100%)` |
| `summaries` | one edit | `model calls 12 (reused 10 / 83%), sessions 4 (skipped 2 / 50%), tool calls 9 (reused 4 / 44%), spent tokens …, files 3 (unchanged 2 / 67%)` |
| `table` | unchanged | `model calls 18 (reused 18 / 100%), sessions 6 (skipped 6 / 100%), tool calls 13 (reused 13 / 100%), files 4 (unchanged 4 / 100%)` |
| `table` | one edit | `model calls 18 (reused 15 / 83%), sessions 6 (skipped 3 / 50%), tool calls 13 (reused 6 / 46%), spent tokens …, files 4 (unchanged 3 / 75%)` |
| `orders` | first | `model calls 15, sessions 5, tool calls 11, spent tokens 41126 in (provider-cached 18896 / 46%), 1434 out, files 4, 18.0s` |
| `orders` | unchanged | `model calls 15 (reused 15 / 100%), sessions 5 (skipped 5 / 100%), tool calls 11 (reused 11 / 100%), files 4 (unchanged 4 / 100%), 0.0s` |
| `orders` | one edit | `model calls 15 (reused 13 / 87%), sessions 5 (skipped 3 / 60%), tool calls 11 (reused 6 / 55%), spent tokens 8229 in (provider-cached 4007 / 49%), 206 out, files 4 (unchanged 3 / 75%), 6.9s` |

The `orders` lines are the real run's own output (2026-11-05); the other two
drives of `orders` measured 40897 and 40977 input tokens on a first run, with
52% provider-cached.

The first drive of these flows (2026-11-04, `anthropic/claude-sonnet-5-5`,
prompts that still named workers and planners) measured 15/15/18 calls on a
first run, 0 unchanged and 2/2/3 after one edit; its findings are in the
next section.

## Measured: the `research` flow against the real web

Measured 2026-11-21 with `openai/gpt-6.1-terra` and the `wikipedia` engine
(no search key), three to five queries written by the orchestrator from the
prompt, from an empty `.coco_db`. The orchestrator followed recipe 5: it
opened the target and a `web_search` source in one turn, fanned out over the
12 pages the searches found, reduced the leads, opened a `web_pages` source
over them (five of six leads fetched: the Gradle incremental-build guide,
the GCC incremental-compiler wiki, an arXiv abstract and two PDFs read
natively; one PDF link was a 404), fanned out over those, reduced the report
from both collections and declared it.

| Run | Model calls | `stats:` |
|---|---|---|
| fresh | 46 (orchestrator 8, workers 34, reducers 4) | `model calls 46, sessions 20, tool calls 27, spent tokens 193265 in (provider-cached 23209 / 12%), 8068 out, files 1, 63.1s` |
| unchanged | 0 | `model calls 46 (reused 46 / 100%), sessions 20 (skipped 20 / 100%), tool calls 27 (reused 27 / 100%), files 1 (unchanged 1 / 100%), 1.1s` |

The `research_seeded` flow adds a page the user controls as the starting
page (`$RESEARCH_SEED`: the raw URL of a secret gist, a Markdown page of
notes on the topic with two links), so that a changed source can be staged.
Same model and engine, same day:

| Run | Model calls | `stats:` |
|---|---|---|
| fresh | 52 (orchestrator 8, workers 36, reducers 8) | `model calls 52, sessions 23, tool calls 32, spent tokens 207238 in (provider-cached 27352 / 13%), 12828 out, files 1, 92.7s` |
| unchanged | 0 | `model calls 52 (reused 52 / 100%), sessions 23 (skipped 23 / 100%), tool calls 32 (reused 32 / 100%), files 1 (unchanged 1 / 100%), 1.1s` |
| the seed page changed (a paragraph added) | 4 (the gist's worker 1, the leads reducer 1, the report reducer 2) | `model calls 52 (reused 48 / 92%), sessions 23 (skipped 18 / 78%), tool calls 32 (reused 18 / 56%), spent tokens 37343 in, 3568 out, files 1, 49.2s` |

What the changed-source run shows:

- The validator saw the gist's digest move (`--plan`: `walk of src:given
  changed`), the orchestrator's body re-ran with every turn a hit (0 calls),
  the twelve search pages' workers and the five lead pages' workers were
  skipped, and only the gist's worker re-ran its answer turn (its
  `read_input` turn hit).
- The leads reducer re-ran because its members changed and **picked its
  previous answer** from the replay block (1 call, a byte-identical list of
  URLs), so the `web_pages` source over the leads kept its identity and its
  workers hit. The report reduce had three groups; the leaf holding the gist
  and the root re-ran (2 calls), the other leaf was skipped. The report now
  carries the added paragraph's claim with the gist's URL as its citation.
- A caveat the drive surfaced: the gist's CDN answers with `max-age=300`
  and caches per request variant. For about five minutes after the edit, a
  conditional request from the flow's client was still answered `304` with
  the old validator while a differently shaped request already saw the new
  text, so the first two reruns after the edit were skipped whole, correctly
  from what the origin told them. A page behind a caching CDN can lag its
  own edit by the CDN's `max-age`; the flow cannot see what the server does
  not tell it.

What the runs show:

- A first drive with the recipe's original wording ("three to five short
  search queries") read only three pages per round: the orchestrator wrote
  long strings of keywords, which Wikipedia's search matched to AI articles.
  The recipe now asks for two-to-four-word, title-like queries, and the next
  drive read twelve relevant pages. That is the one prompt change the drive
  caused.
- The unchanged rerun makes no model call and runs no tool: the root
  session's one record per web source is validated by searching again (five
  queries) and sending one conditional request per page (17 pages), and the
  whole run is skipped in about a second. An earlier unchanged rerun had
  re-run the orchestrator's body with no inference because a page's ETag had
  rotated while its content had not; validity now rests on the normalized
  content, and the record refreshes its validators.
- The orchestrator followed the recipe's starting-page clause in the seeded
  drive: it opened the `web_pages` source over the gist in the first turn
  beside the search, read it with its own fan-out, and reduced the leads
  over both collections; the gist's link to the "Build systems à la carte"
  page became one of the five leads. With 18 answers the report reduce had
  three groups (two leaves and the root: six calls cold).

## Measured: the `drive` flow against a real account

Not yet measured: the three runs (fresh, unchanged, one file edited in
Drive) are recorded here once a signed-in account and a shared folder are
available.

## What the drives changed in the package

Third drive (`orders`, 2026-11-05, PDFs read natively):

- Files the model reads natively are decided by media type (`application/pdf`
  today), not by flow: `read_input` and a source's `read` return a small
  reference for such a file and the LiteLLM client attaches the document as
  a `user` message with LiteLLM's `file` content part right after the
  step's tool messages, which OpenAI takes as is and LiteLLM turns into
  Anthropic's and Gemini's document inputs (DESIGN.md §3, *Documents*). A
  source's `sample` shows a document's media type and size in place of a
  text head. The protocol text tells every role where the document arrives.
  With that, the orchestrator followed recipe 2 unchanged in three drives,
  and every worker read the ticked box, the table and the totals correctly
  on every order, before and after the edit.
- `cocoindex.ops.litellm` treated an `ImportError` that LiteLLM wraps in its
  connection error (a `litellm` installed without `mcp`) as transient and
  retried it for ten minutes without a word; it now surfaces at once
  (cocoindex PR #2685).

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
