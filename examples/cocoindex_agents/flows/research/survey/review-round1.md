Reviewed the requested files and checked the proposal against the implementation. No files changed; no tests ran.

1. **must-fix — §3 “A page is a resource” / citations: timestamps enter memo keys.**
   `Fetched: <UTC>` enters `read_input`’s tool result and therefore every later turn’s transcript key; copied citations also enter reducer arguments. Excluding the timestamp from the page digest does not fix this: a task or version change can rerun the worker with identical content but a new timestamp.
   **Evidence:** [proposal:123](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:123), proposal:318–322; frozen `DESIGN.md:192–195` explicitly forbids timestamps in tool results.
   **Change:** Remove fetch times from model-visible headers and citations. Keep them in trace metadata. Make the digest cover all remaining worker-visible content, including the title.

2. **must-fix — §3 “Two listings” / records: exclusion depends on unstated session state.**
   “Minus the keys any other listing … has yielded this run” makes membership depend on execution order, including concurrent source openings. The record specifies only “the URL list,” without establishing whether it contains requested URLs or the effective list after exclusion; a validator cannot reproduce the former without the other listings.
   **Evidence:** [proposal:149](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:149), proposal:204–212; frozen `DESIGN.md:197–205` requires independent calls within a turn.
   **Change:** Resolve exclusion against an explicitly defined snapshot, then freeze the effective URL list into the source identity and record. Define same-turn behavior without depending on which fetch finishes first.

3. **must-fix — §3 frontier: the stored set does not fully specify hysteresis.**
   The rule does not define how retained URLs consume per-query quotas when they appear under several queries, or how requested URLs match previously selected final URLs after redirects. It also says the body uses the previous recorded frontier without specifying how the body obtains that history.
   **Evidence:** [proposal:139](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:139), proposal:112–115, 204–210; `use_data_dep` records state and returns `None` (`python/cocoindex/_internal/data_dep.py:168`).
   **Change:** Specify one pure selection function, its complete inputs, deterministic query allocation, and persisted history available to the body. Store candidate URLs and their final-URL mappings separately. Test an incumbent reached through a redirect moving from rank 3 to 5, and incumbents moving between overlapping queries.

4. **must-fix — §7 failures: validator exceptions bypass open-session containment.**
   The proposal promises that search outages and network failures become contained failures in the session opening the source, but those same operations run during validation before that body executes. An exception there fails the run directly.
   **Evidence:** [proposal:363](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:363); `python/cocoindex/_internal/data_dep.py:327–351` explicitly propagates validator exceptions, including processor-free descendant validation. The Drive implementation handles this boundary in `tools/drive.py:139–144`.
   **Change:** After appropriate retries, validators should return an invalid record for environment failures; the body then retries and reports them through the existing containment mechanism. Preserve cancellation and deadline propagation.

5. **must-fix · DECISION — §3 fetching / §7: “unavailable” changes the frozen failure policy.**
   Treating exhausted 429/503 retries and timeouts as reusable absence contradicts §7’s rule that exhausted transient errors become environment failures. Separately, fetching before workers cannot guarantee `failed=0`: model, rendering, or other worker failures still populate the collection’s failure count.
   **Evidence:** [proposal:183](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:183), proposal:190–198, 464–465; frozen `DESIGN.md:842–873`; `tools/agents.py:341–348`.
   **Change:** Obtain an explicit owner decision on amending the frozen failure policy. Otherwise classify permanent outcomes such as 404, robots refusal, and unsupported media as data, and retain environment handling for exhausted transient failures. Replace the zero-failure guarantee with “fetch unavailability does not increment worker failures,” and specify how report readers learn about omitted sources.

6. **should-fix — §3 page cache / §7: the cache lifecycle is incomplete.**
   Validators cannot access the session’s `use_state`, so a changed-page validation followed by a body run can fetch that page twice; refreshed validator metadata can also advance while the skipped body’s cache stays unchanged. The cache has no pruning rule, so the 24-page run budget does not bound accumulated text or PDF storage.
   **Evidence:** [proposal:161](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:161), proposal:204–215, 371–374; `python/cocoindex/_internal/data_dep.py:344–351` states that descendant validation has no component state handler.
   **Change:** Keep the persistent payload cache separate from the current run’s checked results. Define behavior for skipped bodies, refreshed validators, and failed fetches retaining an older payload. Prune entries outside the current listings. The smallest implementation can explicitly permit another fetch after validation misses and correct the network estimates.

7. **should-fix — §3 fetching: cutting a PDF at the byte cap produces invalid input.**
   Streaming “and cut at the cap” can turn a valid oversized PDF into an incomplete document that is nevertheless handed to the model. This also undermines the claim that page-fetch trouble cannot become a worker failure.
   **Evidence:** [proposal:181](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:181); `documents.py:51–52` accepts the supplied bytes without repairing a truncated PDF.
   **Change:** Detect overflow and classify an oversized PDF as `too-large`; never attach a truncated PDF. Distinguish transport-size rejection from the explicitly marked text truncation.

8. **should-fix — §3 extraction / implementation risks: the proposed fallback misses the measured failure.**
   The fallback activates below 500 extracted characters, but the documented GitHub failure produces roughly 3,000 characters of sidebar text. That output passes the fallback threshold and becomes the content fingerprint despite omitting the repository content.
   **Evidence:** [proposal:118](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:118), proposal:531–535.
   **Change:** Add a saved GitHub-page fixture that asserts relevant content and a material edit survive extraction. Either provide a deterministic repository-content path with explicit provenance or report this page type as unsupported. Repeated extraction being identical is insufficient evidence of useful extraction.

9. **should-fix — §0 vocabulary: item keys and resource identities are conflated.**
   A path under a root is not globally unique within `localfs`: two roots can both contain `README.md`. Conversely, one web page is the same resource across the two different listing kinds, `web_search` and `web_pages`.
   **Evidence:** [proposal:29](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:29); existing `tools/sources.py:99–100` includes the root in source identity.
   **Change:** Define resource identity separately from the source-local item key: root plus path, account/folder plus path, or canonical page URL. Describe the web kinds as listing kinds. Cut the prospective title-based output-naming clause until shape A over pages is implemented.

10. **should-fix — §3 `web_pages` / §5: ordered-list and set semantics contradict each other.**
    The source uses first-seen URL order and applies a budget, while §5 calls its identity a URL set and promises that prose changes around the same URLs change nothing downstream. Reordering the same URLs can change the memo key, and beyond the budget can change membership.
    **Evidence:** [proposal:149](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:149), proposal:343–346.
    **Change:** Define admission order separately from identity: choose budgeted membership deterministically, then sort the selected canonical URLs for the source key and record. If ranking changes are intended dependencies, document that exception instead.

11. **should-fix — schema changes: multi-collection reduce lacks merge semantics.**
    Accepting a list is a small change, but concatenation can introduce duplicate item identities; those identities determine hash grouping and reducer slots. The proposal does not define duplicate collection handles, colliding item keys, or malformed list members.
    **Evidence:** [proposal:503](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:503), proposal:524; `tools/agents.py:395–397` derives reducer identities directly from item keys.
    **Change:** Normalize string-or-list input at the boundary into a nonempty list of strings, reject duplicate handles and colliding item keys, and preserve existing keys for valid inputs. Per-kind `open_source` checks can remain in its handler; no larger validation framework is needed.

12. **should-fix — §5 widened counts: the chosen budgets sit on bucket boundaries.**
    Sixteen items render as `16–32`, whereas fifteen render as `8–16`; eight render as `8–16`, whereas seven render as `4–8`. Thus one unavailable page can change both cards in the ordinary full-budget case.
    **Evidence:** [proposal:334](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:334); `tools/stats.py:15–16,38–39` defines half-open buckets.
    **Change:** Correct the examples and the no-orchestrator-inference claims. If stable cards across those transitions are required, implement recorded count hysteresis rather than leaving it as a target.

13. **should-fix — cost estimates: reducer sessions are counted as single calls.**
    Cold reducers first call `read_input`, then answer; the proposed 60-call total counts four reducer sessions as four calls instead of eight. The fixed change-cost figures also assume a stable tree shape and bounded worker-answer sizes, neither of which the proposal establishes.
    **Evidence:** [proposal:456](/home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/examples/cocoindex_agents/flows/research/DESIGN-proposal.md:456); `flows/README.md:109` measures two calls for one cold reducer; frozen `DESIGN.md:673–683` limits locality claims to unchanged tree shapes.
    **Change:** Count two calls per cold reducer session and one answer call per changed, previously mounted reducer session. Under the stated four-session assumption the first-run total is approximately 64. Present regrouping and network retries separately, derive token estimates from explicit answer-size assumptions, and reconcile the differing survey totals before quoting them as evidence.

**Verdict: needs rework.** The frontier’s persisted history, exclusion inputs, and cache lifecycle need a precise shared contract before implementation. Remove timestamps from model inputs, settle the unavailable-page policy with the owner, and specify validator failure handling; then revise the acceptance cases and cost table around those decisions.
