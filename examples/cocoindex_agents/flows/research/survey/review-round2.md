# Research flow proposal — round 2 review

Reviewed revision 2 of DESIGN-proposal.md against the frozen design, the existing implementation, the round-1 report, and the supplied measurements. No worktree files were edited and no tests were run. This is a design review; the proposed acceptance tests have not been implemented or executed.

Paths below are relative to /home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/. “P” means examples/cocoindex_agents/flows/research/DESIGN-proposal.md, “D” means examples/cocoindex_agents/DESIGN.md, and package paths such as tools/agents.py are under examples/cocoindex_agents/cocoindex_agents/.

## Disposition of every round-1 finding

1. **Timestamps in memo keys — partially resolved; DECISION remains.** P:121–152 now includes the title in the digest and preserves the cached stamp across identical 200s, 304s, and task changes. That resolves the ordinary repeated-fetch problem, but option (a) still explicitly amends the frozen no-time rule, and its stability argument does not cover changing requested aliases for the same final resource; see new finding 2. P:429–432 also incorrectly asserts that changed content necessarily changes the findings and therefore the stamp cannot cause an extra reducer run. Option (b) closes the timestamp finding without changing the frozen rule. The owner must choose; a review cannot approve that policy on their behalf.

2. **Exclusion and hidden session state — partially resolved.** The handle-table snapshot and sorted effective requested-URL list are now explicit (P:190–204), so ordinary direct-URL exclusion and same-turn ordering are defined and independently validatable. A lead that reaches an already-read final URL through a previously unseen alias still survives exclusion, creates a duplicate worker, and collides in multi-collection reduce; see new finding 1. The global no-two-fan-outs claim at P:58–61 is also stronger than the deliberately permitted same-turn overlap.

3. **Undefined frontier/history — resolved at the design-contract level.** P:156–183 supplies the pure function, removes per-query allocation, distinguishes requested and final identities, and gives the body and validator the same previous requested-URL set. The acceptance cases at P:501 cover the previously missing overlap and redirect history. A separate limit on the rank-jitter guarantee remains; see new finding 4. Frontier comparison should use selected membership, not the incidental rank order of retained members.

4. **Validator exception containment — resolved.** P:310–316 explicitly returns an invalid outcome with the prior state for exhausted transient failures and engine outages, while propagating cancellation and deadlines; P:504 requires coverage. This matches the existing Drive boundary and the processor-free validator contract.

5. **Unavailable/failure classification and failed=0 — partially resolved; DECISION remains.** P:252–288 now distinguishes permanent outcomes from exhausted transient environment failures, records a never-valid failure per transiently unavailable URL, and correctly limits the claim about failed. The owner still must approve containing a failed sub-operation while the tool returns ok=True. Before presenting that decision, correct P:273–275 and P:614: under frozen D:857–865 an open_source environment failure is an ok=False tool result contained by the open session, not automatically a failed research run. Also correct P:278–279: an unrepaired model-facing error does not itself mark a worker failed; D:850–856 permits the worker to finish successfully after such a result. The plan/stats channel for omissions is now specified; that is the proposed product choice, not evidence that the report file itself communicates omissions.

6. **Page-cache lifecycle — partially resolved.** P:211–238 specifies pruning, skipped bodies, stale payload retention without serving it, refreshed record validators, and the accepted second fetch after validation misses. Those parts are resolved. The cache is still indexed by requested URL rather than final resource identity, leaving the alias/payload/version interaction in new findings 1 and 2. The run-level claims should say once per requested URL in a body run unless final-response reuse is also specified; validation plus body is explicitly two rounds.

7. **Truncated PDF — resolved.** P:243–247 rejects an oversized PDF as too-large instead of attaching partial bytes, and P:503 requires that case. HTML transport truncation is separately marked.

8. **Extraction fallback and meaningful-change coverage — partially resolved.** P:386–395 stops claiming the under-500-character fallback repairs a 3k-character sidebar extraction and states the unsupported-quality cases. However, P:392–394 and P:506 still require a material main-content edit to change the digest for each page type, explicitly including a repository page, while P:387–390 accepts extracting only its sidebar. A README-only edit in that known case cannot satisfy both statements. State which page types must pass positive content-coverage tests; make the repository fixture either pass that requirement through a real extraction change or explicitly document an accepted limitation outside that guarantee. Do not call a fixture that merely records the sidebar a passing material-change test.

9. **Resource identity versus item key and premature naming — resolved.** P:33–60 distinguishes global resource identity, source-local keys, and the two listing kinds sharing a page resource. P:69–71 removes the speculative title-based output naming. No additional model-facing resource API is needed for this vocabulary change.

10. **Ordered admission versus set identity — resolved.** P:190–204 defines admission order, exclusion, budget selection, sorted identity, and the membership-changing exception at the budget boundary. The rewording/reordering acceptance case at P:502 now has a coherent meaning.

11. **Multi-collection reduce semantics — resolved.** P:647–651 defines a string or nonempty list of distinct handles, colliding item keys as a model-facing error, and summed omitted counts; P:507 specifies tests. Preserve existing item keys for valid inputs and explicitly type-check list members at the handler boundary, as round 1 requested. This remains a small change; no generic schema framework is necessary. Its collision rejection correctly reveals, rather than fixes, the redirect-deduplication bug in new finding 1.

12. **Budgets on count boundaries — resolved.** P:444–451 moves full sources to 12 and 6, states the half-open buckets, limits the stability claim to the common case, and leaves count hysteresis deferred. The common full-budget one-page-loss case no longer crosses a bucket. This does not imply all sparse search results fit those buckets.

13. **Cost accounting — partially resolved.** P:570–591 now states page counts, approximate answer sizes, one-group assumptions, two cold calls per worker/reducer session, and separate validator/body requests. The 48-call cold arithmetic follows those assumptions. P:592 is still wrong: if every child's task changes, each reducer's read_input turn misses too, because the SessionSpec is part of every turn key (D:432–433; session.py:273–279), so the two reducers cost four calls, not two. Replaying old orchestrator actions also preserves their old child task strings; separate that case from genuinely composing new child tasks. Limit the three-call material-edit row to a search-round page with unchanged follow membership; an edited follow-round page normally costs two calls, and changed leads can add cold follow workers. Finally, the survey totals remain unreconciled: P:535 says 21 of 24 ETags versus survey/results.md:13's 22, and P:537 says Cache-Control 33/45 and restrictive directives 26 versus survey/results.md:15's 35/45 and 25. Correct these from the raw evidence or label the figures unresolved.

## New findings and newly exposed interactions

1. **must-fix — §3 “Two listings” / resource deduplication: exclusion before redirects does not ensure one worker per page.**

   **Problem:** Suppose the search source requested A and yielded final page F; the leads source later requests B, where B is absent from the snapshot's requested/final sets but redirects to F. B survives exclusion, becomes a second item keyed F, and the prescribed final reduce rejects the two collections for colliding item keys.

   **Evidence:** P:178–183 deduplicates requested URLs by final URL within a search listing; P:190–204 excludes the leads' requested URLs before fetching. P:647–651 rejects duplicate item keys across collections. P:58–61 nevertheless promises no duplicate workers across fan-outs. The scenario requires no same-turn race.

   **Change:** Apply final-resource exclusion after resolving redirects as well as requested-URL exclusion before fetching. Freeze the relevant snapshot exclusions in the record so validation can reproduce that filtering without handles. Keep the requested-to-final observation for a suppressed alias, so a later redirect from B to a new page G invalidates the listing and admits G. Distinguish a requested-URL response/redirect cache from a final-resource payload store. Add the A→F, B→F, then B→G acceptance case. Limit the one-worker claim to the recipe/defined exclusion scope; same-turn sibling sources explicitly can overlap.

2. **must-fix · DECISION — §3 version stamp / cache: option (a) does not yet make the stamp a stable property of a resource version.**

   **Problem:** The stamp lives in a cache keyed by requested URL, so replacing A→F with a previously unseen B→F can give the unchanged final resource F and unchanged digest a new stamp; the worker's memo key does not cover that change. Independently, a normalized page edit can leave topic-relevant findings byte-identical, while the forced new citation stamp changes reducer input and causes inference that value-based reuse would otherwise avoid.

   **Evidence:** P:137–152 says the stamp moves only with the digest and calls it a content-version property; P:211–219 stores it per requested URL and prunes old aliases; P:473–476 keys the worker on final URL plus digest. P:429–432 asserts that changed content implies changed findings, which is not a valid inference.

   **Change:** Prefer option (b) if preserving the frozen no-time contract is the goal. If the owner chooses (a), persist the stamp with the final resource version and reuse it across alias replacement while that resource remains live; define its lifecycle under pruning and reintroduction and ensure every model-visible change is covered by the worker and read-validation contracts. Add an alias-replacement followed by a task-change test. Explicitly accept the potential extra reducer reruns when findings stay identical; remove the claim that timestamps cost no extra reducer calls. Owner approval is still required for the clock-in-tool-results amendment.

3. **must-fix — §3 “Search engines” / §7 invalidation: naming an engine in stored records does not detect a change of selected engine.**

   **Problem:** If a successful keyless run stored Wikipedia records and the next run selects Tavily, the root can still validate the old records with Wikipedia and skip before open_source observes the new selection. The proposal promises invalidation but does not specify a tracked dependency on the currently selected engine.

   **Evidence:** P:294–303 reconstructs validation from the stored engine name; P:346–349 puts selection behind a context key “as the model is” and claims that another engine reruns the search. ContextKey defaults to detect_change=False (python/cocoindex/_internal/context_keys.py:104); the existing agent config deliberately uses detect_change=True (tools/__init__.py:275–277). The frozen design explicitly excludes the model client object from change tracking (D:764–768), so copying that arrangement does not supply this guarantee.

   **Change:** Specify that the selected engine's stable identity and result-affecting configuration are a tracked input of the root memoized session/config, or provide an equivalent invalidation mechanism before ancestor reuse. Do not fingerprint credentials. Validators must remain usable without a live source object or descendant component context. Add a same-state-directory Wikipedia→Tavily test asserting that the old Wikipedia frontier is not silently reused.

4. **should-fix — §3 frontier / §5 volatility: rank jitter can expand an underfilled frontier.**

   **Problem:** With one query, k=4, budget=12, and prev={A,B,C,D}, moving D from rank 4 to 5 while E moves from 5 to 4 retains all four incumbents and admits E into a free place. Thus the specified function is deterministic, but rank movement wholly within top-2k can change membership and run a new worker.

   **Evidence:** P:165–171 admits all top-k newcomers whenever the budget has room; P:185–188 and P:452–455 describe rank jitter as changing nothing, and P:501 requires rank jitter to keep the set without stating a full-frontier precondition.

   **Change:** Keep the defined function and qualify the stability guarantee and tests: jitter alone preserves membership when the frontier is full, or when it admits no eligible newcomer. Add the underfilled example as an expected expansion. If stability for underfilled sources is a requirement, change the admission rule explicitly rather than asserting that the present rule provides it.

5. **should-fix — §3 citations / PDF input: timestamp option (a) has no model-visible stamp for a PDF.**

   **Problem:** PDF workers are instructed to copy a version stamp from the page header, but their read_input result is a document reference with no header or stamp. The existing reference contains only key, media_type, bytes and digest, so the PDF path cannot follow the proposed citation recipe under option (a).

   **Evidence:** P:337 specifies a document reference for PDFs; P:407–419 requires the copied stamp in findings and Sources. documents.py:54–61 defines the reference, and tools/sources.py:267–268 returns it directly after attaching the bytes.

   **Change:** Under option (a), include the stable page-version metadata in the small PDF read result/reference and apply the same covered-input contract as text pages; never prepend textual headers to PDF bytes. Add a PDF citation assertion, not just a document-attachment assertion. Under option (b), URL-only citations already fit the existing reference and this finding closes.

6. **should-fix — §8 acceptance / recipe: expected empty research outcomes are untested.**

   **Problem:** A query can return no hits, or every admitted page can be permanently unavailable, leaving the first collection empty and the recipe's leads reduce unable to run. No current acceptance row defines the report/completion behavior for that expected state or its recovery on the next run.

   **Evidence:** P:83–88 prescribes the first fan-out followed by a leads reduce; P:254–258 omits permanent unavailable pages from items; P:498–507 covers individual unavailable outcomes but no all-empty case. Frozen D:649–650 makes an empty reduce a model-facing tool error.

   **Change:** Add end-to-end cases for no search hits, all initial pages unavailable, and no follow leads. Specify that an empty initial corpus produces an honest no-evidence report/status rather than an apparently successful substantive report, and verify recovery when one page becomes available. The no-follow-leads case should permit reducing the nonempty first collection together with an empty second collection. Reuse existing tools and model-facing empty-reduce behavior; no new public API is needed.

## Verdict

**Ready after the must-fixes and the two recorded owner decisions.** The core frontier and record contracts are now concrete enough to implement once final-resource deduplication and selected-engine invalidation are specified. Timestamp option (a) also needs the alias/version and PDF contracts above; option (b) avoids that additional model-visible state. Correct the remaining extraction-test contradiction, rank-jitter wording, and cost/measurement claims, then implement the acceptance cases before claiming the stated reuse behavior. No implementation approval or policy choice is implied by this review.
