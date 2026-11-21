# Research flow proposal — round 3 review

Reviewed revision 3 against the complete round-2 report, the frozen design, the existing handle/mount contracts, and the saved survey results. No worktree files were edited. No application tests were run; the proposed tests remain acceptance requirements, not executed evidence. The only additional computation was a read-only recount of the saved survey JSON.

Paths below are relative to /home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/. P means examples/cocoindex_agents/flows/research/DESIGN-proposal.md; D means examples/cocoindex_agents/DESIGN.md. Package paths such as tools/agents.py are under examples/cocoindex_agents/cocoindex_agents/.

## Disposition of the six new round-2 findings

1. **Alias B→F bypassing exclusion — resolved for the recipe's deduplication rule.** P:215–242 now separates requested-URL filtering from final-resource filtering, freezes both exclusion sets in the record, and retains suppressed aliases so B→G later invalidates the observation. P:575 specifies the exact A→F, B→F, B→G acceptance case. P:61–64 limits the one-worker claim to the sequential recipe and explicitly allows same-turn overlap. A newly exposed source-identity issue with those frozen exclusions remains in new finding 2; the two-stage filtering algorithm itself is now specified.

2. **Version stamp as an unstable resource property — resolved under the recommended option (b); owner DECISION remains.** P:137–170 removes time from model-visible text in the path revision 3 commits to describing. Under the retained option (a), the stamp belongs to the final resource version, alias replacement preserves it, and extra reducer work when only the stamp changes is acknowledged. This closes the former claim that a clock field necessarily costs no inference. Selecting (a) would still amend the frozen no-time rule and requires the owner's decision. The cache coherence requirement in new finding 1 applies to both options; choosing (b) does not require implementing the optional stamp machinery.

3. **Engine selection not tracked before reuse — resolved.** P:362–374 puts the result-affecting engine identity in the change-detected AgentsConfig and separates it from the untracked client object and credentials. P:578 requires a same-state-directory engine-switch test. Adding the field to AgentsConfig.__coco_memo_key__ is part of implementing this stated contract; merely adding a dataclass field would not suffice because the class has an explicit memo-key method.

4. **Underfilled frontier grows under jitter — resolved.** P:183–213 defines selection as a membership set and qualifies stability by a full frontier or no eligible newcomer. The D/E example is explicitly expected expansion, and P:574 requires that behavior. No further hysteresis mechanism is required for this rule.

5. **PDF cannot supply the citation stamp — resolved under option (b), with option (a)'s change explicitly specified.** P:149–164 supplies URL-only citations in the default text and requires the stamp in the small PDF reference only if the owner selects (a). No textual prefix is added to PDF bytes. This is sufficient at the design level; an implementation of (a) must test the citation metadata, not only attachment of the PDF.

6. **Empty research outcomes undefined — resolved.** P:94–99 and P:495–501 define an honest no-evidence report for an empty initial collection and allow an empty follow collection when the union is nonempty. P:580–582 requires no-hit, all-unavailable, no-lead and recovery cases, and makes empty-member semantics explicit for multi-collection reduce. No new tool is needed.

## Carry-over dispositions from the round-2 review of round 1

These cover the additional unresolved matters recorded in round 2, so none is silently dropped.

1. **Timestamps — resolved under (b), pending the existing owner DECISION.** The frozen rule holds for the proposed URL-only header/citations (P:137–170, 474–493). Do not build both options before the owner chooses.

2. **Exclusion and hidden state — partially resolved.** Snapshot selection, replayable exclusion records and post-redirect filtering are resolved (P:215–242, 336–347). The source's own memo identity still omits a result-affecting exclusion input; see new finding 2.

3. **Frontier function/history — resolved.** P:174–213 defines the pure function, body history, requested identities, final mapping, and membership comparison. The corresponding test cases are listed at P:574.

4. **Validator exceptions — resolved.** P:354–360 preserves the invalid-record boundary for environment failures and propagates cancellation/deadlines. P:577 requires coverage.

5. **Failure policy/failed count — resolved technically, pending the existing per-page-containment DECISION.** P:293–330 correctly identifies exhausted transient fetches as environment failures and records them so reuse cannot hide recovery; permanent outcomes remain data. It also corrects the previous assertion that a model-facing error inherently marks a worker failed. The whole-tool alternative is now described as a contained ok=False result rather than an automatically failed app run. The further statements that the model could do nothing and that no report would be written are recipe expectations, not consequences of §7: the model could still write a no-evidence report. That wording should not be used as the technical justification for the policy decision. Omission visibility remains a stated plan/stats choice, not a guarantee about the report file.

6. **Cache lifecycle — partially resolved.** P:249–279 now defines separate requested-response and final-payload stores, pruning, skipped bodies, retained old payloads that are not served after failure, and separate validation/body fetch rounds. The response-to-payload version association is still missing; see new finding 1.

7. **Truncated PDF — resolved.** P:284–288 rejects oversized PDFs rather than attaching truncated bytes; P:576 requires it.

8. **Normalizer coverage contradiction — resolved.** P:444–460 explicitly scopes the positive material-edit/boilerplate-edit fixture requirement to covered page types and records repository/sidebar behavior as a limitation outside that guarantee. P:581 follows that distinction. This is an honest limit, not a claim that the normalizer detects every relevant edit on the open web. The examples and README must preserve that scope.

9. **Vocabulary/identity — resolved at the resource/item level.** P:33–64 separates resource identity, item keys, and listing kinds, and P:72–74 leaves page-output naming out of scope. New finding 2 concerns the identity of the filtered listing handle, not the page resource identity.

10. **Admission order versus identity — resolved for order and budget membership.** P:215–242 specifies ordered admission followed by a sorted effective requested list and documents the budget-boundary exception. The additional exclusion-dependent identity issue is new finding 2.

11. **Multi-collection reduce — resolved.** P:732–738 and P:582 require typed distinct handle names, collision rejection, unchanged item keys, summed omissions, and a nonempty union while allowing empty members. This remains the smallest honest extension to the existing tool.

12. **Count buckets — resolved.** P:513–526 covers the full-budget case, explicitly allows sparse results to cross buckets, and defers count hysteresis. No general zero-orchestrator-inference guarantee should be inferred outside the stated unchanged-card case.

13. **Cost arithmetic and measurements — resolved as estimates under the stated assumptions.** P:647–674 counts both turns when a task changes, separates reused tasks from newly composed tasks, distinguishes search-round and follow-round edits, and accounts for new follow workers. The saved raw JSON at /tmp/research-survey/data/results.json independently gives 45 pages fetched in both passes, 24 ETag pages, 21 answering If-None-Match with 304, 22 ETag pages answering either conditional with 304, 35 Cache-Control pages, and 25 with the stated restrictive directives. Thus P:611's 21 and survey/results.md's 22 describe different predicates; the explicit If-None-Match wording resolves the earlier discrepancy. P:613 matches the raw counts. The residual precise call/network figures are scenarios, not upper bounds or acceptance results. Changing a digest need not change the worker answer or final findings; identical outputs can still stop downstream work.

## New findings

1. **must-fix — §3 “Two stores in use_state” / conditional requests: validators are not tied to the payload version they validate.**

   **Problem:** A requested response stores only final URL plus HTTP validators, while the final URL points to one mutable payload; after another alias advances that payload, the older alias's validator can no longer safely authorize reusing it. Likewise, if A→F returns content C1/ETag E1 and B→F later returns C2/E2 in one body run, “normalize once” can leave B's E2 associated with C1; a later 304 for E2 would then serve the wrong content.

   **Evidence:** P:249–257 defines the response fields without a payload digest/version and allows one payload per final URL; P:267–270 says a 304 simply keeps that payload. P:265 retains older payload/cache information after a failed fetch, so cross-run alias entries can also refer to different observed versions. P:579 currently tests normalization once but not the validator/payload association.

   **Change:** Store the normalized payload digest/version that each response validator was obtained with, and reuse a 304 only when the matching payload is actually available. If the single final-payload slot holds another version, retry that requested URL unconditionally or retain immutable payload versions while a live response entry references them. For multiple aliases in one body run, either reuse one final-URL response snapshot before issuing another final GET, or process differing successful responses coherently; do not attach validators from an ignored response to the first payload. “Normalize once” is an optimization conditional on reusing the same response snapshot, not a correctness rule for different bytes. Add a fixture where two aliases observe different versions, and one where an old alias is retained after a failure while another alias advances the payload; ensure a subsequent 304 never returns a payload the validator did not validate. Align §0's “fetched once per run” wording with the actual once-per-requested-URL-per-body-run contract.

2. **must-fix — §3 web_pages identity / §7 open-session reuse: the filtered source's memo key must include its frozen exclusions.**

   **Problem:** The dependency record includes the exclusion sets, but the source's memo identity is still only the effective requested list, even though post-redirect exclusions can change its items without changing that list. An open child passed the source can therefore reuse a memo validated against the old exclusions while receiving a logically different source on the new run.

   **Evidence:** P:218–220 and P:237 define source identity as the sorted effective requested list; P:225–229 makes items additionally depend on the final-URL exclusion set; P:336–338 includes those sets only in the record key. The existing API allows sources as inputs to open children (tools/agents.py:108–123); D:784–807 requires those children to skip only when their arguments and recorded reads still cover the source they received.

   **Counterexample:** In run 1, the earlier search lists F, the page-set source requests B→F and suppresses F, and a planner child given that source observes zero items. In run 2 the search no longer lists F, but the same B still redirects to unchanged F, so the new page-set source has one item. Its requested list and current stated memo key are still [B]; the child's old record replays its frozen exclusion of F and still validates as empty. The child can incorrectly skip and return the old zero-item plan.

   **Change:** Use the full immutable filtered-listing identity for both the source memo key and its record key: listing kind, sorted admitted requested URLs, and the relevant frozen requested/final exclusion sets. These are identity/scope inputs, not page payloads. A source passed across a mount then changes identity whenever the filtering semantics change, while its closed workers remain keyed only on the resulting page items. Add the open-child counterexample above as an acceptance case. No model-facing exclude argument is needed.

## Verdict

**Ready after the two must-fixes and the two existing owner decisions.** The round-2 findings are closed for the recommended no-time path, apart from the cache and source-identity interactions identified above; no further product surface or engine change is needed to address them. Keep timestamps out of model text unless the owner explicitly selects the alternative, and obtain the per-page containment decision before implementation. The remaining corrections are narrow contracts that belong in the source/cache implementation and its acceptance tests; another broad redesign is not indicated.
