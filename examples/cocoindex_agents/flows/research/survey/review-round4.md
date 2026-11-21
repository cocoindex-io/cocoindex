# Research flow proposal — round 4 review

Reviewed revision 4 against the two round-3 must-fixes and the carry-over failure-policy wording note. No worktree files were edited. No application tests were run: the acceptance rows describe required implementation checks, not completed evidence. Project knowledge could not be read during this turn; this review uses the supplied proposal, frozen design, source contracts, and previous review reports, without treating an earlier project-knowledge response as current.

Paths below are relative to /home/jiangzhou/.reduce/projects/cocoindex/.claude/worktrees/cocoindex-agents-a-research-flow/. P means examples/cocoindex_agents/flows/research/DESIGN-proposal.md; D means examples/cocoindex_agents/DESIGN.md.

## Disposition of the round-3 must-fixes

1. **Response validators versus payload version — resolved at the design-contract level.**

   P:258–290 now ties each response entry's validators to a specific digest and stores immutable payload versions under (final URL, digest). A 304 can reuse only that version, and absence of the version requires an unconditional refetch. Keeping an old alias after a failed fetch no longer lets another alias silently replace the payload its validators authorize.

   P:270–277 also closes the within-run alias counterexample: hop-by-hop redirects reuse an already obtained final-URL response snapshot, including its validators and digest. The one-normalization behavior follows from sharing that snapshot rather than ignoring a different successful response. P:279–290 defines reference-based pruning so live response entries retain their versions without accumulating unreferenced history.

   **Evidence:** P:259–269 supplies the response-to-version association and missing-version fallback; P:270–290 supplies snapshot reuse, pruning, and failure retention; P:584–589 repeats the integration contract. P:609 requires the different-version aliases, missing-version 304, and shared-response-snapshot cases. The once-per-requested/final-URL wording is corrected in §0 at P:59–64.

   **Implementation check:** The per-body-run response snapshot must include in-flight fetches, not only completed entries, so concurrent aliases share the same final fetch. This implements the stated one-snapshot contract; it is not an additional API or design requirement. Test the invariant with concurrent aliases as well as sequential ones. A record-only validator may compare a 304 against its stored digest without loading payload bytes; the body is the path that must possess the named payload or refetch it.

2. **Filtered source memo identity omits exclusions — resolved.**

   P:217–246 now makes a web_pages source's memo identity the listing kind, sorted admitted requested URLs, and both frozen exclusion sets, exactly as its dependency-record key. Consequently, a planner receiving [B] with F excluded and later receiving [B] with F admitted receives a changed source identity even while B→F and the page content remain unchanged. Its former empty-source memo cannot match the new input.

   **Evidence:** P:221–225 states the full identity across a mount; P:240–246 explains the planner counterexample; P:361–368 makes the record/source identity equality explicit; P:590–595 repeats it under integration. P:610 requires the exact open-child regression case.

   **Implementation check:** Construct one immutable identity value and use it for both memo-key rendering and record-key construction, rather than maintaining two parallel tuples. Canonicalize the exclusion sets themselves as well as the admitted URL list. This follows the existing identity contract and prevents insertion order from introducing unrelated misses.

## Carry-over wording note

**Resolved.** P:335–342 now describes the whole-tool alternative as an environment failure contained by the open session, followed by the recipe's no-evidence report, with the eleven successfully fetched pages deferred to a later run. It distinguishes that recipe behavior from an app-run failure and no longer claims that the model cannot write any report. This fairly states the consequence of the owner choosing whole-tool rather than per-page containment; it does not change D §7 by itself.

## New findings

None. No new must-fix, should-fix, or nit finding is raised in this round. The two fixes close the previously demonstrated correctness counterexamples without requiring another public tool, a model-facing exclusion argument, or an engine change. The implementation checks above are verification of the now-explicit contracts, not new review blockers.

## Owner decisions still pending

- **DECISION — citation time (P:147–171, 738–743).** The proposal is written for recommended option (b): URL-only citations, with fetch provenance in the plan trace. That preserves the frozen no-time rule. Selecting alternative (a) deliberately amends that rule and carries the already-stated PDF, alias/version, and reducer-reuse consequences. Implement the selected option rather than exposing a new configuration knob for both.
- **DECISION — exhausted transient page fetches (P:324–342, 744–748).** Recommended per-page containment keeps open_source successful for the fetched subset while recording a never-valid failure for each exhausted page. It still needs the owner's approval as an amendment to §7's tool-level ok=False contract. The whole-tool alternative remains technically coherent and has a defined no-evidence outcome.

## Verdict

**Ready to implement once the two recorded owner decisions are made.** No technical must-fix remains in the design for the recommended URL-only citation path. Close the design review and move verification to the stated implementation tests and real-model drives; those results must establish the reuse and cost claims before they are reported as measured behavior. This verdict does not approve either policy choice on the owner's behalf and does not claim that unimplemented tests have passed.
