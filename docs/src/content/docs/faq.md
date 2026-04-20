---
title: FAQ
description: Frequently asked questions about CocoIndex behavior, change detection, and common patterns.
---

<!--
Guidelines for adding FAQ entries:

1. Cross-area topics: topics that span multiple docs and don't belong to a single
   concept page. Write the full explanation here (this is the canonical location).

2. Subtle details: behavior that's OK to not learn on first read and would clutter
   the main docs. Write the full explanation here.

3. Frequently asked: questions people commonly ask, where the answer already exists
   in the docs but the question phrasing differs from how the docs frame it. Write a
   short answer + link to the canonical doc. Only add when someone searching wouldn't
   find the answer because they'd use different words.
-->

# FAQ

## Change detection

### Why do logic changes propagate transitively but input changes don't?

In the call chain `foo(a)` → `bar(b)`:

- **Logic changes propagate**: if `bar`'s logic changes (code, `deps`, `version`), the output of `foo(a)` could be different too, so `foo`'s memo must be invalidated.
- **Input changes don't propagate**: `b` is the result of applying part of `foo`'s logic to `a`. As long as `foo`'s logic and `a` are unchanged, `b` won't change — there's nothing to propagate.

### How does logic change propagation work?

Logic changes propagate based on **runtime invocations**, not static call graphs. Two consequences:

- **Unannotated functions don't break the chain.** If `f1()` → `f2()` → `f3()`, and `f1` and `f3` are decorated with `@coco.fn` but `f2` is not, a logic change in `f3` still invalidates `f1`'s memo.
- **Conditional calls are tracked precisely.** If `f1()` calls `f2()` only in one branch, then invocations of `f1()` that didn't call `f2()` are not invalidated when `f2`'s logic changes — only invocations that actually called `f2()` are affected.

### What about hidden dependencies like global variables or files?

Like any memoization system (e.g., `@functools.cache`), CocoIndex's change detection assumes functions depend only on their declared inputs. If a function reads a global variable, a file, or external state not passed through arguments, changes to those won't be detected automatically.

CocoIndex provides mechanisms to capture some of these dependencies:

- **[`deps`](./programming_guide/function.md#deps)** — declares module-level values (like a prompt string or model name) as part of the function's logic. Changes to these values invalidate dependent memos, just like any other logic change. Note: `deps` is snapshotted once at decoration time.
- **[`use_context()`](./programming_guide/context.md)** — retrieves shared resources via `ContextKey`. With [`detect_change=True`](./programming_guide/context.md#change-detection), changes to the provided value invalidate dependent memos.

For per-call values that change at runtime, pass them as regular function arguments instead.

## Target states and syncing

### What happens if my pipeline crashes mid-update?

CocoIndex's internal state is always consistent — even after a crash or `kill -9`. On the next `app.update()`, CocoIndex automatically recovers: it computes the current desired state and reconciles against all possible previous states, converging the target to the correct state. No manual cleanup is needed. See [Error Handling — Interrupted updates and recovery](./advanced_topics/exception_handlers.md#interrupted-updates-and-recovery) for details.

### Are target state writes transactional across targets?

Not across targets. When a processing component finishes, CocoIndex sends all its target state changes to each target backend as a unit — all writes happen after processing completes, never partially during execution. Each target backend applies its batch atomically when supported (e.g., within a database transaction). But changes across *different* target backends (e.g., Postgres and local files) are not transactional with each other. See [How target states sync](./programming_guide/processing_component.md#how-target-states-sync) for details.
