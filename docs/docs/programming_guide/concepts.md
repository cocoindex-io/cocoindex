---
title: Concepts
---

# Concepts

This document explains the core concepts of CocoIndex.

## Function

A **function** is just a Python function decorated with `@coco.function`. You can call it like any normal function. The decorator gives CocoIndex superpowers:

- **Change tracking.** When a function’s implementation changes, it signals dependents to be reprocessed. We’ll also expose advanced controls, e.g. manually-controlled behavior version and ttl-based invalidation.
- **Reuse computations.** Save from unnecessary recomputation by memoization (`memo=True`).
- **Tracing.** Invocations are recorded for debugging and profiling.

A function always takes `Scope` as the first argument, which carries CocoIndex runtime information and users should pass on when calling other CocoIndex functions or declare effects.

## Effect

An **effect** is a unit of desired external state. Users declare effects; CocoIndex takes Actions to sync external systems to match those Effects.

Example effects and corresponding actions taken by CocoIndex:

<table>
  <thead>
    <tr>
      <th rowspan="2">the <i>effect</i> users declare</th>
      <th colspan="3">the <i>action</i> CocoIndex takes for the external system</th>
    </tr>
    <tr>
      <th>when declared</th>
      <th>when declared differently</th>
      <th>when no longer declared</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>a SQL table</td>
      <td>create the table</td>
      <td>alter the table</td>
      <td>drop the table</td>
    </tr>
    <tr>
      <td>a row in a SQL table</td>
      <td>insert the row</td>
      <td>update the row</td>
      <td>delete the row</td>
    </tr>
    <tr>
      <td>a state subscribed by downstream services on change (e.g. through Kafka)</td>
      <td>publish a message to indicate insertion</td>
      <td>publish a message to indicate update</td>
      <td>publish a message to indicate deletion</td>
    </tr>
  </tbody>
</table>

## Component

A component is a long-lived instance that defines the boundary where CocoIndex syncs external effects. You create one by calling `coco.mount(fn, sub_scope, *args)`. Here fn is a CocoIndex function that provides the logic for the component, and all the following arguments will be passed to fn. You need to specify a sub scope based on the current scope.

CocoIndex runs the function asynchronously. During the run, the function may call `declare_effect(...)` zero or more times. Those effects are owned by the component.

When the run finishes, CocoIndex diffs effects this run against effects from the previous run, then applies a bundled change to external systems (creates, updates, deletes) to keep them in sync.

Each component should have a distinct scope, each with a unique path. The unique path should be stable, and CocoIndex uses it to identify effects declared for the same component across runs. This is essential to make sure effects declared by the same component are synced atomically whenever possible.

For example, imagine the following scenarios:

- If a file like `1.txt` changes (for instance, its title is updated), the corresponding component at `/Papers/files/1.txt` is re-executed, and CocoIndex updates the target table with a single atomic operation (such as a delete followed by an insert within a transaction).
- If `2.txt` is removed, the component for `/Papers/files/2.txt` is not remounted. When the parent component (e.g., `/Papers`) completes, CocoIndex notices the absence of the child component and its effects, and removes the corresponding row from the target table.

For a small set of data, you can always use a single top-level component (e.g., `process_papers_main`) that owns all effects – all external actions are taken in one transaction. Once the data size is larger, you can use smaller components (one per file), so changes for each can happen piece by piece, which makes changes faster.

## Context

**Context** is how your code gets handles to external resources (databases, object stores, HTTP clients) that live outside any single component run. You register them during `@coco.global_lifespan` so they’re available to all components and to out-of-band operations (e.g., `cocoindex drop`).

## App

An app bundles your top-level function and arguments into something runnable, and mounts that function as the app’s top-level component.
