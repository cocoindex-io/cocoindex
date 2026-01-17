---
title: Core Concepts
description: Briefly introduce the core concepts of CocoIndex, covering state-driven sync, Effects, Components, Apps, and incremental execution across data and code changes.
---

# Core Concepts

CocoIndex is a **state-driven** computing framework that **transforms** your data and keeps **persistent external state** in **sync**.
You describe the state you want in your external systems; CocoIndex computes what changed since the last run and applies only the necessary updates — **incrementally** for both data and code changes.

## State-driven from sources

CocoIndex evaluates against the **current state of your sources** (files, APIs, databases, etc.).
You don’t wire event handlers.
Reruns simply re-evaluate your logic on up-to-date inputs, keeping programs deterministic and easier to reason about.

## Effects: desired targets in external systems

An ***Effect*** is a unit of **desired external state** produced by your transformations.
On each run, CocoIndex compares the newly declared Effects with the previous run and applies the changes needed so targets match your intent (including removals when something is no longer declared).

Examples:

<table>
  <thead>
    <tr>
      <th rowspan="2">Effect you declare</th>
      <th colspan="3">CocoIndex’s action on the target</th>
    </tr>
    <tr>
      <th>on first declaration</th>
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
      <td>a change-feed record (e.g., message in a Kafka topic)</td>
      <td>publish an “insert” event</td>
      <td>publish an “update” event</td>
      <td>publish a “delete/tombstone” event</td>
    </tr>
  </tbody>
</table>

## Components: the sync boundaries

A ***Component*** is a long-lived instance (identified by a stable path) that **owns** the Effects declared within it.
After each run, CocoIndex compares that Component’s current Effects with its prior run at the same path and applies the resulting changes **as a unit**.
This boundary provides clear ownership and predictable scoping of updates.

## Apps: the runnable unit

An ***App*** is the top-level thing you run.
It names your pipeline, binds a top-level function and its parameters, which establishes the root Component, and all work happens within the component tree rooted there.
Given the same code and inputs, runs are repeatable; when data or code changes, only the necessary parts re-execute.

## Incremental computation: data + code

CocoIndex minimizes work through **function-level memoization** and **change tracking**:

* **Data changes:** If a memoized function’s **inputs and version** are unchanged, its prior result is reused without re-running the function. If the top-level call for a Component is a full memo hit, the Component does not execute.
* **Code changes:** When a function — or any function it depends on — changes, CocoIndex tracks the call graph and marks exactly the call sites that must re-execute. Unaffected memoized results remain valid, avoiding full re-evaluation.

This yields fast feedback when you edit code and efficient steady-state operation as data evolves.
