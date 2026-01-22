---
title: Core Concepts
description: Briefly introduce the core concepts of CocoIndex, covering state-driven sync, Target States, Apps, Processing Components, and incremental execution across data and code changes.
---

# Core Concepts

CocoIndex is a **state-driven** computing framework that **transforms** your data and keeps **persistent external state** in **sync**.
You describe the state you want in your external systems; CocoIndex computes what changed since the last run and applies only the necessary updates — **incrementally** for both data and code changes.

## State-driven from sources

CocoIndex evaluates against the **current state of your sources** (files, APIs, databases, etc.).
You don’t wire event handlers.
Reruns simply re-evaluate your logic on up-to-date inputs, keeping programs deterministic and easier to reason about.

## Target States: desired targets in external systems

A ***Target State*** is a unit of **desired external state** produced by your transformations.
On each run, CocoIndex compares the newly declared Target States with the previous run and applies the changes needed so targets match your intent (including removals when something is no longer declared).

Examples:

<table>
  <thead>
    <tr>
      <th rowspan="2">Target State you declare</th>
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

## Apps: the runnable bundle

An ***App*** is what you run in CocoIndex.
It binds a function and its parameters — during execution, the function processes your data and declares target states.
The App owns these target states across runs: CocoIndex tracks what was declared, and on each rerun it updates changed target states and removes target states that are no longer declared.
Given the same code and inputs, runs are repeatable; when data or code changes, only the necessary parts re-execute.

## Processing Components: independent work and target states

Your App often processes many items — files, rows, entities — where each can be handled independently.
A ***Processing Component*** groups an item's processing together with its output target states.
Each Processing Component runs on its own and applies its target states as soon as it completes, without waiting for the rest of the App.

Processing Components form a tree: an App establishes a root Processing Component, which can mount child Processing Components, and so on.
When a Processing Component finishes, CocoIndex compares its declared target states against the previous run and applies only the necessary changes — including cleaning up target states from children that are no longer mounted.

## Incremental computation: data + code

CocoIndex minimizes work through **function-level memoization** and **change tracking**:

* **Data changes:** If a memoized function's **inputs and version** are unchanged, its prior result is reused without re-running the function. If the top-level call for a Processing Component is a full memo hit, the Processing Component does not execute.
* **Code changes:** When a function — or any function it depends on — changes, CocoIndex tracks the call graph and marks exactly the call sites that must re-execute. Unaffected memoized results remain valid, avoiding full re-evaluation.

This yields fast feedback when you edit code and efficient steady-state operation as data evolves.
