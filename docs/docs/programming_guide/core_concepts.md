---
title: Core Concepts
description: Briefly introduce the core concepts of CocoIndex, covering state-driven sync, Effects, Apps, Processing Units, and incremental execution across data and code changes.
---

import { ProcessDiagram, ProcessDiagramAnimated, ProcessingUnitTimeline } from '@site/src/components/ProcessDiagram';

# Core Concepts

CocoIndex is a **state-driven** computing framework that **transforms** your data and keeps **persistent external state** in **sync**.
You describe the state you want in your external systems; CocoIndex computes what changed since the last run and applies only the necessary updates — **incrementally** for both data and code changes.

<ProcessDiagramAnimated />

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

## Apps: the runnable bundle

An ***App*** is what you run in CocoIndex.
It binds a function and its parameters — during execution, the function processes your data and declares effects.
The App owns these effects across runs: CocoIndex tracks what was declared, and on each rerun it updates changed effects and removes effects that are no longer declared.
Given the same code and inputs, runs are repeatable; when data or code changes, only the necessary parts re-execute.

## Processing Units: independent work and effects

Your App often processes many items — files, rows, entities — where each can be handled independently.
A ***Processing Unit*** groups an item's processing together with its output effects.
Each Processing Unit runs on its own and applies its effects as soon as it completes, without waiting for the rest of the App.

Processing Units form a tree: an App establishes a root Processing Unit, which can mount child Processing Units, and so on.
When a Processing Unit finishes, CocoIndex compares its declared effects against the previous run and applies only the necessary changes — including cleaning up effects from children that are no longer mounted.

<ProcessDiagram />

<ProcessingUnitTimeline />

## Incremental computation: data + code

CocoIndex minimizes work through **function-level memoization** and **change tracking**:

* **Data changes:** If a memoized function's **inputs and version** are unchanged, its prior result is reused without re-running the function. If the top-level call for a Processing Unit is a full memo hit, the Processing Unit does not execute.
* **Code changes:** When a function — or any function it depends on — changes, CocoIndex tracks the call graph and marks exactly the call sites that must re-execute. Unaffected memoized results remain valid, avoiding full re-evaluation.

This yields fast feedback when you edit code and efficient steady-state operation as data evolves.
