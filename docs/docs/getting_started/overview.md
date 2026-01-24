---
title: Overview
slug: /
---

# Welcome to CocoIndex

CocoIndex is an ultra-performant compute framework for AI workloads, with incremental processing.


## Programming model
CocoIndex follows a *Persistent-State-Driven* model. Each transformation creates a new field solely based on input fields, without hidden state or value mutation. All data before/after each transformation is observable, with lineage out of the box.

## CocoIndex features
### High-performance Rust 🦀 engine
CocoIndex compiles transformations and executes them as resilient, scalable data pipelines on a high-performance Rust engine.

### Easy to code
- Developers use familiar languages (e.g., Python) to write simple transformations without learning new DSLs
- Developers write simple transformations without worrying about deltas. CocoIndex runs them incrementally in both batch and live mode, continuously updating results — no separate DAGs, operators, or orchestration logic required.

### Incremental & low-latency
CocoIndex tracks fine-grained dependencies and only recomputes what changed in the input data or the code. End-to-end updates drop from hours/days to seconds while keeping full correctness.

### Full lineage & explainability
Every transform step, intermediate dataset, and execution path is inspectable. This helps it remain compliant with the EU AI Act for transparency, and satisfies enterprise auditability/traceability requirements.

### Open integration model
Sources and sinks plug in through a standard, open interface (no vendor lock-in). Developers can leverage the full Python ecosystem for models, UDFs, and libraries.

### High throughput + controlled concurrency
Pipelines automatically parallelize with managed concurrency and request batching — reducing GPU cost, RPC fanout, and end-to-end latency.

### Fault-tolerant runtime
The engine gracefully retries transient failures and resumes from previous progress after interruptions — eliminating manual backfills and replays.

### Low operational overhead
CocoIndex removes the need for elaborate plumbing: refreshing datasets, maintaining state, handling backfills, ensuring correctness, coordinating GPUs, scaling workers, and managing infra are all handled by the engine.

## Incremental data processing
CocoIndex applies a "persistent data processing" approach, where it continuously maintains and tracks state while processing only new or changed data. It is designed to support incremental indexing from day zero.

What incremental processing means:
- Avoid unnecessarily recomputing transformations on data, based on multi-level change detection
  - Row level: only reprocess source rows with changes
  - Function level: within a row’s processing, also memoize expensive function calls and reuse when possible
  - Apply minimum necessary changes (insertions, updates, deletions) to the target.
- Support multiple mechanisms to capture source changes (CDC, poll-based) out of the box.

In CocoIndex, developers write simple batch-style transformation code — no delta logic, no state handling. CocoIndex automatically manages the pipeline (incrementally) and maintains the output for serving, training, or feature computation.
