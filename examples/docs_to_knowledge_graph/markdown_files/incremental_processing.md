# Incremental Processing

CocoIndex is a data transformation framework built around incremental
processing. A pipeline declares a target state as a function of its source
state, and the engine keeps the target in sync as the source changes.

Incremental processing means CocoIndex only reprocesses what actually changed.
When a source file is added, modified, or deleted, the engine detects the change
and updates the affected target states — it does not rebuild everything.

Memoization makes this efficient. Each processing function can cache its result
keyed by its inputs and code, so an expensive step such as an embedding or an
LLM call is skipped when nothing relevant changed.

Incremental processing keeps a derived index fresh. A vector index, a knowledge
graph, or a set of converted files stays consistent with its source data without
a full rebuild on every run.
