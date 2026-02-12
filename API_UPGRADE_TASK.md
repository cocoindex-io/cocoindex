# API Upgrade Task List

Implementation plan for the v1 API sugar. See [API_UPGRADE_SPEC.md](API_UPGRADE_SPEC.md) for full API details.

## Phase 1: Non-Breaking Additions

These are new APIs added alongside the existing ones. No existing code breaks.

- [X] **1.** Add `Symbol` type to `StableKey` union
- [X] **2.** Implement `coco_aio.map(fn, items, *args, **kwargs)`
- [ ] **3.** Implement `coco_aio.mount_target(target)` (sugar over `mount_run()`)
- [ ] **4.** Implement `coco_aio.mount_each(fn, items, *args, **kwargs)` where `items` is `Iterable[tuple[StableKey, T]]`
- [ ] **5.** Update source connectors to return keyed iterables (e.g., `items()` returning `Iterator[tuple[StableKey, T]]`)

## Phase 2: Migrate Examples to Non-Breaking Sugar

Update all examples to use `map()`, `mount_target()`, `mount_each()`. Still using `mount_run()` where `mount_target()` doesn't apply.

- [ ] **6.** Migrate examples:
  amazon_s3_embedding, code_embedding, code_embedding_lancedb, custom_source_hn, docs_to_knowledge_graph, files_transform, gdrive_text_embedding, hn_trending_topics, image_search, image_search_colpali, manuals_llm_extraction, meeting_notes_graph, multi_codebase_summarization, paper_metadata, patient_intake_extraction, patient_intake_extraction_baml, patient_intake_extraction_dspy, pdf_elements_embedding, pdf_embedding, pdf_to_markdown, postgres_source, product_recommendation, text_embedding, text_embedding_lancedb, text_embedding_qdrant

## Phase 3: Breaking Change

- [ ] **7.** Rename `mount_run()` to `use_mount()` and change async return from handle to direct value (`T` instead of `Handle[T]`)
- [ ] **8.** Update internal code and tests for `use_mount()`

## Phase 4: Migrate Remaining Usages

- [ ] **9.** Update any remaining `mount_run()` calls in examples and connectors to `use_mount()`

## Phase 5: Documentation

- [ ] **10.** Update programming guide docs (processing_component.md, function.md, app.md, sdk_overview.md)
- [ ] **11.** Update quickstart and other getting-started docs
