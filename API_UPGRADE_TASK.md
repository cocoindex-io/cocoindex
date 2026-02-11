# API Upgrade Task List

Implementation plan for the v1 API sugar. See [API_UPGRADE_SPEC.md](API_UPGRADE_SPEC.md) for full API details.

## Phase 1: Non-Breaking Additions

These are new APIs added alongside the existing ones. No existing code breaks.

- [ ] **1.** Add `Symbol` type to `StableKey` union
- [ ] **2.** Add `__coco_stable_key__()` protocol (StableKeyProvider) and implement it on `FileLike`, `AsyncFileLike`, targets, and other relevant types
- [ ] **3.** Implement `coco_aio.map(fn, items, *args, **kwargs)`
- [ ] **4.** Implement `coco_aio.mount_target(target)` (sugar over `mount_run()`)
- [ ] **5.** Implement `coco_aio.mount_each(fn, items, *args, **kwargs)`
- [ ] **6.** Support optional `ComponentSubpath` as first arg in `mount()` (implicit key from first passthrough arg)

## Phase 2: Migrate Examples to Non-Breaking Sugar

Update all examples to use `map()`, `mount_target()`, `mount_each()`, and implicit subpaths. Still using `mount_run()` where `mount_target()` doesn't apply.

- [ ] **7.** Migrate examples:
  amazon_s3_embedding, code_embedding, code_embedding_lancedb, custom_source_hn, docs_to_knowledge_graph, files_transform, gdrive_text_embedding, hn_trending_topics, image_search, image_search_colpali, manuals_llm_extraction, meeting_notes_graph, multi_codebase_summarization, paper_metadata, patient_intake_extraction, patient_intake_extraction_baml, patient_intake_extraction_dspy, pdf_elements_embedding, pdf_embedding, pdf_to_markdown, postgres_source, product_recommendation, text_embedding, text_embedding_lancedb, text_embedding_qdrant

## Phase 3: Breaking Change

- [ ] **8.** Rename `mount_run()` to `use_mount()` and change async return from handle to direct value (`T` instead of `Handle[T]`)
- [ ] **9.** Update internal code and tests for `use_mount()`

## Phase 4: Migrate Remaining Usages

- [ ] **10.** Update any remaining `mount_run()` calls in examples and connectors to `use_mount()`

## Phase 5: Documentation

- [ ] **11.** Update programming guide docs (processing_component.md, function.md, app.md, sdk_overview.md)
- [ ] **12.** Update quickstart and other getting-started docs
