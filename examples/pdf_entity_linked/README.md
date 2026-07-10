# PDF to Entity-Linked Markdown

Converts local PDF files to Markdown, extracts proper nouns, and hyperlinks each mention to a canonical entity ID stored in PostgreSQL:

```
[Albert Einstein](entities/703c4484-...) published his paper...
```

## The Problem This Demonstrates

This is a concurrency problem. CocoIndex processes files concurrently, and all writes are deferred to a submit phase after processing completes. When two components both mention Einstein, they both read the entity table during processing and both see "not found" — because neither has submitted yet. Both independently create a new ID for the same entity. This is **write skew**.

The fix is the planned `get_or_create` API: an atomic read-then-write during processing that is tracked immediately, so concurrent components see each other's decisions as soon as they're made. Until that API lands, this example uses `declare_row` — which means the same entity can get different IDs across concurrently processed documents.

## Prerequisites

1. Install dependencies:

```sh
pip install -e .
```

2. Start a local Postgres:

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

## Run

Place PDF files in `pdf_files/` (a sample is already included). Then:

```sh
cocoindex update main
```

Linked `.md` files appear in `./out/`. The entity table is maintained in PostgreSQL under the `entity_demo` schema.
