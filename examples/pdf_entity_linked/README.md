<h1 align="center">Turn PDFs into <em>entity-linked Markdown</em>.</h1>

<p align="center">
  <b>Convert PDFs with <em>docling</em>, give repeated entity names one canonical UUID, and keep the linked Markdown and PostgreSQL table in sync.</b><br/>
  Concurrent files can discover the same entity immediately—without embeddings or API keys.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/" title="Read the CocoIndex documentation"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

This example turns a folder of PDFs into Markdown, then replaces configured entity names with links such as `[Albert Einstein](entities/703c4484-...)`. Every document proposes its own random UUID, but CocoIndex elects one winner and stores exactly one canonical row in PostgreSQL. You declare the result in native Python—`target_state = transformation(source_state)`—while the Rust engine handles incremental processing, concurrent claims, confirmation, and cleanup.

## How it works

A single docling `DocumentConverter` is built once and pinned to CPU. `process_file` runs as one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per PDF. When it finds a configured entity name, `_resolve_entity_id` first queries PostgreSQL and normally redeclares an existing row. If the row is absent, it proposes a UUID and calls `optimistic_declare_row`. Read it in [`main.py`](main.py):

```python
async def _resolve_entity_id(
    entity_table: postgres.TableTarget[Entity], name: str
) -> str:
    pool = coco.use_context(PG_DB)

    for _ in range(_RESOLVE_ATTEMPTS):
        existing = await _lookup_entity_id(pool, name)
        if existing is not None:
            entity_table.declare_row(row=Entity(name=name, id=existing))
            return existing

        proposed = str(uuid.uuid4())
        if await entity_table.optimistic_declare_row(
            row=Entity(name=name, id=proposed)
        ):
            return proposed

        await asyncio.sleep(_RESOLVE_BACKOFF_SECONDS)

    raise RuntimeError(f"could not resolve an entity id for {name!r}")
```

The method is one CAS-backed operation:

- `True` means this component atomically claimed logical absence, declared the row, and made it visible in PostgreSQL immediately.
- `False` means a pending or confirmed writer already owns the same primary key, so the caller waits briefly and reads the winner's row.
- An eager sink exception propagates, while the ordinary declaration remains registered so a caught failure can still heal during normal submit.

The winning write is recorded before PostgreSQL I/O, re-applied through normal submit, and confirmed atomically with CocoIndex tracking. If the component fails, cleanup deletes the eager row before releasing its claim.

## Why it's worth a star ⭐

- **One entity, one ID under concurrency.** Two PDFs can race with different UUID proposals; the AppStore CAS elects exactly one winner.
- **Immediate visibility.** The winner's eager row is queryable by sibling components before the normal submit phase.
- **Incremental by default.** `@coco.fn(memo=True)` skips unchanged PDFs, so docling and the optimistic path do no work on a memo hit.
- **Declarative ownership.** Confirmed rows are redeclared normally, keeping cleanup and change tracking consistent across reruns.
- **Recovery is scoped.** A crashed writer is recovered lazily when that component is next reconciled—there is no whole-app startup scan.

## Run it

**1. Start PostgreSQL:**

The example uses port `55432` so it does not collide with a PostgreSQL server already using the default port.

```sh
docker run --rm -d \
  --name cocoindex-pdf-entity-linked-postgres \
  -e POSTGRES_USER=cocoindex \
  -e POSTGRES_PASSWORD=cocoindex \
  -e POSTGRES_DB=cocoindex \
  -p 127.0.0.1:55432:5432 \
  pgvector/pgvector:pg17

docker exec cocoindex-pdf-entity-linked-postgres \
  sh -c 'until pg_isready -U cocoindex -d cocoindex; do sleep 1; done'
```

**2. Add PDFs** to `pdf_files/`. The included samples both mention Albert Einstein so they exercise the concurrent get-or-create path.

**3. Run the update:**

```sh
uv run cocoindex update main.py -f
```

**4. Inspect the result:**

```sh
rg 'entities/' out/
docker exec cocoindex-pdf-entity-linked-postgres \
  psql -U cocoindex -d cocoindex -c 'TABLE entity_demo.entities;'
```

Both Markdown files should link Albert Einstein to the same UUID, and PostgreSQL should contain exactly one entity row. Add, replace, or delete a PDF and rerun the update—only changed files are reprocessed, and removed outputs are cleaned up automatically.

Set `POSTGRES_URL` only when using another PostgreSQL instance. Stop the bundled local database with `docker stop cocoindex-pdf-entity-linked-postgres`.

## Current boundaries

- The CAS operation supports absence-to-row creation, not arbitrary compare-and-swap updates against an expected prior value.
- A sibling can currently commit a reference to an optimistic row immediately before its winning writer fails; dependency revalidation or shared ownership is future work.
- Direct PostgreSQL writes do not participate in CocoIndex's AppStore CAS.
- Concurrent live processes sharing one AppStore remain unsupported during the narrow cleanup/submit overlap.

---

<p align="center">
  If this saved you a concurrency problem, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a>—it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/programming_guide/target_state/">Target states</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/pdf_entity_linked" alt="" width="1" height="1" />
