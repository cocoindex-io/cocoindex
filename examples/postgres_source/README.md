<p align="center">
  <a href="https://cocoindex.io/docs/examples/postgres-source/" title="Use an existing Postgres table as a CocoIndex source — derive fields, embed each row, and write the vectors back to Postgres pgvector, incrementally, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/postgres-source/cover.svg" alt="Postgres as a source with CocoIndex — read product rows from an existing table, derive a description and total_value, embed each row with sentence-transformers, and write the enriched rows plus vectors back to Postgres with pgvector" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn an existing Postgres table into a <em>semantic</em> index.</h1>

<p align="center">
  <b>Read product rows from a Postgres table, <em>derive</em> fields and <em>embed</em> each one, and write the enriched rows plus their vectors back to Postgres with pgvector.</b><br/>
  Your structured data, searchable by meaning — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/postgres-source/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Most data already lives in a database. This example takes an existing Postgres table of products, reads it row by row, derives a couple of fields, embeds each row, and writes the result — including the vector — back into Postgres with [pgvector](https://github.com/pgvector/pgvector). You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only the rows that changed get re-embedded and re-upserted.

## How it works

`app_main` wires the source to the target: it mounts the Postgres target table, opens the source table with [`PgTableSource`](https://cocoindex.io/docs/connectors/postgres/), and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per source row. Passing `row_type=SourceProduct` maps each row straight into the dataclass; `items(...)` tags each one with its `(product_category, product_name)` composite key. Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_product(product: SourceProduct, table: postgres.TableTarget[OutputProduct]) -> None:
    full_description = f"Category: {product.product_category}\nName: {product.product_name}\n\n{product.description}"
    total_value = product.price * product.amount
    embedding = await coco.use_context(EMBEDDER).embed(full_description)
    table.declare_row(row=OutputProduct(..., total_value=total_value, embedding=embedding))

@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            OutputProduct, primary_key=["product_category", "product_name"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    source = postgres.PgTableSource(
        coco.use_context(SOURCE_POOL), table_name="source_products", row_type=SourceProduct)
    await coco.mount_each(
        process_product,
        source.fetch_rows().items(lambda p: (p.product_category, p.product_name)),
        target_table,
    )
```

We embed the *composed* description — category and name included — so a search for "wireless audio" matches even when the body never says it. `embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/postgres-source/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the source/target row shapes, the derived fields, the embedder wiring, and the SQL query.
</p>

## Why it's worth a star ⭐

- **Your database is the source.** `PgTableSource` reads an existing table directly — point it at any table and you have a semantic index over your structured data, no export step.
- **Source and target, same engine.** The same Postgres instance can hold both, or set `SOURCE_DATABASE_URL` to read from a separate database. `mount_table_target` creates and manages the target table — schema, idempotent upserts, orphan cleanup.
- **Embed what matters.** The composed `full_description` carries the category and name into the vector, so meaning-based search works even when the query words never appear in the body.
- **Incremental by default.** `@coco.fn(memo=True)` skips a row whose content and code are unchanged; the output's primary key is derived from the source row, so only changed rows are re-embedded and upserted and vanished rows are deleted.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set POSTGRES_URL and SOURCE_DATABASE_URL (can be the same instance)
pip install -e .
```

**3. Seed the source table** — create `source_products` with the sample rows:

```sh
psql "$SOURCE_DATABASE_URL" -f ./prepare_source_data.sql
```

**4. Build the index** — the Postgres source runs as a one-shot catch-up (scan the source table, sync the target, exit):

```sh
cocoindex update main
```

**5. Search from the command line:**

```sh
python main.py "wireless headphones"
```

The most semantically similar products come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

---

<p align="center">
  If this turned your table into a semantic index, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/postgres-source/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/postgres_source" alt="" width="1" height="1" />
