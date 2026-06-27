<p align="center">
  <a href="https://cocoindex.io/docs/examples/product-recommendation/" title="Build a product recommendation graph with LLM taxonomy extraction and CocoIndex — Neo4j, incremental, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/product-recommendation/cover.svg" alt="Build a product recommendation graph with CocoIndex and Neo4j — an LLM tags what each product is and what complements it, and the shared taxonomy edges power 'people who bought this also need…' queries" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn a product catalog into a <em>recommendation</em> graph.</h1>

<p align="center">
  <b>An LLM tags what each product <em>is</em> and what <em>pairs</em> with it; the shared taxonomy edges become a "people who bought this also need…" engine — in plain async Python.</b><br/>
  Point it at a folder of product JSON, and it re-extracts only what changes as you edit the catalog.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/product-recommendation/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A pile of product listings has the recommendations hiding in plain sight — a *pen* pairs with *ink refills* and a *notebook*; a *monitor* pairs with a *stand* and an *HDMI cable*. But that knowledge is locked in prose. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so editing one product re-extracts one product, not the catalog.

## How it works

Two node types, two relationship types, and the recommendation falls out of the graph:

- **`Product`** nodes — one per listing (title, price).
- **`Taxonomy`** nodes — one per distinct label (`gel pen`, `notebook`, `ink refill`), keyed by value and **shared** across products.
- **`PRODUCT_TAXONOMY`** edges — `Product → Taxonomy`: what the product is.
- **`PRODUCT_COMPLEMENTARY_TAXONOMY`** edges — `Product → Taxonomy`: what a buyer might also need.

Products whose *complementary* taxonomy matches another product's *is-a* taxonomy are the things to recommend together.

Because taxonomy labels are shared across products, the pipeline runs in two phases — read it top-to-bottom in [`main.py`](main.py):

```python
@coco.fn(memo=True)  # caches each extraction by content — re-tag only changed products
async def extract_taxonomy(detail: str) -> ProductTaxonomyInfo:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL), response_model=ProductTaxonomyInfo,
        messages=[{"role": "system", "content": TAXONOMY_PROMPT}, {"role": "user", "content": detail}],
    )
    return ProductTaxonomyInfo.model_validate(result.model_dump())

@coco.fn(memo=True)   # Phase 1 — per product: declare the node, extract, carry labels forward
async def process_file(file: FileLike, product_table: neo4j.TableTarget[Product]) -> ProductTaxonomies:
    raw = json.loads(await file.read_text())
    product_id = file.file_path.path.name.removesuffix(".json")
    product_table.declare_record(row=Product(id=product_id, title=raw["title"], price=...))
    info = await extract_taxonomy(PRODUCT_TEMPLATE.render(**raw))
    return ProductTaxonomies(product_id, [t.name for t in info.taxonomies], ...)

@coco.fn              # Phase 2 — one pass owns the shared Taxonomy nodes + both edge types
async def build_graph(products, taxonomy_table, product_taxonomy_rel, complementary_rel) -> None:
    for value in {t for p in products for t in (*p.taxonomies, *p.complementary)}:
        taxonomy_table.declare_record(row=Taxonomy(value=value))
    for p in products:
        for t in set(p.taxonomies):    product_taxonomy_rel.declare_relation(from_id=p.product_id, to_id=t)
        for t in set(p.complementary): complementary_rel.declare_relation(from_id=p.product_id, to_id=t)
```

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/product-recommendation/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the data model, the two-phase flow, the extraction schema, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **Shared nodes, done right.** Taxonomy labels are deduplicated and owned by a single graph pass, so `gel pen` is one node every product can point at — not a copy per product.
- **Incremental by default.** `@coco.fn(memo=True)` caches each LLM extraction by content; edit one product and only that product re-extracts, then the graph diffs — adding new nodes/edges and removing ones no longer supported anywhere.
- **The graph IS the recommender.** No separate model. One Cypher query walks *complementary → is-a* edges to surface what to cross-sell.
- **Plain Python, your stack.** Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) — swap `LLM_MODEL` for any provider (OpenAI, Ollama, …). No DSL.
- **Honest cache busting.** `LLM_MODEL` is declared with `detect_change=True`, so swapping the model re-extracts everything against it with no cache to clear by hand.

## Run it

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure & install:**

```sh
cp .env.example .env     # set OPENAI_API_KEY (or LLM_MODEL=ollama/llama3.2)
pip install -e .
```

**3. Build the graph** — the example ships a `products/` folder of sample listings (pens, notebooks, monitors, …):

```sh
cocoindex update main
```

On the 9 sample products that's **9 `Product` nodes, ~40 `Taxonomy` nodes**, and the two edge types wired up.

**4. Explore the recommendations** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`) and ask the graph:

```cypher
-- Recommend products to pair with anything that is a "gel pen":
-- find products whose is-a taxonomy matches a pen's complementary taxonomy
MATCH (:Taxonomy {value: "gel pen"})<-[:PRODUCT_TAXONOMY]-(:Product)
      -[:PRODUCT_COMPLEMENTARY_TAXONOMY]->(need:Taxonomy)
MATCH (rec:Product)-[:PRODUCT_TAXONOMY]->(need)
RETURN DISTINCT rec.title
```

On the sample data, recommending for a pen surfaces the notepad and the multipurpose paper — exactly the cross-sell you'd want.

---

<p align="center">
  If this turned your catalog into a recommender, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/product-recommendation/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/product_recommendation" alt="" width="1" height="1" />
