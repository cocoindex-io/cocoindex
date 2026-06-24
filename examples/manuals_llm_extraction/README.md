<p align="center">
  <a href="https://cocoindex.io/docs/examples/manuals-llm-extraction/" title="Turn PDF manuals into typed records with CocoIndex — convert to Markdown with docling, LLM-extract a structured module summary, store in Postgres, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/manuals-llm-extraction/cover.svg" alt="Turn a folder of PDF manuals into typed records with CocoIndex — convert each PDF to Markdown with docling on a GPU runner, LLM-extract a structured module summary of classes, methods, and arguments, and store a row per manual in Postgres" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn PDF manuals into <em>structured</em> records.</h1>

<p align="center">
  <b>Convert each PDF manual to Markdown with docling, LLM-extract a typed summary — <em>classes, methods, arguments</em> — and store it in Postgres — in plain async Python.</b><br/>
  Manuals are full of structure laid out for humans, not machines; this pulls it back out into a nested schema.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/manuals-llm-extraction/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Manuals, datasheets, and reference docs are full of structure — classes, functions, parameters, defaults — laid out for humans, not machines. This pipeline pulls that structure out: convert each PDF to Markdown with [docling](https://github.com/docling-project/docling), LLM-extract a typed summary of the module it documents, and store the result in Postgres. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (the GPU PDF parse, incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one manual re-parses and re-extracts only that one.

## How it works

The output type is nested Pydantic, and the structure itself tells the model what to pull out — a `ModuleInfo` has `classes` (each with `methods`) and module-level `methods` (each with `args`). Per manual, two transforms and a row: `pdf_to_markdown` runs docling on a GPU runner, `extract_module` does the [instructor](https://github.com/instructor-ai/instructor)-over-[LiteLLM](https://docs.litellm.ai/) extraction, and `process_file` declares one Postgres row with the summary counts plus the full structure as JSON. Read it in [`main.py`](main.py):

```python
@coco.fn.as_async(runner=coco.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="manual.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()


@coco.fn(memo=True)
async def extract_module(markdown: str) -> ModuleInfo:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL), response_model=ModuleInfo,
        messages=[{"role": "system", "content": EXTRACT_PROMPT},
                  {"role": "user", "content": markdown}],
    )
    return ModuleInfo.model_validate(result.model_dump())


@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[ModuleRecord]) -> None:
    markdown = await pdf_to_markdown(await file.read())
    info = await extract_module(markdown)
    table.declare_row(row=ModuleRecord(
        filename=file.file_path.path.name, title=info.title, description=info.description,
        num_classes=len(info.classes), num_methods=len(info.methods),
        module_info=json.dumps(info.model_dump()),
    ))
```

You *declare* the row; CocoIndex inserts, updates, or deletes it to match. `app_main` mounts the Postgres table, walks the source for `*.pdf`, and runs one `process_file` component per manual with `mount_each`.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/manuals-llm-extraction/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the nested extraction schema, the GPU PDF parse, the Postgres row, and the per-manual results.
</p>

## Why it's worth a star ⭐

- **The schema is the prompt.** A nested `ModuleInfo` — module → classes → methods → args — tells the model exactly what to pull out, no hand-tuned prompt for each level.
- **Heavy parse on a GPU runner.** `pdf_to_markdown` is decorated `@coco.fn.as_async(runner=coco.GPU)`, so the docling parse runs where the hardware is while the rest stays async.
- **Incremental by default.** `@coco.fn(memo=True)` caches both the PDF parse and the extraction by content, so editing one manual re-parses and re-extracts only that one — the row is updated in place.
- **Plain Python, your stack.** Extraction is instructor over LiteLLM, so swapping `LLM_MODEL` switches providers (OpenAI, Gemini, a local Ollama model). No DSL.
- **Honest cache busting.** `LLM_MODEL` is declared with `detect_change=True`, so swapping the model re-extracts everything against it with no cache to clear by hand.

## Run it

**1. Start Postgres:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — the example ships a `manuals/` folder of Python module reference PDFs (`array`, `base64`, `copy`):

```sh
cp .env.example .env     # set POSTGRES_URL and OPENAI_API_KEY (or LLM_MODEL=gemini/gemini-2.0-flash, ollama/llama3.2, …)
pip install -e .
```

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
cocoindex update main       # catch-up run
cocoindex update -L main    # live run — watch the manuals/ folder for changes
```

This produces one row per manual in `coco_examples.modules_info`, and the extraction is faithful to each module's shape — `base64` comes out function-based (22 module functions, no classes), while `array` is a single class.

**4. Explore the results:**

```sql
SELECT filename, title, num_classes, num_methods FROM coco_examples.modules_info;

-- pull the full nested structure for one module
SELECT module_info::jsonb -> 'classes' -> 0 -> 'methods'
FROM coco_examples.modules_info WHERE filename = 'copy.pdf';
```

Re-run `cocoindex update main` anytime — only changed manuals are re-parsed and re-extracted.

---

<p align="center">
  If this turned your manuals into structured rows, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/manuals-llm-extraction/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/manuals_llm_extraction" alt="" width="1" height="1" />
