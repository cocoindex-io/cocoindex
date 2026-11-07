# Python and Rust pages

The docs have one tree per SDK language, built from the same source files:

| Language | URL | Example |
|---|---|---|
| Python | `/docs/<slug>/` | `/docs/programming_guide/function/` |
| Rust | `/docs/rust/<slug>/` | `/docs/rust/programming_guide/function/` |

Each page shows one language. A switch at the top of the sidebar moves between
the two copies of a page, the sidebar lists only the current language's pages,
and the breadcrumb shows a "Python SDK" or "Rust SDK" badge. The machinery is
in `src/lib/lang.ts`; this file is the authoring contract.

## Frontmatter

| Key | Meaning |
|---|---|
| `languages: [python, rust]` | The trees the page is in. Absent means both. Use `[python]` for a Python-only page (for example `ops/litellm`) and `[rust]` for a Rust-only page. |
| `rust_title`, `rust_description` | Title and lede on the Rust page, when they differ from `title` / `description`. Same `*accent*` markup as `title`. Quote a value that contains ` #` (YAML reads it as a comment): `rust_title: "Declaring *functions* with #[cocoindex::function]"`. |
| `route: <slug>` | The page's slug in its trees, when it is not the file path. A Rust-only file can take the slot of a Python page this way: `getting_started/rust_quickstart.mdx` has `languages: [rust]` and `route: getting_started/quickstart`, so it is `/docs/rust/getting_started/quickstart/`. |
| `python_counterpart: <slug>`, `rust_counterpart: <slug>` | Where the language switch goes from a one-language page (for example `rust_counterpart: ops/model_apis` on `ops/litellm`). Without it the switch goes to the page in the same slot, else the section index, else the tree's root. |
| `neutral: true` | The page reads the same in both languages (FAQ, About, Contributing). The Rust copy's canonical URL is the Python one, so search engines see one page. |

## Components

These need no `import` in a page.

### `<LangTabs>`: a Python / Rust pair

Renders only the slot of the page's language, with no tab UI. Keep the two
samples side by side in the source:

````mdx
<LangTabs>
<Fragment slot="python">

```python
@coco.fn
async def process_file(file: FileLike) -> str:
    return await file.read_text()
```

</Fragment>
<Fragment slot="rust">

```rust
#[cocoindex::function]
async fn process_file(_ctx: &Ctx, file: FileEntry) -> Result<String> {
    Ok(file.content_str()?.to_string())
}
```

</Fragment>
</LangTabs>
````

A code block outside `LangTabs` and outside `Lang` shows on both pages. On a
page in both trees, put every language-specific code block in one of them.

### `<Lang only="python">` / `<Lang only="rust">`: prose for one language

Inline:

```mdx
Mark the function <Lang only="python">`@coco.fn`</Lang><Lang only="rust">`#[cocoindex::function]`</Lang> to memoize it.
```

Around blocks (blank lines inside the tags), including headings, lists and
code. A heading inside appears only in that language's table of contents:

```mdx
<Lang only="python">

## Async adapter

`@coco.fn` keeps the sync or async nature of the function.

</Lang>
```

Do not nest `Lang` in `Lang`.

### `<RustNote>`: a difference from Python

Shown on Rust pages only, as a callout titled "Different from Python". Use it
where the Rust SDK differs on purpose, at the point it matters:

```mdx
<RustNote>
The Rust SDK has no sync/async adapter: a `#[cocoindex::function]` body is an
`async fn`, and every function takes the component's `&Ctx` first.
</RustNote>
```

`<RustNote title="…">` changes the label.

## Links

- A **source-relative** link (`./app`, `../connectors/postgres`) opens the
  linked file's page in the reader's language when the page has one, else in
  the language it has. Write these by default.
- A **root-relative** link (`/docs/cli/`, `/docs/rust/cli/`) is taken as
  written. Use it to point at one language on purpose, for example "see the
  [Python CLI](/docs/cli/)" on a Rust page.

## Agent-facing files

Each language has its own `.md` twins (`/docs/<slug>.md`,
`/docs/rust/<slug>.md`), `llms.txt` and `llms-full.txt`. The twins keep only
the page's language: `Lang`, `LangTabs` slots and `RustNote` of the other
language are dropped. `scripts/check-agent-output.mjs` checks that every page
has a twin per language and that every code block appears in one of them.

## Reference page

`programming_guide/function.mdx` uses each piece: `rust_title`, `rust_description`, inline `Lang`, a block `Lang` heading pair, a `RustNote`, and `LangTabs`.

## Checklist for a page

1. Set `languages` if the page is not in both trees.
2. Set `rust_title` / `rust_description` if the title or lede names a Python API.
3. Put every Python-only and Rust-only code block in `LangTabs` or `Lang`.
4. Wrap prose that names one language's API (`@coco.fn`, `coco.mount`,
   `asyncpg.Pool`, `#[cocoindex::function]`, `Ctx`) in `Lang`, or reword it
   so it holds for both.
5. Replace "the Rust SDK …" asides on shared prose with `RustNote` or a
   `Lang only="rust"` block.
6. Build (`npm run build`) and read both copies of the page.
