# Files Transform

Rust equivalent of the Python [`files_transform`](../../files_transform)
example.

It walks a directory of markdown files, memoizes the markdown-to-HTML transform
per file, and writes one HTML file per markdown input.

## Build

```sh
cd examples/rust/files_transform
cargo build --release
```

## Usage

Run against the sample data from the Python example:

```sh
cargo run -- ../../files_transform/data ./output_html
```

Defaults:

- source dir: `../../files_transform/data`
- output dir: `./output_html`

## Notes

- The markdown→HTML render is memoized, so unchanged inputs skip the work.
- Output uses a **declarative `DirTarget`** (the Rust analogue of Python's
  `localfs` directory target): files are written/updated, unchanged files are
  skipped, and an output whose source markdown was **deleted is removed
  automatically** on the next run.
- Like the Python example (whose `walk_dir` defaults to `recursive=False`), only
  **top-level** `*.md` files are processed.
- Markdown rendering uses `pulldown-cmark` with GFM options (tables,
  strikethrough, tasklists). It is not byte-identical to Python's
  `markdown-it-py("gfm-like")` — notably, pulldown-cmark does not "linkify" bare
  URLs (only angle-bracket `<https://…>` autolinks render).
