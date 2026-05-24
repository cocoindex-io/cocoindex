# Files Transform

Rust equivalent of the Python [`files_transform`](../../files_transform)
example.

It walks a directory of markdown files, memoizes the markdown-to-HTML transform
per file, and writes one HTML file per markdown input.

## Build

```sh
cd examples/rust/files-transform
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

- The transform itself is memoized, so unchanged markdown files skip the HTML
  render work.
- The output write stays outside the memoized function so cached runs still
  materialize the expected HTML files.
