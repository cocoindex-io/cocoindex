# Files Transform

This example watches a directory of local Markdown files, converts each one to HTML using [markdown-it-py](https://github.com/executablebooks/markdown-it-py), and writes the resulting `.html` files to an output folder. It runs in live mode, so it continues watching for file changes after the initial sync.

## Prerequisites

- Python 3.11+
- No external services required.

## Run

Install deps:

```sh
pip install -e .
```

Place the Markdown files you want to transform in a `data/` directory (sample files are already included).

Build/update the index (converts Markdown and writes HTML to `output_html/`). Pick one of the two modes:

- **Catch-up run** — scan sources, sync changes, exit:

  ```sh
  cocoindex update main
  ```

- **Live run** — catch up, then keep watching for file changes (the source declares `live=True` in `main.py`):

  ```sh
  cocoindex update -L main
  ```

The converted `.html` files will appear in `./output_html/`, with each file named after the path parts of the original Markdown file joined by `__` (e.g. `subdir__file.html`).
