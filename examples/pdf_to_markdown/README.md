# PDF to Markdown

This example walks a directory of local PDF files, converts each one to Markdown using [docling](https://github.com/DS4SD/docling), and writes the resulting `.md` files to an output folder.

## Prerequisites

- Python 3.11+
- No external services required — all processing runs locally on CPU.

## Run

Install deps:

```sh
pip install -e .
```

Place the PDF files you want to convert in a `pdf_files/` directory (a sample PDF — the "Attention Is All You Need" paper — is already included).

Build/update the index (converts PDFs and writes Markdown to `out/`):

```sh
cocoindex update main
```

The converted `.md` files will appear in `./out/`, with each file named after the original PDF (e.g. `1706.03762v7.md`).
