# PDF to Markdown (Rust)

Rust port of the Python [`pdf_to_markdown`](../../pdf_to_markdown) example.

Walks local PDF files, extracts their text, and writes one `.md` output per PDF
through a declarative `DirTarget`: files are written/updated, unchanged files
are memo-skipped, and outputs whose source PDF was deleted are removed
automatically.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                       |
| ---------------- | ---------------------------------------- | ----------------------------------------- |
| Source           | `localfs.walk_dir` (`**/*.pdf`)          | `cocoindex::fs::walk` (`**/*.pdf`)        |
| PDF → markdown   | `docling` (PDF → Markdown, ML pipeline)  | `lopdf` text extraction                   |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] convert_pdf` |
| Output           | `localfs.declare_file` (`<stem>.md`)     | `DirTarget::declare_file` (`<stem>.md`)   |

**Deviation from Python:** Python uses `docling` (a heavy ML document-understanding
pipeline) for high-fidelity PDF→Markdown. There is no Rust equivalent, so this
port extracts plain text with `lopdf`; output is text rather than richly
structured Markdown, and quality varies by PDF. The declarative directory target
and `<stem>.md` naming mirror Python.

## Run

```bash
cargo run                          # ./pdf_files -> ./out
cargo run -- /path/to/pdfs ./out   # custom source / output dirs
```
