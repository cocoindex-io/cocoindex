Simple example for cocoindex: extract structured information from a Markdown file.

## Prerequisite
Follow [Setup Postgres](../../#setup-postgres) section on the root directory to setup Postgres database.

## Run

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
python manual_extraction.py cocoindex setup
```

Update index:

```bash
python manual_extraction.py cocoindex update
```

Run:

```bash
python manual_extraction.py
```
