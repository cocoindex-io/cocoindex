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

## CocoInsight 
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
python manual_extraction.py cocoindex server -c https://cocoindex.io/cocoinsight
```
