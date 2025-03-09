Simple example for cocoindex: build embedding index based on local files.

## Prerequisite
Follow [Prerequisite](https://cocoindex.io/docs/getting_started/quickstart#prerequisite-install-cocoindex-environment) to install cocoIndex environment.

## Run

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
python pdf_embedding.py cocoindex setup
```

Update index:

```bash
python pdf_embedding.py cocoindex update
```

Run:

```bash
python pdf_embedding.py
```

## CocoInsight 
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
python pdf_embedding.py cocoindex server -c https://cocoindex.io/cocoinsight
```
