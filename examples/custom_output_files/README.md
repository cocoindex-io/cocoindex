# Export markdown files to local Html with Custom Targets
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we will build index flow to load data from a local directory, convert them to HTML, and save the data to another local directory powered by [CocoIndex Custom Targets](https://cocoindex.io/docs/custom_ops/custom_targets).

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow

1. We ingest a list of local markdown files from the `data/` directory.
2. For each file, convert them to HTML using [markdown-it-py](https://markdown-it-py.readthedocs.io/).
3. We will save the HTML files to a local directory `output_html/`.

## Prerequisite

[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

Install dependencies:

```bash
pip install -e .
```

Update the target:

```bash
cocoindex update --setup main.py
```

You can add new files to the `data/` directory, delete or update existing files.
Each time when you run the `update` command, cocoindex will only re-process the files that have changed, and keep the target in sync with the source.

You can also run `update` command in live mode, which will keep the target in sync with the source in real-time:

```bash
cocoindex update --setup -L main.py
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
