# HackerNews Custom Source Example

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we use [CocoIndex Custom Source](https://cocoindex.io/docs/custom_ops/custom_targets) to define a source to get HackerNews recent content, by calling [HackerNews API](https://hn.algolia.com/api).
We build index for HackerNews threads and their comments, and provides a lightweight query handler to search by keywords.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow

1. We define a custom source connector `HackerNews` to get HackerNews recent threads by calling HackerNews API.
2. We build index for HackerNews threads and their comments.

## Prerequisite

[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

Install dependencies:

```bash
pip install -e .
```

Update the target:

```bash
cocoindex update main
```

Each time when you run the `update` command, cocoindex will only re-process threads that have changed, and keep the target in sync with the recent 500 threads from HackerNews.

You can also run `update` command in live mode, which will keep the target in sync with the source continuously:

```bash
cocoindex update -L main.py
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci -L main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
