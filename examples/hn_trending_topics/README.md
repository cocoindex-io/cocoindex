# HackerNews Trending Topics Example

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we use [CocoIndex Custom Source](https://cocoindex.io/docs/custom_ops/custom_targets) to define a source to get HackerNews recent content by calling [HackerNews API](https://hn.algolia.com/api).
We build an index for HackerNews threads and their comments, and use LLM to extract trending topics from the text.

The pipeline uses `ExtractByLlm` to identify topics like product names, technologies, models, and company names mentioned in threads and comments, storing them in canonical form (avoiding acronyms unless very popular).

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Features

- **Custom Source Integration**: Fetches HackerNews threads and comments via API
- **LLM Topic Extraction**: Automatically extracts topics using `ExtractByLlm` function
- **Canonical Topic Forms**: Topics are stored in canonical form (e.g., "Large Language Model" instead of "LLM")
- **Multiple Query Handlers**:
  - `search_by_topic`: Search content by specific topic
  - `get_trending_topics`: Get trending topics ranked by mention count

## Steps

### Indexing Flow

1. We define a custom source connector `HackerNews` to get HackerNews recent threads by calling HackerNews API.
2. For each thread and comment, we extract topics using LLM (`ExtractByLlm`).
3. We build two indexes:
   - `hn_messages`: Full text of threads and comments
   - `hn_topics`: Extracted topics with references to their source content, keyed by (topic, message_id)

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

## Query Examples

After running the pipeline, you can query the extracted topics:

```bash
# Get trending topics
cocoindex query main.py get_trending_topics --limit 20

# Search content by specific topic
cocoindex query main.py search_by_topic --topic "Claude"

# Search by text content
cocoindex query main.py search_text --query "artificial intelligence"
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
cocoindex server -ci -L main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
