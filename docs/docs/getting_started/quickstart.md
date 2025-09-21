---
title: Quickstart
description: Get started with CocoIndex in 10 minutes
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex-quickstart" margin="0 0 24px 0"/>
<YouTubeButton url="https://www.youtube.com/watch?v=gv5R8nOXsWU" margin="0 0 24px 0"/>

In this tutorial, we’ll build an index with text embeddings, keeping it minimal and focused on the core indexing flow.


## Flow Overview
![Flow](/img/examples/simple_vector_index/flow.png)

1. Read text files from the local filesystem
2. Chunk each document
3. For each chunk, embed it with a text embedding model
4. Store the embeddings in a vector database for retrieval


## Setup
1.  Install CocoIndex:

    ```bash
    pip install -U 'cocoindex[embeddings]'
    ```

2.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres).

3.  Create a new directory for your project:

    ```bash
    mkdir cocoindex-quickstart
    cd cocoindex-quickstart
    ```

4.  Place input files in a directory `markdown_files`. You may download from [markdown_files.zip](markdown_files.zip).


## Define a flow

Create a new file `main.py` and define a flow.

```python title="main.py"
import cocoindex

@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # ... See subsections below for function body
```

###  Add Source and Collector

```python title="main.py"
# add source
data_scope["documents"] = flow_builder.add_source(
    cocoindex.sources.LocalFile(path="markdown_files"))

# add data collector
doc_embeddings = data_scope.add_collector()
```

`flow_builder.add_source` will create a table with sub fields (`filename`, `content`)

<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Source" />

<DocumentationButton url="https://cocoindex.io/docs/core/flow_def#data-collector" text="Data Collector" />

### Process each document

With CocoIndex, it is easy to process nested data structures.

```python title="main.py"
with data_scope["documents"].row() as doc:
    # ... See subsections below for function body
```


#### Chunk each document

```python title="main.py"
doc["chunks"] = doc["content"].transform(
    cocoindex.functions.SplitRecursively(),
    language="markdown", chunk_size=2000, chunk_overlap=500)
```

We extend a new field `chunks` to each row by *transforming* the `content` field using `SplitRecursively`. The output of the `SplitRecursively` is a KTable representing each chunk of the document.

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#splitrecursively" text="SplitRecursively" margin="0 0 16px 0" />

![Chunking](/img/examples/simple_vector_index/chunk.png)



#### Embed each chunk and collect the embeddings

```python title="main.py"
with doc["chunks"].row() as chunk:
    # embed
    chunk["embedding"] = chunk["text"].transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )

    # collect
    doc_embeddings.collect(
        filename=doc["filename"],
        location=chunk["location"],
        text=chunk["text"],
        embedding=chunk["embedding"],
    )
```

This code embeds each chunk using the SentenceTransformer library and collects the results.

![Embedding](/img/examples/simple_vector_index/embed.png)

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#sentencetransformerembed" text="SentenceTransformerEmbed" margin="0 0 16px 0" />

### Export the embeddings to Postgres

```python title="main.py"
doc_embeddings.export(
    "doc_embeddings",
    cocoindex.storages.Postgres(),
    primary_key_fields=["filename", "location"],
    vector_indexes=[
        cocoindex.VectorIndexDef(
            field_name="embedding",
            metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
        )
    ],
)
```

CocoIndex supports other vector databases as well, with 1-line switch.

<DocumentationButton url="https://cocoindex.io/docs/ops/targets" text="Targets" />


## Run the indexing pipeline

- Specify the database URL by environment variable:

    ```bash
    export COCOINDEX_DATABASE_URL="postgresql://cocoindex:cocoindex@localhost:5432/cocoindex"
    ```

- Build the index:

    ```bash
    cocoindex update --setup main.py
    ```

CocoIndex will run for a few seconds and populate the target table with data as declared by the flow. It will output the following statistics:

```
documents: 3 added, 0 removed, 0 updated
```

That's it for the main indexing flow.


## End to end: Query the index (Optional)

If you want to build a end to end query flow that also searches the index, you can follow the [simple_vector_index](https://cocoindex.io/docs/examples/simple_vector_index#query-the-index) example.


## Next Steps

Next, you may want to:

*   Learn about [CocoIndex Basics](../core/basics.md).
*   Explore more of what you can build with CocoIndex in the [examples](https://cocoindex.io/docs/examples) directory.
