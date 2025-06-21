---
title: Overview
slug: /
---

# Welcome to CocoIndex

CocoIndex is an ultra-performant real-time data transformation framework for AI, with incremental processing.

As a data framework, CocoIndex takes it to the next level on data freshness. **Incremental processing** is one of the core values provided by CocoIndex.

![Incremental Processing](/img/incremental-etl.gif)

## Programming Model
CocoIndex follows the idea of [Dataflow programming](https://en.wikipedia.org/wiki/Dataflow_programming) model. Each transformation creates a new field solely based on input fields, without hidden states and value mutation. All data before/after each transformation is observable, with lineage out of the box.

The gist of an example data transformation:
```python
# import
data['content'] = flow_builder.add_source(...)

# transform
data['out'] = data['content']
    .transform(...)
    .transform(...)

# collect data
collector.collect(...)

# export to db, vector db, graph db ...
collector.export(...)
```

Get Started:
- [Quick Start](https://cocoindex.io/docs/getting_started/quickstart)
