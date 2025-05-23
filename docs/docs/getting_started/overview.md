---
title: Overview
slug: /
---

# Welcome to CocoIndex

CocoIndex is a ultra performant real-time data transformation framework for AI, with incremental processing. 

As a data framework, CocoIndex takes it to the next level on data freshness. **Incremental processing** is one of the core values provided by CocoIndex.

<p align="center">
    <img src="https://github.com/user-attachments/assets/f4eb29b3-84ee-4fa0-a1e2-80eedeeabde6" alt="Incremental Processing" width="700">
</p>


## Programming Model
CocoIndex follows the idea of [Dataflow programming](https://en.wikipedia.org/wiki/Dataflow_programming) model. Each transformation creates a new field solely based on input fields, without hidden states and value mutation. All data before/after each transformation is observable, with lineage out of the box.

The gist of an example data transformation:
```
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


An example dataflow diagram:
<p align="center">
<img width="700" alt="DataFlow" src="https://github.com/user-attachments/assets/22069379-99b1-478b-a131-15e2a9539d35" />
</p>


Get Started:
- [Quick Start](https://cocoindex.io/docs/getting_started/quickstart)

