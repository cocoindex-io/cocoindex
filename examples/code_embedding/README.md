# Build embedding index for codebase

![Build embedding index for codebase](https://cocoindex.io/blogs/assets/images/cover-9bf0a7cff69b66a40918ab2fc1cea0c7.png)

In this example, we will build an embedding index for a codebase using CocoIndex. CocoIndex provides built-in support for code base chunking, with native Tree-sitter support. [Tree-sitter](https://en.wikipedia.org/wiki/Tree-sitter_%28parser_generator%29) is a parser generator tool and an incremental parsing library, it is available in Rust 🦀 - [GitHub](https://github.com/tree-sitter/tree-sitter). CocoIndex has built-in Rust integration with Tree-sitter to efficiently parse code and extract syntax trees for various programming languages.


Please give [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Tutorials
- Blog with step by step tutorial [here](https://cocoindex.io/blogs/index-code-base-for-rag).
- Video walkthrough [here](https://youtu.be/G3WstvhHO24?si=Bnxu67Ax5Lv8b-J2)


## Prerequisite
[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

Install dependencies:
```bash
pip install -e .
```

Setup:

```bash
python main.py cocoindex setup
```

Update index:

```bash
python main.py cocoindex update
```

Run:

```bash
python main.py
```

## CocoInsight
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
python main.py cocoindex server -ci
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
