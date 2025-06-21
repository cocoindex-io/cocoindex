# Structured Data Extraction from PDF with Ollama and CocoIndex

![Structured data extraction with Ollama and CocoIndex](https://cocoindex.io/blogs/assets/images/cocoindex-ollama-structured-extraction-from-pdf-6ee15b1e0fe304063dc78f04153fb385.png)


In this example, we

*   Converts PDFs (generated from a few Python docs) into Markdown.
*   Extract structured information from the Markdown using LLM.
*   Use a custom function to further extract information from the structured output.

Please give [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite

Before running the example, you need to:

*   [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
*   Install / configure LLM API. In this example we use Ollama, which runs LLM model locally. You need to get it ready following [this guide](https://cocoindex.io/docs/ai/llm#ollama). Alternatively, you can also follow the comments in source code to switch to OpenAI, and [configure OpenAI API key](https://cocoindex.io/docs/ai/llm#openai) before running the example.

## Run


### Build the index

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
cocoindex setup main.py
```

Update index:

```bash
cocoindex update main.py
```

### Query the index

After index is build, you have a table with name `modules_info`. You can query it any time, e.g. start a Postgres shell:

```bash
psql postgres://cocoindex:cocoindex@localhost/cocoindex
```

And run the SQL query:

```sql
SELECT filename, module_info->'title' AS title, module_summary FROM modules_info;
```
You should see results like:

![Module Info Index](https://cocoindex.io/blogs/assets/images/module_info_index-ffaec6042ec3a18eaf94bed5b227a085.png)


## CocoInsight
CocoInsight is a tool to help you understand your data pipeline and data index. CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
cocoindex server -ci main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight). It connects to your local CocoIndex server with zero data retention.

You can view the pipeline flow and the data preview in the CocoInsight UI:
![CocoInsight UI](https://cocoindex.io/blogs/assets/images/cocoinsight-edd71690dcc35b6c5cf1cb31b51b6f6f.png)
