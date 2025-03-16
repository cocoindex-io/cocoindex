In this example, we

*   Converts PDFs (generated from a few Python docs) into Markdown.
*   Extract structured information from the Markdown using LLM.
*   Use a custom function to further extract information from the structured output.

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
python main.py cocoindex setup
```

Update index:

```bash
python main.py cocoindex update
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

## CocoInsight 
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```
python main.py cocoindex server -c https://cocoindex.io
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).