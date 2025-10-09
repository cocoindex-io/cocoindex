# Structured Data Extraction from PDF with AWS Bedrock and CocoIndex

In this example, we

*   Converts PDFs (generated from a few Python docs) into Markdown.
*   Extract structured information from the Markdown using an AWS Bedrock LLM.
*   Use a custom function to further extract information from the structured output.

Please give [Cocoindex on Github](https://github.com/cocoindex-io/cocoindex) a star to support us if you like our work. Thank you so much with a warm coconut hug 🥥🤗. [![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## Prerequisite

Before running the example, you need to:

*   [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.
*   Configure your AWS Bedrock credentials. In this example we use AWS Bedrock. You need to get it ready by following [this guide](https://docs.aws.amazon.com/bedrock/latest/userguide/api-keys.html) to create an API key. Alternatively, you can also follow the comments in source code to switch to other LLMs.

First, copy the example environment file:

```bash
cp .env.example .env
```

Then, open the `.env` file and fill in your AWS Bedrock credentials. The `.env` file is ignored by git, so your secrets will not be committed.

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

```
      filename       |         title          |      module_summary
---------------------+------------------------+--------------------------
 manuals/asyncio.pdf | "asyncio — Asynchronous" | {"num_classes": 0, "num_methods": 0}
 manuals/json.pdf    | "json — JSON encoder"  | {"num_classes": 0, "num_methods": 0}
(2 rows)
```

The output may vary depending on the model you are using. The important part is that the `module_info` and `module_summary` columns are populated with the extracted data.
