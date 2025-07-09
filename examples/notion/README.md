This example builds an embedding index based on Notion databases and pages.
It continuously updates the index as content is added / updated / deleted in Notion:
it keeps the index in sync with your Notion workspace effortlessly.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Notion integration.
    - Create a Notion integration at https://www.notion.so/my-integrations
    - Copy the integration token (starts with `secret_`)
    - Share your databases and pages with the integration

3.  Create a `.env` file with your Notion token and database/page IDs.
    Start from copying the `.env.example`, and then edit it to fill in your configuration.

    ```bash
    cp .env.example .env
    $EDITOR .env
    ```

    Example `.env` file:
    ```
    # Database Configuration
    COCOINDEX_DATABASE_URL=postgresql://localhost:5432/cocoindex

    # Notion Configuration
    NOTION_TOKEN=secret_your_notion_integration_token_here
    NOTION_DATABASE_IDS=database_id_1,database_id_2
    NOTION_PAGE_IDS=page_id_1,page_id_2
    ```

    Note: You can specify either database IDs, page IDs, or both. The system will index all specified resources.

## Run

Install dependencies:

```sh
pip install -e .
```

Run:

```sh
python main.py
```

During running, it will keep observing changes in your Notion workspace and update the index automatically.
At the same time, it accepts queries from the terminal, and performs search on top of the up-to-date index.

## CocoInsight
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
cocoindex server -ci main.py
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
cocoindex server -ci -L main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
