Simple example for cocoindex: build embedding index based on Google Drive files.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Google Drive:

    -   Setup a service account in Google Cloud, and download the credential file.
    -   Share folders containing files you want to import with the service account's email address.

    See [Setup for Google Drive](https://cocoindex.io/docs/ops/sources#setup-for-google-drive) for more details.

3.  Create `.env` file with your credential file and folder IDs.
    Starting from copying the `.env.example`, and then edit it to fill in your credential file path and folder IDs.

    ```bash
    cp .env.exmaple .env
    $EDITOR .env
    ```

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
python main.py cocoindex server -c https://cocoindex.io
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).