This example builds embedding index based on Google Drive files.
It continuously updates the index as files are added / updated / deleted in the source folders:
it keeps the index in sync with the source folders effortlessly.

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

```sh
pip install -e .
```

Setup:

```sh
python main.py cocoindex setup
```

Run:

```sh
python main.py
```

During running, it will keep observing changes in the source folders and update the index automatically.
At the same time, it accepts queries from the terminal, and performs search on top of the up-to-date index.


## CocoInsight 
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
python main.py cocoindex server -ci
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
python main.py cocoindex server -ci -L
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).