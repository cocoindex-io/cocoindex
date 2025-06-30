This example builds an embedding index based on files stored in an Amazon S3 bucket.
It continuously updates the index as files are added / updated / deleted in the source bucket:
it keeps the index in sync with the Amazon S3 bucket effortlessly.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Amazon S3.
    See [Setup for AWS S3](https://cocoindex.io/docs/ops/sources#setup-for-amazon-s3) for more details.

3.  Create a `.env` file with your Amazon S3 bucket name and (optionally) prefix.
    Start from copying the `.env.example`, and then edit it to fill in your bucket name and prefix.

    ```bash
    cp .env.example .env
    $EDITOR .env
    ```

    Example `.env` file:
    ```
    # Database Configuration
    DATABASE_URL=postgresql://localhost:5432/cocoindex

    # Amazon S3 Configuration
    AMAZON_S3_BUCKET_NAME=your-bucket-name
    AMAZON_S3-SQS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/123456789/S3ChangeNotifications
    ```

## Run

Install dependencies:

```sh
pip install -e .
```

Setup:

```sh
cocoindex setup main.py
```

Run:

```sh
python main.py
```

During running, it will keep observing changes in the Amazon S3 bucket and update the index automatically.
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
