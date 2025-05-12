This example builds an embedding index based on files stored in an Amazon S3 bucket.
It continuously updates the index as files are added / updated / deleted in the source bucket:
it keeps the index in sync with the Amazon S3 bucket effortlessly.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Amazon S3:

    -   **Create an Amazon S3 bucket:**
        - Go to the [AWS S3 Console](https://s3.console.aws.amazon.com/s3/home) and click **Create bucket**. Give it a unique name and choose a region.
        - Or, use the AWS CLI:
          ```sh
          aws s3 mb s3://your-s3-bucket-name
          ```

    -   **Upload your files to the bucket:**
        - In the AWS Console, click your bucket, then click **Upload** and add your `.md`, `.txt`, `.docx`, or other files.
        - Or, use the AWS CLI:
          ```sh
          aws s3 cp localfile.txt s3://your-s3-bucket-name/
          aws s3 cp your-folder/ s3://your-s3-bucket-name/ --recursive
          ```

    -   **Set up AWS credentials:**
        - The easiest way is to run:
          ```sh
          aws configure
          ```
          Enter your AWS Access Key ID, Secret Access Key, region (e.g., `us-east-1`), and output format (`json`).
        - This creates a credentials file at `~/.aws/credentials` and config at `~/.aws/config`.
        - Alternatively, you can set environment variables:
          ```sh
          export AWS_ACCESS_KEY_ID=your-access-key-id
          export AWS_SECRET_ACCESS_KEY=your-secret-access-key
          export AWS_DEFAULT_REGION=us-east-1
          ```
        - If running on AWS EC2 or Lambda, you can use an [IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) with S3 read permissions.

    -   **(Optional) Specify a prefix** to restrict to a subfolder in the bucket by setting `AMAZON_S3_PREFIX` in your `.env`.

    See [AWS S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) for more details.

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
    AMAZON_S3_PREFIX=optional/prefix/path
    ```

## Run

Install dependencies:

```sh
uv pip install -r requirements.txt
```

Setup:

```sh
uv run main.py cocoindex setup
```

Run:

```sh
uv run main.py
```

During running, it will keep observing changes in the Amazon S3 bucket and update the index automatically.
At the same time, it accepts queries from the terminal, and performs search on top of the up-to-date index.


## CocoInsight 
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
uv run main.py cocoindex server -ci
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
uv run main.py cocoindex server -ci -L
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).