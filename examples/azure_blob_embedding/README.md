This example builds an embedding index based on files stored in an Azure Blob Storage container.
It continuously updates the index as files are added / updated / deleted in the source container:
it keeps the index in sync with the Azure Blob Storage container effortlessly.

## Quick Start (Public Test Container)

🚀 **Try it immediately!** We provide a public test container with sample documents:
- **Account:** `testnamecocoindex1`
- **Container:** `testpublic1` (public access)
- **No authentication required!**

Just copy `.env.example` to `.env` and run - it works out of the box with anonymous access.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Azure Blob Storage.
    You'll need an Azure Storage account and container. Supported authentication methods:
    - **Connection String** (recommended for development)
    - **SAS Token** (recommended for production)
    - **Account Key** (full access)
    - **Anonymous access** (for public containers only)

3.  Create a `.env` file with your Azure Blob Storage configuration.
    Start from copying the `.env.example`, and then edit it to fill in your credentials.

    ```bash
    cp .env.example .env
    $EDITOR .env
    ```

    Example `.env` file with connection string:
    ```
    # Database Configuration
    DATABASE_URL=postgresql://localhost:5432/cocoindex

    # Azure Blob Storage Configuration
    AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
    AZURE_BLOB_CONTAINER_NAME=mydocuments
    AZURE_BLOB_PREFIX=

    # Authentication (choose one)
    AZURE_BLOB_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey123;EndpointSuffix=core.windows.net
    ```

## Run

Install dependencies:

```sh
pip install -e .
```

Run:

```sh
python main.py
```

During running, it will keep observing changes in the Azure Blob Storage container and update the index automatically.
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

## Authentication Methods & Troubleshooting

### Connection String (Recommended for Development)
```bash
AZURE_BLOB_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=testnamecocoindex1;AccountKey=your-key;EndpointSuffix=core.windows.net"
```
- **Pros:** Easiest to set up, contains all necessary information
- **Cons:** Contains account key (full access)
- **⚠️ Important:** Use **Account Key** connection string, NOT SAS connection string!

### SAS Token (Recommended for Production)
```bash
AZURE_BLOB_SAS_TOKEN="sp=r&st=2024-01-01T00:00:00Z&se=2025-12-31T23:59:59Z&spr=https&sv=2022-11-02&sr=c&sig=..."
```
- **Pros:** Fine-grained permissions, time-limited
- **Cons:** More complex to generate and manage

**SAS Token Requirements:**
- `sp=r` - Read permission (required)
- `sp=rl` - Read + List permissions (recommended)
- `sr=c` - Container scope (to access all blobs)
- Valid time range (`st` and `se` in UTC)

### Account Key
```bash
AZURE_BLOB_ACCOUNT_KEY="your-account-key-here"
```
- **Pros:** Simple to use
- **Cons:** Full account access, security risk

### Anonymous Access
Leave all authentication options empty - only works with public containers.

## Common Issues

### 401 Authentication Error
```
Error: server returned error status which will not be retried: 401
Error Code: NoAuthenticationInformation
```

**Solutions:**
1. **Check authentication priority:** Connection String > SAS Token > Account Key > Anonymous
2. **Verify SAS token permissions:** Must include `r` (read) and `l` (list) permissions
3. **Check SAS token expiry:** Ensure `se` (expiry time) is in the future
4. **Verify container scope:** Use `sr=c` for container-level access

### Connection String Issues

**⚠️ CRITICAL: Use Account Key Connection String, NOT SAS Connection String!**

**✅ Correct (Account Key Connection String):**
```
DefaultEndpointsProtocol=https;AccountName=testnamecocoindex1;AccountKey=your-key;EndpointSuffix=core.windows.net
```

**❌ Wrong (SAS Connection String - will not work):**
```
BlobEndpoint=https://testnamecocoindex1.blob.core.windows.net/;SharedAccessSignature=sp=r&st=...
```

**Other tips:**
- Don't include quotes in the actual connection string value
- Account name in connection string should match `AZURE_STORAGE_ACCOUNT_NAME`
- Connection string must contain `AccountKey=` parameter

### Container Access Issues
- Verify container exists and account has access
- Check `AZURE_BLOB_CONTAINER_NAME` spelling
- For anonymous access, container must be public
