---
title: Real-time data transformation from Google Drive
description: Build real-time data transformation from Google Drive with CocoIndex.
sidebar_class_name: hidden
slug: /examples/google_drive
canonicalUrl: '/examples/google_drive'
sidebar_custom_props:
  image: /img/integrations/google_drive/cover.png
  tags: [vector-index, google-drive, realtime, etl]
image: /img/integrations/google_drive/cover.png
---
import { DocumentationButton, GitHubButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/gdrive_text_embedding" margin="0 0 24px 0" />

![Text Embedding from Google Drive](/img/integrations/google_drive/cover.png)

This guide shows how to build a real-time data pipeline with CocoIndex to transform and index files from Google Drive. It walks through setting up Google credentials, configuring CocoIndex, and builds a vector index for semantic search.


## Prerequisites
### Install Postgres
If you don't have Postgres installed, please refer to the [installation guide](https://cocoindex.io/docs/getting_started/installation).

### Enable Google Drive access by service account
CocoIndex provides a native built-in integration to support Google Drive as a source.

<DocumentationButton url="https://cocoindex.io/docs/sources/googledrive" text="GoogleDrive Source" margin="0 0 16px 0" />

### 1. Register / login in Google Cloud.
First, you need to create a Google Cloud account if you don't have one already. Go to the [Google Cloud Console](https://console.cloud.google.com/) and sign up or sign in.

### 2. Select or create a GCP project

Once you've logged into Google Cloud Console, you need to select an existing project or create a new one. Click on the project selector dropdown at the top of the page:

![Select or Create a GCP Project](/img/integrations/google_drive/select_project.png)



### 3. Create a Service Account
1.  In Google Cloud Console, search for Service Accounts, to enter the IAM & Admin / Service Accounts page.
    ![Service Account Search](/img/integrations/google_drive/service_account_search.png)

2.  Click on "CREATE SERVICE ACCOUNT" at the top of the page:

    ![Create Service Account](/img/integrations/google_drive/create_service_account.png)

3.  Fill in the service account name, e.g. `cocoindex-test`.

    ![Create Service Account Form](/img/integrations/google_drive/create_service_account_form.png)

    And make a note on that email address, you will need it in the later step.

4.  Click on "CREATE" to create the service account.
    You will see the service account created successfully.
    ![Service Account Listing](/img/integrations/google_drive/service_account_listing.png)

### 4. Create and download the key for the service account
1.  Click on "Actions" and select "Manage Keys".
    ![Manage Keys](/img/integrations/google_drive/manage_keys.png)

2.  Select "Add Key" and select "Create new key".
    ![Create New Key](/img/integrations/google_drive/create_new_key.png)

    Choose "JSON" as the key type and click "Create".
    ![Create JSON Key](/img/integrations/google_drive/create_new_key_form.png)

3.  The key file will be downloaded to your computer. Depending on the browser settings, it starts downloading automatically or may pop up a dialog for the download location. Keep this file secure as it provides access to your Google Drive resources. It looks like this:
    ```json
    {
    "type": "service_account",
    "project_id": "cocoindexdriveexample",
    "private_key_id": "key_id",
    "private_key": "PRIVATE_KEY",
    "client_email": "cocoindex-test@cocoindexdriveexample.iam.gserviceaccount.com",
    "client_id": "id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/cocoindex-test%40cocoindexdriveexample.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }
    ```


### 5. Enable Google Drive API
Search for "Google Drive API" in Google Cloud Console and enable it.

### 6. Prepare and share a folder
1.  Create a new folder or use an existing folder in your Google Drive.
    -   For this project, we will create a folder in my own Google Drive, and share it with the service account email address we created in [Step 3](#3-create-a-service-account). For example, `cocoindex-test@cocoindexdriveexample.iam.gserviceaccount.com`.
    -   My example Google Drive folder is [here](https://drive.google.com/drive/folders/1Yerp-CTs1TQUH52oy7eRqR1WHzRYhtJW?dmr=1&ec=wgc-drive-globalnav-goto).
    -   The files are also available in the [example repo](https://github.com/cocoindex-io/cocoindex/tree/main/examples/gdrive_text_embedding/data).
2.  Share the folder with the service account. Enter the service account email address (e.g., `cocoindex-test@cocoindexdriveexample.iam.gserviceaccount.com`) and give it "Viewer" access.

    ![Create a new folder in Google Drive](/img/integrations/google_drive/drive_folder.png)

3.  Note the folder ID from the URL when you open the folder. The URL will look like:
    ```
    https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOpQrStUvWxYz
    ```

    The folder ID is the part after `folders/` (in this example: `1AbCdEfGhIjKlMnOpQrStUvWxYz`).
    You'll need this folder ID when connecting to the Google Drive API.


## Project setup

1.  Create a `pyproject.toml` file in the root directory.

    ```toml
    [project]
    name = "gdrive-text-embedding"
    version = "0.1.0"
    description = "Simple example for cocoindex: build embedding index based on Google Drive files."
    requires-python = ">=3.11"
    dependencies = ["cocoindex>=0.2.4", "python-dotenv>=1.0.1"]
    ```

2.  Set up `.env`
    Create a `.env` file in the root directory and add the following:
    You can copy it from the [`.env.example`](https://github.com/cocoindex-io/cocoindex/blob/main/examples/gdrive_text_embedding/.env.example) file.

    ```
    # Postgres database address for cocoindex
    COCOINDEX_DATABASE_URL=postgres://cocoindex:cocoindex@localhost/cocoindex

    # Google Drive service account credential path.
    #! PLEASE FILL IN
    GOOGLE_SERVICE_ACCOUNT_CREDENTIAL=/path/to/service_account_credential.json

    # Google Drive root folder IDs, comma separated.
    #! PLEASE FILL IN YOUR GOOGLE DRIVE FOLDER ID
    GOOGLE_DRIVE_ROOT_FOLDER_IDS=1AbCdEfGhIjKlMnOpQrStUvWxYz
    ```

## Define CocoIndex Flow

Let's define the CocoIndex flow to build text embeddings from Google Drive.

First, let's load the files from Google Drive as a source. CocoIndex provides a `GoogleDrive` source as a native built-in [source](https://cocoindex.io/docs/sources). You just need to provide the service account credential path and the root folder IDs.

<DocumentationButton url="https://cocoindex.io/docs/sources/googledrive" text="GoogleDrive Source" margin="0 0 16px 0" />

### 1. Load the files from Google Drive
```python
@cocoindex.flow_def(name="GoogleDriveTextEmbedding")
def gdrive_text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds text into a vector database.
    """
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.GoogleDrive(
            service_account_credential_path=credential_path,
            root_folder_ids=root_folder_ids))

    doc_embeddings = data_scope.add_collector()
```

`flow_builder.add_source` will create a table with the following sub fields, see [documentation](https://cocoindex.io/docs/sources) here.
- `filename` (key, type: `str`): the filename of the file, e.g. `dir1/file1.md`
- `content` (type: `str` if `binary` is `False`, otherwise `bytes`): the content of the file


### Rest of the flow
For the rest of the flow, we can follow the tutorial
[Simple Vector Index](https://cocoindex.io/docs/examples/simple_vector_index).
The entire project is available [here](https://github.com/cocoindex-io/cocoindex/tree/main/examples/gdrive_text_embedding).


### Query and test your index
🎉 Now you are all set!

#### Run the following command to setup and update the index.
    ```sh
    cocoindex update --setup main
    ```

    You'll see the index updates state in the terminal. For example, you'll see the following output:
    ```sh
    documents: 3 added, 0 removed, 0 updated
    ```

#### CocoInsight

    CocoInsight is a comprehensive web interface to understand your data pipeline and interact with the index. CocoInsight has zero data retention with your pipeline data.

    ```sh
    cocoindex server -ci main
    ```
