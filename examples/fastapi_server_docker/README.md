## Run docker container with a simple query endpoint via fastapi

In this example, we will build index for text embedding from local markdown files, and provide a simple query endpoint via fastapi.
We provide a simple docker container using docker compose to build pgvector17 along with a simple python fastapi script

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.


## Run locally without docker

In the `.env` file, use local Postgres URL

```
# For local testing
COCOINDEX_DATABASE_URL=postgres://cocoindex:cocoindex@localhost/cocoindex
```

- Install dependencies:

    ```bash
    pip install -e .
    ```
    **Note**: This example uses SentenceTransformerEmbed which requires the sentence-transformers library. The dependency is automatically included when you install from requirements.txt, but if you encounter import errors, you can install it directly:

    ```bash
    pip install sentence-transformers
    ```

- Setup:

    ```bash
    cocoindex setup main.py
    ```

- Update index:

    ```bash
    cocoindex update main.py
    ```

- Run:

    ```bash
    uvicorn main:fastapi_app --reload --host 0.0.0.0 --port 8000
    ```

 ## Query the endpoint

    ```bash
    curl "http://localhost:8000/search?q=model&limit=3"
    ```


## Run Docker

In the `.env` file, use Docker Postgres URL

```
COCOINDEX_DATABASE_URL=postgres://cocoindex:cocoindex@coco_db:5436/cocoindex
```

Build the docker container via:
```bash
docker compose up --build
```
**Note**: The Docker build automatically includes all required dependencies, including sentence-transformers for embedding functionality.

The Docker section note is optional since Docker handles dependencies automatically, but it's good to mention that the sentence-transformers dependency is included in the build.

The main change is in the local installation section where users might run into dependency issues if they're not using the full requirements.txt file.

Test the endpoint:
```bash
curl "http://0.0.0.0:8080/search?q=model&limit=3"
```
