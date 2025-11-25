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

    ```sh
    pip install -e .
    ```

- Update index:

    ```sh
    cocoindex update main
    ```

- Run:

    ```sh
    uvicorn main:fastapi_app --reload --host 0.0.0.0 --port 8000
    ```

## Query the endpoint

    ```sh
    curl "http://localhost:8000/search?q=model&limit=3"
    ```

## Run Docker

In the `.env` file, use Docker Postgres URL

```
COCOINDEX_DATABASE_URL=postgres://cocoindex:cocoindex@coco_db:5436/cocoindex
```

Build the docker container via:

```sh
docker compose up --build
```

Test the endpoint:

```sh
curl "http://0.0.0.0:8080/search?q=model&limit=3"
```
