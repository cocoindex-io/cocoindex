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

Test the endpoint:
```bash
curl "http://0.0.0.0:8080/search?q=model&limit=3"
```
