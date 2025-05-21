## Run docker container with a simple query endpoint via fastapi

In this example, we will build index for text embedding from local markdown files, and provide a simple query endpoint via fastapi.
We provide a simple docker container using docker compose to build pgvector17 along with a simple python fastapi script

## Run locally without docker
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
 
 ## Query the endpoint

    ```bash
    curl "http://localhost:8000/search?q=model&limit=3"
    ```


## Run Docker
Build the docker container via: 
```docker compose up```
