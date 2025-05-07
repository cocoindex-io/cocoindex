## Run cocoindex docker container with a simple query endpoint via fastapi
In this example, we provide a simple docker container using docker compose to build pgvector17 along with a simple python fastapi script than runs a simple query endpoint. This example uses the code from the code embedding example.

## How to run
Edit the sample code directory to include the code you want to query over in
```sample_code/``` 

Edit the configuration code from the file ```src/cocoindex_funs.py``` line 23 to 25.

Finally build the docker container via: ```docker compose up``` while inside the directory of the example.
