import cocoindex
import uvicorn

from fastapi import FastAPI
from dotenv import load_dotenv

from src.cocoindex_funs import code_embedding_flow, code_to_embedding

fastapi_app = FastAPI()
    
query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
    name="SemanticsSearch",
    flow=code_embedding_flow,
    target_name="code_embeddings",
    query_transform_flow=code_to_embedding,
    default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY
)

@fastapi_app.get("/query")
def query_endpoint(string: str):
    results, _ = query_handler.search(string, 10)
    return results

@cocoindex.main_fn()
def _run():
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8080)
    
if __name__ == "__main__":
    load_dotenv(override=True)
    _run()
