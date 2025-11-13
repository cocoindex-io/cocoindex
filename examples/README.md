# Examples

This folder contains example CocoIndex projects, designed to help you learn CocoIndex features and inspire you to build powerful indexing solutions.

Check out our [examples documentation](https://cocoindex.io/docs/examples) for more details.

## Vector Search & Embedding

- 📄 [**text_embedding**](./text_embedding) - Build text embedding index from local markdown files and perform semantic search
- 📄 [**text_embedding_lancedb**](./text_embedding_lancedb) - Build text embedding index with LanceDB as vector database
- 📄 [**text_embedding_qdrant**](./text_embedding_qdrant) - Build text embedding index with Qdrant as vector database
- 📄 [**pdf_embedding**](./pdf_embedding) - Build embedding index from PDF files and query with natural language
- 🖼️ [**image_search**](./image_search) - Build live image search using multimodal embedding models
- 🖼️ [**pdf_elements_embedding**](./pdf_elements_embedding) - Extract text and images from PDFs and build multimodal search
- 🖼️ [**multi_format_indexing**](./multi_format_indexing) - Build visual document index from PDFs and images with ColPali
- 👤 [**face_recognition**](./face_recognition) - Recognize faces in images and build embedding index

## Cloud Storage Sources

- ☁️ [**amazon_s3_embedding**](./amazon_s3_embedding) - Build embedding index from Amazon S3 bucket with continuous sync
- ☁️ [**azure_blob_embedding**](./azure_blob_embedding) - Build embedding index from Azure Blob Storage with continuous sync
- ☁️ [**gdrive_text_embedding**](./gdrive_text_embedding) - Build embedding index from Google Drive files with real-time sync

## Code & Documentation

- 💻 [**code_embedding**](./code_embedding) - Build real-time index for codebase using Tree-sitter for syntax-aware chunking
- 📚 [**docs_to_knowledge_graph**](./docs_to_knowledge_graph) - Build real-time knowledge graph from documents using LLM to extract relationships

## Structured Data Extraction

- 🏥 [**patient_intake_extraction**](./patient_intake_extraction) - Extract structured data from patient intake forms (PDF, Docx) using LLM
- 🏥 [**patient_intake_extraction_baml**](./patient_intake_extraction_baml) - Extract structured data from patient intake PDFs using BAML
- 📖 [**manuals_llm_extraction**](./manuals_llm_extraction) - Extract structured information from PDF manuals using Ollama
- 📄 [**paper_metadata**](./paper_metadata) - Extract metadata (title, authors, abstract) from research papers in PDF

## Custom Sources & Targets

- 🌐 [**custom_source_hn**](./custom_source_hn) - Custom source example: index HackerNews content via API
- 🌐 [**hn_trending_topics**](./hn_trending_topics) - Extract trending topics from HackerNews using LLM
- 📝 [**custom_output_files**](./custom_output_files) - Export markdown files to local HTML with custom targets

## Database Integration

- 🗄️ [**postgres_source**](./postgres_source) - Use Postgres tables as source for CocoIndex flows

## Production & Deployment

- 🐳 [**fastapi_server_docker**](./fastapi_server_docker) - Run docker container with FastAPI query endpoint
- 🔄 [**live_updates**](./live_updates) - Demonstrates live update feature to keep index synchronized with local directory

## Recommendation Systems

- 🛍️ [**product_recommendation**](./product_recommendation) - Build real-time recommendation engine with LLM and graph database

> [!NOTE]
> New to CocoIndex? Check out the [Getting Started](https://cocoindex.io/docs/getting_started) guide first!
We also welcome contributions! Submit a [pull request](https://github.com/cocoindex-io/cocoindex/pulls) to add more examples.
