// Metadata for the /docs/examples listing and per-example pages.
//
// Card metadata for the /docs/examples listing. Markdown walkthrough bodies live
// in src/content/example-posts/<slug>.md and are rendered by
// src/pages/examples/[slug].astro beneath the shared hero. Titles may use
// *asterisks* to mark the italic-coral accent — see consts.titleMarkup.
//
// This file grows as new walkthroughs land in src/content/example-posts.

export type Category = 'search' | 'ingest' | 'llm' | 'agents' | 'image';

export const CATEGORY_META: Record<Category, { label: string; lead: string; thumbClass: string }> = {
  search: { label: 'Semantic search', lead: 'Embed your documents, store the vectors, and answer by meaning.', thumbClass: 'search' },
  ingest: { label: 'Ingest and transform', lead: 'Move and reshape data with the same declarative flow: your own source, target, or parser.', thumbClass: 'ingest' },
  llm: { label: 'Structured extraction', lead: 'Extract typed, structured data from code and documents with an LLM, through instructor, BAML, or DSPy.', thumbClass: 'llm' },
  agents: { label: 'Knowledge graphs', lead: 'Give agents a persistent, graph-shaped memory built from conversations, meetings, and products.', thumbClass: 'agents' },
  image: { label: 'Multimodal search', lead: 'Search images, PDFs, slides, and faces by meaning: the same vector index with a different encoder.', thumbClass: 'pink' },
};

export type ExampleCard = {
  slug: string;                      // becomes /docs/examples/<slug>
  title: string;                     // asterisks → italic-coral
  index: string;                     // e.g. '01 / 02' — shown top-right of thumb
  category: Category;
  thumbClass?: string;
  thumbLabel: string;
  description: string;
  tags: Array<{ kind: 'src' | 'tgt' | 'llm' | 'ops' | 'lvl'; label: string }>;
  footMeta: string;
  sourceSlug?: string;               // override GitHub path when the listing slug differs from the repo dir
  featured?: boolean;
};


export const examples: ExampleCard[] = [
  {
    slug: 'text-embedding',
    title: 'Semantic search 101',
    index: '01 / 32',
    category: 'search',
    thumbLabel: 'Markdown · embeddings',
    description: 'Chunk Markdown files, embed each chunk, store the vectors in Postgres, and search them in plain English. The simplest end-to-end vector index, and the one the other search examples build on.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~6 min',
    sourceSlug: 'text_embedding',
  },
  {
    slug: 'index-codebase',
    title: 'Index your codebase',
    index: '02 / 32',
    category: 'search',
    thumbLabel: 'Code · Tree-sitter',
    description: 'Walk a repo, split by syntax with Tree-sitter, embed, and query your codebase in English. A live vector index for AI coding agents, in ~100 lines.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'Tree-sitter' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~10 min',
    sourceSlug: 'code_embedding',
  },
  {
    slug: 'multi-codebase-summarization',
    title: 'Multi-codebase summarization',
    index: '03 / 32',
    category: 'llm',
    thumbLabel: 'Code · structured output',
    description: 'Walk Python repos, extract typed summaries of classes, functions, and Mermaid call graphs with an LLM, and update a wiki page per project as code changes.',
    tags: [
      { kind: 'src', label: 'Multi-repo' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'ops', label: 'Structured output' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~35 min',
    sourceSlug: 'multi_codebase_summarization',
    featured: true,
  },
  {
    slug: 'pdf-to-markdown',
    title: 'PDF to Markdown',
    index: '04 / 32',
    category: 'ingest',
    thumbLabel: 'PDF · custom blocks',
    description: 'Convert a folder of PDFs to Markdown incrementally, with custom building blocks: only new or changed PDFs are converted again.',
    tags: [
      { kind: 'src', label: 'PDF' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'ops', label: 'Custom blocks' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~18 min',
    sourceSlug: 'pdf_to_markdown',
  },
  {
    slug: 'podcast-to-knowledge-graph',
    title: 'Podcasts to a knowledge graph',
    index: '05 / 32',
    category: 'agents',
    thumbLabel: 'YouTube · LLM + graph',
    description: 'Turn YouTube podcasts into a queryable knowledge graph — diarized transcription, two-step LLM extraction, embedding-based entity resolution, and a SurrealDB graph.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'YouTube' },
      { kind: 'llm', label: 'OpenAI' },
      { kind: 'ops', label: 'Entity resolution' },
      { kind: 'tgt', label: 'SurrealDB' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~40 min',
    sourceSlug: 'conversation_to_knowledge',
  },
  {
    slug: 'docs-to-knowledge-graph',
    title: 'Docs to a knowledge graph',
    index: '06 / 32',
    category: 'agents',
    thumbLabel: 'Markdown · LLM + Neo4j',
    description: 'Turn a folder of Markdown docs into a Neo4j concept graph — LLM-extracted (subject, predicate, object) triples that stay in sync as the docs change.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'tgt', label: 'Neo4j' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'docs_to_knowledge_graph',
  },
  {
    slug: 'meeting-notes-to-knowledge-graph',
    title: 'Meeting notes to a knowledge graph',
    index: '07 / 32',
    category: 'agents',
    thumbLabel: 'Google Drive · LLM + Neo4j',
    description: 'Turn Google Drive meeting notes into a Neo4j knowledge graph — LLM extraction of organizers, attendees, and tasks, plus embedding-based person entity resolution, kept in sync as notes change.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Google Drive' },
      { kind: 'llm', label: 'Any LLM' },
      { kind: 'ops', label: 'Entity resolution' },
      { kind: 'tgt', label: 'Neo4j' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~25 min',
    sourceSlug: 'meeting_notes_graph_neo4j',
  },
  {
    slug: 'csv-to-kafka',
    title: 'CSV to Kafka',
    index: '08 / 32',
    category: 'ingest',
    thumbLabel: 'CSV · live Kafka target',
    description: 'Watch a folder of CSV files and publish each row as a JSON message to a Kafka topic — declarative target states, only-changed-rows produces, and live mode in ~60 lines.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Kafka' },
      { kind: 'ops', label: 'Live mode' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~12 min',
    sourceSlug: 'csv_to_kafka',
  },
  {
    slug: 'pdf-embedding',
    title: 'Semantic search over PDFs',
    index: '09 / 32',
    category: 'search',
    thumbLabel: 'PDF · docling + embeddings',
    description: 'Convert local PDFs to Markdown with docling on a GPU runner, chunk, embed, and store the vectors in Postgres — then query in natural language. A vector index over your documents.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'PDF' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'docling + embed' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: '~12 min',
    sourceSlug: 'pdf_embedding',
  },
  {
    slug: 'image-search',
    title: 'Search images by text',
    index: '10 / 32',
    category: 'image',
    thumbLabel: 'Images · CLIP + Qdrant',
    description: 'Embed images with CLIP, store the vectors in Qdrant, and search your photos in natural language through a FastAPI + React app — live updates, no tags or captions.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'CLIP' },
      { kind: 'tgt', label: 'Qdrant' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~15 min',
    sourceSlug: 'image_search',
  },
  {
    slug: 'audio-to-text',
    title: 'Audio to text',
    index: '11 / 32',
    category: 'ingest',
    thumbLabel: "Audio \u00b7 LiteLLM",
    description: "Transcribe local audio files with a LiteLLM speech-to-text model and store one transcript row per file in Postgres, keyed by filename.",
    tags: [
      { kind: 'src', label: "Audio files" },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'llm', label: "LiteLLM" },
      { kind: 'ops', label: "Transcription" },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~12 min",
    sourceSlug: 'audio_to_text',
  },
  {
    slug: 'hackernews-trending-topics',
    title: 'Trending topics from HackerNews',
    index: '12 / 32',
    category: 'llm',
    thumbLabel: "HN API \u00b7 LLM topics",
    description: "Scrape recent HackerNews threads and comments via the Algolia HN API, extract topics with an LLM, and rank what is trending in Postgres.",
    tags: [
      { kind: 'src', label: "HackerNews API" },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'llm', label: "LiteLLM" },
      { kind: 'ops', label: "Custom source" },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~10 min",
    sourceSlug: 'hn_trending_topics',
  },
  {
    slug: 'paper-metadata',
    title: 'Index academic papers',
    index: '13 / 32',
    category: 'llm',
    thumbLabel: "PDF \u00b7 LLM extract",
    description: "LLM-extract title, authors, and abstract from academic PDFs into typed rows, embed them, and store it all in Postgres with pgvector.",
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: "PDF" },
      { kind: 'llm', label: 'OpenAI' },
      { kind: 'tgt', label: "Postgres" },
      { kind: 'ops', label: "pgvector" },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~12 min",
    sourceSlug: 'paper_metadata',
  },
  {
    slug: 'patient-intake-baml',
    title: 'Patient intake forms to typed JSON with BAML',
    index: '14 / 32',
    category: 'llm',
    thumbLabel: "PDF \u00b7 BAML",
    description: "Extract schema-validated patient records from intake-form PDFs with one type-safe BAML Gemini-vision call per file, writing a JSON file per form.",
    tags: [
      { kind: 'src', label: "PDF" },
      { kind: 'tgt', label: "JSON files" },
      { kind: 'llm', label: "Gemini" },
      { kind: 'ops', label: "BAML" },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~10 min",
    sourceSlug: 'patient_intake_extraction_baml',
  },
  {
    slug: 'patient-intake-dspy',
    title: 'Patient intake forms to typed JSON with DSPy',
    index: '15 / 32',
    category: 'llm',
    thumbLabel: "PDF \u00b7 DSPy",
    description: "Render patient intake PDFs to images and extract a typed Patient with a DSPy ChainOfThought vision module on Gemini, writing one validated JSON file per form.",
    tags: [
      { kind: 'src', label: "PDF" },
      { kind: 'tgt', label: 'JSON files' },
      { kind: 'llm', label: "DSPy" },
      { kind: 'llm', label: "Gemini" },
      { kind: 'ops', label: 'Structured extraction' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~10 min",
    sourceSlug: 'patient_intake_extraction_dspy',
  },
  {
    slug: 'postgres-source',
    title: 'Postgres as a source',
    index: '16 / 32',
    category: 'ingest',
    thumbLabel: "Postgres \u00b7 pgvector",
    description: "Read rows from an existing Postgres table, derive fields, embed each row, and write the vectors back to Postgres with pgvector.",
    tags: [
      { kind: 'src', label: "Postgres" },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'ops', label: "Embeddings" },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~8 min",
    sourceSlug: 'postgres_source',
  },
  {
    slug: 'files-transform',
    title: 'Transform a folder of files',
    index: '17 / 32',
    category: 'ingest',
    thumbLabel: "Markdown \u00b7 markdown-it",
    description: "The smallest end-to-end CocoIndex pipeline \u2014 watch a folder of Markdown, render each file to HTML, and write the outputs to a local folder incrementally.",
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: 'Local FS' },
      { kind: 'ops', label: "markdown-it-py" },
      { kind: 'ops', label: "Custom transform" },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~6 min",
    sourceSlug: 'files_transform',
  },
  {
    slug: 'kafka-to-lancedb',
    title: 'Consume Kafka into LanceDB',
    index: '18 / 32',
    category: 'ingest',
    thumbLabel: "Kafka \u00b7 dispatch",
    description: "Consume JSON messages off a Kafka topic and dispatch each one \u2014 by its shape \u2014 into the matching LanceDB table.",
    tags: [
      { kind: 'src', label: "Kafka" },
      { kind: 'tgt', label: "LanceDB" },
      { kind: 'ops', label: 'Live mode' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~10 min",
    sourceSlug: 'kafka_to_lancedb',
  },
  {
    slug: 'entire-session-search',
    title: 'Search your AI coding sessions',
    index: '19 / 32',
    category: 'search',
    thumbLabel: "Entire \u00b7 Embeddings",
    description: "Index AI coding sessions captured by Entire \u2014 transcripts, prompts, and context summaries \u2014 into Postgres for natural-language semantic search.",
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: "Entire sessions" },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'ops', label: 'Incremental' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~10 min",
    sourceSlug: 'entire_session_search',
  },
  {
    slug: 'image-search-colpali',
    title: 'Image search with ColPali',
    index: '20 / 32',
    category: 'image',
    thumbLabel: "Images \u00b7 ColPali",
    description: "Embed images and queries into multi-vector ColPali bags of patch vectors, store them in Qdrant, and rank with late-interaction MaxSim through a FastAPI app.",
    tags: [
      { kind: 'src', label: "Images" },
      { kind: 'llm', label: "ColPali" },
      { kind: 'tgt', label: 'Qdrant' },
      { kind: 'ops', label: "FastAPI" },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: "~12 min",
    sourceSlug: 'image_search_colpali',
  },
  {
    slug: 'text-embedding-qdrant',
    title: 'Semantic search with Qdrant',
    index: '21 / 32',
    category: 'search',
    thumbLabel: "Markdown \u00b7 Qdrant",
    description: "Upsert Markdown chunk vectors into a Qdrant collection. Chunking and embedding run locally; only the target changes from Semantic search 101.",
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: "Qdrant" },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~5 min",
    sourceSlug: 'text_embedding_qdrant',
  },
  {
    slug: 'text-embedding-lancedb',
    title: 'Semantic search with LanceDB',
    index: '22 / 32',
    category: 'search',
    thumbLabel: "Markdown \u00b7 LanceDB",
    description: "Store Markdown chunk vectors in LanceDB, an embedded, file-based store with no server to run. Only the target changes from Semantic search 101.",
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: "LanceDB" },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~5 min",
    sourceSlug: 'text_embedding_lancedb',
  },
  {
    slug: 'text-embedding-turbopuffer',
    title: 'Semantic search with Turbopuffer',
    index: '23 / 32',
    category: 'search',
    thumbLabel: "Markdown \u00b7 Turbopuffer",
    description: "Upsert Markdown chunk vectors into a Turbopuffer namespace, a serverless vector store. Only the target changes from Semantic search 101.",
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'tgt', label: "Turbopuffer" },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~7 min",
    sourceSlug: 'text_embedding_turbopuffer',
  },
  {
    slug: 'amazon-s3-embedding',
    title: 'Embed Markdown from Amazon S3',
    index: '24 / 32',
    category: 'search',
    thumbLabel: "S3 \u00b7 pgvector",
    description: "Read Markdown from an Amazon S3 bucket instead of a local folder, then chunk, embed, and store the vectors in Postgres.",
    tags: [
      { kind: 'src', label: "Amazon S3" },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~6 min",
    sourceSlug: 'amazon_s3_embedding',
  },
  {
    slug: 'google-drive-embedding',
    title: 'Semantic search over Google Drive',
    index: '25 / 32',
    category: 'search',
    thumbLabel: "Google Drive \u00b7 Embed",
    description: "Read documents from Google Drive, then chunk, embed, and store the vectors in Postgres with pgvector. Only the source changes from Semantic search 101.",
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: "Google Drive" },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'ops', label: "Embeddings" },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~6 min",
    sourceSlug: 'gdrive_text_embedding',
  },
  {
    slug: 'oci-object-storage-embedding',
    title: 'Embed OCI Object Storage',
    index: '26 / 32',
    category: 'search',
    thumbLabel: "OCI \u00b7 pgvector",
    description: "Chunk and embed Markdown objects from an Oracle Cloud (OCI) Object Storage bucket into Postgres/pgvector, with optional live updates via OCI Streaming.",
    tags: [
      { kind: 'ops', label: 'Embeddings' },
      { kind: 'src', label: "OCI Object Storage" },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'llm', label: "sentence-transformers" },
      { kind: 'ops', label: "OCI Streaming (live)" },
      { kind: 'lvl', label: 'Starter' },
    ],
    footMeta: "~7 min",
    sourceSlug: 'oci_object_storage_embedding',
  },
  {
    slug: 'face-recognition',
    title: 'Build your own face search',
    index: '27 / 32',
    category: 'image',
    thumbLabel: 'Photos · face embeddings',
    description: 'Detect every face in a folder of photos, embed each into a 128-d vector with face_recognition (dlib), and index them in Qdrant — then search your photos by face.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'Face detection' },
      { kind: 'tgt', label: 'Qdrant' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~15 min',
    sourceSlug: 'face_recognition',
  },
  {
    slug: 'product-recommendation',
    title: 'Product recommendation graph',
    index: '28 / 32',
    category: 'agents',
    thumbLabel: 'Products · taxonomy graph',
    description: 'LLM-extract what each product is and what pairs with it from product docs, then build a Neo4j graph for complementary-product recommendations.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'LLM extraction' },
      { kind: 'tgt', label: 'Neo4j' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'product_recommendation',
  },
  {
    slug: 'manuals-llm-extraction',
    title: 'Manuals to structured data',
    index: '29 / 32',
    category: 'llm',
    thumbLabel: 'PDF manuals · typed records',
    description: 'Convert PDF manuals to Markdown with docling, LLM-extract a typed module summary (classes, methods, arguments), and store one structured record per manual in Postgres.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'docling + LLM' },
      { kind: 'tgt', label: 'Postgres' },
      { kind: 'lvl', label: 'Intermediate' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'manuals_llm_extraction',
  },
  {
    slug: 'multi-format-indexing',
    title: 'Multi-format visual search',
    index: '30 / 32',
    category: 'image',
    thumbLabel: 'PDFs + images · ColPali',
    description: 'Index PDFs and images as page screenshots with the ColPali multi-vector model — no OCR, no chunking — into Qdrant with MaxSim, and search the visual content by text.',
    tags: [
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'ColPali' },
      { kind: 'tgt', label: 'Qdrant' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~20 min',
    sourceSlug: 'multi_format_indexing',
  },
  {
    slug: 'slides-to-speech',
    title: 'Slides to narrated search',
    index: '31 / 32',
    category: 'image',
    thumbLabel: 'Slides · vision + TTS',
    description: 'Render each slide, write speaker notes with a vision LLM, narrate them with Pocket TTS locally on the CPU, and embed the notes into LanceDB — search a deck by meaning and play back the narration.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'Vision LLM + TTS' },
      { kind: 'tgt', label: 'LanceDB' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~25 min',
    sourceSlug: 'slides_to_speech',
  },
  {
    slug: 'sec-edgar-analytics',
    title: 'SEC filing hybrid search',
    index: '32 / 32',
    category: 'search',
    thumbLabel: 'Filings · vector + full-text',
    description: 'Scrub, chunk, embed, and tag multi-format SEC filings into Apache Doris with both a vector and a full-text index — then retrieve with hybrid (semantic + keyword) RRF search.',
    tags: [
      { kind: 'llm', label: 'sentence-transformers' },
      { kind: 'src', label: 'Local FS' },
      { kind: 'ops', label: 'Embed + tag' },
      { kind: 'tgt', label: 'Apache Doris' },
      { kind: 'lvl', label: 'Advanced' },
    ],
    footMeta: '~25 min',
    sourceSlug: 'sec_edgar_analytics',
  },
];


// Comprehensive homepage-style card illustration per example, served from the
// blobs image repo and co-located with the example's other images at
// docs-v1/img/examples/<slug>/. Generated by blobs/_diagrams-src/_cards/gen-cards.mjs.
export const exampleCardImg = (slug: string): string =>
  `https://cocoindex.io/blobs/docs-v1/img/examples/${slug}/card.svg`;

export const byCategory = (cat: Category): ExampleCard[] =>
  examples.filter((e) => e.category === cat);

export const findExample = (slug: string): ExampleCard | undefined =>
  examples.find((e) => e.slug === slug);

// Sidebar facets shown on the listing — every entry is demonstrated by a
// runnable example in EXAMPLE_CATALOG below (keep the two in sync).
/** Tag kinds exposed as filters on the listing, in display order. */
export type FacetKind = 'src' | 'tgt' | 'llm' | 'lvl';
export const FACETS: Array<{ kind: FacetKind; label: string; any: string }> = [
  { kind: 'src', label: 'Source', any: 'Any source' },
  { kind: 'tgt', label: 'Target', any: 'Any target' },
  { kind: 'llm', label: 'Model',  any: 'Any model' },
  { kind: 'lvl', label: 'Level',  any: 'Any level' },
];

const LEVEL_ORDER = ['Starter', 'Intermediate', 'Advanced'];

/** Distinct values for one facet, with how many examples carry each, derived
 *  from the tag data so a filter can never point at zero examples. Levels keep
 *  their difficulty order; everything else sorts by count, then name. */
export const facetValues = (kind: FacetKind): Array<{ label: string; count: number }> => {
  const counts = new Map<string, number>();
  for (const ex of examples) {
    for (const label of new Set(ex.tags.filter((t) => t.kind === kind).map((t) => t.label))) {
      counts.set(label, (counts.get(label) ?? 0) + 1);
    }
  }
  const rows = Array.from(counts, ([label, count]) => ({ label, count }));
  if (kind === 'lvl') return rows.sort((a, b) => LEVEL_ORDER.indexOf(a.label) - LEVEL_ORDER.indexOf(b.label));
  return rows.sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
};

/** Values of one facet on one example, for the card's data-* filter hooks. */
export const facetOf = (ex: ExampleCard, kind: FacetKind): string[] =>
  Array.from(new Set(ex.tags.filter((t) => t.kind === kind).map((t) => t.label)));

/** The example's pipeline, source → transform → model → target, drawn from its
 *  tags. Ops tags that describe a mode or a serving layer rather than a step
 *  (live mode, incremental, an API) are left out. `compact` keeps three nodes
 *  for cards: source → the transform (or the model) → target. */
export type PipelineStage = { role: 'src' | 'ops' | 'llm' | 'tgt'; caption: string; label: string };
const NOT_A_STEP = new Set(['Live mode', 'Incremental', 'Vector index', 'pgvector', 'FastAPI', 'Custom source', 'OCI Streaming (live)']);
export const pipelineStages = (ex: ExampleCard, compact = false): PipelineStage[] => {
  const first = (kind: PipelineStage['role']) =>
    ex.tags.find((t) => t.kind === kind && !(kind === 'ops' && NOT_A_STEP.has(t.label)))?.label;
  const ops = first('ops');
  const roles: Array<[PipelineStage['role'], string]> = compact
    ? [['src', 'Source'], ops ? ['ops', 'Transform'] : ['llm', 'Model'], ['tgt', 'Target']]
    : [['src', 'Source'], ['ops', 'Transform'], ['llm', 'Model'], ['tgt', 'Target']];
  const out: PipelineStage[] = [];
  for (const [role, caption] of roles) {
    const label = first(role);
    if (label && !out.some((s) => s.label === label)) out.push({ role, caption, label });
  }
  return out;
};

/** Time estimate from footMeta ("~6 min"). */
export const timeOf = (ex: ExampleCard): string => ex.footMeta.split('·')[0].trim();
export const levelOf = (ex: ExampleCard): string | null => ex.tags.find((t) => t.kind === 'lvl')?.label ?? null;

// Full catalog of runnable examples in the cocoindex monorepo, used to build the
// machine-readable index at /docs/llms.txt so agents can see the whole set in one
// fetch (the on-page listing curates only a few). `dir` is the folder under
// examples/ in the main repo; entries with `docs` also have a step-by-step
// walkthrough on the site, the rest link to source on GitHub. The groups render
// as subsections in llms.txt so agents can navigate by use case. Add an entry to
// the right group when a new example lands under examples/.
export type ExampleCatalogEntry = {
  dir: string;       // examples/<dir> in github.com/cocoindex-io/cocoindex
  title: string;
  description: string;
  docs?: string;     // docs slug when a full walkthrough exists (→ /docs/examples/<docs>)
  run?: string;      // shortest useful command for agents after install/env setup
};

export type ExampleCatalogGroup = {
  title: string;     // subsection header in llms.txt
  blurb: string;     // one-liner under the header — helps agents pick a group
  entries: ExampleCatalogEntry[];
};

const RUN_MAIN = 'cocoindex update main';
const RUN_MAIN_PY = 'cocoindex update main.py';

export const EXAMPLE_CATALOG_GROUPS: ExampleCatalogGroup[] = [
  {
    title: 'Documented walkthroughs',
    blurb: 'Step-by-step guides on the docs site — the best entry points.',
    entries: [
      { dir: 'text_embedding', docs: 'text-embedding', title: 'Semantic Search 101', description: 'Chunk local Markdown, embed each chunk, store the vectors in Postgres (pgvector), and search in plain English. The simplest end-to-end vector index.', run: RUN_MAIN },
      { dir: 'code_embedding', docs: 'index-codebase', title: 'Index Your Codebase', description: 'Walk a repo, split by syntax with Tree-sitter, embed, and query your codebase in English — a live pgvector index for AI coding agents.', run: RUN_MAIN },
      { dir: 'multi_codebase_summarization', docs: 'multi-codebase-summarization', title: 'Multi-codebase Summarization', description: 'Walk Python repos, LLM-extract typed per-file info (classes, functions, Mermaid call graphs), and update a Markdown wiki page per project as code changes.', run: RUN_MAIN_PY },
      { dir: 'pdf_to_markdown', docs: 'pdf-to-markdown', title: 'PDF → Markdown', description: 'Incrementally convert a folder of local PDFs to Markdown with docling.', run: RUN_MAIN },
      { dir: 'conversation_to_knowledge', docs: 'podcast-to-knowledge-graph', title: 'Podcasts → Knowledge Graph', description: 'Turn YouTube podcasts into a SurrealDB knowledge graph: diarized transcription, two-step LLM extraction, and embedding-based entity resolution.', run: 'cocoindex update conv_knowledge.app' },
      { dir: 'docs_to_knowledge_graph', docs: 'docs-to-knowledge-graph', title: 'Docs → Knowledge Graph', description: 'Turn a folder of Markdown docs into a Neo4j concept graph: LLM-extracted (subject, predicate, object) triples, declared as nodes and edges that stay in sync.', run: RUN_MAIN },
      { dir: 'meeting_notes_graph_neo4j', docs: 'meeting-notes-to-knowledge-graph', title: 'Meeting Notes → Knowledge Graph', description: 'Turn Google Drive meeting notes into a Neo4j knowledge graph: LLM extraction of organizers, attendees, and tasks, plus embedding-based person entity resolution.', run: RUN_MAIN },
      { dir: 'csv_to_kafka', docs: 'csv-to-kafka', title: 'CSV → Kafka', description: 'Watch a folder of CSV files and publish each row as a JSON message to a Kafka topic: declarative target states, only-changed-rows produces, and live mode.', run: 'cocoindex update -L main.py' },
      { dir: 'pdf_embedding', docs: 'pdf-embedding', title: 'Semantic Search over PDFs', description: 'Convert local PDFs to Markdown with docling (on a GPU runner), chunk, embed, and store the vectors in Postgres (pgvector); natural-language query demo.', run: RUN_MAIN },
      { dir: 'image_search', docs: 'image-search', title: 'Search Images by Text', description: 'Embed images with CLIP, store the vectors in Qdrant, and search your photos in natural language through a FastAPI + React app; live updates.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
    ],
  },
  {
    title: 'Vector search',
    blurb: 'Embed and semantically search text from more sources, into more vector stores.',
    entries: [
      { dir: 'text_embedding_qdrant', docs: 'text-embedding-qdrant', title: 'Text Embedding · Qdrant', description: 'Embed local Markdown files and store the chunks + vectors in Qdrant; semantic-search demo.' },
      { dir: 'text_embedding_lancedb', docs: 'text-embedding-lancedb', title: 'Text Embedding · LanceDB', description: 'Embed local Markdown files and store the chunks + vectors in LanceDB; semantic-search demo.' },
      { dir: 'text_embedding_turbopuffer', docs: 'text-embedding-turbopuffer', title: 'Text Embedding · Turbopuffer', description: 'Embed local Markdown files into a Turbopuffer namespace; semantic-search demo.' },
      { dir: 'code_embedding_lancedb', title: 'Code Embedding · LanceDB', description: 'Extract code chunks from Python/Rust/TOML/Markdown and store code + vectors in LanceDB; semantic code search.' },
      { dir: 'entire_session_search', docs: 'entire-session-search', title: 'Entire Session Search', description: 'Semantic search over AI coding sessions captured by Entire — transcripts, prompts, and context summaries into Postgres (pgvector).' },
      { dir: 'amazon_s3_embedding', docs: 'amazon-s3-embedding', title: 'Amazon S3 Embedding', description: 'Embed Markdown files from an S3 bucket into Postgres (pgvector); semantic-search demo.' },
      { dir: 'azure_blob_embedding', title: 'Azure Blob Storage Embedding', description: 'Embed Markdown files from Azure Blob Storage into Postgres (pgvector); query demo.' },
      { dir: 'gdrive_text_embedding', docs: 'google-drive-embedding', title: 'Google Drive Text Embedding', description: 'Embed text files from Google Drive into Postgres (pgvector); query demo.' },
      { dir: 'oci_object_storage_embedding', docs: 'oci-object-storage-embedding', title: 'OCI Object Storage Embedding', description: 'Embed Markdown files from Oracle Cloud (OCI) Object Storage into Postgres (pgvector); query demo.' },
      { dir: 'postgres_source', docs: 'postgres-source', title: 'Postgres as a Source', description: 'Use an existing PostgreSQL table as a CocoIndex source: derive fields, embed, and store the results back.' },
      { dir: 'sec_edgar_analytics', docs: 'sec-edgar-analytics', title: 'SEC Filing Hybrid Search · Apache Doris', description: 'Scrub, chunk, embed, and tag multi-format SEC filings into Apache Doris with both a vector and a full-text index; hybrid (semantic + keyword) RRF search.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Multimodal',
    blurb: 'Images and audio — same declarative flow, different encoder.',
    entries: [
      { dir: 'image_search_colpali', docs: 'image-search-colpali', title: 'Image Search (ColPali)', description: 'Image search using the ColPali multi-vector model with Qdrant MaxSim; natural-language queries via FastAPI.', run: 'python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000' },
      { dir: 'face_recognition', docs: 'face-recognition', title: 'Build Your Own Face Search', description: 'Detect faces in a folder of photos, embed each with face_recognition (dlib), and index them in Qdrant for face search; query demo.', run: RUN_MAIN },
      { dir: 'audio_to_text', docs: 'audio-to-text', title: 'Audio → Text', description: 'Transcribe local audio files with LiteLLM and store one row per file in Postgres, keyed by filename.', run: RUN_MAIN_PY },
      { dir: 'multi_format_indexing', docs: 'multi-format-indexing', title: 'Multi-format Visual Search (ColPali)', description: 'Index PDFs and images as page screenshots with the ColPali multi-vector model — no OCR, no chunking — into Qdrant with MaxSim; text query demo.', run: RUN_MAIN },
      { dir: 'slides_to_speech', docs: 'slides-to-speech', title: 'Slides to Narrated Search', description: 'Render each slide, write speaker notes with a vision LLM, narrate them with Pocket TTS locally on the CPU, embed the notes into LanceDB; semantic search with playable audio.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Structured extraction',
    blurb: 'LLM-extract typed, structured data — with instructor, BAML, or DSPy.',
    entries: [
      { dir: 'hn_trending_topics', docs: 'hackernews-trending-topics', title: 'HackerNews Trending Topics', description: 'Scrape recent HackerNews threads and comments via the Algolia HN API, LLM-extract topics, and store in Postgres.' },
      { dir: 'paper_metadata', docs: 'paper-metadata', title: 'Paper Metadata', description: 'LLM-extract title, authors, and abstract from PDF papers into Postgres, with embeddings for semantic search.' },
      { dir: 'patient_intake_extraction_baml', docs: 'patient-intake-baml', title: 'Patient Intake Extraction · BAML', description: 'Extract structured data from patient intake forms with BAML.', run: RUN_MAIN_PY },
      { dir: 'patient_intake_extraction_dspy', docs: 'patient-intake-dspy', title: 'Patient Intake Extraction · DSPy', description: 'Extract structured data from patient intake forms with DSPy.', run: RUN_MAIN_PY },
      { dir: 'manuals_llm_extraction', docs: 'manuals-llm-extraction', title: 'Manuals to Structured Data', description: 'Convert PDF manuals to Markdown with docling, LLM-extract a typed module summary (classes, methods, arguments), and store one structured record per manual in Postgres.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Knowledge graphs',
    blurb: 'Extract entities and relationships into graph databases that stay in sync.',
    entries: [
      { dir: 'meeting_notes_graph_falkordb', title: 'Meeting Notes → Knowledge Graph · FalkorDB', description: 'Extract structured info from Google Drive meeting notes into a FalkorDB knowledge graph.' },
      { dir: 'product_recommendation', docs: 'product-recommendation', title: 'Product Recommendation Graph', description: 'LLM-extract what each product is and what pairs with it from product docs, into a Neo4j graph of products and taxonomies that powers recommendations.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Custom building blocks & streaming',
    blurb: 'Bring your own transform or wire pipelines to streaming systems like Kafka.',
    entries: [
      { dir: 'files_transform', docs: 'files-transform', title: 'Files Transform', description: 'Watch a directory of Markdown files and convert each to HTML with markdown-it-py, writing .html outputs incrementally.' },
      { dir: 'bigquery_target', title: 'BigQuery Target', description: 'Write order records to BigQuery with managed table creation, MERGE upserts, and cleanup.', run: RUN_MAIN },
      { dir: 'kafka_to_lancedb', docs: 'kafka-to-lancedb', title: 'Kafka → LanceDB', description: 'Consume JSON messages from a Kafka topic and dispatch them to two LanceDB tables by message shape.', run: 'cocoindex update -L main.py' },
      { dir: 'snowflake_target', title: 'Snowflake Target', description: 'Write order records to Snowflake with managed table creation, MERGE upserts, and cleanup.', run: RUN_MAIN },
    ],
  },
  {
    title: 'Rust',
    blurb: 'The same declarative flows using the CocoIndex Rust API.',
    entries: [
      { dir: 'rust', title: 'Rust Examples', description: 'Rust ports of many of the examples above — the same declarative flows using the CocoIndex Rust API.', run: 'cd rust/<example> && follow its README; common index command: cargo run -- index' },
    ],
  },
];

export const EXAMPLE_CATALOG: ExampleCatalogEntry[] = EXAMPLE_CATALOG_GROUPS.flatMap((g) => g.entries);
